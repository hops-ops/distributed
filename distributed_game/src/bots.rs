//! Load-test bots (`bots` feature).
//!
//! Each bot is a real WebSocket client speaking the binary protocol through
//! the reference [`ClientMirror`]: it mints a signed ticket with the
//! server's [`TicketKey`] (dev/test only — production tickets come from the
//! auth service), joins a zone, sends inputs at a fixed rate from a pluggable
//! [`Behaviour`], pings for RTT and counts Deltas/bytes. [`run_bots`] runs N
//! of them concurrently and returns a [`Summary`].
//!
//! Inputs are game-defined bytes: a [`Behaviour`] yields a 2D direction and
//! an [`Encoder`] turns it into the zone's input payload
//! ([`encode_i8_pair`] matches a `(dx, dy): (i8, i8)` input).
//!
//! The `game-bots` binary wraps this for demos:
//! `cargo run -p distributed_game --features bots --bin game-bots -- --help`.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::net::TicketKey;
use crate::protocol::{ClientMessage, ClientMirror, Hello, Role, ServerMessage, PROTOCOL_VERSION};
use crate::rng::ZoneRng;
use crate::zone::TicketClaims;

/// Produces a movement direction each input tick.
pub trait Behaviour: Send {
    /// Direction for the input sent at `elapsed` since the bot joined.
    fn direction(&mut self, elapsed: Duration, rng: &mut ZoneRng) -> [f32; 2];
}

/// Keeps a heading and occasionally picks a new random one.
#[derive(Debug, Clone)]
pub struct RandomWalk {
    /// Probability per input of turning.
    pub turn_chance: f32,
    heading: [f32; 2],
}

impl RandomWalk {
    /// Walker turning with probability `turn_chance` per input.
    pub fn new(turn_chance: f32) -> Self {
        Self {
            turn_chance,
            heading: [0.0, 0.0],
        }
    }
}

impl Behaviour for RandomWalk {
    fn direction(&mut self, _elapsed: Duration, rng: &mut ZoneRng) -> [f32; 2] {
        if self.heading == [0.0, 0.0] || rng.chance(self.turn_chance) {
            let a = rng.next_f32() * std::f32::consts::TAU;
            self.heading = [a.cos(), a.sin()];
        }
        self.heading
    }
}

/// Walks in a circle with a fixed period.
#[derive(Debug, Clone, Copy)]
pub struct Circle {
    /// Time for one full turn.
    pub period: Duration,
    /// Starting angle (radians).
    pub phase: f32,
}

impl Behaviour for Circle {
    fn direction(&mut self, elapsed: Duration, _rng: &mut ZoneRng) -> [f32; 2] {
        let turns = elapsed.as_secs_f32() / self.period.as_secs_f32().max(1e-3);
        let a = self.phase + turns * std::f32::consts::TAU;
        [-a.sin(), a.cos()]
    }
}

/// Built-in behaviour selector (binary, quick tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BehaviourKind {
    /// [`RandomWalk`] turning 10% of inputs.
    RandomWalk,
    /// [`Circle`] with a 4 s period, phase spread by bot index.
    Circle,
}

impl BehaviourKind {
    /// Behaviour for bot `index`.
    pub fn build(self, index: usize) -> Box<dyn Behaviour> {
        match self {
            Self::RandomWalk => Box::new(RandomWalk::new(0.1)),
            Self::Circle => Box::new(Circle {
                period: Duration::from_secs(4),
                phase: index as f32 * 0.7,
            }),
        }
    }
}

/// Direction → input payload.
pub type Encoder = Arc<dyn Fn([f32; 2]) -> Vec<u8> + Send + Sync>;

/// Encode as two `i8` in `[-1, 1]` (rounded).
pub fn encode_i8_pair(dir: [f32; 2]) -> Vec<u8> {
    dir.iter()
        .map(|v| v.round().clamp(-1.0, 1.0) as i8 as u8)
        .collect()
}

/// Bot run settings.
#[derive(Clone)]
pub struct BotConfig {
    /// Server base URL, e.g. `ws://127.0.0.1:2567` (`{path}/{zone}` is
    /// appended).
    pub url: String,
    /// Path the zone id is appended to: `/zones` on the `net` server,
    /// whatever the worker routes to its zone objects on celld (e.g.
    /// `/rooms`).
    pub path: String,
    /// Zone to join.
    pub zone: String,
    /// Number of bots.
    pub bots: usize,
    /// Inputs per second per bot.
    pub input_rate: f64,
    /// Ping cadence (RTT samples).
    pub ping_interval: Duration,
    /// How long each bot stays after joining.
    pub duration: Duration,
    /// Spread connects uniformly over this window.
    pub ramp_up: Duration,
    /// Signs tickets (must match the server's key).
    pub ticket_key: TicketKey,
    /// Principal prefix (`{prefix}-{index}`).
    pub principal_prefix: String,
    /// Hello content version.
    pub content_version: String,
    /// Seed for behaviours.
    pub seed: u64,
    /// Payload encoder.
    pub encoder: Encoder,
}

impl BotConfig {
    /// Defaults: 20 inputs/s, ping every 500 ms, 10 s, random-walk friendly
    /// `i8` pair encoding.
    pub fn new(url: impl Into<String>, zone: impl Into<String>, ticket_key: TicketKey) -> Self {
        Self {
            url: url.into(),
            path: "/zones".into(),
            zone: zone.into(),
            bots: 10,
            input_rate: 20.0,
            ping_interval: Duration::from_millis(500),
            duration: Duration::from_secs(10),
            ramp_up: Duration::ZERO,
            ticket_key,
            principal_prefix: "bot".into(),
            content_version: String::new(),
            seed: 1,
            encoder: Arc::new(encode_i8_pair),
        }
    }
}

/// One bot's results.
#[derive(Debug, Clone, Default)]
pub struct BotStats {
    /// Bot index.
    pub index: usize,
    /// Welcome received.
    pub joined: bool,
    /// Why it stopped early.
    pub error: Option<String>,
    /// Deltas applied.
    pub deltas: u64,
    /// Delta bytes received.
    pub delta_bytes: u64,
    /// Inputs sent.
    pub inputs_sent: u64,
    /// Highest `ack_seq` seen.
    pub last_ack: u32,
    /// RTT samples.
    pub rtts: Vec<Duration>,
    /// Entities in the mirror at the end.
    pub entities: usize,
    /// Time from Welcome to the end.
    pub session: Duration,
}

/// Aggregate of a run.
#[derive(Debug, Clone, Default)]
pub struct Summary {
    /// Per-bot stats.
    pub bots: Vec<BotStats>,
    /// Wall time of the run.
    pub elapsed: Duration,
}

impl Summary {
    /// Bots that received Welcome.
    pub fn joined(&self) -> usize {
        self.bots.iter().filter(|b| b.joined).count()
    }

    /// Bots that stopped with an error.
    pub fn failed(&self) -> usize {
        self.bots.iter().filter(|b| b.error.is_some()).count()
    }

    /// Fewest Deltas any bot received.
    pub fn min_deltas(&self) -> u64 {
        self.bots.iter().map(|b| b.deltas).min().unwrap_or(0)
    }

    /// Total Deltas.
    pub fn deltas(&self) -> u64 {
        self.bots.iter().map(|b| b.deltas).sum()
    }

    /// Total Delta bytes.
    pub fn delta_bytes(&self) -> u64 {
        self.bots.iter().map(|b| b.delta_bytes).sum()
    }

    /// Delta bytes per second per bot (mean over bot sessions).
    pub fn delta_bytes_per_sec_per_bot(&self) -> f64 {
        let rates: Vec<f64> = self
            .bots
            .iter()
            .filter(|b| !b.session.is_zero())
            .map(|b| b.delta_bytes as f64 / b.session.as_secs_f64())
            .collect();
        if rates.is_empty() {
            0.0
        } else {
            rates.iter().sum::<f64>() / rates.len() as f64
        }
    }

    /// RTT quantile `q` in `[0, 1]` over all samples.
    pub fn rtt_quantile(&self, q: f64) -> Option<Duration> {
        let mut all: Vec<Duration> = self.bots.iter().flat_map(|b| b.rtts.clone()).collect();
        if all.is_empty() {
            return None;
        }
        all.sort();
        let i = ((all.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
        Some(all[i])
    }

    /// Inputs sent.
    pub fn inputs_sent(&self) -> u64 {
        self.bots.iter().map(|b| b.inputs_sent).sum()
    }
}

impl fmt::Display for Summary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = |d: Option<Duration>| {
            d.map_or("-".into(), |d| format!("{:.1}ms", d.as_secs_f64() * 1e3))
        };
        writeln!(
            f,
            "bots: {} joined, {} failed, {} total in {:.1}s",
            self.joined(),
            self.failed(),
            self.bots.len(),
            self.elapsed.as_secs_f64()
        )?;
        writeln!(
            f,
            "deltas: {} total, min/bot {}, {:.0} B/s/bot, {} bytes",
            self.deltas(),
            self.min_deltas(),
            self.delta_bytes_per_sec_per_bot(),
            self.delta_bytes()
        )?;
        writeln!(f, "inputs sent: {}", self.inputs_sent())?;
        write!(
            f,
            "rtt: p50 {} p95 {} max {}",
            ms(self.rtt_quantile(0.5)),
            ms(self.rtt_quantile(0.95)),
            ms(self.rtt_quantile(1.0))
        )?;
        for b in self.bots.iter().filter(|b| b.error.is_some()).take(5) {
            write!(
                f,
                "\n  bot {}: {}",
                b.index,
                b.error.as_deref().unwrap_or("")
            )?;
        }
        Ok(())
    }
}

/// Run `config.bots` bots concurrently; `behaviour(index)` builds each one.
pub async fn run_bots(
    config: BotConfig,
    behaviour: impl Fn(usize) -> Box<dyn Behaviour>,
) -> Summary {
    let started = Instant::now();
    let n = config.bots;
    let config = Arc::new(config);
    let mut tasks = Vec::with_capacity(n);
    for index in 0..n {
        let delay = if n > 1 {
            config.ramp_up.mul_f64(index as f64 / (n - 1) as f64)
        } else {
            Duration::ZERO
        };
        let behaviour = behaviour(index);
        let config = config.clone();
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            run_bot(&config, index, behaviour).await
        }));
    }
    let mut bots = Vec::with_capacity(n);
    for (index, task) in tasks.into_iter().enumerate() {
        bots.push(task.await.unwrap_or_else(|e| BotStats {
            index,
            error: Some(format!("task: {e}")),
            ..BotStats::default()
        }));
    }
    Summary {
        bots,
        elapsed: started.elapsed(),
    }
}

/// Run one bot to completion.
pub async fn run_bot(
    config: &BotConfig,
    index: usize,
    mut behaviour: Box<dyn Behaviour>,
) -> BotStats {
    let mut stats = BotStats {
        index,
        ..BotStats::default()
    };
    if let Err(e) = drive(config, index, behaviour.as_mut(), &mut stats).await {
        stats.error = Some(e);
    }
    stats
}

async fn drive(
    config: &BotConfig,
    index: usize,
    behaviour: &mut dyn Behaviour,
    stats: &mut BotStats,
) -> Result<(), String> {
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let principal = format!("{}-{index}", config.principal_prefix);
    let ticket = config.ticket_key.sign(&TicketClaims {
        principal: principal.clone(),
        zone: config.zone.clone(),
        expires_at: unix + 3600,
        nonce: format!("{principal}-{}-{}", config.seed, unix),
        role: Role::Player,
        name: None,
        extensions: Default::default(),
    });
    let url = format!(
        "{}/{}/{}",
        config.url.trim_end_matches('/'),
        config.path.trim_matches('/'),
        config.zone
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let hello = ClientMessage::Hello(Hello {
        protocol_version: PROTOCOL_VERSION,
        content_version: config.content_version.clone(),
        capabilities: 0,
        ticket,
        reconnect_token: String::new(),
    });
    ws.send(Message::Binary(hello.encode().into()))
        .await
        .map_err(|e| format!("hello: {e}"))?;

    let mut mirror = ClientMirror::new();
    let mut rng = ZoneRng::for_zone(config.seed ^ index as u64, &config.zone);
    let origin = Instant::now();
    let mut joined_at: Option<Instant> = None;
    let mut seq = 0u32;
    let input_every = Duration::from_secs_f64(1.0 / config.input_rate.max(0.01));
    let mut next_input = Instant::now();
    let mut next_ping = Instant::now();
    let end_of = |joined: Instant| joined + config.duration;
    let connect_deadline = origin + Duration::from_secs(10);

    loop {
        let now = Instant::now();
        let deadline = joined_at.map_or(connect_deadline, end_of);
        if now >= deadline {
            if joined_at.is_none() {
                return Err("no Welcome".into());
            }
            break;
        }
        let wake = if joined_at.is_some() {
            next_input.min(next_ping).min(deadline)
        } else {
            deadline
        };
        tokio::select! {
            msg = ws.next() => {
                let bytes = match msg {
                    Some(Ok(Message::Binary(b))) => b,
                    Some(Ok(Message::Close(_))) | None => return Err("closed by server".into()),
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => return Err(format!("recv: {e}")),
                };
                match mirror.apply(&bytes).map_err(|e| format!("decode: {e}"))? {
                    ServerMessage::Welcome(_) => {
                        stats.joined = true;
                        let now = Instant::now();
                        joined_at = Some(now);
                        next_input = now;
                        next_ping = now;
                    }
                    ServerMessage::Delta(d) => {
                        stats.deltas += 1;
                        stats.delta_bytes += bytes.len() as u64;
                        stats.last_ack = stats.last_ack.max(d.ack_seq);
                    }
                    ServerMessage::Pong { client_time, .. } => {
                        let now_ms = origin.elapsed().as_secs_f64() * 1e3;
                        stats.rtts.push(Duration::from_secs_f64(((now_ms - client_time) / 1e3).max(0.0)));
                    }
                    ServerMessage::Kick { code, message } => return Err(format!("kicked {code}: {message}")),
                    ServerMessage::Error { code, message } if !stats.joined => {
                        return Err(format!("refused {code}: {message}"));
                    }
                    _ => {}
                }
            }
            _ = tokio::time::sleep_until(wake) => {
                let Some(joined) = joined_at else { continue };
                let now = Instant::now();
                if now >= next_input {
                    seq += 1;
                    let dir = behaviour.direction(now - joined, &mut rng);
                    let input = ClientMessage::Input {
                        seq,
                        client_tick: mirror.server_tick,
                        payload: (config.encoder)(dir),
                    };
                    ws.send(Message::Binary(input.encode().into())).await.map_err(|e| format!("send: {e}"))?;
                    stats.inputs_sent += 1;
                    next_input += input_every;
                    if next_input < now {
                        next_input = now + input_every;
                    }
                }
                if now >= next_ping {
                    let ping = ClientMessage::Ping { client_time: origin.elapsed().as_secs_f64() * 1e3 };
                    ws.send(Message::Binary(ping.encode().into())).await.map_err(|e| format!("ping: {e}"))?;
                    next_ping = now + config.ping_interval;
                }
            }
        }
    }
    stats.session = joined_at.map_or(Duration::ZERO, |j| j.elapsed());
    stats.entities = mirror.entities.len();
    let _ = ws
        .send(Message::Binary(ClientMessage::Leave.encode().into()))
        .await;
    let _ = ws.close(None).await;
    Ok(())
}
