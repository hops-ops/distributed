//! Crude load test: how many moving players fit in a Mob Pit room, and how
//! many busy rooms fit on one celld.
//!
//! Each step puts `ZONES x BOTS` Rust bots (`distributed_game::bots`, real
//! WebSocket clients signing their own tickets) into `ZONES` rooms (a grid
//! from `pit.0.0`), `BOTS` per room; a bare `BOTS` is one room. Every bot
//! moves (circles near its spawn by default) and holds attack, at the tick
//! rate. For each step it reads every room's `/zones/{room}/info` once the
//! bots are in and again at the end of the hold, and prints one row:
//!
//! - dropped ticks over the hold (%) and late p99 (ms) of the worst room,
//!   and the players the rooms count;
//! - joined / failed bots, RTT p50/p99 (after each bot's first 5 s) and
//!   Delta bytes/s per bot, from the bots;
//! - locally, celld's CPU (cores busy over the hold) and RSS; against a
//!   fleet, optionally the output of `--stats-cmd`.
//!
//! A row is marked `DEGRADED` past 1 % dropped ticks, late p99 > 50 ms, RTT
//! p99 > 150 ms or a bot that did not join.
//!
//! Local (starts its own `celld dev` with the cells worker; build it first
//! with `./run.sh build`):
//!
//! ```sh
//! cargo run --release --example mobpit-load -- --steps 8,16,32,64,96,128
//! cargo run --release --example mobpit-load -- --steps 16x1,64x1,64x8
//! ```
//!
//! Against a running fleet (celld's public listener, the worker's ticket
//! secret, and `ZONE_MAX_PLAYERS` at least the largest step):
//!
//! ```sh
//! cargo run --release --example mobpit-load -- --url ws://celld:8080 \
//!     --secret "$GAME_TICKET_SECRET" --steps 16,32,64
//! ```

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use distributed_game::authoring::GameInput;
use distributed_game::bots::{run_bots, Behaviour, BotConfig, Circle, RandomWalk, Summary};
use distributed_game::net::TicketKey;
use game_demos::devcelld::DevCelld;
use game_demos::mobpit::encode_bot_input;
use game_sim::mobpit::PitInput;
use serde_json::{json, Value};

#[derive(Parser, Debug)]
#[command(about = "Ramp moving bots into one Mob Pit room until it degrades")]
struct Args {
    /// Steps, comma-separated: `BOTS` (one room) or `ZONESxBOTS`.
    #[arg(long, default_value = "8,16,32,48,64,96,128", value_delimiter = ',', value_parser = parse_step)]
    steps: Vec<(usize, usize)>,
    /// celld public listener (ws://…); default: start a local `celld dev`.
    #[arg(long)]
    url: Option<String>,
    /// Room info base (http://…); default: `--url` with http.
    #[arg(long)]
    info_url: Option<String>,
    /// Ticket secret of the worker (with `--url`).
    #[arg(long, env = "GAME_TICKET_SECRET")]
    secret: Option<String>,
    /// Seconds every bot stays once all have joined (the measured window).
    #[arg(long, default_value_t = 30)]
    hold_secs: u64,
    /// Seconds over which a step's bots join.
    #[arg(long, default_value_t = 5)]
    ramp_secs: u64,
    /// Seconds between steps (the room settles).
    #[arg(long, default_value_t = 5)]
    rest_secs: u64,
    /// Movement: `mix` (half random-walk, half circle), `walk` or `circle`.
    /// Walkers can leave through a doorway (a handoff closes them here).
    #[arg(long, default_value = "circle")]
    moves: String,
    /// Move without holding attack (no combat, so far fewer room writes).
    #[arg(long)]
    no_attack: bool,
    /// Inputs per second per bot (the room's tick rate).
    #[arg(long, default_value_t = 30.0)]
    rate: f64,
    /// Shell command run near the end of each hold; its output is reported
    /// (e.g. celld CPU / memory).
    #[arg(long)]
    stats_cmd: Option<String>,
    /// Extra worker var for the local celld, `KEY=VALUE` (repeatable), e.g.
    /// `ZONE_DEV_SETTINGS=0`.
    #[arg(long)]
    var: Vec<String>,
    /// Local celld project dir to keep (state and `celld-*.log`); default a
    /// temporary one, deleted at exit.
    #[arg(long)]
    celld_dir: Option<std::path::PathBuf>,
    /// Write every step as JSON lines here.
    #[arg(long)]
    json: Option<std::path::PathBuf>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), String> {
    let args = Args::parse();
    let largest = args.steps.iter().map(|s| s.1).max().unwrap_or(32);
    // Local: our own fleet, the room cap raised to the largest step.
    let cap = largest.to_string();
    let local = match &args.url {
        Some(_) => None,
        None => {
            let mut vars = vec![("ZONE_MAX_PLAYERS", cap.as_str())];
            vars.extend(args.var.iter().filter_map(|v| v.split_once('=')));
            Some(match &args.celld_dir {
                Some(dir) => DevCelld::in_project_with(dir, None, &vars)?,
                None => DevCelld::start_with(None, &vars)?,
            })
        }
    };
    let (url, secret) = match (&local, &args.url) {
        (Some(celld), _) => (celld.ws_url(), celld.ticket_secret().to_owned()),
        (None, Some(url)) => (
            url.clone(),
            args.secret
                .clone()
                .ok_or("--secret (or GAME_TICKET_SECRET) is required with --url")?,
        ),
        (None, None) => unreachable!(),
    };
    let info_base = args.info_url.clone().unwrap_or_else(|| {
        url.replacen("ws://", "http://", 1)
            .replacen("wss://", "https://", 1)
    });
    let celld_pid = local.as_ref().and_then(DevCelld::pid);
    let mut json_out = match &args.json {
        Some(path) => {
            Some(std::fs::File::create(path).map_err(|e| format!("{}: {e}", path.display()))?)
        }
        None => None,
    };

    println!(
        "{url} · hold {} s · {} inputs/s per bot · worst room's ticks",
        args.hold_secs, args.rate
    );
    println!(
        "{:>9} {:>6} {:>6} {:>6} {:>8} {:>9} {:>8} {:>8} {:>9} {:>6} {:>7}  stats",
        "rooms x n",
        "bots",
        "joined",
        "room",
        "dropped",
        "late p99",
        "rtt p50",
        "rtt p99",
        "B/s/bot",
        "cores",
        "rss MB"
    );
    for (step, &(zones, n)) in args.steps.iter().enumerate() {
        if step > 0 {
            tokio::time::sleep(Duration::from_secs(args.rest_secs)).await;
        }
        let rooms = grid(zones);
        let mut fleets = Vec::with_capacity(zones);
        for (z, room) in rooms.iter().enumerate() {
            let mut config =
                BotConfig::new(url.clone(), room.clone(), TicketKey::new(secret.as_bytes()));
            config.path = "/zones".into();
            config.bots = n;
            config.input_rate = args.rate;
            config.ramp_up = Duration::from_secs(args.ramp_secs);
            // The last bot joins at the end of the ramp; all hold together.
            config.duration = Duration::from_secs(args.hold_secs + args.ramp_secs);
            config.principal_prefix = format!("load{step}z{z}");
            config.seed = (step * 10_000 + z) as u64 + 1;
            config.encoder = if args.no_attack {
                Arc::new(encode_bot_move)
            } else {
                Arc::new(encode_bot_input)
            };
            let moves = args.moves.clone();
            fleets.push(tokio::spawn(run_bots(config, move |index| {
                behaviour(&moves, index)
            })));
        }
        let info_urls: Vec<String> = rooms
            .iter()
            .map(|room| format!("{info_base}/zones/{room}/info"))
            .collect();

        // Window: from shortly after the last join to shortly before the
        // first leave.
        tokio::time::sleep(Duration::from_secs(args.ramp_secs + 2)).await;
        let start = get_all(&info_urls).await;
        let window_start = unix_secs();
        let cpu_start = (celld_pid.and_then(tree_usage), std::time::Instant::now());
        tokio::time::sleep(Duration::from_secs(args.hold_secs.saturating_sub(5))).await;
        let stats = match &args.stats_cmd {
            Some(cmd) => run_stats(cmd).await,
            None => String::new(),
        };
        let end = get_all(&info_urls).await;
        let window = (window_start, unix_secs());
        let usage = match (cpu_start, celld_pid.and_then(tree_usage)) {
            ((Some(before), at), Some(after)) => {
                Some(((after.0 - before.0) / at.elapsed().as_secs_f64(), after.1))
            }
            _ => None,
        };
        let mut summary = Summary {
            bots: Vec::new(),
            elapsed: Duration::ZERO,
        };
        // A bot stuck connecting would hold the step forever.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        for fleet in fleets {
            let Ok(part) = tokio::time::timeout_at(deadline, fleet).await else {
                eprintln!("bots still running 30 s after the hold; step cut short");
                continue;
            };
            let part = part.map_err(|e| format!("bots: {e}"))?;
            summary.bots.extend(part.bots);
            summary.elapsed = summary.elapsed.max(part.elapsed);
        }

        let row = Row::new((zones, n), &start, &end, &summary, usage, stats, window);
        println!("{}", row.line());
        if let Some(out) = json_out.as_mut() {
            use std::io::Write;
            writeln!(out, "{}", row.json()).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Random walkers cross the room; circlers stay near where they spawn.
fn behaviour(moves: &str, index: usize) -> Box<dyn Behaviour> {
    let walk = match moves {
        "walk" => true,
        "circle" => false,
        _ => index.is_multiple_of(2),
    };
    if walk {
        Box::new(RandomWalk::new(0.05))
    } else {
        Box::new(Circle {
            period: Duration::from_millis(3000 + 250 * (index % 8) as u64),
            phase: index as f32 * 0.7,
        })
    }
}

fn unix_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// `ZONESxBOTS` or `BOTS` (one room).
fn parse_step(step: &str) -> Result<(usize, usize), String> {
    let number = |n: &str| {
        n.trim()
            .parse::<usize>()
            .map_err(|e| format!("{step}: {e}"))
    };
    match step.split_once('x') {
        Some((zones, bots)) => Ok((number(zones)?, number(bots)?)),
        None => Ok((1, number(step)?)),
    }
}

/// `zones` rooms in a square grid from `pit.0.0` (row by row).
fn grid(zones: usize) -> Vec<String> {
    let side = (1..).find(|s| s * s >= zones).unwrap_or(1);
    (0..zones)
        .map(|i| format!("pit.{}.{}", i % side, i / side))
        .collect()
}

async fn get_all(urls: &[String]) -> Vec<Value> {
    let mut out = Vec::with_capacity(urls.len());
    for url in urls {
        out.push(get(url).await);
    }
    out
}

/// RTT quantile without each bot's first 5 s (pings every 500 ms): the
/// room may still be waking or admitting the rest of the ramp.
fn steady_rtt(summary: &Summary, q: f64) -> Option<Duration> {
    const SKIP: usize = 10;
    let mut all: Vec<Duration> = summary
        .bots
        .iter()
        .flat_map(|b| b.rtts.iter().skip(SKIP).copied())
        .collect();
    all.sort();
    let i = ((all.len().checked_sub(1)?) as f64 * q).round() as usize;
    all.get(i).copied()
}

/// Bot payload: the direction as a move, no buttons.
fn encode_bot_move(dir: [f32; 2]) -> Vec<u8> {
    let axis = |v: f32| (v.clamp(-1.0, 1.0) * 127.0).round() as i8;
    PitInput {
        mx: axis(dir[0]),
        my: axis(dir[1]),
        ..PitInput::default()
    }
    .to_payload()
}

async fn get(url: &str) -> Value {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    let client = CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("http client")
    });
    match client.get(url).send().await {
        Ok(response) => response.json().await.unwrap_or(Value::Null),
        Err(_) => Value::Null,
    }
}

/// CPU seconds used so far and resident MB of `root` and its descendants.
fn tree_usage(root: u32) -> Option<(f64, f64)> {
    let out = std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,ppid=,time=,rss="])
        .output()
        .ok()?;
    let rows: Vec<(u32, u32, f64, f64)> = String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let mut f = line.split_whitespace();
            let pid = f.next()?.parse().ok()?;
            let ppid = f.next()?.parse().ok()?;
            let time = cpu_seconds(f.next()?)?;
            let rss_kb: f64 = f.next()?.parse().ok()?;
            Some((pid, ppid, time, rss_kb))
        })
        .collect();
    let mut tree = vec![root];
    let mut i = 0;
    while i < tree.len() {
        let parent = tree[i];
        tree.extend(rows.iter().filter(|r| r.1 == parent).map(|r| r.0));
        i += 1;
    }
    let mine = rows.iter().filter(|r| tree.contains(&r.0));
    let (cpu, rss) = mine.fold((0.0, 0.0), |(c, m), r| (c + r.2, m + r.3));
    (cpu > 0.0).then_some((cpu, rss / 1024.0))
}

/// `ps` cumulative time: `[[dd-]hh:]mm:ss.ss`.
fn cpu_seconds(time: &str) -> Option<f64> {
    let (days, rest) = match time.split_once('-') {
        Some((d, rest)) => (d.parse::<f64>().ok()?, rest),
        None => (0.0, time),
    };
    let seconds = rest.split(':').try_fold(0.0, |acc, part| {
        part.parse::<f64>().ok().map(|v| acc * 60.0 + v)
    })?;
    Some(days * 86_400.0 + seconds)
}

async fn run_stats(cmd: &str) -> String {
    match tokio::process::Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .output()
        .await
    {
        Ok(out) => String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" "),
        Err(e) => format!("stats: {e}"),
    }
}

struct Row {
    zones: usize,
    per_zone: usize,
    bots: usize,
    joined: usize,
    failed: usize,
    first_error: Option<String>,
    players: u64,
    dropped_pct: Option<f64>,
    late_p99: f64,
    rtt_p50: Option<f64>,
    rtt_p99: Option<f64>,
    bytes_per_bot: f64,
    /// celld cores busy over the hold and resident MB (local runs).
    usage: Option<(f64, f64)>,
    stats: String,
    /// Measured window, Unix seconds (to line up external samples).
    window: (f64, f64),
}

impl Row {
    fn new(
        (zones, per_zone): (usize, usize),
        start: &[Value],
        end: &[Value],
        summary: &Summary,
        usage: Option<(f64, f64)>,
        stats: String,
        window: (f64, f64),
    ) -> Self {
        let num = |v: &Value, key: &str| v[key].as_f64().unwrap_or(0.0);
        // Per room; counters restart with the activation, so only compare
        // within one. A room without both readings has no number (worst).
        let dropped = start.iter().zip(end).map(|(start, end)| {
            let same = start["activation"] == end["activation"] && !end["activation"].is_null();
            let iterations = num(end, "iterations") - num(start, "iterations");
            (same && iterations > 0.0).then(|| {
                (num(end, "droppedTicks") - num(start, "droppedTicks")) / iterations * 100.0
            })
        });
        let dropped_pct = dropped
            .collect::<Option<Vec<f64>>>()
            .map(|all| all.into_iter().fold(0.0, f64::max));
        let ms = |d: Option<Duration>| d.map(|d| d.as_secs_f64() * 1000.0);
        Self {
            zones,
            per_zone,
            bots: zones * per_zone,
            joined: summary.joined(),
            failed: summary.failed(),
            first_error: summary.bots.iter().find_map(|b| b.error.clone()),
            players: end.iter().map(|e| e["players"].as_u64().unwrap_or(0)).sum(),
            dropped_pct,
            late_p99: end
                .iter()
                .map(|e| e["lateMs"]["p99"].as_f64().unwrap_or(0.0))
                .fold(0.0, f64::max),
            rtt_p50: ms(steady_rtt(summary, 0.5)),
            rtt_p99: ms(steady_rtt(summary, 0.99)),
            bytes_per_bot: summary.delta_bytes_per_sec_per_bot(),
            usage,
            stats,
            window,
        }
    }

    fn degraded(&self) -> bool {
        self.joined < self.bots
            || self.dropped_pct.is_none_or(|d| d > 1.0)
            || self.late_p99 > 50.0
            || self.rtt_p99.is_none_or(|r| r > 150.0)
    }

    fn line(&self) -> String {
        let opt =
            |v: Option<f64>, digits: usize| v.map_or("—".to_owned(), |v| format!("{v:.digits$}"));
        let mut line = format!(
            "{:>9} {:>6} {:>6} {:>6} {:>7}% {:>9.1} {:>8} {:>8} {:>9.0} {:>6} {:>7}  {}",
            format!("{}x{}", self.zones, self.per_zone),
            self.bots,
            self.joined,
            self.players,
            opt(self.dropped_pct, 2),
            self.late_p99,
            opt(self.rtt_p50, 1),
            opt(self.rtt_p99, 1),
            self.bytes_per_bot,
            opt(self.usage.map(|u| u.0), 2),
            opt(self.usage.map(|u| u.1), 0),
            self.stats
        );
        if self.degraded() {
            line.push_str("  DEGRADED");
        }
        if let Some(error) = &self.first_error {
            line.push_str(&format!(" ({} failed: {error})", self.failed));
        }
        line
    }

    fn json(&self) -> Value {
        json!({
            "zones": self.zones,
            "perZone": self.per_zone,
            "bots": self.bots,
            "joined": self.joined,
            "failed": self.failed,
            "firstError": self.first_error,
            "players": self.players,
            "droppedPct": self.dropped_pct,
            "lateP99Ms": self.late_p99,
            "rttP50Ms": self.rtt_p50,
            "rttP99Ms": self.rtt_p99,
            "bytesPerSecPerBot": self.bytes_per_bot,
            "celldCores": self.usage.map(|u| u.0),
            "celldRssMb": self.usage.map(|u| u.1),
            "stats": self.stats,
            "windowStart": self.window.0,
            "windowEnd": self.window.1,
            "degraded": self.degraded(),
        })
    }
}
