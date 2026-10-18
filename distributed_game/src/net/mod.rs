//! WebSocket transport (`net` feature): one axum server hosting many zones.
//!
//! - Route: `GET /zones/{zone_id}` upgrades to a WebSocket. Zones are created
//!   on first use through the game's factory and each runs on its own Tokio
//!   task driving a [`ZoneHost`].
//! - Binary frames only (one protocol message per frame); text frames get
//!   `Error(BAD_MESSAGE)` and are closed.
//! - Admission: the first frame must be Hello within `hello_timeout`. A fresh
//!   join needs a ticket verified with [`TicketKey`] (signature, expiry,
//!   zone == route zone, single-use nonce). The principal comes **only** from
//!   the verified ticket. A reconnect presents the Welcome's reconnect token
//!   within the host's `reconnect_grace`.
//! - Inputs are rate limited per connection with a token bucket; excess
//!   inputs are dropped (one `Error(RATE_LIMITED)` per second) and sustained
//!   abuse is kicked.
//! - [`NetSim`] adds latency/jitter/loss in both directions for development
//!   (configurable from the environment with [`NetSim::from_env`]).
//! - `GET /metrics` serves per-zone Prometheus metrics ([`crate::metrics`]).
//! - [`GameServer::serve`] shuts down gracefully: every zone kicks its
//!   clients with `Kick(SHUTDOWN)`, runs `on_dispose`, and connections close.

pub mod ticket;

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use ticket::{NonceCache, TicketError, TicketKey};

use crate::clock::{Clock, SystemClock};
use crate::facts::{fact_channel, spawn_fact_worker, FactQueue, FactSink, RetryPolicy};
use crate::host::{HostConfig, JoinError, Joined, Outbound, ZoneHost};
use crate::metrics::GameMetrics;
use crate::protocol::{error, kick, ClientMessage, ServerMessage, PROTOCOL_VERSION};
use crate::rng::ZoneRng;
use crate::zone::{ClientId, Zone};

/// Development network simulation, applied independently to each direction
/// of every connection.
///
/// Semantics over WebSocket (a reliable, ordered stream): a message is never
/// dropped or reordered, because Deltas are encoded against the previous
/// one. `loss` models what TCP does with a lost segment instead: with
/// probability `loss` a message is delayed by an extra `retransmit` (repeated
/// with the same probability, at most 3 times), and since order is preserved
/// every later message queues behind it (head-of-line blocking). Total
/// one-way delay = `latency + U[0, jitter] + k·retransmit`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct NetSim {
    /// Fixed one-way delay.
    pub latency: Duration,
    /// Extra uniform random delay in `[0, jitter]` (order is preserved).
    pub jitter: Duration,
    /// Probability in `[0, 1]` that a message needs a retransmission.
    pub loss: f32,
    /// Delay added per simulated retransmission (TCP minimum RTO ≈ 200 ms).
    pub retransmit: Duration,
}

impl NetSim {
    /// Latency + jitter, no loss.
    pub fn new(latency: Duration, jitter: Duration) -> Self {
        Self {
            latency,
            jitter,
            loss: 0.0,
            retransmit: Duration::from_millis(200),
        }
    }

    /// Add simulated loss (see the type docs for WebSocket semantics).
    pub fn with_loss(mut self, loss: f32, retransmit: Duration) -> Self {
        self.loss = loss.clamp(0.0, 1.0);
        self.retransmit = retransmit;
        self
    }

    /// Read `DG_NET_LATENCY_MS`, `DG_NET_JITTER_MS`, `DG_NET_LOSS` (fraction,
    /// e.g. `0.02`) and `DG_NET_RETRANSMIT_MS` (default 200). `None` when
    /// none of the first three is set or all are zero.
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// [`from_env`](Self::from_env) over any key lookup.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Option<Self> {
        let ms = |key: &str| {
            lookup(key)
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(Duration::from_millis)
        };
        let latency = ms("DG_NET_LATENCY_MS").unwrap_or_default();
        let jitter = ms("DG_NET_JITTER_MS").unwrap_or_default();
        let loss = lookup("DG_NET_LOSS")
            .and_then(|v| v.trim().parse::<f32>().ok())
            .unwrap_or(0.0);
        let retransmit = ms("DG_NET_RETRANSMIT_MS").unwrap_or(Duration::from_millis(200));
        (!latency.is_zero() || !jitter.is_zero() || loss > 0.0)
            .then(|| Self::new(latency, jitter).with_loss(loss, retransmit))
    }

    /// One-way delay for one message.
    fn delay(&self, rng: &mut ZoneRng) -> Duration {
        let mut delay = self.latency + self.jitter.mul_f32(rng.next_f32());
        for _ in 0..3 {
            if self.loss <= 0.0 || !rng.chance(self.loss) {
                break;
            }
            delay += self.retransmit;
        }
        delay
    }
}

/// Server settings.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Settings for every hosted zone.
    pub host: HostConfig,
    /// HMAC key for admission tickets.
    pub ticket_key: TicketKey,
    /// Deadline for the first (Hello) frame.
    pub hello_timeout: Duration,
    /// Sustained inputs per second per connection.
    pub input_rate: f64,
    /// Input burst size.
    pub input_burst: f64,
    /// Outbound frames buffered per connection before it is dropped as slow.
    pub outbound_queue: usize,
    /// Largest accepted inbound frame.
    pub max_frame_bytes: usize,
    /// Optional dev latency simulation.
    pub net_sim: Option<NetSim>,
    /// Zones that get their own `zone` metrics label (see [`crate::metrics`]).
    pub metrics_zone_labels: usize,
}

impl ServerConfig {
    /// Defaults with the given ticket key.
    pub fn new(ticket_key: TicketKey) -> Self {
        Self {
            host: HostConfig::default(),
            ticket_key,
            hello_timeout: Duration::from_secs(5),
            input_rate: 120.0,
            input_burst: 240.0,
            outbound_queue: 512,
            max_frame_bytes: 64 * 1024,
            net_sim: None,
            metrics_zone_labels: 64,
        }
    }
}

enum ZoneCmd {
    Join {
        claims: crate::zone::TicketClaims,
        capabilities: u32,
        content_version: String,
        out: mpsc::Sender<ConnOut>,
        reply: oneshot::Sender<Result<Joined, JoinError>>,
    },
    Reconnect {
        token: String,
        capabilities: u32,
        out: mpsc::Sender<ConnOut>,
        reply: oneshot::Sender<Result<Joined, JoinError>>,
    },
    Input {
        client: ClientId,
        seq: u32,
        client_tick: u32,
        payload: Vec<u8>,
    },
    Disconnect {
        client: ClientId,
        graceful: bool,
    },
}

enum ConnOut {
    Frame(Vec<u8>),
    Close,
}

type ConfigFactory<Z> = dyn Fn(&str) -> Option<<Z as Zone>::Config> + Send + Sync;
type FactsFactory = dyn Fn(&str) -> Option<FactQueue> + Send + Sync;

struct Inner<Z: Zone> {
    config: ServerConfig,
    clock: Arc<dyn Clock>,
    factory: Box<ConfigFactory<Z>>,
    facts: Option<Box<FactsFactory>>,
    nonces: Mutex<NonceCache>,
    zones: Mutex<HashMap<String, mpsc::Sender<ZoneCmd>>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    shutdown: watch::Sender<bool>,
    token_counter: AtomicU64,
    metrics: Arc<GameMetrics>,
}

/// Multi-zone WebSocket game server.
pub struct GameServer<Z: Zone> {
    inner: Arc<Inner<Z>>,
}

impl<Z: Zone> Clone for GameServer<Z> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<Z> GameServer<Z>
where
    Z: Zone,
    Z::Config: Send,
{
    /// Server whose zones are created by `factory(zone_id)`; `None` = 404.
    pub fn new(
        config: ServerConfig,
        factory: impl Fn(&str) -> Option<Z::Config> + Send + Sync + 'static,
    ) -> Self {
        Self::with_clock(config, Arc::new(SystemClock::new()), factory)
    }

    /// Same as [`new`](Self::new) with an explicit clock.
    pub fn with_clock(
        config: ServerConfig,
        clock: Arc<dyn Clock>,
        factory: impl Fn(&str) -> Option<Z::Config> + Send + Sync + 'static,
    ) -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                clock,
                factory: Box::new(factory),
                facts: None,
                nonces: Mutex::new(NonceCache::default()),
                zones: Mutex::new(HashMap::new()),
                tasks: Mutex::new(Vec::new()),
                shutdown,
                token_counter: AtomicU64::new(0),
                metrics: Arc::new(GameMetrics::new(config.metrics_zone_labels)),
                config,
            }),
        }
    }

    /// Commit every zone's facts through `sink` (one worker per zone).
    /// Must be called before the server is cloned or served.
    pub fn with_fact_sink<S: FactSink>(
        mut self,
        sink: S,
        capacity: usize,
        policy: RetryPolicy,
    ) -> Self {
        let sink = Arc::new(sink);
        let inner = Arc::get_mut(&mut self.inner).expect("configure before sharing the server");
        inner.facts = Some(Box::new(move |_zone: &str| {
            let (queue, feed) = fact_channel(capacity, policy);
            spawn_fact_worker(feed, sink.clone());
            Some(queue)
        }));
        self
    }

    /// axum router exposing `GET /zones/{zone_id}` and `GET /metrics`.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/zones/{zone_id}", get(upgrade::<Z>))
            .route("/metrics", get(metrics_text::<Z>))
            .with_state(self.inner.clone())
    }

    /// Per-zone metrics registry (also served at `GET /metrics`).
    pub fn metrics(&self) -> &GameMetrics {
        &self.inner.metrics
    }

    /// Ids of zones currently hosted.
    pub fn zone_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self
            .inner
            .zones
            .lock()
            .expect("zones")
            .keys()
            .cloned()
            .collect();
        ids.sort();
        ids
    }

    /// Begin graceful shutdown of every zone.
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown.send(true);
    }

    /// Serve until `signal` resolves, then shut every zone down and wait for
    /// zone tasks to finish.
    pub async fn serve(
        self,
        listener: tokio::net::TcpListener,
        signal: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let server = self.clone();
        let mut stopped = self.inner.shutdown.subscribe();
        axum::serve(listener, self.router())
            .with_graceful_shutdown(async move {
                tokio::select! {
                    _ = signal => {}
                    _ = stopped.wait_for(|stop| *stop) => {}
                }
                server.shutdown();
            })
            .await?;
        let tasks: Vec<_> = std::mem::take(&mut *self.inner.tasks.lock().expect("tasks"));
        for task in tasks {
            let _ = task.await;
        }
        Ok(())
    }
}

fn zone_sender<Z>(inner: &Arc<Inner<Z>>, zone_id: &str) -> Option<mpsc::Sender<ZoneCmd>>
where
    Z: Zone,
    Z::Config: Send,
{
    if *inner.shutdown.borrow() {
        return None;
    }
    let mut zones = inner.zones.lock().expect("zones");
    if let Some(tx) = zones.get(zone_id).filter(|tx| !tx.is_closed()) {
        return Some(tx.clone());
    }
    let zone_config = (inner.factory)(zone_id)?;
    let (tx, rx) = mpsc::channel(1024);
    zones.insert(zone_id.to_string(), tx.clone());
    let facts = inner.facts.as_ref().and_then(|f| f(zone_id));
    let task = tokio::spawn(run_zone::<Z>(
        inner.clone(),
        zone_id.to_string(),
        zone_config,
        facts,
        rx,
    ));
    inner.tasks.lock().expect("tasks").push(task);
    Some(tx)
}

async fn metrics_text<Z: Zone>(State(inner): State<Arc<Inner<Z>>>) -> Response {
    Response::builder()
        .header(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(axum::body::Body::from(inner.metrics.render()))
        .expect("static response")
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

async fn run_zone<Z: Zone>(
    inner: Arc<Inner<Z>>,
    zone_id: String,
    zone_config: Z::Config,
    facts: Option<FactQueue>,
    mut rx: mpsc::Receiver<ZoneCmd>,
) {
    let key = inner.config.ticket_key.clone();
    let token_inner = inner.clone();
    let token_zone = zone_id.clone();
    let mut host = ZoneHost::<Z>::new(
        zone_id.clone(),
        inner.config.host.clone(),
        inner.clock.clone(),
        zone_config,
    )
    .with_token_source(move || {
        let n = token_inner.token_counter.fetch_add(1, Ordering::Relaxed);
        let seed = format!(
            "token|{token_zone}|{n}|{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        );
        key.hmac(seed.as_bytes())[..16]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    });
    if let Some(queue) = facts {
        host = host.with_facts(queue);
    }
    let mut conns: HashMap<ClientId, mpsc::Sender<ConnOut>> = HashMap::new();
    let mut shutdown = inner.shutdown.subscribe();
    loop {
        let report = host.advance();
        inner.metrics.record_advance(&host, &report);
        route(&mut host, &mut conns);
        let sleep_for = report
            .next_wake
            .map(|at| at.saturating_sub(inner.clock.now()));
        tokio::select! {
            biased;
            _ = shutdown.wait_for(|stop| *stop) => {
                let bye = ServerMessage::Kick { code: kick::SHUTDOWN, message: "server shutting down".into() };
                let frame = bye.encode(&()).expect("kick encodes");
                for out in conns.values() {
                    let _ = out.try_send(ConnOut::Frame(frame.clone()));
                    let _ = out.try_send(ConnOut::Close);
                }
                conns.clear();
                host.dispose();
                host.drain_outbound();
                break;
            }
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { break };
                handle_cmd(&mut host, &mut conns, &inner.metrics, cmd);
                // Drain whatever else is queued before ticking again.
                while let Ok(cmd) = rx.try_recv() {
                    handle_cmd(&mut host, &mut conns, &inner.metrics, cmd);
                }
                route(&mut host, &mut conns);
            }
            _ = async {
                match sleep_for {
                    Some(d) => tokio::time::sleep(d).await,
                    None => std::future::pending().await,
                }
            } => {}
        }
    }
    inner.zones.lock().expect("zones").remove(&zone_id);
    inner.metrics.forget_zone(&zone_id);
}

fn handle_cmd<Z: Zone>(
    host: &mut ZoneHost<Z>,
    conns: &mut HashMap<ClientId, mpsc::Sender<ConnOut>>,
    metrics: &GameMetrics,
    cmd: ZoneCmd,
) {
    match cmd {
        ZoneCmd::Join {
            claims,
            capabilities,
            content_version,
            out,
            reply,
        } => {
            let result = host.join(&claims, capabilities, &content_version);
            if let Ok(joined) = &result {
                conns.insert(joined.client, out);
            }
            let _ = reply.send(result);
        }
        ZoneCmd::Reconnect {
            token,
            capabilities,
            out,
            reply,
        } => {
            let result = host.reconnect(&token, capabilities);
            if let Ok(joined) = &result {
                conns.insert(joined.client, out);
            }
            let _ = reply.send(result);
        }
        ZoneCmd::Input {
            client,
            seq,
            client_tick,
            payload,
        } => {
            let outcome = host.push_input(client, seq, client_tick, &payload);
            metrics.record_input(host.zone_id(), outcome);
        }
        ZoneCmd::Disconnect { client, graceful } => {
            conns.remove(&client);
            host.disconnect(client, graceful);
        }
    }
}

fn route<Z: Zone>(host: &mut ZoneHost<Z>, conns: &mut HashMap<ClientId, mpsc::Sender<ConnOut>>) {
    for outbound in host.drain_outbound() {
        match outbound {
            Outbound::Frame { client, frame } => {
                let Some(out) = conns.get(&client) else {
                    continue;
                };
                if out.try_send(ConnOut::Frame(frame)).is_err() {
                    // Slow or gone: drop the connection; the player keeps its grace window.
                    let out = conns.remove(&client).expect("present");
                    let _ = out.try_send(ConnOut::Close);
                    host.disconnect(client, false);
                }
            }
            Outbound::Close { client } => {
                if let Some(out) = conns.remove(&client) {
                    let _ = out.try_send(ConnOut::Close);
                }
            }
        }
    }
}

async fn upgrade<Z>(
    ws: WebSocketUpgrade,
    Path(zone_id): Path<String>,
    State(inner): State<Arc<Inner<Z>>>,
) -> Response
where
    Z: Zone,
    Z::Config: Send,
{
    let max = inner.config.max_frame_bytes;
    ws.max_message_size(max)
        .on_upgrade(move |socket| connection(inner, zone_id, socket))
}

fn error_frame(code: u32, message: impl Into<String>) -> Vec<u8> {
    ServerMessage::Error {
        code,
        message: message.into(),
    }
    .encode(&())
    .expect("error encodes")
}

async fn reject(mut socket: WebSocket, code: u32, message: impl Into<String>) {
    let _ = socket
        .send(Message::Binary(error_frame(code, message).into()))
        .await;
    let _ = socket.send(Message::Close(None)).await;
}

/// Order-preserving delay line for [`NetSim`].
struct DelayLine<T> {
    sim: Option<NetSim>,
    rng: ZoneRng,
    queue: VecDeque<(Instant, T)>,
    last: Instant,
}

impl<T> DelayLine<T> {
    fn new(sim: Option<NetSim>, seed: u64) -> Self {
        Self {
            sim,
            rng: ZoneRng::from_seed(seed),
            queue: VecDeque::new(),
            last: Instant::now(),
        }
    }

    /// Returns the item immediately when no simulation is configured.
    fn push(&mut self, item: T) -> Option<T> {
        let Some(sim) = self.sim else {
            return Some(item);
        };
        let at = (Instant::now() + sim.delay(&mut self.rng)).max(self.last);
        self.last = at;
        self.queue.push_back((at, item));
        None
    }

    fn next_due(&self) -> Option<Instant> {
        self.queue.front().map(|(at, _)| *at)
    }

    fn pop_due(&mut self) -> Option<T> {
        if self.queue.front()?.0 <= Instant::now() {
            self.queue.pop_front().map(|(_, item)| item)
        } else {
            None
        }
    }
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

struct Bucket {
    tokens: f64,
    rate: f64,
    burst: f64,
    last: Instant,
    dropped_in_row: u32,
    last_warned: Option<Instant>,
}

impl Bucket {
    fn take(&mut self) -> bool {
        let now = Instant::now();
        self.tokens =
            (self.tokens + now.duration_since(self.last).as_secs_f64() * self.rate).min(self.burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            self.dropped_in_row = 0;
            true
        } else {
            self.dropped_in_row += 1;
            false
        }
    }
}

enum Inbound {
    Input {
        seq: u32,
        client_tick: u32,
        payload: Vec<u8>,
    },
    Ping(f64),
}

async fn connection<Z>(inner: Arc<Inner<Z>>, zone_id: String, mut socket: WebSocket)
where
    Z: Zone,
    Z::Config: Send,
{
    let cfg = &inner.config;
    // 1. Hello.
    let first = tokio::time::timeout(cfg.hello_timeout, socket.recv()).await;
    let hello = match first {
        Ok(Some(Ok(Message::Binary(bytes)))) => match ClientMessage::decode(&bytes) {
            Ok(ClientMessage::Hello(hello)) => hello,
            _ => return reject(socket, error::BAD_MESSAGE, "expected Hello").await,
        },
        Ok(Some(Ok(_))) => return reject(socket, error::BAD_MESSAGE, "binary frames only").await,
        _ => return,
    };
    if hello.protocol_version != PROTOCOL_VERSION {
        return reject(
            socket,
            error::VERSION_MISMATCH,
            format!("protocol {} != {PROTOCOL_VERSION}", hello.protocol_version),
        )
        .await;
    }
    let Some(zone) = zone_sender(&inner, &zone_id) else {
        return reject(socket, error::NOT_FOUND, "unknown zone").await;
    };
    let (out_tx, mut out_rx) = mpsc::channel(cfg.outbound_queue.max(1));
    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = if hello.reconnect_token.is_empty() {
        let now = unix_now();
        let claims = match cfg.ticket_key.verify(&hello.ticket, now) {
            Ok(claims) => claims,
            Err(e) => return reject(socket, error::UNAUTHORIZED, format!("{e:?}")).await,
        };
        if claims.zone != zone_id {
            return reject(socket, error::UNAUTHORIZED, "ticket is for another zone").await;
        }
        let replay = inner.nonces.lock().expect("nonces").check_and_insert(
            &claims.nonce,
            claims.expires_at,
            now,
        );
        if let Err(e) = replay {
            return reject(socket, error::UNAUTHORIZED, format!("{e:?}")).await;
        }
        ZoneCmd::Join {
            claims,
            capabilities: hello.capabilities,
            content_version: hello.content_version,
            out: out_tx,
            reply: reply_tx,
        }
    } else {
        ZoneCmd::Reconnect {
            token: hello.reconnect_token,
            capabilities: hello.capabilities,
            out: out_tx,
            reply: reply_tx,
        }
    };
    if zone.send(cmd).await.is_err() {
        return reject(socket, error::NOT_FOUND, "zone stopped").await;
    }
    let client = match reply_rx.await {
        Ok(Ok(joined)) => joined.client,
        Ok(Err(e)) => return reject(socket, e.code, e.message).await,
        Err(_) => return reject(socket, error::NOT_FOUND, "zone stopped").await,
    };

    // 2. Session loop.
    let seed = u64::from(client.0) ^ unix_now();
    let mut outbound: DelayLine<ConnOut> = DelayLine::new(cfg.net_sim, seed);
    let mut inbound: DelayLine<Inbound> = DelayLine::new(cfg.net_sim, !seed);
    let mut bucket = Bucket {
        tokens: cfg.input_burst,
        rate: cfg.input_rate,
        burst: cfg.input_burst,
        last: Instant::now(),
        dropped_in_row: 0,
        last_warned: None,
    };
    let mut graceful = false;
    let mut notify_zone = true;
    'session: loop {
        // Flush due delayed items.
        while let Some(item) = outbound.pop_due() {
            match send_out(&mut socket, item).await {
                Ok(true) => {}
                _ => {
                    notify_zone = false;
                    break 'session;
                }
            }
        }
        while let Some(item) = inbound.pop_due() {
            if !forward_in(&inner, &zone, &mut socket, &mut outbound, client, item).await {
                break 'session;
            }
        }
        let next = match (outbound.next_due(), inbound.next_due()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        tokio::select! {
            out = out_rx.recv() => {
                let Some(out) = out else { notify_zone = false; break };
                if let Some(out) = outbound.push(out) {
                    if !send_out(&mut socket, out).await.unwrap_or(false) {
                        notify_zone = false;
                        break;
                    }
                }
            }
            msg = socket.recv() => {
                let bytes = match msg {
                    Some(Ok(Message::Binary(bytes))) => bytes,
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                    Some(Ok(Message::Text(_))) => {
                        let _ = socket.send(Message::Binary(error_frame(error::BAD_MESSAGE, "binary frames only").into())).await;
                        break;
                    }
                    _ => break,
                };
                let item = match ClientMessage::decode(&bytes) {
                    Ok(ClientMessage::Input { seq, client_tick, payload }) => {
                        if !bucket.take() {
                            inner.metrics.record_rate_limited(&zone_id);
                            if bucket.dropped_in_row as f64 > cfg.input_burst {
                                let kick = ServerMessage::Kick { code: kick::RATE_LIMITED, message: "input flood".into() };
                                let _ = socket.send(Message::Binary(kick.encode(&()).expect("kick").into())).await;
                                break;
                            }
                            let now = Instant::now();
                            if bucket.last_warned.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1)) {
                                bucket.last_warned = Some(now);
                                let _ = socket.send(Message::Binary(error_frame(error::RATE_LIMITED, "input dropped").into())).await;
                            }
                            continue;
                        }
                        Inbound::Input { seq, client_tick, payload }
                    }
                    Ok(ClientMessage::Ping { client_time }) => Inbound::Ping(client_time),
                    Ok(ClientMessage::Leave) => { graceful = true; break; }
                    Ok(ClientMessage::Hello(_)) => {
                        let _ = socket.send(Message::Binary(error_frame(error::BAD_MESSAGE, "already joined").into())).await;
                        continue;
                    }
                    Err(e) => {
                        let _ = socket.send(Message::Binary(error_frame(error::BAD_MESSAGE, e.to_string()).into())).await;
                        continue;
                    }
                };
                if let Some(item) = inbound.push(item) {
                    if !forward_in(&inner, &zone, &mut socket, &mut outbound, client, item).await {
                        break;
                    }
                }
            }
            _ = sleep_until_opt(next) => {}
        }
    }
    if notify_zone {
        let _ = zone.send(ZoneCmd::Disconnect { client, graceful }).await;
    }
    let _ = socket.send(Message::Close(None)).await;
}

/// Ok(false) when the connection should end.
async fn send_out(socket: &mut WebSocket, out: ConnOut) -> Result<bool, axum::Error> {
    match out {
        ConnOut::Frame(frame) => {
            socket.send(Message::Binary(frame.into())).await?;
            Ok(true)
        }
        ConnOut::Close => Ok(false),
    }
}

async fn forward_in<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    zone: &mpsc::Sender<ZoneCmd>,
    socket: &mut WebSocket,
    outbound: &mut DelayLine<ConnOut>,
    client: ClientId,
    item: Inbound,
) -> bool {
    match item {
        Inbound::Input {
            seq,
            client_tick,
            payload,
        } => zone
            .send(ZoneCmd::Input {
                client,
                seq,
                client_tick,
                payload,
            })
            .await
            .is_ok(),
        Inbound::Ping(client_time) => {
            let pong = ServerMessage::Pong {
                client_time,
                server_time: inner.clock.now().as_secs_f64() * 1000.0,
            }
            .encode(&())
            .expect("pong encodes");
            match outbound.push(ConnOut::Frame(pong)) {
                Some(out) => send_out(socket, out).await.unwrap_or(false),
                None => true,
            }
        }
    }
}

#[cfg(test)]
mod netsim_tests {
    use super::*;

    #[test]
    fn netsim_from_env_and_loss_semantics() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert_eq!(NetSim::from_lookup(env(&[])), None);
        let sim = NetSim::from_lookup(env(&[
            ("DG_NET_LATENCY_MS", "40"),
            ("DG_NET_JITTER_MS", "10"),
            ("DG_NET_LOSS", "0.5"),
        ]))
        .unwrap();
        assert_eq!(sim.latency, Duration::from_millis(40));
        assert_eq!(sim.jitter, Duration::from_millis(10));
        assert_eq!(sim.retransmit, Duration::from_millis(200));
        let mut rng = ZoneRng::from_seed(7);
        let delays: Vec<Duration> = (0..2000).map(|_| sim.delay(&mut rng)).collect();
        let base_max = Duration::from_millis(50);
        let retransmitted = delays.iter().filter(|d| **d > base_max).count();
        // P(at least one retransmit) = 0.5.
        assert!((850..1150).contains(&retransmitted), "{retransmitted}");
        assert!(delays.iter().all(|d| *d >= sim.latency));
        assert!(delays.iter().all(|d| *d <= base_max + sim.retransmit * 3));
        // Without loss the delay is bounded by latency + jitter.
        let clean = NetSim::new(Duration::from_millis(40), Duration::from_millis(10));
        assert!((0..500).all(|_| clean.delay(&mut rng) <= base_max));
    }
}
