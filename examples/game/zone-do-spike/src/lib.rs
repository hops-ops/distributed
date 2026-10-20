//! Spike gr-p42: a live Mob Pit room as a celld Durable Object.
//!
//! - `GET /ticket?room=&name=` (dev only): a signed join ticket, the same
//!   HS256 format the native zone server admits (`distributed_game::ticket`).
//! - `GET /rooms/{room}` (WebSocket upgrade): the room object named `room`.
//!   The client speaks the existing `distributed_game` wire protocol: Hello
//!   with the ticket, then Input / Ping / Leave; the room answers Welcome,
//!   Delta, Event, Pong.
//! - `GET /rooms/{room}/stats`: tick timing numbers of the room (JSON).
//! - `GET /placement?rooms=a,b`: which node owns each room (operator view).
//! - everything else: the static debug page (`public/`).
//!
//! The room (`PitRoom`) runs a `ZoneHost<Pit>` in the object's memory. It
//! ticks with a `setTimeout` loop while any player is in it, writes its
//! hibernation snapshot to object storage every few seconds and when the
//! last player leaves, then stops; celld may then evict it. The next join
//! reactivates it on whichever node owns it and restores the snapshot.
//!
//! celld does not tell application code which node runs it, so the room
//! asks every node's operator listener (`NODES`) whether it holds the room.

mod pit;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use distributed_game::protocol::{error, ClientMessage, ServerMessage, PROTOCOL_VERSION};
use distributed_game::ticket::TicketKey;
use distributed_game::{Clock, HostConfig, Outbound, Role, TicketClaims, ZoneHost};
use pit::Pit;
use serde::{Deserialize, Serialize};
use serde_json::json;
use worker::*;

const SNAPSHOT_KEY: &str = "pit-snapshot";
const PERSIST_EVERY_MS: f64 = 5_000.0;

// ------------------------------------------------------------------ time

fn performance_now() -> f64 {
    let global = js_sys::global();
    js_sys::Reflect::get(&global, &"performance".into())
        .ok()
        .and_then(|p| {
            let now = js_sys::Reflect::get(&p, &"now".into()).ok()?;
            let now: js_sys::Function = now.into();
            now.call0(&p).ok()?.as_f64()
        })
        .unwrap_or_else(js_sys::Date::now)
}

/// Monotonic clock over `Date.now()` (`Instant` panics on wasm32).
struct JsClock {
    origin_ms: f64,
    last_us: AtomicU64,
}

impl JsClock {
    fn new() -> Self {
        Self {
            origin_ms: js_sys::Date::now(),
            last_us: AtomicU64::new(0),
        }
    }
}

impl Clock for JsClock {
    fn now(&self) -> Duration {
        let us = ((js_sys::Date::now() - self.origin_ms).max(0.0) * 1000.0) as u64;
        let us = self.last_us.fetch_max(us, Ordering::Relaxed).max(us);
        Duration::from_micros(us)
    }

    fn unix_ms(&self) -> u64 {
        self.origin_ms as u64 + self.now().as_millis() as u64
    }
}

fn random_id() -> String {
    format!(
        "{:08x}",
        (js_sys::Math::random() * f64::from(u32::MAX)) as u32
    )
}

thread_local! {
    /// One id per V8 isolate (module instance).
    static ISOLATE: String = random_id();
    /// Rooms alive in this isolate.
    static ISOLATE_ROOMS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

// ------------------------------------------------------------------ stats

/// Loop timing over the life of an activation.
#[derive(Default)]
struct TickStats {
    /// How late each wake-up was against its deadline (ms).
    late: VecDeque<f64>,
    /// Interval between consecutive loop iterations (ms).
    interval: VecDeque<f64>,
    /// Work per iteration (ms; advance + sends).
    work: VecDeque<f64>,
    /// Fixed steps dropped by the catch-up cap.
    dropped_ticks: u64,
    /// Iterations that ran more than one fixed step (catch-up).
    catch_up: u64,
    iterations: u64,
    last_wake: Option<f64>,
    late_ema: f64,
    work_ema: f64,
    late_max_window: f64,
    window: u32,
    late_max: f64,
}

const STAT_WINDOW: usize = 20_000;

fn push(q: &mut VecDeque<f64>, v: f64) {
    if q.len() == STAT_WINDOW {
        q.pop_front();
    }
    q.push_back(v);
}

fn pct(q: &VecDeque<f64>, p: f64) -> f64 {
    if q.is_empty() {
        return 0.0;
    }
    let mut v: Vec<f64> = q.iter().copied().collect();
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() - 1) as f64 * p).round() as usize]
}

// ------------------------------------------------------------------ room

#[derive(Serialize, Deserialize)]
struct Attachment {
    client: u32,
    activation: String,
}

struct Room {
    name: String,
    id_hex: String,
    activation: String,
    constructed_ms: f64,
    host: Option<ZoneHost<Pit>>,
    sockets: HashMap<u32, WebSocket>,
    ticking: bool,
    hz: u32,
    key: TicketKey,
    nonces: HashSet<String>,
    stats: TickStats,
    first_tick_ms: Option<f64>,
    restored: bool,
    node: String,
    welcome_ms: Vec<f64>,
}

impl Room {
    fn flush(&mut self) {
        let Some(host) = self.host.as_mut() else {
            return;
        };
        for out in host.drain_outbound() {
            match out {
                Outbound::Frame { client, frame } => {
                    if let Some(ws) = self.sockets.get(&client.0) {
                        let _ = ws.send_with_bytes(frame);
                    }
                }
                Outbound::Close { client } => {
                    if let Some(ws) = self.sockets.remove(&client.0) {
                        let _ = ws.close(Some(1000), Some("closed by zone"));
                    }
                }
            }
        }
    }

    fn occupied(&self) -> bool {
        self.host
            .as_ref()
            .is_some_and(|h| h.player_count() > 0 || h.client_count() > 0)
    }

    fn snapshot(&self) -> Option<String> {
        let state = self.host.as_ref()?.hibernate_state()?;
        String::from_utf8(state).ok()
    }

    fn sync_host_info(&mut self) {
        let isolate_rooms = ISOLATE_ROOMS.with(|r| r.borrow().len()) as u16;
        let (late, late_max, work) = (
            self.stats.late_ema,
            self.stats.late_max,
            self.stats.work_ema,
        );
        let wake_ms = self
            .first_tick_ms
            .map(|t| (t - self.constructed_ms).max(0.0) as u32)
            .unwrap_or(0);
        let (node, restored, hz) = (self.node.clone(), self.restored, self.hz as u8);
        if let Some(host) = self.host.as_mut() {
            let zone = host.zone_mut();
            let activations = zone.activations;
            let info = &mut zone.host;
            info.node = node;
            info.restored = restored;
            info.activations = activations;
            info.hz = hz;
            info.late_ms = late as f32;
            info.late_max_ms = late_max as f32;
            info.work_us = (work * 1000.0) as u32;
            info.wake_ms = wake_ms;
            info.isolate_rooms = isolate_rooms;
        }
    }
}

#[durable_object]
pub struct PitRoom {
    state: State,
    env: Env,
    room: Rc<RefCell<Room>>,
}

impl DurableObject for PitRoom {
    fn new(state: State, env: Env) -> Self {
        console_error_panic_hook::set_once();
        let constructed_ms = performance_now();
        let id_hex = state.id().to_string();
        let name = state.id().name().unwrap_or_default();
        let hz = env
            .var("TICK_HZ")
            .ok()
            .and_then(|v| v.to_string().parse().ok())
            .unwrap_or(30);
        let secret = env
            .var("TICKET_SECRET")
            .map(|v| v.to_string())
            .unwrap_or_default();
        let room = Room {
            name,
            id_hex,
            activation: random_id(),
            constructed_ms,
            host: None,
            sockets: HashMap::new(),
            ticking: false,
            hz,
            key: TicketKey::new(secret.into_bytes()),
            nonces: HashSet::new(),
            stats: TickStats::default(),
            first_tick_ms: None,
            restored: false,
            node: "?".into(),
            welcome_ms: Vec::new(),
        };
        Self {
            state,
            env,
            room: Rc::new(RefCell::new(room)),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        if let Some(name) = req.headers().get("x-room")? {
            let mut room = self.room.borrow_mut();
            if room.name.is_empty() {
                room.name = name;
            }
        }
        self.ensure_host().await?;
        if url.path().ends_with("/stats") {
            if url.query_pairs().any(|(k, _)| k == "reset") {
                self.room.borrow_mut().stats = TickStats::default();
            }
            return Response::from_json(&self.stats_json());
        }
        if req.headers().get("upgrade")?.as_deref() != Some("websocket") {
            return Response::error("expected a WebSocket upgrade", 426);
        }
        let pair = WebSocketPair::new()?;
        self.state.accept_web_socket(&pair.server);
        Response::from_websocket(pair.client)
    }

    async fn websocket_message(
        &self,
        ws: WebSocket,
        message: WebSocketIncomingMessage,
    ) -> Result<()> {
        let WebSocketIncomingMessage::Binary(bytes) = message else {
            let _ = ws.close(Some(1003), Some("binary frames only"));
            return Ok(());
        };
        self.ensure_host().await?;
        let msg = match ClientMessage::decode(&bytes) {
            Ok(m) => m,
            Err(_) => {
                reject(&ws, error::BAD_MESSAGE, "bad message");
                return Ok(());
            }
        };
        let attached: Option<Attachment> = ws.deserialize_attachment().ok().flatten();
        let activation = self.room.borrow().activation.clone();
        let client = match attached {
            Some(a) if a.activation == activation => Some(a.client),
            // The socket outlived the activation that admitted it (the room
            // was evicted and woke up again): its player is gone.
            Some(_) => {
                let _ = ws.close(Some(1012), Some("room restarted; rejoin"));
                return Ok(());
            }
            None => None,
        };
        match (client, msg) {
            (None, ClientMessage::Hello(hello)) => {
                let t0 = performance_now();
                if hello.protocol_version != PROTOCOL_VERSION {
                    reject(&ws, error::VERSION_MISMATCH, "protocol version");
                    return Ok(());
                }
                let joined = {
                    let mut room = self.room.borrow_mut();
                    let now = (js_sys::Date::now() / 1000.0) as u64;
                    let claims = match room.key.verify(&hello.ticket, now) {
                        Ok(c) => c,
                        Err(e) => {
                            drop(room);
                            reject(&ws, error::UNAUTHORIZED, &format!("{e:?}"));
                            return Ok(());
                        }
                    };
                    if claims.zone != room.name || !room.nonces.insert(claims.nonce.clone()) {
                        drop(room);
                        reject(&ws, error::UNAUTHORIZED, "wrong room or replayed ticket");
                        return Ok(());
                    }
                    room.sync_host_info();
                    let host = room.host.as_mut().expect("host loaded");
                    match host.join(&claims, hello.capabilities, &hello.content_version) {
                        Ok(joined) => {
                            let client = joined.client.0;
                            ws.serialize_attachment(Attachment {
                                client,
                                activation: activation.clone(),
                            })?;
                            room.sockets.insert(client, ws.clone());
                            room.flush();
                            let elapsed = performance_now() - t0;
                            room.welcome_ms.push(elapsed);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                };
                match joined {
                    Ok(()) => self.start_loop(),
                    Err(e) => reject(&ws, e.code, &e.message),
                }
            }
            (
                Some(client),
                ClientMessage::Input {
                    seq,
                    client_tick,
                    payload,
                },
            ) => {
                let mut room = self.room.borrow_mut();
                if let Some(host) = room.host.as_mut() {
                    host.push_input(
                        distributed_game::ClientId(client),
                        seq,
                        client_tick,
                        &payload,
                    );
                }
            }
            (_, ClientMessage::Ping { client_time }) => {
                let room = self.room.borrow();
                let server_time = room.host.as_ref().map_or(0.0, |h| h.server_time_ms());
                let pong = ServerMessage::Pong {
                    client_time,
                    server_time,
                }
                .encode(&())
                .expect("pong encodes");
                let _ = ws.send_with_bytes(pong);
            }
            (Some(client), ClientMessage::Leave) => {
                let mut room = self.room.borrow_mut();
                room.sockets.remove(&client);
                if let Some(host) = room.host.as_mut() {
                    host.disconnect(distributed_game::ClientId(client), true);
                }
                room.flush();
                let _ = ws.close(Some(1000), Some("left"));
            }
            _ => reject(&ws, error::BAD_MESSAGE, "unexpected message"),
        }
        Ok(())
    }

    async fn websocket_close(
        &self,
        ws: WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> Result<()> {
        self.drop_socket(&ws);
        Ok(())
    }

    async fn websocket_error(&self, ws: WebSocket, _error: Error) -> Result<()> {
        self.drop_socket(&ws);
        Ok(())
    }
}

fn reject(ws: &WebSocket, code: u32, message: &str) {
    if let Ok(frame) = (ServerMessage::Error {
        code,
        message: message.into(),
    })
    .encode(&())
    {
        let _ = ws.send_with_bytes(frame);
    }
    let _ = ws.close(Some(1008), Some(message));
}

impl PitRoom {
    fn drop_socket(&self, ws: &WebSocket) {
        let attached: Option<Attachment> = ws.deserialize_attachment().ok().flatten();
        let mut room = self.room.borrow_mut();
        if let Some(a) = attached.filter(|a| a.activation == room.activation) {
            room.sockets.remove(&a.client);
            if let Some(host) = room.host.as_mut() {
                host.disconnect(distributed_game::ClientId(a.client), false);
            }
        }
    }

    /// Create the zone host on first use: restore the stored snapshot and
    /// find out which node this is.
    async fn ensure_host(&self) -> Result<()> {
        if self.room.borrow().host.is_some() {
            return Ok(());
        }
        let stored: Option<String> = self.state.storage().get(SNAPSHOT_KEY).await?;
        let mut room = self.room.borrow_mut();
        if room.host.is_some() {
            return Ok(());
        }
        // `name@20` runs the room at 20 Hz (measurements).
        if let Some(hz) = room
            .name
            .split_once('@')
            .and_then(|(_, hz)| hz.parse().ok())
        {
            room.hz = hz;
        }
        let config = HostConfig {
            tick_rate: room.hz,
            patch_rate: room.hz.min(20),
            // The Durable Object stops ticking when empty instead.
            idle_tick_rate: None,
            boot_epoch: (js_sys::Date::now() * 1000.0) as u64,
            reconnect_grace: Duration::from_secs(5),
            ..HostConfig::default()
        };
        let name = room.name.clone();
        let mut host = ZoneHost::<Pit>::new(name.clone(), config, Arc::new(JsClock::new()), ())
            .with_token_source(|| format!("{}{}", random_id(), random_id()));
        if let Some(state) = stored {
            host.restore(state.as_bytes(), "");
            room.restored = true;
        }
        {
            let activation = room.activation.clone();
            let info = &mut host.zone_mut().host;
            info.activation = activation;
            info.isolate = ISOLATE.with(|i| i.clone());
        }
        room.host = Some(host);
        ISOLATE_ROOMS.with(|r| r.borrow_mut().insert(name));
        drop(room);
        self.find_node();
        Ok(())
    }

    /// Ask each node's operator listener whether it holds this room.
    fn find_node(&self) {
        let nodes = self
            .env
            .var("NODES")
            .map(|v| v.to_string())
            .unwrap_or_default();
        let room = self.room.clone();
        let id_hex = room.borrow().id_hex.clone();
        wasm_bindgen_futures::spawn_local(async move {
            for entry in nodes.split(',').filter(|e| !e.is_empty()) {
                let Some((node, addr)) = entry.split_once('=') else {
                    continue;
                };
                let Ok(mut resp) = Fetch::Url(
                    format!("http://{addr}/state")
                        .parse()
                        .expect("operator url"),
                )
                .send()
                .await
                else {
                    continue;
                };
                let Ok(state) = resp.json::<serde_json::Value>().await else {
                    continue;
                };
                let mine = state["deployment"]["cells"]
                    .as_object()
                    .is_some_and(|cells| cells.keys().any(|k| k.ends_with(&id_hex)));
                if mine {
                    let mut room = room.borrow_mut();
                    room.node = node.to_string();
                    room.sync_host_info();
                    return;
                }
            }
        });
    }

    fn start_loop(&self) {
        {
            let mut room = self.room.borrow_mut();
            if room.ticking {
                return;
            }
            room.ticking = true;
        }
        let room = self.room.clone();
        let storage = self.state.storage();
        wasm_bindgen_futures::spawn_local(async move {
            let step_ms = 1000.0 / f64::from(room.borrow().hz);
            let start = performance_now();
            let mut k: u64 = 1;
            let mut last_persist = start;
            loop {
                let deadline = start + k as f64 * step_ms;
                let wait = (deadline - performance_now()).max(0.0);
                Delay::from(Duration::from_micros((wait * 1000.0) as u64)).await;
                let woke = performance_now();
                // Skip deadlines we already missed (the fixed timestep
                // catches up with several steps in one advance).
                k = (((woke - start) / step_ms).floor() as u64 + 1).max(k + 1);
                let (stop, snapshot) = {
                    let mut room = room.borrow_mut();
                    let late = (woke - deadline).max(0.0);
                    let interval = room.stats.last_wake.map(|l| woke - l);
                    let report = room.host.as_mut().map(|h| h.advance());
                    if room.first_tick_ms.is_none() {
                        room.first_tick_ms = Some(woke);
                    }
                    room.flush();
                    let work = performance_now() - woke;
                    let s = &mut room.stats;
                    s.iterations += 1;
                    s.last_wake = Some(woke);
                    push(&mut s.late, late);
                    if let Some(i) = interval {
                        push(&mut s.interval, i);
                    }
                    push(&mut s.work, work);
                    if let Some(r) = &report {
                        s.dropped_ticks += u64::from(r.dropped_ticks);
                        if r.ticks > 1 {
                            s.catch_up += 1;
                        }
                    }
                    s.late_ema = if s.iterations == 1 {
                        late
                    } else {
                        s.late_ema * 0.95 + late * 0.05
                    };
                    s.work_ema = if s.iterations == 1 {
                        work
                    } else {
                        s.work_ema * 0.95 + work * 0.05
                    };
                    s.late_max_window = s.late_max_window.max(late);
                    s.window += 1;
                    if s.window >= room.hz {
                        let s = &mut room.stats;
                        s.late_max = s.late_max_window;
                        s.late_max_window = 0.0;
                        s.window = 0;
                    }
                    room.sync_host_info();
                    let stop = !room.occupied();
                    let persist = stop || woke - last_persist >= PERSIST_EVERY_MS;
                    if stop {
                        room.ticking = false;
                    }
                    (stop, persist.then(|| room.snapshot()).flatten())
                };
                if let Some(snapshot) = snapshot {
                    last_persist = woke;
                    if let Err(e) = storage.put(SNAPSHOT_KEY, snapshot).await {
                        console_error!("persist failed: {e}");
                    }
                }
                if stop {
                    return;
                }
            }
        });
    }

    fn stats_json(&self) -> serde_json::Value {
        let room = self.room.borrow();
        let s = &room.stats;
        let host = room.host.as_ref();
        json!({
            "room": room.name,
            "node": room.node,
            "isolate": ISOLATE.with(|i| i.clone()),
            "activation": room.activation,
            "restored": room.restored,
            "hz": room.hz,
            "ticking": room.ticking,
            "players": host.map_or(0, |h| h.player_count()),
            "tick": host.map_or(0, |h| h.tick()),
            "lifetime_ticks": host.map_or(0, |h| h.zone().ticks()),
            "kills": host.map_or(0, |h| h.zone().kills),
            "activations": host.map_or(0, |h| h.zone().activations),
            "isolate_rooms": ISOLATE_ROOMS.with(|r| r.borrow().len()),
            "wake_ms": room.first_tick_ms.map(|t| t - room.constructed_ms),
            "welcome_ms": room.welcome_ms,
            "iterations": s.iterations,
            "dropped_ticks": s.dropped_ticks,
            "catch_up_iterations": s.catch_up,
            "late_ms": {"p50": pct(&s.late, 0.5), "p99": pct(&s.late, 0.99), "max": pct(&s.late, 1.0)},
            "interval_ms": {"p50": pct(&s.interval, 0.5), "p99": pct(&s.interval, 0.99), "max": pct(&s.interval, 1.0)},
            "work_ms": {"p50": pct(&s.work, 0.5), "p99": pct(&s.work, 0.99), "max": pct(&s.work, 1.0)},
        })
    }
}

// ------------------------------------------------------------------ worker

fn ticket(env: &Env, room: &str, name: &str) -> Result<String> {
    let secret = env.var("TICKET_SECRET")?.to_string();
    let key = TicketKey::new(secret.into_bytes());
    let exp = (js_sys::Date::now() / 1000.0) as u64 + 300;
    let principal = format!("guest:{name}");
    let claims = TicketClaims::new(
        principal,
        room,
        exp,
        random_id() + &random_id(),
        Role::Player,
    )
    .with_name(name);
    Ok(key.sign(&claims))
}

async fn placement(env: &Env, rooms: &str) -> Result<Response> {
    let ns = env.durable_object("PIT_ROOM")?;
    let mut ids = Vec::new();
    for room in rooms.split(',').filter(|r| !r.is_empty()) {
        ids.push((room.to_string(), ns.id_from_name(room)?.to_string()));
    }
    let nodes = env.var("NODES")?.to_string();
    let mut out = serde_json::Map::new();
    let mut node_rows = Vec::new();
    for entry in nodes.split(',').filter(|e| !e.is_empty()) {
        let Some((node, addr)) = entry.split_once('=') else {
            continue;
        };
        let state = match Fetch::Url(format!("http://{addr}/state").parse()?)
            .send()
            .await
        {
            Ok(mut r) => r.json::<serde_json::Value>().await.ok(),
            Err(_) => None,
        };
        let Some(state) = state else {
            node_rows.push(json!({"node": node, "up": false}));
            continue;
        };
        let cells = state["deployment"]["cells"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        for (room, id) in &ids {
            if cells.keys().any(|k| k.ends_with(id.as_str())) {
                out.insert(room.clone(), json!(node));
            }
        }
        node_rows.push(json!({
            "node": node,
            "up": true,
            "resident_cells": state["node_load"]["resident_cells"],
            "owned_cells": state["owned_cells"],
            "cpu_percent": state["node_load"]["cpu_percent_x100"].as_f64().map(|c| c / 100.0),
            "rebalanced": state["rebalanced"],
        }));
    }
    Response::from_json(&json!({"rooms": out, "nodes": node_rows}))
}

#[event(fetch)]
async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let url = req.url()?;
    let path = url.path().to_string();
    let query: HashMap<String, String> = url.query_pairs().into_owned().collect();
    if path == "/ticket" {
        let room = query.get("room").cloned().unwrap_or_else(|| "pit-1".into());
        let name = query.get("name").cloned().unwrap_or_else(|| "guest".into());
        return Response::from_json(&json!({"ticket": ticket(&env, &room, &name)?}));
    }
    if path == "/placement" {
        return placement(&env, query.get("rooms").map_or("", |s| s.as_str())).await;
    }
    if let Some(rest) = path.strip_prefix("/rooms/") {
        let raw = rest.split('/').next().unwrap_or_default();
        let room: String = js_sys::decode_uri_component(raw)
            .map(String::from)
            .unwrap_or_else(|_| raw.to_string());
        if room.is_empty() {
            return Response::error("room required", 400);
        }
        let ns = env.durable_object("PIT_ROOM")?;
        let stub = ns.id_from_name(&room)?.get_stub()?;
        // Name the room for the object (its id has no name on some paths).
        let headers = req.headers().clone();
        headers.set("x-room", &room)?;
        let mut init = RequestInit::new();
        init.with_method(req.method()).with_headers(headers);
        let forwarded = Request::new_with_init(req.url()?.as_str(), &init)?;
        return stub.fetch_with_request(forwarded).await;
    }
    let assets = env.assets("ASSETS")?;
    assets.fetch_request(req).await
}
