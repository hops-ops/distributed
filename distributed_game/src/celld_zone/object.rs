//! The Durable Object side of the celld zone host (wasm32 only).

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use distributed::cell_host::{
    CELL_INTERNAL_SECRET_ENV, CELL_INTERNAL_SECRET_HEADER, CELL_PRINCIPAL_PARTITION_HEADER,
    CELL_SERVICE_ID_HEADER,
};
use distributed::USER_ID_KEY;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use worker::send::{SendFuture, SendWrapper};
use worker::{
    Delay, Env, Headers, Method, Request, RequestInit, Response, Result, State, Storage, WebSocket,
    WebSocketIncomingMessage, WebSocketPair,
};

use super::plan::{
    cell_binding, classify_wait_path, zone_from_path, ObjectOptions, RoomState, StoredFact,
    TickSchedule, Window, CLOSE_RESTARTED, NETSIM_SHARD, OUTBOX_PREFIX, ZONE_DEV_KIND, ZONE_HEADER,
};
use crate::command_sink::{CommandFactSink, FactBatchHost, FactCall, FactHost, FactRoute};
use crate::facts::{
    fact_channel, BoxFuture, Fact, FactBatchSink, FactError, FactQueue, FactReceipt, FactSink,
    RetryPolicy,
};
use crate::handoff::{HandoffRequest, Transfer};
use crate::inbox::ZoneInbound;
use crate::netsim::{DelayLine, NetSim, NetSimJson};
use crate::protocol::{error, ClientMessage, Notice, Role, ServerMessage, PROTOCOL_VERSION};
use crate::stamp::FactStamp;
use crate::ticket::{NonceCache, TicketKey, VerifiedTicket};
use crate::world::content_rollout::{zone_step, ContentState, ZoneStep, CONTENT_ROLLOUT_KIND};
use crate::world::watch::{
    next_reconcile_ms, topic, view_version, watch_query_string, watcher_address, TopicVersions,
    WatchUpdate, Watches, PUSH_ROUTE, WATCH_LEASE_MS,
};
use crate::world::zone_activity::{
    zone_principal, RecordActivity, ACTIVITY_ACTIVE, ACTIVITY_DORMANT, ACTIVITY_HIBERNATED,
    ZONE_ACTIVITY_KIND,
};
use crate::world::FreezeState;
use crate::zone::Viewer;
use crate::{ClientId, Clock, HostConfig, Outbound, TicketClaims, Zone, ZoneHost};

/// A future that runs on the object's (single) thread.
pub type LocalFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// The zone's fact sink on celld: commands to cells, object to object.
pub type ObjectFactSink = CommandFactSink<CelldObjectHost>;

const SNAPSHOT_KEY: &str = "zone:snapshot";
const STATUS_KEY: &str = "zone:status";
const NONCE_PREFIX: &str = "nonce:";
/// Transfer tickets (doorways) live this long.
const TRANSFER_TTL_SECS: u64 = 30;
/// Unsettled facts one activation may hold.
const FACT_CAPACITY: usize = 512;
/// Retry stored facts this often while any is unsettled (ms).
const OUTBOX_RETRY_MS: f64 = 10_000.0;
/// Shortest crash watchdog period (each re-arm is a durable alarm write).
const WATCHDOG_MIN_MS: f64 = 60_000.0;
/// Re-read watched cells this often when the object cannot take pushes
/// (no class binding: built with [`ZoneObject::new`] and no `ZONE_BINDING`).
const POLL_MS: f64 = 1_000.0;

/// Server → client, through the socket's simulated network.
enum OutItem {
    Frame(Vec<u8>),
    Close,
}

/// Client → server, through the socket's simulated network.
enum InItem {
    Input {
        seq: u32,
        client_tick: u32,
        payload: Vec<u8>,
    },
    Ping(f64),
}

/// A game zone the framework can host as a celld Durable Object: one object
/// per zone id (celld places it, forwards to it, hibernates and moves it).
pub trait CelldZone: Zone {
    /// The zone's construction parameters for `zone_id` (validate the id
    /// here: `Err` refuses the join).
    fn object_config(zone_id: &str, env: &ObjectEnv) -> std::result::Result<Self::Config, String>;

    /// Host tuning; the object sets the tick and patch rates, reconnect
    /// grace and boot epoch from its options. Default: [`HostConfig`]
    /// defaults.
    fn host_config(_zone_id: &str, _env: &ObjectEnv) -> HostConfig {
        HostConfig::default()
    }

    /// Fact routes of the zone (lifecycle facts too, if the host config
    /// emits them). The zone-activity route is added by the framework.
    fn fact_routes(sink: ObjectFactSink) -> ObjectFactSink {
        sink
    }

    /// Async work after a player joined (e.g. read the player's profile
    /// from its cell); the message it returns is delivered into the zone
    /// ([`Zone::on_message`]). Runs off the tick; the zone must not wait
    /// for it.
    fn after_join(
        _env: &ObjectEnv,
        _principal: &str,
    ) -> Option<LocalFuture<Option<Self::Message>>> {
        None
    }

    /// The world cells the zone watches ([`Watches`]): each is read on
    /// wake, pushed by its cell after every change and re-read every ~45 s;
    /// its view becomes a message ([`Zone::on_message`]). Eventually
    /// consistent: world flavour, never reward settlement.
    ///
    /// ```ignore
    /// fn watch(zone_id: &str) -> Watches<PitMessage> {
    ///     Watches::new().cell(HOOD_KIND, &hood_of(zone_id), |read: HoodRead| read.state.map(PitMessage::Hood))
    /// }
    /// ```
    fn watch(_zone_id: &str) -> Watches<Self::Message> {
        Watches::new()
    }

    /// How often [`refresh`](Self::refresh) runs while the zone ticks
    /// (`None`: never). It also runs once on every wake. For cell state,
    /// prefer [`watch`](Self::watch) (pushed, no polling).
    fn refresh_every() -> Option<Duration> {
        None
    }

    /// Async work that keeps the zone in step with the world around it —
    /// e.g. read the cells of the tiers it belongs to — off the tick; the
    /// message it returns is delivered into the zone ([`Zone::on_message`]).
    /// `round` counts the calls of this activation (0 on wake), so a zone
    /// can read slower tiers less often.
    fn refresh(
        _env: &ObjectEnv,
        _zone_id: &str,
        _round: u64,
    ) -> Option<LocalFuture<Option<Self::Message>>> {
        None
    }
}

// ------------------------------------------------------------------ env

/// The object's environment for game hooks: vars and cell reads.
#[derive(Clone)]
pub struct ObjectEnv {
    env: Env,
}

impl ObjectEnv {
    /// Wrap a worker env.
    pub fn new(env: Env) -> Self {
        Self { env }
    }

    /// A var or secret.
    pub fn var(&self, name: &str) -> Option<String> {
        self.env
            .var(name)
            .map(|v| v.to_string())
            .or_else(|_| self.env.secret(name).map(|v| v.to_string()))
            .ok()
    }

    /// The worker env.
    pub fn env(&self) -> &Env {
        &self.env
    }

    /// `GET /{kind}/{shard}` on a cell of this worker (its read route),
    /// object to object, with the internal secret.
    pub async fn read_cell(&self, kind: &str, shard: &str) -> std::result::Result<Value, String> {
        self.read_cell_watching(kind, shard, None).await
    }

    /// [`read_cell`](Self::read_cell) that also watches the cell for
    /// `watcher` ([`crate::world::watch`]): the cell pushes its next
    /// versions until the lease ends.
    pub async fn read_cell_watching(
        &self,
        kind: &str,
        shard: &str,
        watcher: Option<&str>,
    ) -> std::result::Result<Value, String> {
        let secret = self
            .var(CELL_INTERNAL_SECRET_ENV)
            .ok_or_else(|| format!("{CELL_INTERNAL_SECRET_ENV} is required"))?;
        let headers = Headers::new();
        headers
            .set(CELL_INTERNAL_SECRET_HEADER, &secret)
            .map_err(|e| e.to_string())?;
        let mut init = RequestInit::new();
        init.with_method(Method::Get).with_headers(headers);
        let mut url = format!("https://cell/{kind}/{}", encode_segment(shard));
        if let Some(watcher) = watcher {
            url.push('?');
            url.push_str(&watch_query_string(watcher, WATCH_LEASE_MS));
        }
        let request = Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
        let stub = stub_for(&self.env, kind, shard).map_err(|e| e.to_string())?;
        let mut response = stub
            .fetch_with_request(request)
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status_code();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if (200..300).contains(&status) {
            Ok(body)
        } else {
            Err(format!("HTTP {status}: {body}"))
        }
    }
}

/// The stub of cell `kind/shard` (binding [`cell_binding`]`(kind)`).
fn stub_for(env: &Env, kind: &str, shard: &str) -> Result<worker::Stub> {
    let namespace = env.durable_object(&cell_binding(kind))?;
    let id = namespace.id_from_name(shard)?;
    id.get_stub()
}

fn encode_segment(segment: &str) -> String {
    let mut encoded = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b':') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

// ------------------------------------------------------------------ fact host

/// [`FactHost`] over the cells of the same worker: the cell wait-path
/// request (`POST /{kind}/{shard}/{command}`, internal secret, principal,
/// `x-game-*` stamp) sent straight to the cell's Durable Object stub —
/// object to object, no HTTP hop through a node's public listener. The
/// binding of kind `k` is [`cell_binding`]`(k)`.
pub struct CelldObjectHost {
    env: SendWrapper<Env>,
    secret: String,
    service_id: String,
}

impl CelldObjectHost {
    /// A host for `env`'s cells under command-ledger `service_id`.
    pub fn new(env: Env, secret: impl Into<String>, service_id: impl Into<String>) -> Self {
        Self {
            env: SendWrapper::new(env),
            secret: secret.into(),
            service_id: service_id.into(),
        }
    }

    async fn post(&self, call: &FactCall<'_>) -> std::result::Result<FactReceipt, FactError> {
        let headers = Headers::new();
        let set = |name: &str, value: &str| {
            headers
                .set(name, value)
                .map_err(|e| FactError::Permanent(format!("header {name}: {e}")))
        };
        set(CELL_INTERNAL_SECRET_HEADER, &self.secret)?;
        set(CELL_SERVICE_ID_HEADER, &self.service_id)?;
        set(CELL_PRINCIPAL_PARTITION_HEADER, &call.principal)?;
        set(USER_ID_KEY, &call.principal)?;
        set("content-type", "application/json")?;
        for (name, value) in call.session.variables() {
            if name.starts_with(crate::stamp::SESSION_PREFIX) {
                set(name, value)?;
            }
        }
        let body = json!({ "commandId": call.command_id, "input": call.input });
        let (status, body) = self
            .send(call.kind, &call.shard, call.command, headers, body)
            .await?;
        classify_wait_path(status, &body)
    }

    /// `calls` (one aggregate) in one `POST /{kind}/{shard}/__batch`
    /// ([`super::cells::serve_cell`]); each entry carries its own command
    /// id, principal and stamp. A cell without the route gets them one by
    /// one.
    async fn post_batch(
        &self,
        calls: Vec<FactCall<'_>>,
    ) -> Vec<std::result::Result<FactReceipt, FactError>> {
        let Some(first) = calls.first() else {
            return Vec::new();
        };
        let (kind, shard) = (first.kind, first.shard.clone());
        let entries: Vec<Value> = calls
            .iter()
            .map(|call| {
                let mut session = serde_json::Map::new();
                session.insert(USER_ID_KEY.into(), json!(call.principal));
                for (name, value) in call.session.variables() {
                    if name.starts_with(crate::stamp::SESSION_PREFIX) {
                        session.insert(name.clone(), json!(value));
                    }
                }
                json!({
                    "command": call.command,
                    "commandId": call.command_id,
                    "input": call.input,
                    "partition": call.principal,
                    "session": session,
                })
            })
            .collect();
        let headers = Headers::new();
        let set = |name: &str, value: &str| headers.set(name, value).is_ok();
        if !(set(CELL_INTERNAL_SECRET_HEADER, &self.secret)
            && set(CELL_SERVICE_ID_HEADER, &self.service_id)
            && set("content-type", "application/json"))
        {
            return vec![Err(FactError::Permanent("batch headers".into()))];
        }
        let sent = self
            .send(
                kind,
                &shard,
                super::cells::BATCH_ROUTE,
                headers,
                json!({ "entries": entries }),
            )
            .await;
        match sent {
            Ok((404, _)) => {
                let mut results = Vec::with_capacity(calls.len());
                for call in &calls {
                    let result = self.post(call).await;
                    let stop = matches!(result, Err(FactError::Retryable(_)));
                    results.push(result);
                    if stop {
                        break;
                    }
                }
                results
            }
            Ok((status, body)) if (200..300).contains(&status) => body
                .get("results")
                .and_then(Value::as_array)
                .map(|results| {
                    results
                        .iter()
                        .map(|result| {
                            let status = result
                                .get("status")
                                .and_then(Value::as_u64)
                                .and_then(|status| u16::try_from(status).ok())
                                .unwrap_or(500);
                            classify_wait_path(status, result.get("body").unwrap_or(&Value::Null))
                        })
                        .collect()
                })
                .unwrap_or_default(),
            Ok((status, body)) => vec![classify_wait_path(status, &body)],
            Err(error) => vec![Err(error)],
        }
    }

    /// `POST https://cell/{kind}/{shard}/{route}` to the cell's stub:
    /// `(status, JSON body)`.
    async fn send(
        &self,
        kind: &str,
        shard: &str,
        route: &str,
        headers: Headers,
        body: Value,
    ) -> std::result::Result<(u16, Value), FactError> {
        let retry = |e: worker::Error| FactError::Retryable(e.to_string());
        let mut init = RequestInit::new();
        init.with_method(Method::Post)
            .with_headers(headers)
            .with_body(Some(body.to_string().into()));
        let url = format!(
            "https://cell/{}/{}/{}",
            encode_segment(kind),
            encode_segment(shard),
            encode_segment(route)
        );
        let request = Request::new_with_init(&url, &init).map_err(retry)?;
        let stub = stub_for(&self.env, kind, shard)
            .map_err(|e| FactError::Permanent(format!("cell `{kind}`: {e}")))?;
        let mut response = stub.fetch_with_request(request).await.map_err(retry)?;
        let status = response.status_code();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        Ok((status, body))
    }
}

impl FactHost for CelldObjectHost {
    fn dispatch_fact<'a>(
        &'a self,
        call: FactCall<'a>,
    ) -> BoxFuture<'a, std::result::Result<FactReceipt, FactError>> {
        Box::pin(SendFuture::new(async move { self.post(&call).await }))
    }

    /// One `__batch` request per cell (the feed's `ZONE_FACT_BATCH` bounds
    /// it).
    fn fact_batches(&self) -> Option<&dyn FactBatchHost> {
        Some(self)
    }
}

impl FactBatchHost for CelldObjectHost {
    fn dispatch_fact_batch<'a>(
        &'a self,
        calls: Vec<FactCall<'a>>,
    ) -> BoxFuture<'a, Vec<std::result::Result<FactReceipt, FactError>>> {
        Box::pin(SendFuture::new(async move {
            if calls.len() == 1 {
                let call = calls.into_iter().next().expect("one call");
                return vec![self.post(&call).await];
            }
            self.post_batch(calls).await
        }))
    }
}

/// The durable fact outbox around the sink: every emitted fact is written
/// to object storage right after the call that emitted it (from the fact
/// queues' journals, [`FactQueue::with_journal`]) and deleted here once it
/// settled (committed or permanently refused). The next activation (or the
/// outbox alarm) resends what is left; the cells' command ledger makes a
/// resend a replay.
#[derive(Clone)]
struct DurableFacts {
    sink: ObjectFactSink,
    storage: SendWrapper<Rc<Storage>>,
}

impl FactSink for DurableFacts {
    fn commit<'a>(
        &'a self,
        fact: &'a Fact,
    ) -> BoxFuture<'a, std::result::Result<FactReceipt, FactError>> {
        Box::pin(SendFuture::new(async move {
            let result = self.sink.commit(fact).await;
            match &result {
                Err(FactError::Permanent(e)) => {
                    worker::console_error!("fact {} refused: {e}", fact.key())
                }
                Err(FactError::Retryable(e)) => {
                    worker::console_warn!("fact {} will be retried: {e}", fact.key())
                }
                Ok(_) => {}
            }
            if !matches!(result, Err(FactError::Retryable(_))) {
                let _ = self
                    .storage
                    .delete(&StoredFact::storage_key(fact.key()))
                    .await;
            }
            result
        }))
    }

    /// Batches when the cells take them.
    fn batches(&self) -> Option<&dyn FactBatchSink> {
        self.sink.batches().map(|_| self as &dyn FactBatchSink)
    }
}

impl FactBatchSink for DurableFacts {
    /// One batched delete for the facts the batch settled.
    fn commit_batch<'a>(
        &'a self,
        facts: &'a [Fact],
    ) -> BoxFuture<'a, Vec<std::result::Result<FactReceipt, FactError>>> {
        Box::pin(SendFuture::new(async move {
            let results = self.sink.commit_batch(facts).await;
            let mut settled = Vec::new();
            for (fact, result) in facts.iter().zip(&results) {
                match result {
                    Err(FactError::Permanent(e)) => {
                        worker::console_error!("fact {} refused: {e}", fact.key())
                    }
                    Err(FactError::Retryable(e)) => {
                        worker::console_warn!("fact {} will be retried: {e}", fact.key());
                        continue;
                    }
                    Ok(_) => {}
                }
                settled.push(StoredFact::storage_key(fact.key()));
            }
            if !settled.is_empty() {
                let _ = self.storage.delete_multiple(settled).await;
            }
            results
        }))
    }
}

/// Facts per cell batch from the zone's options.
fn fact_batch(options: &Option<ObjectOptions>) -> usize {
    options
        .as_ref()
        .map_or(crate::facts::DEFAULT_MAX_BATCH, |options| {
            options.fact_batch
        })
}

// ------------------------------------------------------------------ time

fn now_ms() -> f64 {
    js_sys::Date::now()
}

/// Monotonic host clock over `Date.now()` (`Instant` panics on
/// wasm32-unknown-unknown).
struct JsClock {
    origin_ms: f64,
    last_us: AtomicU64,
}

impl JsClock {
    fn new() -> Self {
        Self {
            origin_ms: now_ms(),
            last_us: AtomicU64::new(0),
        }
    }
}

impl Clock for JsClock {
    fn now(&self) -> Duration {
        let us = ((now_ms() - self.origin_ms).max(0.0) * 1000.0) as u64;
        Duration::from_micros(self.last_us.fetch_max(us, Ordering::Relaxed).max(us))
    }

    fn unix_ms(&self) -> u64 {
        self.origin_ms as u64 + self.now().as_millis() as u64
    }
}

/// `n` random bytes as hex, from `crypto.getRandomValues`.
fn random_hex(n: usize) -> String {
    let bytes = js_sys::Uint8Array::new_with_length(n as u32);
    let crypto = js_sys::Reflect::get(&js_sys::global(), &"crypto".into()).ok();
    let filled = crypto.and_then(|crypto| {
        let get = js_sys::Reflect::get(&crypto, &"getRandomValues".into()).ok()?;
        let get: js_sys::Function = get.into();
        get.call1(&crypto, &bytes).ok()
    });
    if filled.is_none() {
        for i in 0..n {
            bytes.set_index(i as u32, (js_sys::Math::random() * 256.0) as u8);
        }
    }
    bytes.to_vec().iter().map(|b| format!("{b:02x}")).collect()
}

async fn yield_now() {
    let _ = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(
        &wasm_bindgen::JsValue::NULL,
    ))
    .await;
}

thread_local! {
    /// One id per V8 isolate (module instance), for the debug info.
    static ISOLATE: String = random_hex(4);
}

// ------------------------------------------------------------------ object

#[derive(Serialize, Deserialize)]
struct Attachment {
    client: u32,
    activation: String,
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: String,
    state: String,
}

#[derive(Default)]
struct TickStats {
    late: Window,
    work: Window,
    dropped: u64,
    iterations: u64,
}

struct Inner<Z: CelldZone> {
    zone_id: String,
    activation: String,
    activation_epoch: u64,
    options: Option<ObjectOptions>,
    key: Option<TicketKey>,
    host: Option<ZoneHost<Z>>,
    restored: bool,
    wakes: u64,
    state: RoomState,
    sockets: HashMap<u32, WebSocket>,
    /// Observers whose ticket grants `wake` ([`TicketClaims::wake`]).
    may_wake: HashSet<u32>,
    /// Of those, the ones near right now: they keep the zone awake.
    near: HashSet<u32>,
    ticking: bool,
    nonces: NonceCache,
    sink: Option<DurableFacts>,
    activity: Option<FactQueue>,
    activity_seq: u64,
    outbox_loaded: bool,
    last_status: String,
    node: String,
    stats: TickStats,
    last_persist_ms: f64,
    /// A snapshot write is in flight (see [`ZoneObject::persist_later`]).
    persisting: bool,
    /// Players at the last tick; a drop (leave, handoff, grace expired)
    /// snapshots on that tick, not at the next interval.
    players_seen: usize,
    /// When the ticking room last became empty (it goes dormant after
    /// `dormant_after` of that).
    empty_since_ms: Option<f64>,
    /// A player left since the last snapshot.
    persist_soon: bool,
    spooled: u64,
    /// Dev network simulation in effect (from the dev settings object).
    net_sim: Option<NetSim>,
    /// Per socket: server → client and client → server delay lines.
    out_lines: HashMap<u32, DelayLine<OutItem>>,
    in_lines: HashMap<u32, DelayLine<InItem>>,
    /// When the pending drain task wakes up (one per earliest deadline).
    drain_at: Option<f64>,
    /// Spawns a drain task waking at the given time (set by the object).
    drainer: Option<Rc<dyn Fn(f64)>>,
    /// Watched world cells ([`CelldZone::watch`] plus the dev settings and
    /// content rollout), built on the first wake.
    watches: Option<Rc<Watches<Z::Message>>>,
    /// Highest version applied per watched topic (this activation).
    world_versions: TopicVersions,
    /// When each watched topic is read next (ms).
    world_due: HashMap<String, f64>,
    /// [`CelldZone::refresh`] calls of this activation, and the last one.
    refresh_round: u64,
    last_refresh_ms: f64,
}

impl<Z: CelldZone> Inner<Z> {
    /// Send `item` to `client` through its simulated network.
    fn send_out(&mut self, client: u32, item: OutItem) {
        let sim = self.net_sim;
        let line = self
            .out_lines
            .entry(client)
            .or_insert_with(|| DelayLine::new(u64::from(client) ^ 0x5eed_0u64));
        match line.push(sim, now_ms(), item) {
            Some(item) => self.deliver(client, item),
            None => {
                if let Some(due) = line.next_due() {
                    self.want_drain(due);
                }
            }
        }
    }

    fn deliver(&mut self, client: u32, item: OutItem) {
        match item {
            OutItem::Frame(frame) => {
                if let Some(ws) = self.sockets.get(&client) {
                    let _ = ws.send_with_bytes(frame);
                }
            }
            OutItem::Close => {
                self.forget(client);
                if let Some(ws) = self.sockets.remove(&client) {
                    let _ = ws.close(Some(1000), Some("closed by zone"));
                }
            }
        }
    }

    /// Take `item` from `client` through its simulated network.
    fn receive(&mut self, client: u32, item: InItem) {
        let sim = self.net_sim;
        let line = self
            .in_lines
            .entry(client)
            .or_insert_with(|| DelayLine::new(u64::from(client) ^ 0xfeed_0u64));
        match line.push(sim, now_ms(), item) {
            Some(item) => self.apply(client, item),
            None => {
                if let Some(due) = line.next_due() {
                    self.want_drain(due);
                }
            }
        }
    }

    fn apply(&mut self, client: u32, item: InItem) {
        match item {
            InItem::Input {
                seq,
                client_tick,
                payload,
            } => {
                if let Some(host) = self.host.as_mut() {
                    host.push_input(ClientId(client), seq, client_tick, &payload);
                }
            }
            InItem::Ping(client_time) => {
                let server_time = self.host.as_ref().map_or(0.0, |h| h.server_time_ms());
                if let Ok(pong) = (ServerMessage::Pong {
                    client_time,
                    server_time,
                })
                .encode(&())
                {
                    self.send_out(client, OutItem::Frame(pong));
                }
            }
        }
    }

    /// Deliver and apply everything due at `now`.
    fn drain_due(&mut self, now: f64) {
        let clients: Vec<u32> = self.in_lines.keys().copied().collect();
        for client in clients {
            while let Some(item) = self.in_lines.get_mut(&client).and_then(|l| l.pop_due(now)) {
                self.apply(client, item);
            }
        }
        let clients: Vec<u32> = self.out_lines.keys().copied().collect();
        for client in clients {
            while let Some(item) = self.out_lines.get_mut(&client).and_then(|l| l.pop_due(now)) {
                self.deliver(client, item);
            }
        }
    }

    fn next_due(&self) -> Option<f64> {
        self.in_lines
            .values()
            .filter_map(DelayLine::next_due)
            .chain(self.out_lines.values().filter_map(DelayLine::next_due))
            .min_by(f64::total_cmp)
    }

    /// Make sure a drain task wakes up by `due`.
    fn want_drain(&mut self, due: f64) {
        if self.drain_at.is_some_and(|at| at <= due) {
            return;
        }
        self.drain_at = Some(due);
        if let Some(drainer) = self.drainer.clone() {
            drainer(due);
        }
    }

    /// Forget a socket's delay lines and nearness.
    fn forget(&mut self, client: u32) {
        self.out_lines.remove(&client);
        self.in_lines.remove(&client);
        self.may_wake.remove(&client);
        self.near.remove(&client);
    }

    /// Players keep a zone awake (also inside their reconnect grace), and
    /// so do near observers; other observers are passive watchers.
    fn occupied(&self) -> bool {
        self.host.as_ref().is_some_and(|h| {
            h.player_count() > 0 || self.near.iter().any(|c| self.sockets.contains_key(c))
        })
    }

    /// An observer declares itself near (or not). Only an observer whose
    /// ticket grants `wake` can be near (others are told `WAKE_DENIED`);
    /// returns whether the zone must start ticking (it was not and is
    /// occupied now).
    fn set_near(&mut self, client: u32, near: bool) -> bool {
        if !self.sockets.contains_key(&client) {
            return false;
        }
        if near && self.may_wake.contains(&client) {
            self.near.insert(client);
        } else {
            self.near.remove(&client);
            if near {
                if let Ok(frame) = (ServerMessage::Error {
                    code: error::WAKE_DENIED,
                    message: "ticket does not grant wake".into(),
                })
                .encode(&())
                {
                    self.send_out(client, OutItem::Frame(frame));
                }
            }
        }
        self.occupied() && !self.ticking
    }

    fn flush(&mut self) {
        let Some(host) = self.host.as_mut() else {
            return;
        };
        let outbound = host.drain_outbound();
        for out in outbound {
            match out {
                Outbound::Frame { client, frame } => {
                    self.send_out(client.0, OutItem::Frame(frame));
                }
                Outbound::Close { client } => self.send_out(client.0, OutItem::Close),
            }
        }
    }

    /// Facts emitted since the last call (zone and activity queues).
    fn emitted(&mut self) -> Vec<Fact> {
        let mut facts = self
            .host
            .as_mut()
            .map(ZoneHost::take_emitted_facts)
            .unwrap_or_default();
        if let Some(queue) = self.activity.as_mut() {
            facts.extend(queue.take_journal());
        }
        facts
    }

    fn snapshot(&self) -> Option<Snapshot> {
        let host = self.host.as_ref()?;
        let state = host.hibernate_state()?;
        use base64::Engine as _;
        Some(Snapshot {
            version: host.config().content_version.clone(),
            state: base64::engine::general_purpose::STANDARD.encode(state),
        })
    }

    /// Queue an activity report (a zone fact into its zone-activity cell).
    fn report(&mut self, status: &str) {
        if let Some(queue) = self.activity.as_mut() {
            queue.drain();
        }
        let Some(queue) = self.activity.as_mut() else {
            return;
        };
        self.activity_seq += 1;
        let input = RecordActivity {
            status: status.to_string(),
            activation: self.activation.clone(),
            restored: self.restored,
            players: self.host.as_ref().map_or(0, |h| h.player_count() as u32),
            node: self.node.clone(),
            isolate: ISOLATE.with(|i| i.clone()),
        };
        let stamp = FactStamp {
            zone_id: self.zone_id.clone(),
            tick: self.host.as_ref().map_or(0, |h| h.tick()),
            emitted_at_ms: now_ms() as u64,
            activation: self.activation_epoch,
            seq: self.activity_seq,
            ..FactStamp::default()
        };
        let key = format!(
            "zone_activity.record:{}:{}:{}",
            self.zone_id, self.activation_epoch, self.activity_seq
        );
        match serde_json::to_value(&input) {
            Ok(value) => {
                let fact = Fact::trusted(
                    key,
                    crate::world::zone_activity::RECORD,
                    value,
                    Some(zone_principal(&self.zone_id)),
                    stamp,
                );
                if let Err(e) = queue.try_send(fact) {
                    worker::console_error!("zone {}: activity report dropped: {e:?}", self.zone_id);
                }
            }
            Err(e) => worker::console_error!("zone activity encode: {e}"),
        }
        self.last_status = status.to_string();
    }
}

/// Hosts one [`CelldZone`] as a celld Durable Object. The game's
/// `#[durable_object]` delegates to it (see [`crate::celld_zone_object!`]).
///
/// Routes (on the object, via [`forward_to_zone`]): a WebSocket upgrade
/// joins the zone (the `distributed_game` wire protocol: Hello with a
/// ticket, a transfer ticket or a reconnect token); `GET …/info` returns
/// debug JSON (activation, node, isolate, tick timing) without waking a
/// hibernated zone.
pub struct ZoneObject<Z: CelldZone> {
    state: Rc<State>,
    env: ObjectEnv,
    inner: Rc<RefCell<Inner<Z>>>,
    /// Namespace binding of the object's class (`PIT_ROOM`): where watched
    /// cells push. Empty: no pushes, watched cells are polled.
    binding: Rc<str>,
}

impl<Z: CelldZone> ZoneObject<Z> {
    /// The object of Durable Object class `class` (bound, by convention, as
    /// [`class_binding`](crate::world::watch::class_binding)`(class)`, e.g.
    /// `PitRoom` → `PIT_ROOM`); [`crate::celld_zone_object!`] calls it.
    pub fn of_class(state: State, env: Env, class: &str) -> Self {
        let mut object = Self::new(state, env);
        object.binding = crate::world::watch::class_binding(class).into();
        object
    }

    /// The object for `state` (constructor of the Durable Object). Watched
    /// cells push to it only if `ZONE_BINDING` names its class binding;
    /// prefer [`of_class`](Self::of_class).
    pub fn new(state: State, env: Env) -> Self {
        let zone_id = state.id().name().unwrap_or_default();
        let env = ObjectEnv::new(env);
        let options = ObjectOptions::from_vars(|name| env.var(name));
        if let Err(e) = &options {
            worker::console_error!("celld zone: {e}");
        }
        let options = options.ok();
        let key = options
            .as_ref()
            .map(|o| TicketKey::new(o.ticket_secret.clone().into_bytes()));
        let node = options
            .as_ref()
            .and_then(|o| o.node.clone())
            .unwrap_or_default();
        let inner = Inner {
            zone_id,
            activation: random_hex(6),
            activation_epoch: (now_ms() * 1000.0) as u64,
            options,
            key,
            host: None,
            restored: false,
            wakes: 0,
            state: RoomState::Cold,
            sockets: HashMap::new(),
            may_wake: HashSet::new(),
            near: HashSet::new(),
            ticking: false,
            nonces: NonceCache::default(),
            sink: None,
            activity: None,
            activity_seq: 0,
            outbox_loaded: false,
            last_status: String::new(),
            node,
            stats: TickStats::default(),
            last_persist_ms: 0.0,
            persisting: false,
            players_seen: 0,
            empty_since_ms: None,
            persist_soon: false,
            spooled: 0,
            net_sim: None,
            out_lines: HashMap::new(),
            in_lines: HashMap::new(),
            drain_at: None,
            drainer: None,
            watches: None,
            world_versions: TopicVersions::new(),
            world_due: HashMap::new(),
            refresh_round: 0,
            last_refresh_ms: 0.0,
        };
        let inner = Rc::new(RefCell::new(inner));
        let weak = Rc::downgrade(&inner);
        inner.borrow_mut().drainer = Some(Rc::new(move |due: f64| {
            wasm_bindgen_futures::spawn_local(drain_task(weak.clone(), due));
        }));
        let binding = env.var("ZONE_BINDING").unwrap_or_default().into();
        Self {
            state: Rc::new(state),
            env,
            inner,
            binding,
        }
    }

    /// The game hook environment.
    pub fn env(&self) -> &ObjectEnv {
        &self.env
    }

    /// Durable Object `fetch`.
    pub async fn fetch(&self, req: Request) -> Result<Response> {
        if let Some(zone) = req.headers().get(ZONE_HEADER)? {
            let mut inner = self.inner.borrow_mut();
            if inner.zone_id.is_empty() {
                inner.zone_id = zone;
            }
        }
        if self.inner.borrow().zone_id.is_empty() {
            return Response::error("zone id required", 400);
        }
        let path = req.url()?.path().to_string();
        if req.method() == Method::Post && path.ends_with(&format!("/{PUSH_ROUTE}")) {
            return self.take_push(req).await;
        }
        if path.ends_with("/info") {
            // Debug JSON without secrets; pages on other origins may read it.
            let response = Response::from_json(&self.info())?;
            response.headers().set("access-control-allow-origin", "*")?;
            return Ok(response);
        }
        if req.headers().get("upgrade")?.as_deref() != Some("websocket") {
            return Response::error("expected a WebSocket upgrade", 426);
        }
        if self.inner.borrow().options.is_none() {
            return Response::error("zone host is not configured", 503);
        }
        // Not woken here: only a player's Hello wakes a sleeping zone.
        let pair = WebSocketPair::new()?;
        self.state.accept_web_socket(&pair.server);
        Response::from_websocket(pair.client)
    }

    /// Durable Object `websocket_message`.
    pub async fn websocket_message(
        &self,
        ws: WebSocket,
        message: WebSocketIncomingMessage,
    ) -> Result<()> {
        let WebSocketIncomingMessage::Binary(bytes) = message else {
            let _ = ws.close(Some(1003), Some("binary frames only"));
            return Ok(());
        };
        let attached: Option<Attachment> = ws.deserialize_attachment().ok().flatten();
        let activation = self.inner.borrow().activation.clone();
        let client = match attached {
            Some(a) if a.activation == activation => Some(a.client),
            // The socket outlived the activation that admitted it.
            Some(_) => {
                let _ = ws.close(Some(CLOSE_RESTARTED), Some("zone restarted; rejoin"));
                return Ok(());
            }
            None => None,
        };
        let msg = match ClientMessage::decode(&bytes) {
            Ok(m) => m,
            Err(_) => {
                reject(&ws, error::BAD_MESSAGE, "bad message");
                return Ok(());
            }
        };
        match (client, msg) {
            (None, ClientMessage::Hello(hello)) => self.hello(&ws, hello).await?,
            (
                Some(client),
                ClientMessage::Input {
                    seq,
                    client_tick,
                    payload,
                },
            ) => {
                let mut inner = self.inner.borrow_mut();
                if inner.host.is_some() {
                    inner.receive(
                        client,
                        InItem::Input {
                            seq,
                            client_tick,
                            payload,
                        },
                    );
                } else {
                    drop(inner);
                    let _ = ws.close(Some(CLOSE_RESTARTED), Some("zone asleep; rejoin"));
                }
            }
            (Some(client), ClientMessage::Ping { client_time }) => {
                self.inner
                    .borrow_mut()
                    .receive(client, InItem::Ping(client_time));
            }
            (None, ClientMessage::Ping { client_time }) => {
                let server_time = self
                    .inner
                    .borrow()
                    .host
                    .as_ref()
                    .map_or(0.0, |h| h.server_time_ms());
                if let Ok(pong) = (ServerMessage::Pong {
                    client_time,
                    server_time,
                })
                .encode(&())
                {
                    let _ = ws.send_with_bytes(pong);
                }
            }
            (Some(client), ClientMessage::Near { near }) => {
                let start = self.inner.borrow_mut().set_near(client, near);
                if start {
                    self.keep_awake().await;
                }
            }
            (Some(client), ClientMessage::Focus { x, y }) => {
                let mut inner = self.inner.borrow_mut();
                if let Some(host) = inner.host.as_mut() {
                    host.observer_focus(ClientId(client), x, y);
                }
            }
            (Some(client), ClientMessage::Content { content_version }) => {
                let mut inner = self.inner.borrow_mut();
                if let Some(host) = inner.host.as_mut() {
                    host.content_patched(ClientId(client), &content_version);
                }
            }
            (Some(client), ClientMessage::Leave) => {
                {
                    let mut inner = self.inner.borrow_mut();
                    inner.sockets.remove(&client);
                    inner.forget(client);
                    if let Some(host) = inner.host.as_mut() {
                        host.disconnect(ClientId(client), true);
                    }
                    inner.flush();
                }
                self.spool().await;
                let _ = ws.close(Some(1000), Some("left"));
            }
            _ => reject(&ws, error::BAD_MESSAGE, "unexpected message"),
        }
        Ok(())
    }

    /// Durable Object `websocket_close`.
    pub async fn websocket_close(
        &self,
        ws: WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> Result<()> {
        self.drop_socket(&ws);
        Ok(())
    }

    /// Durable Object `websocket_error`.
    pub async fn websocket_error(&self, ws: WebSocket, _error: worker::Error) -> Result<()> {
        self.drop_socket(&ws);
        Ok(())
    }

    /// Durable Object `alarm`: hibernate a room that stayed empty, resend
    /// unsettled facts, and after a crash mark the room asleep.
    pub async fn alarm(&self) -> Result<Response> {
        if self.inner.borrow().options.is_none() {
            return Response::ok("unconfigured");
        }
        self.load_zone_id().await;
        self.start_facts().await;
        let (cold, due, hibernate_after) = {
            let inner = self.inner.borrow();
            let after = inner
                .options
                .as_ref()
                .map_or(Duration::from_secs(10), |o| o.hibernate_after);
            (
                inner.host.is_none(),
                !inner.occupied() && inner.state.hibernate_due(now_ms(), after),
                after,
            )
        };
        if due {
            self.hibernate().await;
        } else if cold {
            // A fresh activation woken by its alarm: the last one crashed or
            // was evicted while awake. Its snapshot is what it left.
            let stored: Option<String> = self.state.storage().get(STATUS_KEY).await.ok().flatten();
            if stored.as_deref().is_some_and(|s| s != ACTIVITY_HIBERNATED) {
                self.inner.borrow_mut().report(ACTIVITY_HIBERNATED);
                self.store_status().await;
            }
        } else {
            let since = match self.inner.borrow().state {
                RoomState::Dormant { since_ms } => Some(since_ms),
                _ => None,
            };
            if let Some(since) = since {
                let wait = since + hibernate_after.as_millis() as f64 - now_ms();
                self.arm(wait.max(10.0)).await;
            } else if let Some(watchdog) = self.watchdog_ms() {
                // Still ticking: keep the crash watchdog (see `tick_loop`).
                self.arm(watchdog).await;
            }
        }
        self.arm_outbox().await;
        Response::ok("ok")
    }

    // -------------------------------------------------------------- internals

    /// Sockets accepted before an eviction reach a new instance without a
    /// zone id from a request; the id is kept in storage.
    async fn load_zone_id(&self) {
        if !self.inner.borrow().zone_id.is_empty() {
            return;
        }
        if let Ok(Some(zone)) = self.state.storage().get::<String>("zone:id").await {
            let mut inner = self.inner.borrow_mut();
            if inner.zone_id.is_empty() {
                inner.zone_id = zone;
            }
        }
    }

    /// Start the fact pipeline once per activation and resend what the
    /// previous activation left in the outbox.
    async fn start_facts(&self) {
        if self.inner.borrow().sink.is_some() {
            return;
        }
        let (zone_id, service, secret) = {
            let inner = self.inner.borrow();
            let Some(options) = inner.options.as_ref() else {
                return;
            };
            (
                inner.zone_id.clone(),
                options.fact_service.clone(),
                options.internal_secret.clone(),
            )
        };
        let Some(secret) = secret else {
            worker::console_error!(
                "zone {zone_id}: {CELL_INTERNAL_SECRET_ENV} is not set; facts are disabled"
            );
            return;
        };
        let host = CelldObjectHost::new(self.env.env().clone(), secret, &service);
        let sink = match ObjectFactSink::new(host, service, zone_principal(&zone_id)) {
            Ok(sink) => {
                Z::fact_routes(sink.route(FactRoute::by_zone::<RecordActivity>(ZONE_ACTIVITY_KIND)))
            }
            Err(e) => {
                worker::console_error!("zone {zone_id}: fact sink: {e}");
                return;
            }
        };
        let durable = DurableFacts {
            sink,
            storage: SendWrapper::new(Rc::new(self.state.storage())),
        };
        let (queue, feed) = fact_channel(FACT_CAPACITY, retry_policy());
        let queue = queue.with_journal();
        let batch = fact_batch(&self.inner.borrow().options);
        wasm_bindgen_futures::spawn_local(
            feed.with_max_concurrent_commits(4)
                .with_max_batch(batch)
                .run(durable.clone(), sleep),
        );
        {
            let mut inner = self.inner.borrow_mut();
            if inner.sink.is_some() {
                return;
            }
            inner.sink = Some(durable);
            inner.activity = Some(queue);
        }
        let _ = self.state.storage().put("zone:id", &zone_id).await;
        // Resend the previous activations' unsettled facts.
        let stored = self
            .state
            .storage()
            .list_with_options(worker::ListOptions::new().prefix(OUTBOX_PREFIX))
            .await;
        let mut inner = self.inner.borrow_mut();
        inner.outbox_loaded = true;
        if let Ok(map) = stored {
            let Some(queue) = inner.activity.as_mut() else {
                return;
            };
            for value in map.values().into_iter().flatten() {
                let Some(stored) = value
                    .as_string()
                    .and_then(|text| serde_json::from_str::<StoredFact>(&text).ok())
                else {
                    continue;
                };
                if let Some(fact) = stored.into_fact() {
                    let _ = queue.try_send(fact);
                }
            }
            // Already stored.
            queue.take_journal();
        }
    }

    /// Make sure the zone is in memory (restoring its snapshot).
    async fn wake(&self) -> Result<()> {
        self.start_facts().await;
        if self.inner.borrow().host.is_some() {
            return Ok(());
        }
        let stored: Option<Snapshot> = self.state.storage().get(SNAPSHOT_KEY).await.ok().flatten();
        let host = {
            let inner = self.inner.borrow();
            if inner.host.is_some() {
                return Ok(());
            }
            let options = inner
                .options
                .as_ref()
                .ok_or_else(|| worker::Error::RustError("zone host is not configured".into()))?;
            let zone_id = inner.zone_id.clone();
            let config = Z::object_config(&zone_id, &self.env).map_err(worker::Error::RustError)?;
            let mut host_config = Z::host_config(&zone_id, &self.env);
            host_config.tick_rate = options.tick_hz;
            host_config.patch_rate = options.patch_hz;
            // The object stops ticking when empty instead of idling.
            host_config.idle_tick_rate = None;
            host_config.reconnect_grace = options.reconnect_grace;
            host_config.boot_epoch = (now_ms() * 1000.0) as u64;
            // The content version the zone slept on (a rollout it missed is
            // caught up by its first content rollout read).
            if let Some(snapshot) = &stored {
                host_config.content_version = snapshot.version.clone();
            }
            let key = inner.key.clone().expect("ticket key with options");
            let issuer_zone = zone_id.clone();
            let generation = inner.activation_epoch;
            let mut host =
                ZoneHost::<Z>::new(zone_id, host_config, Arc::new(JsClock::new()), config)
                    .with_token_source(|| random_hex(24))
                    .with_transfer_issuer(move |request: &HandoffRequest| {
                        let expires_at = (now_ms() / 1000.0) as u64 + TRANSFER_TTL_SECS;
                        Some(key.sign_transfer(&Transfer {
                            claims: TicketClaims {
                                principal: request.principal.clone(),
                                zone: request.target_zone.clone(),
                                expires_at,
                                nonce: random_hex(16),
                                role: Role::Player,
                                name: request.name.clone(),
                                extensions: request.extensions.clone(),
                                wake: false,
                            },
                            source_zone: request.source_zone.clone(),
                            issuer: issuer_zone.clone(),
                            generation,
                            carried: request.carried.clone(),
                            departed_at_ms: request.departed_at_ms,
                        }))
                    });
            let (queue, feed) = fact_channel(FACT_CAPACITY, retry_policy());
            if let Some(sink) = inner.sink.clone() {
                wasm_bindgen_futures::spawn_local(
                    feed.with_max_concurrent_commits(4)
                        .with_max_batch(fact_batch(&inner.options))
                        .run(sink, sleep),
                );
                host = host.with_facts(queue.with_journal());
            }
            let restored = match &stored {
                Some(snapshot) => {
                    use base64::Engine as _;
                    match base64::engine::general_purpose::STANDARD.decode(&snapshot.state) {
                        Ok(state) => {
                            host.restore(&state, &snapshot.version);
                            true
                        }
                        Err(_) => false,
                    }
                }
                None => false,
            };
            (host, restored)
        };
        let (host, restored) = host;
        {
            let mut inner = self.inner.borrow_mut();
            if inner.host.is_some() {
                return Ok(());
            }
            inner.host = Some(host);
            inner.restored = restored;
            inner.wakes += 1;
            inner.refresh_round = 0;
            // Each wake is a new activation: sockets of an earlier one are
            // told to rejoin (1012).
            if inner.wakes > 1 {
                inner.activation = random_hex(6);
            }
            inner.state = RoomState::Active;
            inner.last_persist_ms = now_ms();
            inner.report(ACTIVITY_ACTIVE);
        }
        self.store_status().await;
        self.find_node();
        // Hydrate every watched cell, off the join path: a wake (often a
        // doorway into a sleeping zone, with the player's inputs waiting)
        // does not wait for them. The snapshot restored the zone's last
        // content version; the rollout read catches up with one it slept
        // through (a Hello on another version waits for its own read, see
        // `hello`).
        self.inner.borrow_mut().world_versions.clear();
        self.reconcile_world(true);
        self.refresh();
        Ok(())
    }

    /// Run [`CelldZone::refresh`] off the tick and deliver its message.
    fn refresh(&self) {
        let (zone_id, round) = {
            let mut inner = self.inner.borrow_mut();
            inner.last_refresh_ms = now_ms();
            let round = inner.refresh_round;
            inner.refresh_round += 1;
            (inner.zone_id.clone(), round)
        };
        let Some(work) = Z::refresh(&self.env, &zone_id, round) else {
            return;
        };
        let inner = self.inner.clone();
        wasm_bindgen_futures::spawn_local(async move {
            if let Some(message) = work.await {
                let mut inner = inner.borrow_mut();
                if let Some(host) = inner.host.as_mut() {
                    host.deliver(ZoneInbound::Message(message));
                    inner.flush();
                }
            }
        });
    }

    /// [`refresh`](Self::refresh) when [`CelldZone::refresh_every`] is due.
    fn refresh_if_due(&self) {
        let Some(every) = Z::refresh_every() else {
            return;
        };
        let due = now_ms() - self.inner.borrow().last_refresh_ms >= every.as_millis() as f64;
        if due {
            self.refresh();
        }
    }

    /// The world whose content rollout the zone follows, if any.
    fn content_world(&self) -> Option<String> {
        let inner = self.inner.borrow();
        inner
            .options
            .as_ref()
            .and_then(|o| o.content_rollout.clone())
    }

    /// Every watched topic: the dev settings (`ZONE_DEV_SETTINGS=1`), the
    /// content rollout (`ZONE_CONTENT_ROLLOUT`) and the game's
    /// [`CelldZone::watch`].
    fn world_topics(&self) -> Vec<String> {
        let (dev, zone_id) = {
            let inner = self.inner.borrow();
            let dev = inner.options.as_ref().is_some_and(|o| o.dev_settings);
            (dev, inner.zone_id.clone())
        };
        let watches = self
            .inner
            .borrow_mut()
            .watches
            .get_or_insert_with(|| Rc::new(Z::watch(&zone_id)))
            .clone();
        let mut topics = Vec::with_capacity(watches.len() + 2);
        if dev {
            topics.push(topic(ZONE_DEV_KIND, NETSIM_SHARD));
        }
        if let Some(world) = self.content_world() {
            topics.push(topic(CONTENT_ROLLOUT_KIND, &world));
        }
        topics.extend(watches.iter().map(|w| w.topic()));
        topics
    }

    /// Read the watched topics that are due (all of them with `all`, on
    /// wake) off the tick. With a class binding each read renews the
    /// zone's watch and the next comes ~45 s later (pushes carry the
    /// changes); without one the cells are polled every [`POLL_MS`].
    fn reconcile_world(&self, all: bool) {
        let now = now_ms();
        let pushed = !self.binding.is_empty();
        for topic in self.world_topics() {
            let due = {
                let mut inner = self.inner.borrow_mut();
                let next = inner.world_due.get(&topic).copied().unwrap_or(0.0);
                if !all && now < next {
                    continue;
                }
                let after = if pushed {
                    next_reconcile_ms(now as u64, js_sys::Math::random()) as f64
                } else {
                    now + POLL_MS
                };
                inner.world_due.insert(topic.clone(), after);
                true
            };
            if due {
                let this = self.handle();
                wasm_bindgen_futures::spawn_local(async move { this.reconcile(&topic).await });
            }
        }
    }

    /// Read watched `topic` (`kind/shard`), watching it when the object
    /// can take pushes, and apply its view.
    async fn reconcile(&self, topic: &str) {
        let Some((kind, shard)) = topic.split_once('/') else {
            return;
        };
        let watcher = (!self.binding.is_empty()).then(|| {
            let zone_id = self.inner.borrow().zone_id.clone();
            watcher_address(&self.binding, &zone_id)
        });
        match self
            .env
            .read_cell_watching(kind, shard, watcher.as_deref())
            .await
        {
            Ok(view) => self.apply_view(topic, view),
            Err(e) => worker::console_warn!("world view {topic}: {e}"),
        }
    }

    /// A pushed update (`POST …/__world`, internal secret): applied when
    /// the zone is in memory and watches the topic; a sleeping zone
    /// hydrates on wake instead (a push never wakes it).
    async fn take_push(&self, mut req: Request) -> Result<Response> {
        if let Err(error) = super::cells::authenticate(&req, self.env.env()) {
            return super::cells::error_response(error);
        }
        let Ok(update) = req.json::<WatchUpdate>().await else {
            return Response::error("a world update is required", 400);
        };
        let awake = self.inner.borrow().host.is_some();
        if awake && self.world_topics().contains(&update.topic) {
            self.apply_view(&update.topic, update.view);
        }
        Response::empty().map(|r| r.with_status(204))
    }

    /// Apply `view` of `topic` if it is newer than what the zone has.
    fn apply_view(&self, topic: &str, view: Value) {
        let version = view_version(&view);
        if !self
            .inner
            .borrow_mut()
            .world_versions
            .accept(topic, version)
        {
            return;
        }
        let (kind, shard) = topic.split_once('/').unwrap_or((topic, ""));
        if kind == ZONE_DEV_KIND && shard == NETSIM_SHARD {
            let sim = serde_json::from_value::<NetSimJson>(view)
                .unwrap_or_default()
                .to_sim();
            self.inner.borrow_mut().net_sim = sim;
            return;
        }
        if kind == CONTENT_ROLLOUT_KIND && self.content_world().as_deref() == Some(shard) {
            let state = view
                .get("state")
                .cloned()
                .and_then(|state| serde_json::from_value::<ContentState>(state).ok());
            if let Some(state) = state {
                self.apply_content(&state);
            }
            return;
        }
        let watches = self.inner.borrow().watches.clone();
        let Some(message) = watches
            .as_ref()
            .and_then(|w| w.get(topic))
            .and_then(|watch| watch.message(view))
        else {
            return;
        };
        let mut inner = self.inner.borrow_mut();
        if let Some(host) = inner.host.as_mut() {
            host.deliver(ZoneInbound::Message(message));
            inner.flush();
        }
    }

    fn apply_content(&self, state: &ContentState) {
        let mut inner = self.inner.borrow_mut();
        let Some(host) = inner.host.as_mut() else {
            return;
        };
        let frozen = host.freeze_state() != FreezeState::Running;
        let current = host.config().content_version.clone();
        match zone_step(state, &current, frozen) {
            ZoneStep::Stay => return,
            ZoneStep::Freeze => {
                host.request_freeze(Notice {
                    reason: "content_update".into(),
                    version: state.target.clone(),
                    notes: state.notes.clone(),
                });
            }
            ZoneStep::Apply(version) => {
                host.set_content_version(version.clone());
                if frozen {
                    host.resume(Notice {
                        reason: "content_updated".into(),
                        version,
                        notes: state.notes.clone(),
                    });
                }
                if state.hard {
                    host.reload_stale_players();
                }
            }
        }
        inner.flush();
    }

    async fn hello(&self, ws: &WebSocket, hello: crate::protocol::Hello) -> Result<()> {
        if hello.protocol_version != PROTOCOL_VERSION {
            reject(ws, error::VERSION_MISMATCH, "protocol version");
            return Ok(());
        }
        let cold = self.inner.borrow().host.is_none();
        if cold && !hello.reconnect_token.is_empty() {
            // Sessions die with the activation: rejoin with a ticket (which
            // wakes the zone for a player, not for an observer).
            reject(ws, error::RECONNECT_FAILED, "zone restarted; rejoin");
            return Ok(());
        }
        let now = (now_ms() / 1000.0) as u64;
        // Transfer tickets are single use across activations too.
        let mut principal = None;
        let mut wake_grant = false;
        let joined = if hello.reconnect_token.is_empty() {
            let verified = {
                let inner = self.inner.borrow();
                let key = inner.key.as_ref().expect("configured");
                key.verify_ticket(&hello.ticket, now)
            };
            let verified = match verified {
                Ok(v) => v,
                Err(e) => {
                    reject(ws, error::UNAUTHORIZED, &format!("{e:?}"));
                    return Ok(());
                }
            };
            let zone_id = self.inner.borrow().zone_id.clone();
            let (claims, transfer) = match verified {
                VerifiedTicket::Join(claims) => (claims, None),
                VerifiedTicket::Transfer(transfer) => (transfer.claims.clone(), Some(transfer)),
            };
            if claims.zone != zone_id {
                reject(ws, error::UNAUTHORIZED, "ticket is for another zone");
                return Ok(());
            }
            // A passive watcher never wakes a sleeping zone; a near one
            // whose ticket grants `wake` does.
            let observer = claims.role == Role::Observer;
            wake_grant = observer && claims.wake;
            if cold && observer && !(hello.near && wake_grant) {
                reject(ws, error::ASLEEP, "zone asleep");
                return Ok(());
            }
            let replayed = self
                .inner
                .borrow_mut()
                .nonces
                .check_and_insert(&claims.nonce, claims.expires_at, now)
                .is_err();
            let nonce_key = format!("{NONCE_PREFIX}{}", claims.nonce);
            let used_before = transfer.is_some()
                && self
                    .state
                    .storage()
                    .get::<u64>(&nonce_key)
                    .await
                    .ok()
                    .flatten()
                    .is_some();
            if replayed || used_before {
                reject(ws, error::UNAUTHORIZED, "ticket already used");
                return Ok(());
            }
            if transfer.is_some() {
                let _ = self
                    .state
                    .storage()
                    .put(&nonce_key, claims.expires_at)
                    .await;
            }
            if let Err(e) = self.wake().await {
                reject(ws, error::NOT_FOUND, &e.to_string());
                return Ok(());
            }
            if claims.role == Role::Player {
                principal = Some(claims.principal.clone());
            }
            // A client on another content version than the zone: the zone
            // may be behind the world (it slept through a rollout), so it
            // learns the live version before deciding.
            let behind = {
                let inner = self.inner.borrow();
                inner
                    .options
                    .as_ref()
                    .is_some_and(|o| o.content_rollout.is_some())
                    && inner
                        .host
                        .as_ref()
                        .is_some_and(|h| h.config().content_version != hello.content_version)
            };
            if behind {
                if let Some(world) = self.content_world() {
                    self.reconcile(&topic(CONTENT_ROLLOUT_KIND, &world)).await;
                }
            }
            let mut inner = self.inner.borrow_mut();
            let Some(host) = inner.host.as_mut() else {
                drop(inner);
                reject(ws, error::ZONE_UNAVAILABLE, "zone asleep");
                return Ok(());
            };
            match &transfer {
                Some(transfer) => {
                    host.join_transfer(transfer, hello.capabilities, &hello.content_version)
                }
                None => host.join(&claims, hello.capabilities, &hello.content_version),
            }
        } else {
            let mut inner = self.inner.borrow_mut();
            let Some(host) = inner.host.as_mut() else {
                drop(inner);
                reject(ws, error::RECONNECT_FAILED, "zone restarted; rejoin");
                return Ok(());
            };
            host.reconnect_with_version(
                &hello.reconnect_token,
                hello.capabilities,
                &hello.content_version,
            )
        };
        match joined {
            Ok(joined) => {
                let mut observer = matches!(joined.viewer, Viewer::Observer(_));
                {
                    let mut inner = self.inner.borrow_mut();
                    let activation = inner.activation.clone();
                    ws.serialize_attachment(Attachment {
                        client: joined.client.0,
                        activation,
                    })?;
                    inner.sockets.insert(joined.client.0, ws.clone());
                    if observer && wake_grant {
                        inner.may_wake.insert(joined.client.0);
                    }
                    if observer && hello.near {
                        // A near watcher (with the grant) keeps the zone
                        // awake like a player; without it, it is told so
                        // after its Welcome and stays passive.
                        inner.flush();
                        inner.set_near(joined.client.0, true);
                        observer = !wake_grant;
                    }
                    if observer && !inner.ticking {
                        // A dormant zone does not tick: greet the watcher
                        // with its current state (it stays dormant).
                        if let Some(host) = inner.host.as_mut() {
                            host.patch_now();
                        }
                    }
                    inner.flush();
                    if !observer && inner.state != RoomState::Active {
                        inner.state = RoomState::Active;
                        inner.report(ACTIVITY_ACTIVE);
                    }
                }
                if observer {
                    return Ok(());
                }
                self.store_status().await;
                self.start_loop();
                if let Some(principal) = principal {
                    self.after_join(principal);
                }
            }
            Err(e) => reject(ws, e.code, &e.message),
        }
        Ok(())
    }

    /// A near watcher arrived on an awake zone that is not ticking
    /// (dormant): it is active again and ticks.
    async fn keep_awake(&self) {
        {
            let mut inner = self.inner.borrow_mut();
            if inner.host.is_none() {
                return;
            }
            if inner.state != RoomState::Active {
                inner.state = RoomState::Active;
                inner.report(ACTIVITY_ACTIVE);
            }
        }
        self.store_status().await;
        self.start_loop();
    }

    fn after_join(&self, principal: String) {
        let Some(work) = Z::after_join(&self.env, &principal) else {
            return;
        };
        let inner = self.inner.clone();
        wasm_bindgen_futures::spawn_local(async move {
            if let Some(message) = work.await {
                let mut inner = inner.borrow_mut();
                if let Some(host) = inner.host.as_mut() {
                    host.deliver(ZoneInbound::Message(message));
                    inner.flush();
                }
                // Facts the message produced are written on the next tick.
            }
        });
    }

    fn drop_socket(&self, ws: &WebSocket) {
        let attached: Option<Attachment> = ws.deserialize_attachment().ok().flatten();
        let mut inner = self.inner.borrow_mut();
        if let Some(a) = attached.filter(|a| a.activation == inner.activation) {
            inner.sockets.remove(&a.client);
            inner.forget(a.client);
            if let Some(host) = inner.host.as_mut() {
                host.disconnect(ClientId(a.client), false);
            }
            inner.flush();
            drop(inner);
            self.spool_later();
        }
    }

    fn start_loop(&self) {
        {
            let mut inner = self.inner.borrow_mut();
            if inner.ticking || inner.host.is_none() {
                return;
            }
            inner.ticking = true;
        }
        let this = self.handle();
        wasm_bindgen_futures::spawn_local(async move { this.tick_loop().await });
    }

    fn handle(&self) -> Self {
        Self {
            state: self.state.clone(),
            env: self.env.clone(),
            inner: self.inner.clone(),
            binding: self.binding.clone(),
        }
    }

    async fn tick_loop(&self) {
        let (hz, persist_every, dormant_after) = {
            let mut inner = self.inner.borrow_mut();
            inner.empty_since_ms = None;
            let options = inner.options.as_ref().expect("configured");
            (
                options.tick_hz,
                options.persist_every.as_millis() as f64,
                options.dormant_after.as_millis() as f64,
            )
        };
        let mut schedule = TickSchedule::new(now_ms(), hz);
        // Crash watchdog: if this activation dies while awake, the alarm
        // wakes a fresh one that reports the room asleep. Armed once here and
        // again from `alarm` while ticking — never per snapshot: each
        // `set_alarm` leaves a wake entry celld scans when it starts.
        if let Some(watchdog) = self.watchdog_ms() {
            self.arm_later(watchdog);
        }
        loop {
            Delay::from(Duration::from_micros(
                (schedule.wait_ms(now_ms()) * 1000.0) as u64,
            ))
            .await;
            let woke = now_ms();
            let late = schedule.woke(woke);
            let (stop, persist) = {
                let mut inner = self.inner.borrow_mut();
                let Some(host) = inner.host.as_mut() else {
                    inner.ticking = false;
                    return;
                };
                let report = host.advance();
                let players = host.player_count();
                if players < inner.players_seen {
                    inner.persist_soon = true;
                }
                inner.players_seen = players;
                inner.flush();
                let stats = &mut inner.stats;
                stats.iterations += 1;
                stats.late.push(late);
                stats.work.push(now_ms() - woke);
                stats.dropped += u64::from(report.dropped_ticks);
                if let Some(queue) = inner.activity.as_mut() {
                    queue.drain();
                }
                let stop = if inner.occupied() {
                    inner.empty_since_ms = None;
                    false
                } else {
                    woke - *inner.empty_since_ms.get_or_insert(woke) >= dormant_after
                };
                let persist =
                    stop || inner.persist_soon || woke - inner.last_persist_ms >= persist_every;
                if stop {
                    inner.ticking = false;
                }
                (stop, persist)
            };
            // Storage stays off the loop: the simulation keeps its pace
            // while a write waits on the bucket.
            self.spool_later();
            self.reconcile_world(false);
            self.refresh_if_due();
            if stop {
                self.persist().await;
                self.go_dormant().await;
                return;
            }
            if persist {
                self.persist_later();
            }
        }
    }

    /// Write the zone's snapshot (after the fact outbox writes queued by
    /// this tick, so a crash never keeps an outcome without its fact).
    async fn persist(&self) {
        yield_now().await;
        let snapshot = {
            let mut inner = self.inner.borrow_mut();
            inner.last_persist_ms = now_ms();
            inner.persist_soon = false;
            inner.snapshot()
        };
        if let Some(snapshot) = snapshot {
            if let Err(e) = self.state.storage().put(SNAPSHOT_KEY, snapshot).await {
                worker::console_error!("zone snapshot write failed: {e}");
            }
        }
    }

    /// [`persist`](Self::persist) off the tick loop. One snapshot write at
    /// a time: while one is in flight a due tick leaves it to a later tick.
    /// Spawned after the tick's [`spool_later`](Self::spool_later), so the
    /// outbox write is issued before the snapshot that reflects it.
    fn persist_later(&self) {
        {
            let mut inner = self.inner.borrow_mut();
            if inner.persisting {
                return;
            }
            inner.persisting = true;
        }
        let this = self.handle();
        wasm_bindgen_futures::spawn_local(async move {
            this.persist().await;
            this.inner.borrow_mut().persisting = false;
        });
    }

    async fn go_dormant(&self) {
        let after = {
            let mut inner = self.inner.borrow_mut();
            if inner.occupied() {
                return;
            }
            inner.state = RoomState::Dormant { since_ms: now_ms() };
            inner.report(ACTIVITY_DORMANT);
            inner
                .options
                .as_ref()
                .map_or(10_000.0, |o| o.hibernate_after.as_millis() as f64)
        };
        self.store_status().await;
        self.arm(after).await;
    }

    async fn hibernate(&self) {
        self.persist().await;
        {
            let mut inner = self.inner.borrow_mut();
            if inner.occupied() {
                return;
            }
            inner.report(ACTIVITY_HIBERNATED);
            // Watchers of a sleeping zone are let go (they may come back
            // and are told it is asleep).
            for (_, ws) in inner.sockets.drain() {
                let _ = ws.close(Some(1000), Some("zone asleep"));
            }
            inner.out_lines.clear();
            inner.in_lines.clear();
            inner.may_wake.clear();
            inner.near.clear();
            // Dropped, so the next join restores from storage.
            inner.host = None;
            inner.state = RoomState::Cold;
            inner.ticking = false;
        }
        self.store_status().await;
    }

    /// Write the facts emitted since the last call to the outbox.
    async fn spool(&self) {
        let facts = self.inner.borrow_mut().emitted();
        if facts.is_empty() {
            return;
        }
        let storage = self.state.storage();
        // One batched write (the storage API takes at most 128 keys).
        for chunk in facts.chunks(128) {
            let batch = js_sys::Object::new();
            for fact in chunk {
                let text = serde_json::to_string(&StoredFact::of(fact)).unwrap_or_default();
                let _ = js_sys::Reflect::set(
                    &batch,
                    &StoredFact::storage_key(fact.key()).into(),
                    &text.into(),
                );
            }
            if let Err(e) = storage.put_multiple_raw(batch).await {
                worker::console_error!("fact outbox write failed: {e}");
            }
        }
        self.inner.borrow_mut().spooled += facts.len() as u64;
        self.arm_outbox().await;
    }

    /// [`spool`](Self::spool) from a synchronous handler.
    fn spool_later(&self) {
        let this = self.handle();
        wasm_bindgen_futures::spawn_local(async move { this.spool().await });
    }

    async fn store_status(&self) {
        self.spool().await;
        let status = self.inner.borrow().last_status.clone();
        if !status.is_empty() {
            let _ = self.state.storage().put(STATUS_KEY, status).await;
        }
    }

    /// Set the object's alarm `in_ms` from now.
    async fn arm(&self, in_ms: f64) {
        if let Err(e) = self
            .state
            .storage()
            .set_alarm(Duration::from_millis(in_ms.max(1.0) as u64))
            .await
        {
            worker::console_error!("zone alarm: {e}");
        }
    }

    /// The crash watchdog's period while ticking (three snapshot
    /// intervals, at least a minute), when configured and ticking.
    fn watchdog_ms(&self) -> Option<f64> {
        let inner = self.inner.borrow();
        let options = inner.options.as_ref()?;
        (inner.ticking && inner.host.is_some())
            .then(|| (options.persist_every.as_millis() as f64 * 3.0).max(WATCHDOG_MIN_MS))
    }

    /// [`arm`](Self::arm) off the caller's path.
    fn arm_later(&self, in_ms: f64) {
        let this = self.handle();
        wasm_bindgen_futures::spawn_local(async move { this.arm(in_ms).await });
    }

    /// Keep an alarm while unsettled facts wait in the outbox.
    async fn arm_outbox(&self) {
        let pending = {
            let mut inner = self.inner.borrow_mut();
            // Settlements only leave the queue on `drain`; a hibernated
            // room has no tick loop to drain it, and would otherwise re-arm
            // this alarm forever on a report that settled long ago.
            if let Some(queue) = inner.activity.as_mut() {
                queue.drain();
            }
            inner.activity.as_ref().map_or(0, |q| q.in_flight())
                + inner.host.as_ref().map_or(0, |h| h.facts_in_flight())
        };
        let alarm = self.state.storage().get_alarm().await.ok().flatten();
        if pending > 0 && alarm.is_none() {
            self.arm(OUTBOX_RETRY_MS).await;
        }
    }

    /// Debug only, off by default: ask each configured operator listener
    /// whether it holds this object.
    fn find_node(&self) {
        let (nodes, id) = {
            let inner = self.inner.borrow();
            let Some(options) = inner.options.as_ref() else {
                return;
            };
            if options.nodes.is_empty() || options.node.is_some() {
                return;
            }
            (options.nodes.clone(), self.state.id().to_string())
        };
        let inner = self.inner.clone();
        wasm_bindgen_futures::spawn_local(async move {
            for (node, addr) in nodes {
                let Ok(url) = format!("http://{addr}/state").parse() else {
                    continue;
                };
                let Ok(mut response) = worker::Fetch::Url(url).send().await else {
                    continue;
                };
                let Ok(state) = response.json::<Value>().await else {
                    continue;
                };
                let mine = state["deployment"]["cells"]
                    .as_object()
                    .is_some_and(|cells| cells.keys().any(|k| k.ends_with(&id)));
                if mine {
                    let mut inner = inner.borrow_mut();
                    inner.node = node;
                    if inner.state == RoomState::Active {
                        inner.report(ACTIVITY_ACTIVE);
                    }
                    return;
                }
            }
        });
    }

    fn info(&self) -> Value {
        let inner = self.inner.borrow();
        let host = inner.host.as_ref();
        let stats = &inner.stats;
        json!({
            "zone": inner.zone_id,
            "status": inner.last_status,
            "awake": host.is_some(),
            "activation": inner.activation,
            "restored": inner.restored,
            "wakes": inner.wakes,
            "node": inner.node,
            "isolate": ISOLATE.with(|i| i.clone()),
            "players": host.map_or(0, |h| h.player_count()),
            "observers": host.map_or(0, |h| h.observer_count()),
            "near": inner.near.len(),
            "tick": host.map_or(0, |h| h.tick()),
            "hz": inner.options.as_ref().map_or(0, |o| o.tick_hz),
            "ticking": inner.ticking,
            "factsInFlight": host.map_or(0, |h| h.facts_in_flight())
                + inner.activity.as_ref().map_or(0, |q| q.in_flight()),
            "spooledFacts": inner.spooled,
            "iterations": stats.iterations,
            "droppedTicks": stats.dropped,
            "lateMs": { "p50": stats.late.percentile(0.5), "p99": stats.late.percentile(0.99) },
            "netsim": NetSim::to_json(inner.net_sim),
            "content": host.map(|h| json!({
                "version": h.config().content_version,
                "freeze": format!("{:?}", h.freeze_state()).to_lowercase(),
            })),
            "workMs": { "p50": stats.work.percentile(0.5), "p99": stats.work.percentile(0.99) },
        })
    }
}

/// Deliver what the delay lines hold as it falls due, then stop (a newer
/// task with an earlier deadline takes over).
async fn drain_task<Z: CelldZone>(inner: Weak<RefCell<Inner<Z>>>, mut target: f64) {
    loop {
        Delay::from(Duration::from_micros(
            ((target - now_ms()).max(0.0) * 1000.0) as u64,
        ))
        .await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let mut inner = inner.borrow_mut();
        inner.drain_due(now_ms());
        if inner.drain_at != Some(target) {
            return;
        }
        match inner.next_due() {
            Some(next) => {
                inner.drain_at = Some(next);
                target = next;
            }
            None => {
                inner.drain_at = None;
                return;
            }
        }
    }
}

fn retry_policy() -> RetryPolicy {
    RetryPolicy::until_settled(Duration::from_millis(100), Duration::from_secs(5))
}

fn sleep(d: Duration) -> Delay {
    Delay::from(d)
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

/// Worker entry helper: route `"{prefix}{zone}[/…]"` to the zone's object
/// in Durable Object namespace `binding` (one object per zone id; celld
/// places it). Returns `None` when the path is not under `prefix`.
pub async fn forward_to_zone(
    req: Request,
    env: &Env,
    binding: &str,
    prefix: &str,
) -> Result<Option<Response>> {
    let path = req.url()?.path().to_string();
    let Some((zone, _)) = zone_from_path(prefix, &path) else {
        return Ok(None);
    };
    let stub = env
        .durable_object(binding)?
        .id_from_name(&zone)?
        .get_stub()?;
    let headers = req.headers().clone();
    headers.set(ZONE_HEADER, &zone)?;
    let mut init = RequestInit::new();
    init.with_method(req.method()).with_headers(headers);
    let forwarded = Request::new_with_init(req.url()?.as_str(), &init)?;
    stub.fetch_with_request(forwarded).await.map(Some)
}

/// Declare the Durable Object class hosting zone type `$zone`:
///
/// ```ignore
/// distributed_game::celld_zone_object!(PitRoom, MobPit);
/// ```
///
/// The crate needs `worker` as a dependency. Bind the class in
/// `wrangler.jsonc` (a SQLite class) and route to it with
/// [`forward_to_zone`].
#[macro_export]
macro_rules! celld_zone_object {
    ($name:ident, $zone:ty) => {
        #[::worker::durable_object]
        pub struct $name {
            zone: $crate::celld_zone::ZoneObject<$zone>,
        }

        impl ::worker::DurableObject for $name {
            fn new(state: ::worker::State, env: ::worker::Env) -> Self {
                Self {
                    zone: $crate::celld_zone::ZoneObject::of_class(
                        state,
                        env,
                        ::core::stringify!($name),
                    ),
                }
            }

            async fn fetch(&self, req: ::worker::Request) -> ::worker::Result<::worker::Response> {
                self.zone.fetch(req).await
            }

            async fn websocket_message(
                &self,
                ws: ::worker::WebSocket,
                message: ::worker::WebSocketIncomingMessage,
            ) -> ::worker::Result<()> {
                self.zone.websocket_message(ws, message).await
            }

            async fn websocket_close(
                &self,
                ws: ::worker::WebSocket,
                code: usize,
                reason: String,
                was_clean: bool,
            ) -> ::worker::Result<()> {
                self.zone.websocket_close(ws, code, reason, was_clean).await
            }

            async fn websocket_error(
                &self,
                ws: ::worker::WebSocket,
                error: ::worker::Error,
            ) -> ::worker::Result<()> {
                self.zone.websocket_error(ws, error).await
            }

            async fn alarm(&self) -> ::worker::Result<::worker::Response> {
                self.zone.alarm().await
            }
        }
    };
}
