//! WebSocket transport (`net` feature): one axum server hosting many zones.
//!
//! - Route: `GET /zones/{zone_id}` upgrades to a WebSocket. Zones are created
//!   on first use through the game's async [`ZoneFactory`] and each runs on
//!   its own Tokio task driving a [`ZoneHost`]. Creation (ownership claim,
//!   snapshot, [`ZoneAllocation`](crate::world::ZoneAllocation) record,
//!   factory) happens before the first tick; joins that arrive meanwhile
//!   wait and are admitted afterwards, or answered `Error(NOT_FOUND)` (the
//!   factory knows no such zone) / `Error(ZONE_UNAVAILABLE)` (it failed or
//!   timed out).
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
//!
//! World structure (see [`world`] and `crate::world`):
//!
//! - **Placement**: with a [`ZoneDirectory`], a Hello for a zone owned by
//!   another host gets `Redirect(zone, url)` (ticket not consumed).
//! - **Ownership**: every activation acquires a fenced claim
//!   ([`ZoneOwnership`], default [`LocalOwnership`]) and renews it; losing
//!   it disposes the zone without persisting anything.
//! - **Activate on demand / hibernate**: zones are created on first use;
//!   one that stays Dormant for `hibernate_after` is snapshotted
//!   ([`Zone::hibernate`]) into the [`ZoneStateStore`] under its fencing
//!   token, disposed, and restored on its next activation.
//! - **Handoff**: [`Ctx::handoff`](crate::Ctx::handoff) issues a signed
//!   transfer ticket; the target reserves/confirms it with the issuing host
//!   (single use), a refused transfer may return to its source.
//! - **Tier-up events** are routed to the parent zone, locally or through
//!   the admin endpoint of the host that owns it.
//! - **Barrier**: [`GameServer::prepare_barrier`] / `commit_barrier` /
//!   `abort_barrier`, driven by a [`world::WorldCoordinator`].

pub mod inbox;
pub mod peer;
pub use crate::ticket;
pub mod transfer;
pub mod world;

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use crate::facts::BoxFuture;
pub use inbox::{InboxError, ZoneInboxClient, ZONE_MESSAGE_PATH};
pub use ticket::{
    decode_hs256, NonceCache, SharedTicketVerifier, TicketError, TicketKey, TicketVerifier,
    VerifiedTicket,
};
pub use transfer::{TransferOp, TransferRegistry};
pub use world::{
    AbortNotes, BarrierParticipant, RemoteHost, RolloutError, RolloutReport, WorldCoordinator,
};

use crate::clock::{Clock, SystemClock};
use crate::facts::{fact_channel, spawn_fact_worker, FactQueue, FactSink, RetryPolicy};
use crate::handoff::{HandoffRequest, Transfer};
use crate::host::{HostConfig, JoinError, Joined, Outbound, ZoneHost};
use crate::metrics::GameMetrics;
use crate::protocol::Role;
use crate::protocol::{error, kick, ClientMessage, Notice, ServerMessage, PROTOCOL_VERSION};
use crate::rng::ZoneRng;
use crate::world::{
    BarrierMode, ChildEvent, DottedTopology, FreezeState, HostAddr, LocalOwnership, OwnershipError,
    StalePolicy, StateError, StoredZoneState, ZoneActivation, ZoneAllocationStore, ZoneClaim,
    ZoneCreateError, ZoneDirectory, ZoneOwnership, ZoneStateStore, ZoneTopology,
};
use crate::zone::{ClientId, TicketClaims, Tier, Zone};

pub use crate::netsim::NetSim;

/// Server settings.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Settings for every hosted zone.
    pub host: HostConfig,
    /// HMAC key for transfer tickets and server tokens, and (unless
    /// `ticket_verifier` is set) for admission tickets.
    pub ticket_key: TicketKey,
    /// Custom verifier for **join** tickets (e.g. the game's web-BFF JWTs);
    /// `None` = HS256 tickets signed with `ticket_key`. Expiry, route zone
    /// and single use are enforced by the server either way.
    pub ticket_verifier: Option<SharedTicketVerifier>,
    /// Extension claims tickets may carry
    /// ([`ExtensionSchema::of::<E>()`](crate::ExtensionSchema::of) for a
    /// `#[derive(TicketExtension)]` type). `None` = a ticket with any `ext`
    /// claims is refused. Applies to join (any verifier) and transfer
    /// tickets.
    pub ticket_extensions: Option<crate::claims::ExtensionSchema>,
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
    /// Optional dev latency simulation (initial value; change it at runtime
    /// with [`GameServer::set_net_sim`]).
    pub net_sim: Option<NetSim>,
    /// Zones that get their own `zone` metrics label (see [`crate::metrics`]).
    pub metrics_zone_labels: usize,
    /// Hibernate a zone after it has been Dormant this long (None = never).
    pub hibernate_after: Option<Duration>,
    /// Zone ownership claim TTL.
    pub ownership_ttl: Duration,
    /// Claim renewal period (well below `ownership_ttl`).
    pub ownership_renew_every: Duration,
    /// How long an activation waits for a held claim (e.g. the previous
    /// activation still saving its snapshot) before refusing.
    pub activation_timeout: Duration,
    /// Transfer ticket lifetime (and target reservation bound).
    pub transfer_ttl: Duration,
    /// After a zone is disposed, how long to wait for its fact worker to
    /// commit the facts still queued.
    pub settle_timeout: Duration,
    /// Shared secret enabling the `/admin/*` routes and peer calls.
    pub admin_secret: Option<String>,
    /// Peer (host-to-host) request timeout.
    pub peer_timeout: Duration,
    /// Public URL clients are redirected to for a zone owned by another
    /// host (`None` = the directory's [`HostAddr::zone_url`]). For
    /// deployments behind a proxy whose URLs differ from the hosts' raw
    /// addresses; see [`ServerConfig::with_redirect_url`].
    pub redirect_url: Option<RedirectUrl>,
    /// Fact commits each zone's worker may run at once (default 1 = strict
    /// emission order). Above 1, facts of one principal stay sequential and
    /// different principals overlap (see [`crate::facts`]). Read by
    /// [`GameServer::with_fact_sink`].
    pub max_concurrent_commits: usize,
}

/// `(owning host, zone id) -> Redirect URL`.
pub type RedirectUrlFn = dyn Fn(&HostAddr, &str) -> String + Send + Sync;

/// Maps (owning host, zone id) to the URL sent in `Redirect`.
#[derive(Clone)]
pub struct RedirectUrl(pub Arc<RedirectUrlFn>);

impl std::fmt::Debug for RedirectUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedirectUrl(..)")
    }
}

impl ServerConfig {
    /// Build `Redirect` URLs with `url(owner, zone_id)`, e.g.
    /// `|host, zone| format!("wss://game.example/{}/zones/{zone}", host.id)`.
    /// Alternatively advertise per-host public URLs in the directory with
    /// [`HostAddr::with_ws_url`].
    pub fn with_redirect_url(
        mut self,
        url: impl Fn(&HostAddr, &str) -> String + Send + Sync + 'static,
    ) -> Self {
        self.redirect_url = Some(RedirectUrl(Arc::new(url)));
        self
    }

    /// Admit join tickets with `verifier` instead of `ticket_key`.
    pub fn with_ticket_verifier(mut self, verifier: impl TicketVerifier) -> Self {
        self.ticket_verifier = Some(SharedTicketVerifier::new(verifier));
        self
    }

    /// Admit ticket extension claims of type `E`.
    pub fn with_ticket_extensions<E: crate::TicketExtension>(mut self) -> Self {
        self.ticket_extensions = Some(crate::claims::ExtensionSchema::of::<E>());
        self
    }

    /// Defaults with the given ticket key.
    pub fn new(ticket_key: TicketKey) -> Self {
        Self {
            host: HostConfig::default(),
            ticket_key,
            ticket_verifier: None,
            ticket_extensions: None,
            hello_timeout: Duration::from_secs(5),
            input_rate: 120.0,
            input_burst: 240.0,
            outbound_queue: 512,
            max_frame_bytes: 64 * 1024,
            net_sim: None,
            metrics_zone_labels: 64,
            hibernate_after: None,
            ownership_ttl: Duration::from_secs(15),
            ownership_renew_every: Duration::from_secs(5),
            activation_timeout: Duration::from_secs(5),
            transfer_ttl: Duration::from_secs(30),
            settle_timeout: Duration::from_secs(2),
            admin_secret: None,
            peer_timeout: Duration::from_secs(3),
            max_concurrent_commits: 1,
            redirect_url: None,
        }
    }
}

type Inspector = Box<dyn FnOnce(&dyn Any) + Send>;

#[allow(clippy::large_enum_variant)] // Join is rare; boxing buys nothing.
enum ZoneCmd {
    Join {
        claims: TicketClaims,
        transfer: Option<Transfer>,
        capabilities: u32,
        content_version: String,
        out: mpsc::Sender<ConnOut>,
        reply: oneshot::Sender<Result<Joined, JoinError>>,
    },
    Reconnect {
        token: String,
        capabilities: u32,
        content_version: String,
        out: mpsc::Sender<ConnOut>,
        reply: oneshot::Sender<Result<Joined, JoinError>>,
    },
    ChildEvent(ChildEvent),
    Freeze {
        notice: Notice,
        ack: oneshot::Sender<()>,
    },
    Commit {
        version: String,
        notes: String,
        mode: BarrierMode,
    },
    Abort {
        notice: Notice,
    },
    Inspect(Inspector),
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
    /// The client patched its content live.
    Content {
        client: ClientId,
        version: String,
    },
    /// A zone inbox message (decoded into `Z::Message` on the zone task).
    Deliver {
        inbound: inbox::WireInbound,
        reply: oneshot::Sender<Result<crate::inbox::Delivered, InboxError>>,
    },
}

enum ConnOut {
    Frame(Vec<u8>),
    Close,
}

type ZoneFactoryFn<C> =
    dyn Fn(ZoneActivation) -> BoxFuture<'static, Result<Option<C>, ZoneCreateError>> + Send + Sync;

/// Creates the configuration of a zone being activated, or says it does
/// not exist (`Ok(None)` → `Error(NOT_FOUND)`) or cannot be created now
/// (`Err` → `Error(ZONE_UNAVAILABLE)`). Runs on the activation task before
/// the zone exists: it may await storage, and joins wait for it (bounded by
/// `ServerConfig::activation_timeout`). The tick never waits for it.
pub struct ZoneFactory<C>(Box<ZoneFactoryFn<C>>);

impl<C> std::fmt::Debug for ZoneFactory<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ZoneFactory(..)")
    }
}

impl<C: Send + 'static> ZoneFactory<C> {
    /// Async factory: `ZoneFactory::new(|activation| async move { … })`.
    pub fn new<F, Fut>(factory: F) -> Self
    where
        F: Fn(ZoneActivation) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<C>, ZoneCreateError>> + Send + 'static,
    {
        Self(Box::new(move |activation| Box::pin(factory(activation))))
    }

    /// Factory that needs no I/O (the zone id and the allocation record
    /// decide): `ZoneFactory::sync(|a| a.zone_id.starts_with("arena-").then(Config::default))`.
    pub fn sync(factory: impl Fn(&ZoneActivation) -> Option<C> + Send + Sync + 'static) -> Self {
        Self(Box::new(move |activation| {
            let config = factory(&activation);
            Box::pin(async move { Ok(config) })
        }))
    }
}
type FactsFactory = dyn Fn(&str) -> Option<(FactQueue, JoinHandle<()>)> + Send + Sync;
type HostConfigFn = dyn Fn(&str, &mut HostConfig) + Send + Sync;

struct ZoneEntry {
    tx: mpsc::Sender<ZoneCmd>,
    activation: u64,
}

/// World-wide content state of this server.
#[derive(Debug, Clone, Default)]
struct WorldState {
    version: String,
    /// Barrier in progress: zones activated now start frozen.
    barrier: Option<(u64, Notice)>,
}

struct Inner<Z: Zone> {
    config: ServerConfig,
    clock: Arc<dyn Clock>,
    factory: ZoneFactory<Z::Config>,
    allocations: Option<Arc<dyn ZoneAllocationStore>>,
    facts: Option<Box<FactsFactory>>,
    /// Per-zone adjustments of `config.host` (tick rates, capacity …).
    host_config: Option<Box<HostConfigFn>>,
    nonces: Mutex<NonceCache>,
    zones: Mutex<HashMap<String, ZoneEntry>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    shutdown: watch::Sender<bool>,
    token_counter: AtomicU64,
    metrics: Arc<GameMetrics>,
    host_id: String,
    directory: Option<Arc<dyn ZoneDirectory>>,
    ownership: Arc<dyn ZoneOwnership>,
    state_store: Option<Arc<dyn ZoneStateStore>>,
    topology: Arc<dyn ZoneTopology>,
    transfers: Mutex<TransferRegistry>,
    /// Lock order: `zones` before `world`.
    world: Mutex<WorldState>,
    activations: AtomicU64,
    /// Live dev network simulation, read by every connection per message.
    net_sim: watch::Sender<Option<NetSim>>,
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
    /// Server whose zones are configured by `factory` when they activate
    /// (see [`ZoneFactory`]).
    pub fn new(config: ServerConfig, factory: ZoneFactory<Z::Config>) -> Self {
        Self::with_clock(config, Arc::new(SystemClock::new()), factory)
    }

    /// Same as [`new`](Self::new) with an explicit clock.
    pub fn with_clock(
        config: ServerConfig,
        clock: Arc<dyn Clock>,
        factory: ZoneFactory<Z::Config>,
    ) -> Self {
        let (shutdown, _) = watch::channel(false);
        let (net_sim, _) = watch::channel(config.net_sim);
        let version = config.host.content_version.clone();
        Self {
            inner: Arc::new(Inner {
                clock,
                factory,
                allocations: None,
                facts: None,
                host_config: None,
                nonces: Mutex::new(NonceCache::default()),
                zones: Mutex::new(HashMap::new()),
                tasks: Mutex::new(Vec::new()),
                shutdown,
                token_counter: AtomicU64::new(0),
                metrics: Arc::new(GameMetrics::new(config.metrics_zone_labels)),
                config,
                host_id: "local".into(),
                directory: None,
                ownership: Arc::new(LocalOwnership::new()),
                state_store: None,
                topology: Arc::new(DottedTopology),
                transfers: Mutex::new(TransferRegistry::default()),
                world: Mutex::new(WorldState {
                    version,
                    barrier: None,
                }),
                activations: AtomicU64::new(0),
                net_sim,
            }),
        }
    }

    fn configure(mut self, f: impl FnOnce(&mut Inner<Z>)) -> Self {
        f(Arc::get_mut(&mut self.inner).expect("configure before sharing the server"));
        self
    }

    /// This host's id (directory entry, ownership holder prefix, transfer
    /// issuer). Default `"local"`.
    pub fn with_host_id(self, host_id: impl Into<String>) -> Self {
        let host_id = host_id.into();
        self.configure(|inner| inner.host_id = host_id)
    }

    /// Route zones owned by other hosts (Redirect, peer calls).
    pub fn with_directory(self, directory: Arc<dyn ZoneDirectory>) -> Self {
        self.configure(|inner| inner.directory = Some(directory))
    }

    /// Fenced zone ownership shared by every host of the world.
    pub fn with_ownership(self, ownership: Arc<dyn ZoneOwnership>) -> Self {
        self.configure(|inner| inner.ownership = ownership)
    }

    /// Where zone allocation records are read at activation
    /// ([`ZoneActivation::allocation`]); a store error refuses the
    /// activation with `ZONE_UNAVAILABLE`.
    pub fn with_allocation_store(self, store: Arc<dyn ZoneAllocationStore>) -> Self {
        self.configure(|inner| inner.allocations = Some(store))
    }

    /// Where hibernated zone snapshots go.
    pub fn with_state_store(self, store: Arc<dyn ZoneStateStore>) -> Self {
        self.configure(|inner| inner.state_store = Some(store))
    }

    /// Adjust the [`HostConfig`] of each zone at activation, e.g. a slow
    /// tick rate for a province and a hot one for its neighborhoods. `f`
    /// receives the zone id and a copy of `config.host`; the world content
    /// version is applied afterwards and cannot be changed here.
    pub fn with_host_config(
        self,
        f: impl Fn(&str, &mut HostConfig) + Send + Sync + 'static,
    ) -> Self {
        self.configure(|inner| inner.host_config = Some(Box::new(f)))
    }

    /// Zone hierarchy for tier-up events (default [`DottedTopology`]).
    pub fn with_topology(self, topology: Arc<dyn ZoneTopology>) -> Self {
        self.configure(|inner| inner.topology = topology)
    }

    /// Host id.
    pub fn host_id(&self) -> &str {
        &self.inner.host_id
    }

    /// The world content version this server runs.
    pub fn content_version(&self) -> String {
        self.inner.world.lock().expect("world").version.clone()
    }

    /// True when `zone_id` is active (or being created) on this server.
    pub fn is_active(&self, zone_id: &str) -> bool {
        self.inner
            .zones
            .lock()
            .expect("zones")
            .get(zone_id)
            .is_some_and(|e| !e.tx.is_closed())
    }

    /// Run `f` against an **active** zone's host (ops tooling and tests).
    /// None when the zone is not active here.
    pub async fn inspect<R: Send + 'static>(
        &self,
        zone_id: &str,
        f: impl FnOnce(&ZoneHost<Z>) -> R + Send + 'static,
    ) -> Option<R> {
        let tx = {
            let zones = self.inner.zones.lock().expect("zones");
            zones.get(zone_id)?.tx.clone()
        };
        let (reply, rx) = oneshot::channel();
        let inspector: Inspector = Box::new(move |any: &dyn Any| {
            if let Some(host) = any.downcast_ref::<ZoneHost<Z>>() {
                let _ = reply.send(f(host));
            }
        });
        tx.send(ZoneCmd::Inspect(inspector)).await.ok()?;
        rx.await.ok()
    }

    /// Deliver a zone inbox message ([`crate::inbox`]) to `zone_id` on this
    /// server. Runs on the zone's task between ticks. Refused with
    /// [`InboxError::Inactive`] when the zone is not running here (a message
    /// never activates a zone) and [`InboxError::WrongOwner`] when the
    /// directory places it on another host.
    pub async fn send(
        &self,
        zone_id: &str,
        inbound: crate::inbox::ZoneInbound<Z::Message>,
    ) -> Result<crate::inbox::Delivered, InboxError> {
        self.deliver_wire(zone_id, inbox::WireInbound::of(&inbound))
            .await
    }

    pub(crate) async fn deliver_wire(
        &self,
        zone_id: &str,
        inbound: inbox::WireInbound,
    ) -> Result<crate::inbox::Delivered, InboxError> {
        if let Some(owner) = self
            .inner
            .directory
            .as_ref()
            .and_then(|d| d.locate(zone_id))
            .filter(|h| h.id != self.inner.host_id)
        {
            return Err(InboxError::WrongOwner(owner));
        }
        let tx = {
            let zones = self.inner.zones.lock().expect("zones");
            zones
                .get(zone_id)
                .filter(|e| !e.tx.is_closed())
                .map(|e| e.tx.clone())
                .ok_or(InboxError::Inactive)?
        };
        let (reply, rx) = oneshot::channel();
        tx.send(ZoneCmd::Deliver { inbound, reply })
            .await
            .map_err(|_| InboxError::Inactive)?;
        rx.await.map_err(|_| InboxError::Inactive)?
    }

    /// Start activating `zone_id` locally without waiting (it is registered
    /// at once, so barriers and joins already reach it). False only while
    /// shutting down; an unknown zone unregisters itself once its factory
    /// answers.
    pub fn begin_activation(&self, zone_id: &str) -> bool {
        activate_local(&self.inner, zone_id).is_some()
    }

    /// Activate `zone_id` locally (as if a client asked for it) and wait
    /// until it runs. False when the factory knows no such zone, creation
    /// failed, or the server is shutting down.
    pub async fn activate(&self, zone_id: &str) -> bool {
        if activate_local(&self.inner, zone_id).is_none() {
            return false;
        }
        self.inspect(zone_id, |_| ()).await.is_some()
    }

    /// Barrier phase 1 on this server: freeze every active zone (and every
    /// zone activated until commit/abort) with `notice`, then wait up to
    /// `timeout` for all to acknowledge. `Err` lists zones that did not.
    pub async fn prepare_barrier(
        &self,
        barrier: u64,
        notice: Notice,
        timeout: Duration,
    ) -> Result<(), Vec<String>> {
        let targets: Vec<(String, mpsc::Sender<ZoneCmd>)> = {
            let zones = self.inner.zones.lock().expect("zones");
            self.inner.world.lock().expect("world").barrier = Some((barrier, notice.clone()));
            zones
                .iter()
                .map(|(id, e)| (id.clone(), e.tx.clone()))
                .collect()
        };
        let deadline = Instant::now() + timeout;
        let mut waiting = Vec::new();
        for (zone_id, tx) in targets {
            let (ack, rx) = oneshot::channel();
            let cmd = ZoneCmd::Freeze {
                notice: notice.clone(),
                ack,
            };
            match tokio::time::timeout_at(deadline, tx.send(cmd)).await {
                Ok(Ok(())) => waiting.push((zone_id, tx, rx)),
                // Zone task gone (hibernated / stopped): nothing to freeze.
                Ok(Err(_)) => {}
                Err(_) => waiting.push((zone_id, tx, oneshot::channel().1)),
            }
        }
        let mut missing = Vec::new();
        for (zone_id, tx, rx) in waiting {
            match tokio::time::timeout_at(deadline, rx).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) if tx.is_closed() => {}
                _ => missing.push(zone_id),
            }
        }
        if missing.is_empty() {
            Ok(())
        } else {
            missing.sort();
            Err(missing)
        }
    }

    /// Barrier phase 2: switch every zone to `version` and resume it
    /// (`Resume{version, notes}`); `BarrierMode::Hard` also reloads stale
    /// clients.
    pub async fn commit_barrier(
        &self,
        _barrier: u64,
        version: &str,
        notes: &str,
        mode: BarrierMode,
    ) {
        let targets: Vec<mpsc::Sender<ZoneCmd>> = {
            let zones = self.inner.zones.lock().expect("zones");
            let mut world = self.inner.world.lock().expect("world");
            world.version = version.to_string();
            world.barrier = None;
            zones.values().map(|e| e.tx.clone()).collect()
        };
        for tx in targets {
            let _ = tx
                .send(ZoneCmd::Commit {
                    version: version.to_string(),
                    notes: notes.to_string(),
                    mode,
                })
                .await;
        }
    }

    /// Abort a barrier: every zone resumes on the old version.
    pub async fn abort_barrier(&self, _barrier: u64, notes: &str) {
        let (targets, version): (Vec<mpsc::Sender<ZoneCmd>>, String) = {
            let zones = self.inner.zones.lock().expect("zones");
            let mut world = self.inner.world.lock().expect("world");
            world.barrier = None;
            (
                zones.values().map(|e| e.tx.clone()).collect(),
                world.version.clone(),
            )
        };
        let notice = Notice {
            reason: "content_update_aborted".into(),
            version,
            notes: notes.to_string(),
        };
        for tx in targets {
            let _ = tx
                .send(ZoneCmd::Abort {
                    notice: notice.clone(),
                })
                .await;
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
        let concurrency = inner.config.max_concurrent_commits;
        inner.facts = Some(Box::new(move |_zone: &str| {
            let (queue, feed) = fact_channel(capacity, policy);
            let feed = feed.with_max_concurrent_commits(concurrency);
            let worker = spawn_fact_worker(feed, sink.clone());
            Some((queue, worker))
        }));
        self
    }

    /// axum router exposing `GET /zones/{zone_id}`, `GET /metrics` and,
    /// when `admin_secret` is set, the `/admin/*` peer routes.
    pub fn router(&self) -> Router {
        let mut router = Router::new()
            .route("/zones/{zone_id}", get(upgrade::<Z>))
            .route("/metrics", get(metrics_text::<Z>));
        if self.inner.config.admin_secret.is_some() {
            router = router
                .route("/admin/transfer", post(world::admin_transfer::<Z>))
                .route("/admin/child-event", post(world::admin_child_event::<Z>))
                .route(ZONE_MESSAGE_PATH, post(world::admin_zone_message::<Z>))
                .route("/admin/barrier/{phase}", post(world::admin_barrier::<Z>));
        }
        router.with_state(self.inner.clone())
    }

    /// Replace the dev network simulation at runtime (`None` disables it).
    /// Applies to every message sent or received from now on, on existing
    /// and new connections; messages already delayed keep their schedule
    /// and order is always preserved.
    pub fn set_net_sim(&self, sim: Option<NetSim>) {
        self.inner.net_sim.send_replace(sim);
    }

    /// Network simulation currently in effect.
    pub fn net_sim(&self) -> Option<NetSim> {
        *self.inner.net_sim.borrow()
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
            .iter()
            .filter(|(_, e)| !e.tx.is_closed())
            .map(|(id, _)| id.clone())
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
        self.serve_with(listener, Router::new(), signal).await
    }

    /// [`serve`](Self::serve) with additional application routes merged
    /// into [`router`](Self::router) (lobby, dev or health endpoints), keeping
    /// the same graceful shutdown: zones kick their clients and every zone
    /// task finishes before this returns.
    pub async fn serve_with(
        self,
        listener: tokio::net::TcpListener,
        extra: Router,
        signal: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let server = self.clone();
        let mut stopped = self.inner.shutdown.subscribe();
        axum::serve(listener, self.router().merge(extra))
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

enum ZoneRoute {
    Local(mpsc::Sender<ZoneCmd>),
    Remote(HostAddr),
    NotFound,
}

/// Where `zone_id` lives: here (activating it on demand) or on another host.
fn route_zone<Z>(inner: &Arc<Inner<Z>>, zone_id: &str) -> ZoneRoute
where
    Z: Zone,
    Z::Config: Send,
{
    if let Some(host) = inner
        .directory
        .as_ref()
        .and_then(|d| d.locate(zone_id))
        .filter(|h| h.id != inner.host_id)
    {
        return ZoneRoute::Remote(host);
    }
    match activate_local(inner, zone_id) {
        Some(tx) => ZoneRoute::Local(tx),
        None => ZoneRoute::NotFound,
    }
}

/// Sender of the local activation of `zone_id`, creating it on demand.
fn activate_local<Z>(inner: &Arc<Inner<Z>>, zone_id: &str) -> Option<mpsc::Sender<ZoneCmd>>
where
    Z: Zone,
    Z::Config: Send,
{
    if *inner.shutdown.borrow() {
        return None;
    }
    let mut zones = inner.zones.lock().expect("zones");
    if let Some(entry) = zones.get(zone_id).filter(|e| !e.tx.is_closed()) {
        return Some(entry.tx.clone());
    }
    let (tx, rx) = mpsc::channel(1024);
    let activation = inner.activations.fetch_add(1, Ordering::Relaxed) + 1;
    let world = inner.world.lock().expect("world").clone();
    zones.insert(
        zone_id.to_string(),
        ZoneEntry {
            tx: tx.clone(),
            activation,
        },
    );
    let facts = inner.facts.as_ref().and_then(|f| f(zone_id));
    let task = tokio::spawn(run_zone::<Z>(
        inner.clone(),
        zone_id.to_string(),
        facts,
        rx,
        activation,
        world,
    ));
    let mut tasks = inner.tasks.lock().expect("tasks");
    tasks.retain(|t| !t.is_finished());
    tasks.push(task);
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

fn random_token<Z: Zone>(inner: &Inner<Z>, label: &str) -> String {
    let n = inner.token_counter.fetch_add(1, Ordering::Relaxed);
    let seed = format!(
        "{label}|{}|{n}|{}",
        inner.host_id,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    inner.config.ticket_key.hmac(seed.as_bytes())[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Apply a transfer-record operation at the issuing host (local or peer).
async fn transfer_op<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    issuer: &str,
    op: TransferOp,
    nonce: &str,
) -> Result<(), String> {
    let reserve_for = inner.config.transfer_ttl.as_secs().max(1);
    if issuer == inner.host_id {
        return inner
            .transfers
            .lock()
            .expect("transfers")
            .apply(op, nonce, unix_now(), reserve_for)
            .map_err(str::to_string);
    }
    let host = inner
        .directory
        .as_ref()
        .and_then(|d| d.host(issuer))
        .ok_or_else(|| format!("unknown issuing host {issuer}"))?;
    let secret = inner
        .config
        .admin_secret
        .as_deref()
        .ok_or("no admin secret for peer calls")?;
    let body = json!({ "op": op.as_str(), "nonce": nonce });
    match peer::post_json(
        &host.admin_url,
        "/admin/transfer",
        secret,
        &body,
        inner.config.peer_timeout,
    )
    .await?
    {
        (200, _) => Ok(()),
        (_, reply) => Err(reply
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("transfer refused")
            .to_string()),
    }
}

/// Route a tier-up event to `parent` (local activation or peer host).
fn deliver_child<Z>(inner: &Arc<Inner<Z>>, parent: String, event: ChildEvent)
where
    Z: Zone,
    Z::Config: Send,
{
    match route_zone(inner, &parent) {
        ZoneRoute::Local(tx) => {
            if let Err(mpsc::error::TrySendError::Full(cmd)) =
                tx.try_send(ZoneCmd::ChildEvent(event))
            {
                tokio::spawn(async move {
                    let _ = tx.send(cmd).await;
                });
            }
        }
        ZoneRoute::Remote(host) => {
            let Some(secret) = inner.config.admin_secret.clone() else {
                return;
            };
            let timeout = inner.config.peer_timeout;
            let body = json!({ "zone": parent, "event": world::child_event_json(&event) });
            tokio::spawn(async move {
                let _ = peer::post_json(
                    &host.admin_url,
                    "/admin/child-event",
                    &secret,
                    &body,
                    timeout,
                )
                .await;
            });
        }
        ZoneRoute::NotFound => {}
    }
}

enum Exit {
    Hibernate(Vec<ZoneCmd>),
    Shutdown,
    Lost,
}

async fn acquire_claim<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    zone_id: &str,
    holder: &str,
) -> Result<ZoneClaim, String> {
    let deadline = Instant::now() + inner.config.activation_timeout;
    loop {
        let error = match inner
            .ownership
            .acquire(zone_id, holder, inner.config.ownership_ttl)
            .await
        {
            Ok(claim) => return Ok(claim),
            Err(OwnershipError::Held { holder }) => format!("zone is owned by {holder}"),
            Err(e) => format!("{e:?}"),
        };
        if Instant::now() >= deadline {
            return Err(error);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Activation failed: answer everything queued with `code` and unregister.
fn refuse_activation<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    zone_id: &str,
    activation: u64,
    mut rx: mpsc::Receiver<ZoneCmd>,
    code: u32,
    message: &str,
) {
    {
        let mut zones = inner.zones.lock().expect("zones");
        if zones
            .get(zone_id)
            .is_some_and(|e| e.activation == activation)
        {
            zones.remove(zone_id);
        }
        rx.close();
    }
    while let Ok(cmd) = rx.try_recv() {
        match cmd {
            ZoneCmd::Join { reply, .. } | ZoneCmd::Reconnect { reply, .. } => {
                let _ = reply.send(Err(JoinError {
                    code,
                    message: message.to_string(),
                }));
            }
            // Nothing runs here, so nothing to freeze.
            ZoneCmd::Freeze { ack, .. } => {
                let _ = ack.send(());
            }
            ZoneCmd::Deliver { reply, .. } => {
                let _ = reply.send(Err(InboxError::Inactive));
            }
            _ => {}
        }
    }
}

async fn renew_claim(
    ownership: Arc<dyn ZoneOwnership>,
    current: Arc<Mutex<ZoneClaim>>,
    every: Duration,
    ttl: Duration,
    lost: oneshot::Sender<()>,
) {
    loop {
        tokio::time::sleep(every).await;
        let claim = current.lock().expect("claim").clone();
        match ownership.renew(&claim, ttl).await {
            Ok(renewed) => *current.lock().expect("claim") = renewed,
            Err(OwnershipError::Backend(_)) => continue,
            Err(_) => {
                let _ = lost.send(());
                return;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_zone<Z>(
    inner: Arc<Inner<Z>>,
    zone_id: String,
    facts: Option<(FactQueue, JoinHandle<()>)>,
    mut rx: mpsc::Receiver<ZoneCmd>,
    activation: u64,
    world: WorldState,
) where
    Z: Zone,
    Z::Config: Send,
{
    // 1. Fenced ownership.
    let holder = format!("{}#{activation}", inner.host_id);
    let claim = match acquire_claim(&inner, &zone_id, &holder).await {
        Ok(claim) => claim,
        Err(message) => {
            return refuse_activation(&inner, &zone_id, activation, rx, error::NOT_FOUND, &message)
        }
    };
    // 2. Restore the hibernated snapshot, if any.
    let saved = match &inner.state_store {
        Some(store) => match store.load(&zone_id).await {
            Ok(saved) => saved,
            _ => {
                inner.ownership.release(&claim).await;
                return refuse_activation(
                    &inner,
                    &zone_id,
                    activation,
                    rx,
                    error::ZONE_UNAVAILABLE,
                    "zone state unavailable",
                );
            }
        },
        None => None,
    };
    // 3. The allocation record and the game's factory, bounded together.
    let created = tokio::time::timeout(inner.config.activation_timeout, async {
        let allocation = match &inner.allocations {
            Some(store) => store.load(&zone_id).await?,
            None => None,
        };
        (inner.factory.0)(ZoneActivation {
            zone_id: zone_id.clone(),
            activation: claim.token,
            allocation,
        })
        .await
    })
    .await;
    let zone_config = match created {
        Ok(Ok(Some(config))) => config,
        refused => {
            inner.ownership.release(&claim).await;
            let (code, message) = match refused {
                Ok(Ok(None)) => (error::NOT_FOUND, "unknown zone".to_string()),
                Ok(Err(e)) => (error::ZONE_UNAVAILABLE, e.to_string()),
                _ => (
                    error::ZONE_UNAVAILABLE,
                    "zone creation timed out".to_string(),
                ),
            };
            inner.metrics.record_activation_refused(code);
            return refuse_activation(&inner, &zone_id, activation, rx, code, &message);
        }
    };
    let mut host_config = inner.config.host.clone();
    if let Some(adjust) = &inner.host_config {
        adjust(&zone_id, &mut host_config);
    }
    host_config.content_version = world.version.clone();
    // Fencing tokens grow across activations and restarts: a restart-safe
    // epoch for fact keys (`Ctx::boot_epoch`).
    host_config.boot_epoch = claim.token;
    let token_inner = inner.clone();
    let issuer_inner = inner.clone();
    let generation = claim.token;
    let mut host = ZoneHost::<Z>::new(
        zone_id.clone(),
        host_config,
        inner.clock.clone(),
        zone_config,
    )
    .with_token_source(move || random_token(&token_inner, "token"))
    .with_transfer_issuer(move |request: &HandoffRequest| {
        let inner = &issuer_inner;
        let expires_at = unix_now() + inner.config.transfer_ttl.as_secs().max(1);
        let nonce = random_token(inner, "transfer");
        inner
            .transfers
            .lock()
            .expect("transfers")
            .issue(&nonce, expires_at);
        Some(inner.config.ticket_key.sign_transfer(&Transfer {
            claims: TicketClaims {
                principal: request.principal.clone(),
                zone: request.target_zone.clone(),
                expires_at,
                nonce,
                role: Role::Player,
                name: request.name.clone(),
                extensions: request.extensions.clone(),
            },
            source_zone: request.source_zone.clone(),
            issuer: inner.host_id.clone(),
            generation,
            carried: request.carried.clone(),
            departed_at_ms: request.departed_at_ms,
        }))
    });
    if let Some(saved) = &saved {
        host.restore(&saved.state, &saved.content_version);
    }
    let fact_worker = match facts {
        Some((queue, worker)) => {
            host = host.with_facts(queue);
            Some(worker)
        }
        None => None,
    };
    if let Some((_, notice)) = world.barrier {
        host.request_freeze(notice);
    }

    // 4. Keep the claim alive.
    let current = Arc::new(Mutex::new(claim));
    let (lost_tx, mut lost_rx) = oneshot::channel();
    let renewer = tokio::spawn(renew_claim(
        inner.ownership.clone(),
        current.clone(),
        inner.config.ownership_renew_every,
        inner.config.ownership_ttl,
        lost_tx,
    ));

    let mut conns: HashMap<ClientId, mpsc::Sender<ConnOut>> = HashMap::new();
    let mut freeze_acks: Vec<oneshot::Sender<()>> = Vec::new();
    let mut dormant_since: Option<Duration> = None;
    let mut shutdown = inner.shutdown.subscribe();
    let exit = loop {
        let report = host.advance();
        inner.metrics.record_advance(&host, &report);
        route(&mut host, &mut conns);
        forward_upward(&inner, &zone_id, &mut host);
        if host.is_frozen() {
            for ack in freeze_acks.drain(..) {
                let _ = ack.send(());
            }
        }
        let now = inner.clock.now();
        if host.tier() == Tier::Dormant {
            dormant_since.get_or_insert(now);
        } else {
            dormant_since = None;
        }
        let hibernate_at = match (inner.config.hibernate_after, dormant_since) {
            (Some(after), Some(since))
                if conns.is_empty()
                    && host.freeze_state() == FreezeState::Running
                    && host.facts_in_flight() == 0 =>
            {
                Some(since + after)
            }
            _ => None,
        };
        if hibernate_at.is_some_and(|at| now >= at) {
            if let Some(leftovers) = begin_hibernation(&inner, &zone_id, activation, &mut rx) {
                break Exit::Hibernate(leftovers);
            }
        }
        let wake = match (report.next_wake, hibernate_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let sleep_for = wake.map(|at| at.saturating_sub(now));
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
                break Exit::Shutdown;
            }
            _ = &mut lost_rx => {
                // Someone else owns the zone now: stop without persisting.
                host.dispose();
                route(&mut host, &mut conns);
                break Exit::Lost;
            }
            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    host.dispose();
                    route(&mut host, &mut conns);
                    break Exit::Shutdown;
                };
                handle_cmd(&mut host, &mut conns, &inner.metrics, &mut freeze_acks, cmd);
                // Drain whatever else is queued before ticking again.
                while let Ok(cmd) = rx.try_recv() {
                    handle_cmd(&mut host, &mut conns, &inner.metrics, &mut freeze_acks, cmd);
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
    };
    renewer.abort();
    let claim = current.lock().expect("claim").clone();
    match exit {
        Exit::Hibernate(leftovers) => {
            let snapshot = host.hibernate_state();
            let content_version = host.config().content_version.clone();
            host.dispose();
            host.drain_outbound();
            forward_upward(&inner, &zone_id, &mut host);
            if let (Some(state), Some(store)) = (snapshot, inner.state_store.clone()) {
                let id = zone_id.clone();
                let stored = StoredZoneState {
                    token: claim.token,
                    content_version,
                    state,
                };
                // A stale token means a newer owner saved already: ours is
                // dropped, but never silently (metrics + stderr).
                let saved = store.save(&id, &stored).await;
                let outcome = match &saved {
                    Ok(()) => "saved",
                    Err(StateError::Stale { .. }) => "stale",
                    Err(_) => "error",
                };
                inner.metrics.record_hibernation_save(outcome);
                if outcome != "saved" {
                    eprintln!(
                        "distributed_game: hibernation snapshot of zone {zone_id} (token {}) not saved: {saved:?}",
                        claim.token
                    );
                }
            }
            inner.ownership.release(&claim).await;
            drop(host);
            for cmd in leftovers {
                if matches!(cmd, ZoneCmd::Input { .. } | ZoneCmd::Disconnect { .. }) {
                    continue;
                }
                // Messages never activate a zone.
                if let ZoneCmd::Deliver { reply, .. } = cmd {
                    let _ = reply.send(Err(InboxError::Inactive));
                    continue;
                }
                if let Some(tx) = activate_local(&inner, &zone_id) {
                    let _ = tx.send(cmd).await;
                }
            }
        }
        Exit::Shutdown => {
            inner.ownership.release(&claim).await;
            drop(host);
        }
        Exit::Lost => drop(host),
    }
    // The fact worker commits everything still queued once the queue is
    // dropped; give it a bounded moment so nothing is lost on dispose.
    if let Some(worker) = fact_worker {
        let _ = tokio::time::timeout(inner.config.settle_timeout, worker).await;
    }
    let mut zones = inner.zones.lock().expect("zones");
    if zones
        .get(&zone_id)
        .is_some_and(|e| e.activation == activation)
    {
        zones.remove(&zone_id);
    }
    // Keep the series when a newer activation (after hibernation) runs.
    if !zones.contains_key(&zone_id) {
        inner.metrics.forget_zone(&zone_id);
    }
}

/// Unregister the zone and close its inbox atomically; None when commands
/// are pending (hibernation is retried later).
fn begin_hibernation<Z: Zone>(
    inner: &Arc<Inner<Z>>,
    zone_id: &str,
    activation: u64,
    rx: &mut mpsc::Receiver<ZoneCmd>,
) -> Option<Vec<ZoneCmd>> {
    let mut zones = inner.zones.lock().expect("zones");
    if !rx.is_empty() {
        return None;
    }
    if zones
        .get(zone_id)
        .is_some_and(|e| e.activation == activation)
    {
        zones.remove(zone_id);
    }
    rx.close();
    drop(zones);
    // Commands sent between the check and the close are re-routed to the
    // next activation (which waits for our claim release).
    let mut leftovers = Vec::new();
    while let Ok(cmd) = rx.try_recv() {
        leftovers.push(cmd);
    }
    Some(leftovers)
}

fn forward_upward<Z>(inner: &Arc<Inner<Z>>, zone_id: &str, host: &mut ZoneHost<Z>)
where
    Z: Zone,
    Z::Config: Send,
{
    let events = host.drain_upward();
    if events.is_empty() {
        return;
    }
    let Some(parent) = inner.topology.parent(zone_id) else {
        return;
    };
    for event in events {
        deliver_child(inner, parent.clone(), event);
    }
}

fn handle_cmd<Z: Zone>(
    host: &mut ZoneHost<Z>,
    conns: &mut HashMap<ClientId, mpsc::Sender<ConnOut>>,
    metrics: &GameMetrics,
    freeze_acks: &mut Vec<oneshot::Sender<()>>,
    cmd: ZoneCmd,
) {
    match cmd {
        ZoneCmd::Join {
            claims,
            transfer,
            capabilities,
            content_version,
            out,
            reply,
        } => {
            let result = match &transfer {
                Some(transfer) => host.join_transfer(transfer, capabilities, &content_version),
                None => host.join(&claims, capabilities, &content_version),
            };
            if let Ok(joined) = &result {
                conns.insert(joined.client, out);
            }
            let _ = reply.send(result);
        }
        ZoneCmd::Reconnect {
            token,
            capabilities,
            content_version,
            out,
            reply,
        } => {
            let result = host.reconnect_with_version(&token, capabilities, &content_version);
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
            let frozen_before = host.inputs_dropped_frozen();
            let outcome = host.push_input(client, seq, client_tick, &payload);
            if host.inputs_dropped_frozen() > frozen_before {
                metrics.record_frozen_input(host.zone_id());
            } else {
                metrics.record_input(host.zone_id(), outcome);
            }
        }
        ZoneCmd::Disconnect { client, graceful } => {
            conns.remove(&client);
            host.disconnect(client, graceful);
        }
        ZoneCmd::Content { client, version } => {
            host.content_patched(client, &version);
        }
        ZoneCmd::ChildEvent(event) => host.deliver_child_event(event),
        ZoneCmd::Deliver { inbound, reply } => {
            let _ = reply.send(inbound.decode::<Z::Message>().map(|m| host.deliver(m)));
        }
        ZoneCmd::Freeze { notice, ack } => {
            if host.request_freeze(notice) == FreezeState::Frozen {
                let _ = ack.send(());
            } else {
                freeze_acks.push(ack);
            }
        }
        ZoneCmd::Commit {
            version,
            notes,
            mode,
        } => {
            host.set_content_version(version.clone());
            if host.freeze_state() != FreezeState::Running {
                host.resume(Notice {
                    reason: "content_update".into(),
                    version,
                    notes,
                });
            }
            // Hard reloads every stale client; Rolling reloads the ones
            // already inside a safe-room gate (they will not walk into one).
            let gate = host.zone().is_gate()
                && matches!(host.config().stale_policy, StalePolicy::SafeRoom { .. });
            if mode == BarrierMode::Hard || gate {
                host.reload_stale_players();
            }
            freeze_acks.clear();
        }
        ZoneCmd::Abort { notice } => {
            host.cancel_freeze(notice);
            freeze_acks.clear();
        }
        ZoneCmd::Inspect(f) => f(&*host as &dyn Any),
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
    sim: watch::Receiver<Option<NetSim>>,
    rng: ZoneRng,
    queue: VecDeque<(Instant, T)>,
    last: Instant,
}

impl<T> DelayLine<T> {
    fn new(sim: watch::Receiver<Option<NetSim>>, seed: u64) -> Self {
        Self {
            sim,
            rng: ZoneRng::from_seed(seed),
            queue: VecDeque::new(),
            last: Instant::now(),
        }
    }

    /// Returns the item immediately when no simulation is in effect and
    /// nothing is still queued (order is preserved when the simulation is
    /// switched off while messages are in flight).
    fn push(&mut self, item: T) -> Option<T> {
        let sim = *self.sim.borrow();
        let delay = match sim {
            Some(sim) => sim.delay(&mut self.rng),
            None if self.queue.is_empty() => return Some(item),
            None => Duration::ZERO,
        };
        let at = (Instant::now() + delay).max(self.last);
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
    Content(String),
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
    // Placement: another host owns this zone -> Redirect (ticket untouched).
    let first_route = route_zone(&inner, &zone_id);
    match &first_route {
        ZoneRoute::Remote(host) => {
            let url = match &inner.config.redirect_url {
                Some(map) => (map.0)(host, &zone_id),
                None => host.zone_url(&zone_id),
            };
            let redirect = ServerMessage::Redirect {
                zone_id: zone_id.clone(),
                url,
            }
            .encode(&())
            .expect("redirect encodes");
            let _ = socket.send(Message::Binary(redirect.into())).await;
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
        ZoneRoute::NotFound => return reject(socket, error::NOT_FOUND, "unknown zone").await,
        ZoneRoute::Local(_) => {}
    }
    let (out_tx, mut out_rx) = mpsc::channel(cfg.outbound_queue.max(1));
    let (reply_tx, reply_rx) = oneshot::channel();
    // Transfer record to settle once the zone answered: (issuer, nonce).
    let mut reserved: Option<(String, String)> = None;
    // A failed handoff returning to its source presents a ticket whose nonce
    // the target may have recorded on this same host; the issuer's transfer
    // registry (`Return`) keeps it single use, so it gets its own replay key.
    let mut returning = false;
    let cmd = if hello.reconnect_token.is_empty() {
        let now = unix_now();
        let verified = match verify_hello_ticket(cfg, &hello.ticket, &zone_id, now) {
            Ok(verified) => verified,
            Err(e) => return reject(socket, error::UNAUTHORIZED, format!("{e:?}")).await,
        };
        let (claims, transfer) = match verified {
            VerifiedTicket::Join(claims) => {
                if claims.zone != zone_id {
                    return reject(socket, error::UNAUTHORIZED, "ticket is for another zone").await;
                }
                (claims, None)
            }
            VerifiedTicket::Transfer(mut transfer) => {
                let nonce = transfer.claims.nonce.clone();
                if transfer.claims.zone == zone_id {
                    // Arrival at the target: reserve with the issuer first.
                    if let Err(e) =
                        transfer_op(&inner, &transfer.issuer, TransferOp::Reserve, &nonce).await
                    {
                        return reject(socket, error::UNAUTHORIZED, e).await;
                    }
                    reserved = Some((transfer.issuer.clone(), nonce));
                } else if transfer.source_zone == zone_id {
                    // Failed handoff returning to its source.
                    if let Err(e) =
                        transfer_op(&inner, &transfer.issuer, TransferOp::Return, &nonce).await
                    {
                        return reject(socket, error::UNAUTHORIZED, e).await;
                    }
                    returning = true;
                    let never_reached =
                        std::mem::replace(&mut transfer.claims.zone, zone_id.clone());
                    transfer.source_zone = never_reached;
                } else {
                    return reject(socket, error::UNAUTHORIZED, "ticket is for another zone").await;
                }
                (transfer.claims.clone(), Some(transfer))
            }
        };
        let replay_key = if returning {
            format!("return:{}", claims.nonce)
        } else {
            claims.nonce.clone()
        };
        let replay = inner.nonces.lock().expect("nonces").check_and_insert(
            &replay_key,
            claims.expires_at,
            now,
        );
        if let Err(e) = replay {
            if let Some((issuer, nonce)) = reserved.take() {
                let _ = transfer_op(&inner, &issuer, TransferOp::Release, &nonce).await;
            }
            return reject(socket, error::UNAUTHORIZED, format!("{e:?}")).await;
        }
        ZoneCmd::Join {
            claims,
            transfer,
            capabilities: hello.capabilities,
            content_version: hello.content_version,
            out: out_tx,
            reply: reply_tx,
        }
    } else {
        ZoneCmd::Reconnect {
            token: hello.reconnect_token,
            capabilities: hello.capabilities,
            content_version: hello.content_version,
            out: out_tx,
            reply: reply_tx,
        }
    };
    // A zone that just hibernated closes its inbox; retry on the next
    // activation (which restores the snapshot first).
    let mut zone = match first_route {
        ZoneRoute::Local(tx) => tx,
        _ => unreachable!("handled above"),
    };
    let mut pending = cmd;
    let mut attempts = 0;
    loop {
        match zone.send(pending).await {
            Ok(()) => break,
            Err(mpsc::error::SendError(cmd)) => {
                attempts += 1;
                match activate_local(&inner, &zone_id) {
                    Some(tx) if attempts < 4 => {
                        zone = tx;
                        pending = cmd;
                    }
                    _ => {
                        if let Some((issuer, nonce)) = reserved.take() {
                            let _ = transfer_op(&inner, &issuer, TransferOp::Release, &nonce).await;
                        }
                        return reject(socket, error::NOT_FOUND, "zone stopped").await;
                    }
                }
            }
        }
    }
    let result = reply_rx.await;
    if let Some((issuer, nonce)) = reserved.take() {
        if matches!(result, Ok(Ok(_))) {
            let inner = inner.clone();
            tokio::spawn(async move {
                let _ = transfer_op(&inner, &issuer, TransferOp::Confirm, &nonce).await;
            });
        } else {
            // Release before the client hears the refusal: it may present
            // the same ticket to its source right away, and the issuer only
            // accepts that return once the reservation is released.
            let _ = transfer_op(&inner, &issuer, TransferOp::Release, &nonce).await;
        }
    }
    let client = match result {
        Ok(Ok(joined)) => joined.client,
        Ok(Err(e)) => return reject(socket, e.code, e.message).await,
        Err(_) => return reject(socket, error::NOT_FOUND, "zone stopped").await,
    };

    // 2. Session loop.
    let seed = u64::from(client.0) ^ unix_now();
    let mut outbound: DelayLine<ConnOut> = DelayLine::new(inner.net_sim.subscribe(), seed);
    let mut inbound: DelayLine<Inbound> = DelayLine::new(inner.net_sim.subscribe(), !seed);
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
                // The zone closed this connection and already forgot the client.
                Ok(false) => {
                    notify_zone = false;
                    break 'session;
                }
                // The socket died under a write: the zone must still learn
                // the connection is gone (grace + ConnectionLost).
                Err(_) => break 'session,
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
                    match send_out(&mut socket, out).await {
                        Ok(true) => {}
                        Ok(false) => {
                            notify_zone = false;
                            break;
                        }
                        Err(_) => break,
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
                    Ok(ClientMessage::Content { content_version }) => Inbound::Content(content_version),
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

/// Transfer tickets always use the server key; join tickets go through the
/// configured [`TicketVerifier`] and are then checked for zone, expiry and a
/// non-empty replay key.
fn verify_hello_ticket(
    cfg: &ServerConfig,
    ticket: &str,
    zone_id: &str,
    now: u64,
) -> Result<VerifiedTicket, TicketError> {
    let verified = match &cfg.ticket_verifier {
        None => cfg.ticket_key.verify_ticket(ticket, now)?,
        Some(custom) => match cfg.ticket_key.verify_ticket(ticket, now) {
            Ok(VerifiedTicket::Transfer(transfer)) => VerifiedTicket::Transfer(transfer),
            _ => VerifiedTicket::Join(custom.0.verify(ticket, zone_id, now)?),
        },
    };
    if let VerifiedTicket::Join(claims) = &verified {
        if claims.principal.is_empty() {
            return Err(TicketError::MissingClaim("sub"));
        }
        if claims.replay_key().is_empty() {
            return Err(TicketError::MissingClaim("nonce"));
        }
        if claims.expires_at <= now {
            return Err(TicketError::Expired);
        }
    }
    let extensions = match &verified {
        VerifiedTicket::Join(claims) => &claims.extensions,
        VerifiedTicket::Transfer(transfer) => &transfer.claims.extensions,
    };
    crate::claims::validate_extensions(cfg.ticket_extensions.as_ref(), extensions)
        .map_err(|e| TicketError::Invalid(e.to_string()))?;
    Ok(verified)
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
        Inbound::Content(version) => zone
            .send(ZoneCmd::Content { client, version })
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
    fn live_net_sim_changes_preserve_order() {
        let (tx, rx) = watch::channel(None);
        let mut line = DelayLine::new(rx, 1);
        assert_eq!(line.push(1), Some(1), "no simulation: immediate");
        tx.send_replace(Some(NetSim::new(
            Duration::from_millis(200),
            Duration::ZERO,
        )));
        assert_eq!(line.push(2), None, "simulation switched on: delayed");
        let due = line.next_due().expect("queued");
        tx.send_replace(None);
        assert_eq!(line.push(3), None, "switched off: queues behind 2");
        assert_eq!(line.pop_due(), None);
        assert_eq!(line.queue.len(), 2);
        assert!(line.queue.back().unwrap().0 >= due);
        line.queue.clear();
        assert_eq!(line.push(4), Some(4), "queue drained: immediate again");
    }
}
