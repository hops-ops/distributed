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
//! - Admission ([`crate::admission`], shared with the celld zone host): the
//!   first frame must be Hello within `admission.hello_timeout`. A fresh
//!   join needs a ticket verified with the admission key or verifier
//!   (signature, expiry, zone == route zone, extensions, single-use nonce).
//!   The principal comes **only** from the verified ticket. A reconnect
//!   presents the Welcome's reconnect token within the host's
//!   `reconnect_grace`, and only reaches a running zone. Nothing is
//!   activated before the Hello is admitted, and at most `max_zones` zones
//!   run at once (`Error(ZONE_UNAVAILABLE)` beyond).
//! - Every client message after the Welcome but Leave goes through the
//!   connection's [`InputGate`]: excess messages are dropped (one
//!   `Error(RATE_LIMITED)` per second) and a flood is kicked. A zone handles
//!   at most a bounded batch of queued commands between two looks at its
//!   clock.
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
//!   ([`ZoneOwnership`], default [`LocalOwnership`]) and renews it. Without a
//!   successful renewal the zone fences itself at `ttl - renew_every` after
//!   the last one began (renewals time out at `renew_every / 2`); fenced or
//!   `Lost`, it stops at once: nothing more is persisted, queued facts are
//!   dropped uncommitted and clients are kicked. The fact worker obeys the
//!   same fence (no commit starts past it, one in progress is abandoned),
//!   and the claim is released only after the worker is gone.
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
mod peer;
pub use crate::ticket;
mod connection;
pub mod transfer;
pub mod world;
mod zone_task;

use connection::upgrade;
use zone_task::{run_zone, ZONE_ENVELOPE_PATH};

use std::any::Any;
use std::collections::HashMap;
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
/// The hosts' shared secret for peer calls ([`ServerConfig::admin_secret`]).
pub use distributed::microsvc::cell_host::InternalHttpSecret;
pub use inbox::{InboxError, ZoneInboxClient, ZONE_MESSAGE_PATH};
pub use ticket::{
    decode_hs256, NonceCache, SharedTicketVerifier, TicketError, TicketKey, TicketVerifier,
    VerifiedTicket,
};
pub use transfer::{TransferOp, TransferRegistry};
pub use world::{
    AbortNotes, BarrierParticipant, RemoteHost, RolloutError, RolloutReport, WorldCoordinator,
};

use crate::admission::{admit, AdmissionConfig, Admitted, Gate, InputGate};
use crate::clock::{Clock, SystemClock};
use crate::facts::{fact_channel, spawn_fact_worker, FactQueue, FactSink, RetryPolicy};
use crate::handoff::{HandoffRequest, Transfer};
use crate::host::{HostConfig, JoinError, Joined, Outbound, ZoneHost};
use crate::metrics::GameMetrics;
use crate::protocol::Role;
use crate::protocol::{error, kick, ClientMessage, Notice, ServerMessage};
use crate::world::{
    BarrierMode, ChildEvent, DottedTopology, FreezeState, HostAddr, LocalOwnership, OwnershipError,
    StalePolicy, StateError, StoredZoneState, ZoneActivation, ZoneAllocationStore, ZoneClaim,
    ZoneCreateError, ZoneDirectory, ZoneOwnership, ZoneStateStore, ZoneTopology,
};
use crate::zone::{ClientId, TicketClaims, Tier, Zone};
use crate::zone_message::ZoneEnvelope;

use crate::netsim::DelayLine;
pub use crate::netsim::NetSim;

/// Server settings.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Settings for every hosted zone.
    pub host: HostConfig,
    /// Tickets, Hello deadline and the per-connection message rate
    /// ([`crate::admission`]). Its key also signs transfer tickets and
    /// server tokens.
    pub admission: AdmissionConfig,
    /// Zones this server runs at once (activating, running or
    /// hibernating); an activation past it is refused with
    /// `Error(ZONE_UNAVAILABLE)`.
    pub max_zones: usize,
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
    /// Claim renewal period (well below `ownership_ttl`). Each renewal may
    /// take half of it; a zone whose renewals keep failing fences itself
    /// (stops ticking and committing facts) `ownership_ttl -
    /// ownership_renew_every` after its last successful renewal began, so
    /// it is gone before its claim expires and another host may take it.
    pub ownership_renew_every: Duration,
    /// How long an activation waits for a held claim (e.g. the previous
    /// activation still saving its snapshot) before refusing. The default
    /// equals the default `ownership_ttl`, so a claim left by a crashed
    /// incarnation expires within one activation's wait; keep it at least
    /// `ownership_ttl`.
    pub activation_timeout: Duration,
    /// Transfer ticket lifetime (and target reservation bound).
    pub transfer_ttl: Duration,
    /// After a zone is disposed, how long to wait for its fact worker to
    /// commit the facts still queued (its claim is renewed meanwhile and
    /// released after); whatever is unsettled after that, or when the
    /// ownership fence passes first, is dropped (native facts are at most
    /// once, see [`GameServer::with_fact_sink`]).
    pub settle_timeout: Duration,
    /// The hosts' shared internal secret: enables the `/admin/*` routes
    /// (which require it in [`CELL_INTERNAL_SECRET_HEADER`]) and peer calls.
    ///
    /// [`CELL_INTERNAL_SECRET_HEADER`]: distributed::microsvc::cell_host::CELL_INTERNAL_SECRET_HEADER
    pub admin_secret: Option<InternalHttpSecret>,
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

    /// Defaults with the given ticket key.
    pub fn new(ticket_key: TicketKey) -> Self {
        Self {
            host: HostConfig::default(),
            admission: AdmissionConfig::new(ticket_key),
            max_zones: 4096,
            outbound_queue: 512,
            max_frame_bytes: 64 * 1024,
            net_sim: None,
            metrics_zone_labels: 64,
            hibernate_after: None,
            ownership_ttl: Duration::from_secs(15),
            ownership_renew_every: Duration::from_secs(5),
            activation_timeout: Duration::from_secs(15),
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
    /// An observer's `Focus`.
    Focus {
        client: ClientId,
        x: f32,
        y: f32,
    },
    /// A zone inbox message (decoded into `Z::Message` on the zone task).
    Deliver {
        inbound: inbox::WireInbound,
        reply: oneshot::Sender<Result<crate::inbox::Delivered, InboxError>>,
    },
    /// A message from another zone ([`crate::zone_message`]).
    ZoneMessage(ZoneEnvelope),
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
/// Spawns one activation's fact worker, bound by that activation's fence.
type FactsFactory = dyn Fn(watch::Receiver<Instant>) -> (FactQueue, JoinHandle<()>) + Send + Sync;
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

impl WorldState {
    fn is_current(&self, barrier: u64) -> bool {
        self.barrier.as_ref().is_some_and(|(id, _)| *id == barrier)
    }
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
    /// Tells this instance's ownership claims apart from a previous
    /// incarnation's (holder `"{host_id}#{incarnation}#{activation}"`).
    incarnation: String,
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
                incarnation: incarnation(),
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
    /// at once, so barriers and joins already reach it). False while
    /// shutting down or at `max_zones`; an unknown zone unregisters itself
    /// once its factory answers.
    pub fn begin_activation(&self, zone_id: &str) -> bool {
        activate_local(&self.inner, zone_id).is_ok()
    }

    /// Activate `zone_id` locally (as if a client asked for it) and wait
    /// until it runs. False when the factory knows no such zone, creation
    /// failed, the server runs `max_zones` already or is shutting down.
    pub async fn activate(&self, zone_id: &str) -> bool {
        if activate_local(&self.inner, zone_id).is_err() {
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
    /// clients. Only for the current barrier: a commit for another one (a
    /// delayed message) changes nothing and answers false.
    pub async fn commit_barrier(
        &self,
        barrier: u64,
        version: &str,
        notes: &str,
        mode: BarrierMode,
    ) -> bool {
        let targets: Vec<mpsc::Sender<ZoneCmd>> = {
            let zones = self.inner.zones.lock().expect("zones");
            let mut world = self.inner.world.lock().expect("world");
            if !world.is_current(barrier) {
                return false;
            }
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
        true
    }

    /// Abort the current barrier: every zone resumes on the old version.
    /// An abort for another barrier changes nothing and answers false.
    pub async fn abort_barrier(&self, barrier: u64, notes: &str) -> bool {
        let (targets, version): (Vec<mpsc::Sender<ZoneCmd>>, String) = {
            let zones = self.inner.zones.lock().expect("zones");
            let mut world = self.inner.world.lock().expect("world");
            if !world.is_current(barrier) {
                return false;
            }
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
        true
    }

    /// Commit every zone's facts through `sink` (one worker per zone).
    /// Must be called before the server is cloned or served.
    ///
    /// **At most once across a crash.** Each zone's queue and backlog live
    /// in this process: while it runs, every fact is committed exactly once
    /// (retried per `policy` under its idempotent key), but a crash, a
    /// kill, a disposal that outlives [`ServerConfig::settle_timeout`] or a
    /// lost ownership drops the facts not yet settled. Commits are bound by
    /// the zone's ownership fence ([`ServerConfig::ownership_renew_every`]):
    /// none starts after it, and one still running when it passes is
    /// abandoned and settles [`FactError::Permanent`](crate::FactError). The celld zone
    /// object spools its facts to storage instead (at least once).
    pub fn with_fact_sink<S: FactSink>(
        mut self,
        sink: S,
        capacity: usize,
        policy: RetryPolicy,
    ) -> Self {
        let sink = Arc::new(sink);
        let inner = Arc::get_mut(&mut self.inner).expect("configure before sharing the server");
        let concurrency = inner.config.max_concurrent_commits;
        inner.facts = Some(Box::new(move |fence| {
            let (queue, feed) = fact_channel(capacity, policy);
            let feed = feed.with_max_concurrent_commits(concurrency);
            let worker = spawn_fact_worker(feed, zone_task::FencedSink::new(sink.clone(), fence));
            (queue, worker)
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
                .route(ZONE_ENVELOPE_PATH, post(world::admin_zone_envelope::<Z>))
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

/// The host the directory places `zone_id` on, when that is another host
/// (a lookup only: nothing is activated or recorded).
fn remote_owner<Z: Zone>(inner: &Inner<Z>, zone_id: &str) -> Option<HostAddr> {
    inner
        .directory
        .as_ref()
        .and_then(|d| d.locate(zone_id))
        .filter(|h| h.id != inner.host_id)
}

/// Sender of the running (or activating) local activation of `zone_id`.
fn running_zone<Z: Zone>(inner: &Inner<Z>, zone_id: &str) -> Option<mpsc::Sender<ZoneCmd>> {
    let zones = inner.zones.lock().expect("zones");
    zones
        .get(zone_id)
        .filter(|e| !e.tx.is_closed())
        .map(|e| e.tx.clone())
}

/// Sender of the local activation of `zone_id`, creating it on demand;
/// `Err` (why not) while shutting down or at `max_zones`.
fn activate_local<Z>(
    inner: &Arc<Inner<Z>>,
    zone_id: &str,
) -> Result<mpsc::Sender<ZoneCmd>, &'static str>
where
    Z: Zone,
    Z::Config: Send,
{
    if *inner.shutdown.borrow() {
        return Err("server shutting down");
    }
    let mut zones = inner.zones.lock().expect("zones");
    if let Some(entry) = zones.get(zone_id).filter(|e| !e.tx.is_closed()) {
        return Ok(entry.tx.clone());
    }
    if zones.len() - usize::from(zones.contains_key(zone_id)) >= inner.config.max_zones {
        return Err("server at zone capacity");
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
    let task = tokio::spawn(run_zone::<Z>(
        inner.clone(),
        zone_id.to_string(),
        rx,
        activation,
        world,
    ));
    let mut tasks = inner.tasks.lock().expect("tasks");
    tasks.retain(|t| !t.is_finished());
    tasks.push(task);
    Ok(tx)
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
    inner.config.admission.key.hmac(seed.as_bytes())[..16]
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
        .as_ref()
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

/// A nonce telling this server instance apart from every other
/// incarnation of the same host id (start time, process id and a
/// per-process count): a host that crashed and restarted inside the claim
/// TTL must not present its dead incarnation's holder, or it would get
/// that claim and its fencing token (the zones' `boot_epoch`) back.
fn incarnation() -> String {
    static STARTED: AtomicU64 = AtomicU64::new(0);
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros());
    let n = STARTED.fetch_add(1, Ordering::Relaxed);
    format!("{micros:x}.{:x}.{n}", std::process::id())
}
