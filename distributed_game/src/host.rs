//! [`ZoneHost`]: drives one [`Zone`] — lifecycle, fixed timestep, activity
//! tiers, inputs, timers, facts and replication.
//!
//! The host is synchronous and transport-agnostic. Everything from outside
//! that changes the zone is one [`Stimulus`] applied with
//! [`apply`](ZoneHost::apply), which reads the injected [`Clock`] once (the
//! zone sees that time throughout) and, with [`HostConfig::record`], appends
//! it to the host's [`Recording`](crate::replay::Recording). A driver (the
//! `net` server, a celld zone object, a test, a custom loop) calls, as
//! shorthand for `apply`:
//!
//! - [`join`](ZoneHost::join) / [`reconnect`](ZoneHost::reconnect) /
//!   [`disconnect`](ZoneHost::disconnect) for membership,
//! - [`push_input`](ZoneHost::push_input) for wire inputs,
//! - [`advance`](ZoneHost::advance) whenever time may have passed (it runs
//!   due fixed steps and patches, and reports when it next wants to be
//!   called),
//!
//! and [`drain_outbound`](ZoneHost::drain_outbound) to collect encoded
//! frames (outputs are never stimuli).
//!
//! **Cadence.** Ticks run at `tick_rate` with an accumulator and catch-up cap
//! ([`FixedTimestep`]). Patches (Deltas) run at `patch_rate` on the clock,
//! independent of the tick rate.
//!
//! **Tiers.** Hot while any client is connected, any player is inside its
//! reconnect grace window, or [`Zone::has_pending_work`] is true. Otherwise
//! Idle: ticks at `idle_tick_rate` (or not at all when `None`). After
//! `dormant_after` in Idle with no unsettled facts and
//! [`Zone::wants_background`] false the zone becomes Dormant: no ticks,
//! simulation time stops (timers pause) and the zone is eligible for
//! hibernation. Joins, reconnects and inputs wake it to Hot. Transitions are
//! reported in [`AdvanceReport::transitions`] and via
//! [`Zone::on_tier_changed`].

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use crate::authoring::GameEvent;
use crate::clock::Clock;
use crate::codec::Wire;
use crate::collide::{Colliders, Ray, RayHit};
use crate::facts::{EmitError, Fact, FactCommand, FactQueue, FactSettlement};
use crate::handoff::{HandoffError, HandoffRequest, Transfer, TransferIssuer, MAX_CARRIED_BYTES};
use crate::inbox::{Delivered, ZoneInbound};
use crate::input::{ConsumePolicy, InputBuffer, PushOutcome, SequencedInput};
use crate::interest::{InterestConfig, View};
use crate::lifecycle::{
    ConnectionLost, JoinVia, LeftReason, LostCause, PlayerDisconnected, PlayerJoined, PlayerLeft,
    PlayerReconnected,
};
use crate::protocol::{error, feature, kick, Notice, Role, ServerMessage, Welcome};
use crate::replay::{Entry, Observed, Recording};
use crate::replication::{ClientReplica, DeltaStats, EncodedWorld, WorldView};
use crate::rewind::{Rewind, RewindConfig, ShotTiming};
use crate::rng::ZoneRng;
use crate::schema::EntitySchema;
use crate::stamp::FactStamp;
use crate::timers::{TimerId, Timers};
use crate::timestep::{rate_to_step, FixedTimestep};
use crate::world::{ChildEvent, FreezeState, StalePolicy};
use crate::zone::{
    Admission, ClientId, JoinRequest, LeaveReason, ObserverId, ObserverInfo, PlayerId, PlayerInfo,
    TicketClaims, Tier, Viewer, Zone,
};
use crate::zone_message::{SeenIds, VerifiedPrincipal, ZoneEnvelope, ZoneMessageError, ZoneSender};

/// Host tuning.
#[derive(Debug, Clone, PartialEq)]
pub struct HostConfig {
    /// Hot ticks per second.
    pub tick_rate: u32,
    /// Deltas per second (independent of `tick_rate`).
    pub patch_rate: u32,
    /// Idle ticks per second; `None` = no ticks while Idle.
    pub idle_tick_rate: Option<u32>,
    /// Time in Idle before Dormant.
    pub dormant_after: Duration,
    /// Max fixed steps per `advance` before backlog is dropped.
    pub max_catch_up: u32,
    /// Player capacity.
    pub max_players: usize,
    /// Observer capacity.
    pub max_observers: usize,
    /// Per-player input buffer bound.
    pub input_capacity: usize,
    /// How long a dropped player is kept for reconnect.
    pub reconnect_grace: Duration,
    /// Per-client create+update bytes per Delta.
    pub byte_budget: usize,
    /// Radius of views the zone has not set explicitly (centered at origin).
    pub default_view_radius: f32,
    /// Interest grid settings.
    pub interest: InterestConfig,
    /// Content version; when non-empty, clients must match it exactly.
    pub content_version: String,
    /// World seed; the zone RNG derives from it and the zone id.
    pub world_seed: u64,
    /// Lag-compensation history ([`Ctx::rewind`]).
    pub rewind: RewindConfig,
    /// Admission of clients whose content version differs from
    /// `content_version` (see [`StalePolicy`]).
    pub stale_policy: StalePolicy,
    /// Epoch of this activation of the zone ([`Ctx::boot_epoch`]): must
    /// differ between activations and grow across restarts so fact keys
    /// built from it never collide with an earlier run's. `0` (default) =
    /// Unix time in microseconds when the host is created. The `net` server
    /// sets it to the activation's fencing token.
    pub boot_epoch: u64,
    /// Remote config version the zone runs (opaque; e.g. `"combat@12"`):
    /// stamped on every fact ([`FactStamp::config_version`]). Change it at
    /// runtime with [`ZoneHost::set_config_version`] /
    /// [`Ctx::set_config_version`].
    pub config_version: String,
    /// Emit the player lifecycle facts ([`crate::lifecycle`]: joined,
    /// connection lost, reconnected, disconnected, left) on the zone's fact
    /// queue, bound to each player's verified principal. Off by default.
    pub lifecycle_facts: bool,
    /// Keep a [`Recording`](crate::replay::Recording) of every
    /// [`Stimulus`] from creation on ([`ZoneHost::recording`]), for
    /// [`replay`](crate::replay::replay). Costs a state hash per tick and
    /// memory for every stimulus. Off by default.
    pub record: bool,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            tick_rate: 30,
            patch_rate: 20,
            idle_tick_rate: Some(1),
            dormant_after: Duration::from_secs(30),
            max_catch_up: 5,
            max_players: 64,
            max_observers: 16,
            input_capacity: 32,
            reconnect_grace: Duration::from_secs(15),
            byte_budget: 4096,
            default_view_radius: 64.0,
            interest: InterestConfig::default(),
            content_version: String::new(),
            world_seed: 0,
            rewind: RewindConfig::default(),
            stale_policy: StalePolicy::Strict,
            boot_epoch: 0,
            config_version: String::new(),
            lifecycle_facts: false,
            record: false,
        }
    }
}

/// Something for the transport to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outbound {
    /// Send this binary frame.
    Frame {
        /// Destination.
        client: ClientId,
        /// Encoded server message.
        frame: Vec<u8>,
    },
    /// Close the connection after previously queued frames.
    Close {
        /// Connection to close.
        client: ClientId,
    },
}

/// Result of [`ZoneHost::advance`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AdvanceReport {
    /// Fixed steps simulated.
    pub ticks: u32,
    /// Backlog dropped by the catch-up cap.
    pub dropped: Duration,
    /// Whether Deltas were produced.
    pub patched: bool,
    /// Tier transitions since the previous report.
    pub transitions: Vec<(Tier, Tier)>,
    /// Clock time at which the host next wants `advance` (None = only on
    /// external events, i.e. Dormant).
    pub next_wake: Option<Duration>,
    /// Fixed steps dropped by the catch-up cap (`dropped` / step).
    pub dropped_ticks: u32,
    /// Wall time spent in each simulated tick (metrics only; never fed back
    /// into the simulation).
    pub tick_durations: Vec<Duration>,
    /// Replicated entity count when a patch ran.
    pub entities: Option<usize>,
    /// One entry per Delta frame built.
    pub frames: Vec<FrameStats>,
    /// Entity records encoded by the patches
    /// ([`EncodedWorld::encodes`]: shared across clients).
    pub encodes: usize,
    /// Settlements delivered to the zone, with the tick they were delivered on.
    pub settled: Vec<(u64, FactSettlement)>,
    /// `(tick, state hash)` after each tick, when
    /// [`ZoneHost::set_tick_hashing`] is on.
    pub tick_hashes: Vec<(u64, u64)>,
}

/// One Delta frame built by a patch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameStats {
    /// Destination.
    pub client: ClientId,
    /// Encoded frame size.
    pub bytes: usize,
    /// Replication counters.
    pub stats: DeltaStats,
}

/// Admission failure (code from [`protocol::error`](crate::protocol::error)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinError {
    /// Error code.
    pub code: u32,
    /// Message.
    pub message: String,
}

impl JoinError {
    fn new(code: u32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Successful admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Joined {
    /// New connection id.
    pub client: ClientId,
    /// Player or observer it is bound to.
    pub viewer: Viewer,
}

/// Runs one zone. See the module docs.
pub struct ZoneHost<Z: Zone> {
    zone: Z,
    core: Core<Z>,
    schema: EntitySchema,
    entities: crate::Entities<Z>,
    hot: FixedTimestep,
    idle: Option<FixedTimestep>,
    last_advance: Duration,
    idle_since: Duration,
    next_patch: Duration,
    transitions: Vec<(Tier, Tier)>,
    frozen: bool,
    freeze_request: Option<Notice>,
    disposed: bool,
    token_source: Box<dyn FnMut() -> String + Send>,
    tape: Option<Tape>,
    pending: AdvanceReport,
    hash_ticks: bool,
    /// The last patch's world: the next one shares its unchanged entities
    /// and encodes each change once for every client.
    last_world: Option<EncodedWorld>,
    /// The view each patch is projected into (emptied, not reallocated).
    view: WorldView,
}

macro_rules! hook {
    ($host:ident . $method:ident ( $($arg:expr),* )) => {
        $host.zone.$method(&mut Ctx { core: &mut $host.core } $(, $arg)*)
    };
}

mod content;
mod ctx;
mod membership;
mod outbound;
mod stimulus;
mod tick;

pub(crate) use ctx::Core;
pub use ctx::Ctx;
use ctx::{ClientSlot, ObserverSlot, PlayerSlot};
use stimulus::Tape;
pub use stimulus::{Outcome, Stimulus};

impl<Z: Zone> ZoneHost<Z> {
    /// Create the zone (`on_create`). Starts Hot.
    pub fn new(
        zone_id: impl Into<String>,
        config: HostConfig,
        clock: Arc<dyn Clock>,
        zone_config: Z::Config,
    ) -> Self {
        let zone_id = zone_id.into();
        let now = clock.now();
        let mut config = config;
        if config.boot_epoch == 0 {
            config.boot_epoch = unix_micros();
        }
        let mut core = Core {
            rng: ZoneRng::for_zone(config.world_seed, &zone_id),
            zone_id,
            now,
            unix_ms: clock.unix_ms(),
            clock,
            timers: Timers::default(),
            tick: 0,
            sim_time: Duration::ZERO,
            dt: rate_to_step(config.tick_rate),
            dropped_ticks: 0,
            tier: Tier::Hot,
            players: BTreeMap::new(),
            observers: BTreeMap::new(),
            departing: None,
            clients: BTreeMap::new(),
            next_id: 0,
            outbound: Vec::new(),
            facts: None,
            pending_kicks: Vec::new(),
            rewind: Rewind::new(config.rewind, config.tick_rate),
            colliders: Colliders::new(),
            transfer_issuer: None,
            pending_handoffs: Vec::new(),
            upward: Vec::new(),
            child_inbox: Vec::new(),
            dropped_frozen: 0,
            fact_seq: (0, 0),
            stamped: 0,
            stamp_floor_ms: 0,
            pinned_stamp_ms: None,
            backlog: VecDeque::new(),
            zone_out: Vec::new(),
            zone_seq: 0,
            zone_seen: SeenIds::default(),
            config,
        };
        let zone = Z::on_create(&mut Ctx { core: &mut core }, zone_config);
        let hot = FixedTimestep::from_rate(core.config.tick_rate, core.config.max_catch_up);
        let idle = core
            .config
            .idle_tick_rate
            .map(|hz| FixedTimestep::from_rate(hz, core.config.max_catch_up));
        let mut host = Self {
            zone,
            core,
            schema: Z::schema(),
            entities: Z::entities(),
            hot,
            idle,
            last_advance: now,
            idle_since: now,
            next_patch: now,
            transitions: Vec::new(),
            frozen: false,
            freeze_request: None,
            disposed: false,
            token_source: Box::new(default_token),
            tape: None,
            pending: AdvanceReport::default(),
            hash_ticks: false,
            last_world: None,
            view: WorldView::default(),
        };
        if host.core.config.record {
            host.hash_ticks = true;
            host.tape = Some(Tape {
                recording: Recording {
                    zone_id: host.core.zone_id.clone(),
                    // As resolved (the boot epoch), and replayed unrecorded.
                    config: HostConfig {
                        record: false,
                        ..host.core.config.clone()
                    },
                    created_at: now,
                    created_unix_ms: host.core.unix_ms,
                    fact_capacity: None,
                    entries: Vec::new(),
                },
                tokens: Vec::new(),
            });
        }
        host.process_kicks();
        host
    }

    /// Attach the zone side of a fact channel.
    pub fn with_facts(mut self, facts: FactQueue) -> Self {
        if let Some(tape) = &mut self.tape {
            tape.recording.fact_capacity = Some(facts.capacity());
        }
        self.core.facts = Some(facts);
        self
    }

    /// Replace the reconnect/session token generator (must be unpredictable).
    pub fn with_token_source(mut self, source: impl FnMut() -> String + Send + 'static) -> Self {
        self.token_source = Box::new(source);
        self
    }

    /// Install the transfer-ticket issuer used by [`Ctx::handoff`].
    pub fn with_transfer_issuer(
        mut self,
        issuer: impl FnMut(&HandoffRequest) -> Option<String> + Send + 'static,
    ) -> Self {
        self.core.transfer_issuer = Some(Box::new(issuer));
        self
    }

    /// [`Stimulus::Restore`].
    fn restore_snapshot(&mut self, state: &[u8], saved_version: &str) {
        hook!(self.on_restore(state, saved_version));
        self.process_kicks();
    }

    /// Durable snapshot for hibernation ([`Zone::hibernate`]).
    pub fn hibernate_state(&self) -> Option<Vec<u8>> {
        self.zone.hibernate()
    }

    /// Zone id.
    pub fn zone_id(&self) -> &str {
        &self.core.zone_id
    }

    /// The game state.
    pub fn zone(&self) -> &Z {
        &self.zone
    }

    /// Mutable game state (operator tooling): not a [`Stimulus`], so a
    /// recording does not see it and its replay diverges.
    pub fn zone_mut(&mut self) -> &mut Z {
        &mut self.zone
    }

    /// Host configuration.
    pub fn config(&self) -> &HostConfig {
        &self.core.config
    }

    /// Replicated schema.
    pub fn schema(&self) -> &EntitySchema {
        &self.schema
    }

    /// Current tier.
    pub fn tier(&self) -> Tier {
        self.core.tier
    }

    /// Ticks simulated.
    pub fn tick(&self) -> u64 {
        self.core.tick
    }

    /// Players (including those in the grace window).
    pub fn player_count(&self) -> usize {
        self.core.players.len()
    }

    /// Players (including those in the grace window).
    pub fn players(&self) -> impl Iterator<Item = &PlayerInfo> + '_ {
        self.core.players.values().map(|slot| &slot.info)
    }

    /// Observers.
    pub fn observers(&self) -> impl Iterator<Item = &ObserverInfo> + '_ {
        self.core.observers.values().map(|slot| &slot.info)
    }

    /// Live connections.
    pub fn client_count(&self) -> usize {
        self.core.clients.len()
    }

    /// Observers.
    pub fn observer_count(&self) -> usize {
        self.core.observers.len()
    }

    /// Lag-compensation history.
    pub fn rewind(&self) -> &Rewind {
        &self.core.rewind
    }

    /// Report a state hash after every tick in
    /// [`AdvanceReport::tick_hashes`] (record/replay; costs a hash per tick).
    pub fn set_tick_hashing(&mut self, on: bool) {
        self.hash_ticks = on;
    }

    /// Deterministic digest of tick, simulation time, RNG state and
    /// [`Zone::state_hash`] (or, when the zone has none, its replicated
    /// [`WorldView`]).
    pub fn state_hash(&self) -> u64 {
        let zone = self
            .zone
            .state_hash()
            .unwrap_or_else(|| crate::replay::hash_world(&self.world_view()));
        let mut h = crate::rng::Fnv64::new();
        h.write_u64(self.core.tick)
            .write_u64(self.core.sim_time.as_nanos() as u64)
            .write_u64(self.core.rng.clone().next_u64())
            .write_u64(zone);
        h.finish()
    }

    /// Viewer bound to a connection.
    pub fn viewer_of(&self, client: ClientId) -> Option<Viewer> {
        self.core.clients.get(&client).map(|slot| slot.viewer)
    }

    /// Unsettled facts.
    pub fn facts_in_flight(&self) -> usize {
        self.core.in_flight()
    }

    /// Detach the fact queue without committing what it holds (the zone
    /// lost its ownership: nothing more may reach the store). Queued facts
    /// are dropped, the worker stops after any commit already in flight,
    /// and later emits answer [`EmitError::NoSink`]. Returns the facts
    /// left unsettled.
    pub fn abandon_facts(&mut self) -> usize {
        let unsettled = self.core.in_flight();
        self.core.backlog.clear();
        if let Some(queue) = self.core.facts.take() {
            queue.abandon();
        }
        unsettled
    }

    /// Facts emitted since the last call, when the fact queue keeps a
    /// journal ([`FactQueue::with_journal`]); empty otherwise.
    pub fn take_emitted_facts(&mut self) -> Vec<Fact> {
        self.core
            .facts
            .as_mut()
            .map(FactQueue::take_journal)
            .unwrap_or_default()
    }

    /// True after [`dispose`](Self::dispose).
    pub fn is_disposed(&self) -> bool {
        self.disposed
    }

    /// True while frozen.
    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    /// Server clock in ms (for Pong): read now, not frozen.
    pub fn server_time_ms(&self) -> f64 {
        self.core.clock.now().as_secs_f64() * 1000.0
    }

    /// Send every client the current state now, without ticking (a host
    /// that is not ticking — a celld zone with only passive observers —
    /// greets a new observer with it).
    pub fn patch_now(&mut self) {
        if !self.core.clients.is_empty() {
            self.patch();
        }
    }

    /// Frames and closes produced since the last call, in order.
    pub fn drain_outbound(&mut self) -> Vec<Outbound> {
        std::mem::take(&mut self.core.outbound)
    }

    /// Inputs dropped because the zone was frozen.
    pub fn inputs_dropped_frozen(&self) -> u64 {
        self.core.dropped_frozen
    }
}

fn unix_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(1, |d| d.as_micros() as u64)
        .max(1)
}

pub(crate) fn default_token() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut out = String::with_capacity(32);
    for _ in 0..2 {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        hasher.write_u128(nanos);
        out.push_str(&format!("{:016x}", hasher.finish()));
    }
    out
}
