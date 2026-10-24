//! [`ZoneHost`]: drives one [`Zone`] — lifecycle, fixed timestep, activity
//! tiers, inputs, timers, facts and replication.
//!
//! The host is synchronous and transport-agnostic. A driver (the `net`
//! server, a test, a custom loop) calls:
//!
//! - [`join`](ZoneHost::join) / [`reconnect`](ZoneHost::reconnect) /
//!   [`disconnect`](ZoneHost::disconnect) for membership,
//! - [`push_input`](ZoneHost::push_input) for wire inputs,
//! - [`advance`](ZoneHost::advance) whenever time may have passed (it reads
//!   the injected [`Clock`], runs due fixed steps and patches, and reports
//!   when it next wants to be called),
//! - [`drain_outbound`](ZoneHost::drain_outbound) to collect encoded frames.
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
use crate::facts::{EmitError, Fact, FactCommand, FactQueue, FactSettlement};
use crate::handoff::{HandoffError, HandoffRequest, Transfer, TransferIssuer, MAX_CARRIED_BYTES};
use crate::inbox::{Delivered, ZoneInbound};
use crate::input::{ConsumePolicy, InputBuffer, PushOutcome, SequencedInput};
use crate::interest::{InterestConfig, View};
use crate::lifecycle::{
    ConnectionLost, JoinVia, LeftReason, LostCause, PlayerDisconnected, PlayerJoined, PlayerLeft,
    PlayerReconnected,
};
use crate::protocol::{error, kick, Notice, Role, ServerMessage, Welcome};
use crate::replication::{ClientReplica, DeltaStats, EncodedWorld, WorldView};
use crate::rewind::{Rewind, RewindConfig, RewindFrame};
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

struct PlayerSlot<I> {
    info: PlayerInfo,
    inputs: InputBuffer<I>,
    client: Option<ClientId>,
    view: View,
    grace_until: Option<Duration>,
    reconnect_token: String,
    session_id: String,
    content_version: String,
    /// Lifecycle facts emitted for this slot (part of their keys).
    transitions: u32,
}

struct ObserverSlot {
    info: ObserverInfo,
    client: ClientId,
    view: View,
}

struct ClientSlot {
    viewer: Viewer,
    replica: ClientReplica,
}

/// Host state reachable from hooks through [`Ctx`].
pub(crate) struct Core<Z: Zone> {
    zone_id: String,
    config: HostConfig,
    clock: Arc<dyn Clock>,
    rng: ZoneRng,
    timers: Timers<Z::Timer>,
    tick: u64,
    sim_time: Duration,
    dt: Duration,
    /// Ticks dropped (catch-up cap) since the last simulated tick; seen by
    /// the next `on_tick` through `Ctx::dropped_ticks`, then reset.
    dropped_ticks: u32,
    tier: Tier,
    players: BTreeMap<PlayerId, PlayerSlot<Z::Input>>,
    observers: BTreeMap<ObserverId, ObserverSlot>,
    /// Observer being removed while `on_unobserve` runs.
    departing: Option<ObserverInfo>,
    clients: BTreeMap<ClientId, ClientSlot>,
    next_id: u32,
    outbound: Vec<Outbound>,
    facts: Option<FactQueue>,
    pending_kicks: Vec<(PlayerId, String)>,
    rewind: Rewind,
    transfer_issuer: Option<Box<TransferIssuer>>,
    /// Handoffs to finish after the hook: player, `Handoff` message and the
    /// departure stamp time signed into the transfer ticket.
    pending_handoffs: Vec<(PlayerId, ServerMessage, u64)>,
    upward: Vec<ChildEvent>,
    child_inbox: Vec<ChildEvent>,
    dropped_frozen: u64,
    /// Typed facts keyed without dedupe parts this tick: `(tick, count)`.
    fact_seq: (u64, u32),
    /// Facts stamped this activation ([`FactStamp::seq`] of the latest).
    stamped: u64,
    /// Stamps are never earlier than this (Unix ms): raised past a handoff
    /// source's departure time when a transfer is admitted.
    stamp_floor_ms: u64,
    /// Stamp time forced while a handoff departure runs `on_leave`, so the
    /// source's departure facts carry exactly the time in the ticket.
    pinned_stamp_ms: Option<u64>,
    /// Lifecycle facts waiting for queue capacity, in emission order.
    lifecycle_backlog: VecDeque<Fact>,
}

impl<Z: Zone> Core<Z> {
    fn next_id(&mut self) -> u32 {
        self.next_id += 1;
        self.next_id
    }

    fn client_of(&self, viewer: Viewer) -> Option<ClientId> {
        match viewer {
            Viewer::Player(p) => self.players.get(&p).and_then(|s| s.client),
            Viewer::Observer(o) => self.observers.get(&o).map(|s| s.client),
        }
    }

    fn send(&mut self, client: ClientId, frame: Vec<u8>) {
        self.outbound.push(Outbound::Frame { client, frame });
    }

    fn server_message(&mut self, client: ClientId, message: &ServerMessage) {
        let frame = message.encode(&()).expect("non-delta messages encode");
        self.send(client, frame);
    }

    fn server_time_ms(&self) -> f64 {
        self.clock.now().as_secs_f64() * 1000.0
    }

    fn in_flight(&self) -> usize {
        self.facts.as_ref().map_or(0, FactQueue::in_flight) + self.lifecycle_backlog.len()
    }

    /// Queue a lifecycle fact for `principal`'s slot `player` (transition
    /// `n`), keyed restart-safely and stamped now.
    fn lifecycle<C: FactCommand>(&mut self, player: PlayerId, principal: &str, n: u32, input: C) {
        if !self.config.lifecycle_facts || self.facts.is_none() {
            return;
        }
        let key = crate::facts::fact_key(
            C::COMMAND,
            &self.zone_id,
            self.config.boot_epoch,
            &[&format_args!("p{}", player.0), &n],
        );
        let Ok(fact) = Fact::typed(key, &input) else {
            return;
        };
        let fact = fact.bind(Some(principal.to_string()), self.stamp_fact());
        self.lifecycle_backlog.push_back(fact);
        self.flush_lifecycle();
    }

    /// Send backlogged lifecycle facts while the queue has room.
    fn flush_lifecycle(&mut self) {
        let Some(queue) = self.facts.as_mut() else {
            self.lifecycle_backlog.clear();
            return;
        };
        while let Some(fact) = self.lifecycle_backlog.front() {
            match queue.try_send(fact.clone()) {
                Err(EmitError::Backpressure) => return,
                // Sent, or a duplicate already in flight: done either way.
                _ => {
                    self.lifecycle_backlog.pop_front();
                }
            }
        }
    }

    /// Next transition number of a slot.
    fn transition(&mut self, player: PlayerId) -> Option<(String, u32)> {
        let slot = self.players.get_mut(&player)?;
        slot.transitions += 1;
        Some((slot.info.principal.clone(), slot.transitions))
    }

    fn emit_lifecycle<C: FactCommand>(&mut self, player: PlayerId, input: C) {
        if !self.config.lifecycle_facts {
            return;
        }
        if let Some((principal, n)) = self.transition(player) {
            self.lifecycle(player, &principal, n, input);
        }
    }

    /// Wall time stamped on anything produced now (Unix ms).
    fn stamp_ms(&self) -> u64 {
        self.pinned_stamp_ms
            .unwrap_or_else(|| self.clock.unix_ms().max(self.stamp_floor_ms))
    }

    fn stamp(&self) -> FactStamp {
        FactStamp {
            zone_id: self.zone_id.clone(),
            tick: self.tick,
            content_version: self.config.content_version.clone(),
            emitted_at_ms: self.stamp_ms(),
            activation: self.config.boot_epoch,
            config_version: self.config.config_version.clone(),
            seq: self.stamped,
        }
    }

    /// Stamp for a fact: the next emission sequence number.
    fn stamp_fact(&mut self) -> FactStamp {
        self.stamped += 1;
        self.stamp()
    }
}

/// Hook context: everything a zone may touch besides its own state.
pub struct Ctx<'a, Z: Zone> {
    core: &'a mut Core<Z>,
}

impl<Z: Zone> Ctx<'_, Z> {
    /// Zone id.
    pub fn zone_id(&self) -> &str {
        &self.core.zone_id
    }

    /// Ticks simulated so far (the current tick inside `on_tick`).
    pub fn tick(&self) -> u64 {
        self.core.tick
    }

    /// Step length of the current tick (Hot or Idle rate).
    pub fn dt(&self) -> Duration {
        self.core.dt
    }

    /// Ticks the host dropped right before this one: the clock ran further
    /// ahead than `max_catch_up` steps, so the rest of the backlog was not
    /// simulated (0 normally). Simulated ticks then lag wall-clock time; a
    /// zone that paces players' inputs per tick credits these so inputs
    /// sent in real time do not queue up for good.
    pub fn dropped_ticks(&self) -> u32 {
        self.core.dropped_ticks
    }

    /// Step length in seconds.
    pub fn dt_secs(&self) -> f32 {
        self.core.dt.as_secs_f32()
    }

    /// Simulation time (sum of all step lengths).
    pub fn sim_time(&self) -> Duration {
        self.core.sim_time
    }

    /// Host clock (not deterministic across runs with a real clock).
    pub fn now(&self) -> Duration {
        self.core.clock.now()
    }

    /// Current activity tier.
    pub fn tier(&self) -> Tier {
        self.core.tier
    }

    /// Content version.
    pub fn content_version(&self) -> &str {
        &self.core.config.content_version
    }

    /// Content version `player`'s client runs (what it presented on its
    /// last Hello; differs from [`content_version`](Self::content_version)
    /// for a stale client kept under [`StalePolicy::SafeRoom`]).
    pub fn player_content_version(&self, player: PlayerId) -> Option<&str> {
        self.core
            .players
            .get(&player)
            .map(|slot| slot.content_version.as_str())
    }

    /// True when `player`'s client runs older content than the zone (it
    /// may only be handed off into a gate zone, unless the policy roams).
    pub fn is_stale(&self, player: PlayerId) -> bool {
        let expected = &self.core.config.content_version;
        !expected.is_empty()
            && self
                .player_content_version(player)
                .is_some_and(|version| version != expected)
    }

    /// The host's [`StalePolicy`].
    pub fn stale_policy(&self) -> StalePolicy {
        self.core.config.stale_policy
    }

    /// Remote config version ([`HostConfig::config_version`]).
    pub fn config_version(&self) -> &str {
        &self.core.config.config_version
    }

    /// Switch the remote config version (e.g. on a config-changed
    /// message); facts and stamps produced afterwards carry it.
    pub fn set_config_version(&mut self, version: impl Into<String>) {
        self.core.config.config_version = version.into();
    }

    /// Epoch of this zone activation ([`HostConfig::boot_epoch`]): unique
    /// per activation and increasing across restarts and hibernation.
    /// Tick counts and entity generations restart with the process, so a
    /// fact key built only from them **collides after a restart** (and the
    /// sink replays the old commit instead of recording the new outcome);
    /// include this epoch, e.g. via [`fact_key`](Self::fact_key).
    pub fn boot_epoch(&self) -> u64 {
        self.core.config.boot_epoch
    }

    /// Restart-safe fact idempotency key:
    /// `"{prefix}:{zone}:{boot_epoch}:{part}:{part}…"` (see
    /// [`fact_key`](crate::facts::fact_key)).
    pub fn fact_key(&self, prefix: &str, parts: &[&dyn std::fmt::Display]) -> String {
        crate::facts::fact_key(
            prefix,
            &self.core.zone_id,
            self.core.config.boot_epoch,
            parts,
        )
    }

    /// The zone's deterministic RNG.
    pub fn rng(&mut self) -> &mut ZoneRng {
        &mut self.core.rng
    }

    /// One-shot timer after `after` of simulation time.
    pub fn set_timeout(&mut self, after: Duration, timer: Z::Timer) -> TimerId {
        let now = self.core.sim_time;
        self.core.timers.once(now, after, timer)
    }

    /// Repeating timer every `every` of simulation time.
    pub fn set_interval(&mut self, every: Duration, timer: Z::Timer) -> TimerId {
        let now = self.core.sim_time;
        self.core.timers.interval(now, every, timer)
    }

    /// Cancel a timer.
    pub fn cancel_timer(&mut self, id: TimerId) -> bool {
        self.core.timers.cancel(id)
    }

    /// Players (connected or within the reconnect grace window).
    pub fn players(&self) -> impl Iterator<Item = &PlayerInfo> + '_ {
        self.core.players.values().map(|slot| &slot.info)
    }

    /// One player.
    pub fn player(&self, id: PlayerId) -> Option<&PlayerInfo> {
        self.core.players.get(&id).map(|slot| &slot.info)
    }

    /// Observers currently subscribed.
    pub fn observers(&self) -> impl Iterator<Item = &ObserverInfo> + '_ {
        self.core.observers.values().map(|slot| &slot.info)
    }

    /// One observer (verified principal, display name). Also answers for
    /// the observer leaving during [`Zone::on_unobserve`].
    pub fn observer(&self, id: ObserverId) -> Option<&ObserverInfo> {
        self.core.observers.get(&id).map(|slot| &slot.info).or(self
            .core
            .departing
            .as_ref()
            .filter(|info| info.id == id))
    }

    /// Observers whose verified principal is `principal` (a principal may
    /// observe through several connections).
    pub fn observers_of<'p>(&'p self, principal: &'p str) -> impl Iterator<Item = ObserverId> + 'p {
        self.observers()
            .filter(move |info| info.principal == principal)
            .map(|info| info.id)
    }

    /// Consume buffered inputs of `player`; advances its ack.
    pub fn take_inputs(
        &mut self,
        player: PlayerId,
        policy: ConsumePolicy,
    ) -> Vec<SequencedInput<Z::Input>> {
        self.core
            .players
            .get_mut(&player)
            .map(|slot| slot.inputs.take(policy))
            .unwrap_or_default()
    }

    /// Typed fact for a cell command input, keyed restart-safely:
    /// `"{C::COMMAND}:{zone}:{boot_epoch}:{parts…}"` with the input's
    /// [`dedupe_parts`](FactCommand::dedupe_parts), or `t{tick}:{n}` (the
    /// n-th such fact this tick) when it has none. Emit it with
    /// [`emit_fact_for`](Self::emit_fact_for) (e.g. to keep it in a retry
    /// backlog) or use [`emit`](Self::emit) directly.
    pub fn fact<C: FactCommand>(&mut self, input: &C) -> Result<Fact, EmitError> {
        let parts = input.dedupe_parts();
        let key = if parts.is_empty() {
            let tick = self.core.tick;
            if self.core.fact_seq.0 != tick {
                self.core.fact_seq = (tick, 0);
            }
            let n = self.core.fact_seq.1;
            self.core.fact_seq.1 += 1;
            self.fact_key(C::COMMAND, &[&format_args!("t{tick}"), &n])
        } else {
            let parts: Vec<&dyn std::fmt::Display> =
                parts.iter().map(|p| p as &dyn std::fmt::Display).collect();
            self.fact_key(C::COMMAND, &parts)
        };
        Fact::typed(key, input).map_err(|e| EmitError::Encode(e.to_string()))
    }

    /// [`fact`](Self::fact) with explicit dedupe parts (overriding the
    /// input's own).
    pub fn fact_with_key<C: FactCommand>(
        &self,
        input: &C,
        parts: &[&dyn std::fmt::Display],
    ) -> Result<Fact, EmitError> {
        Fact::typed(self.fact_key(C::COMMAND, parts), input)
            .map_err(|e| EmitError::Encode(e.to_string()))
    }

    /// Emit a typed cell command input on behalf of `player`: the principal
    /// is bound from the player's verified ticket (the input carries no
    /// identity), the key is derived as in [`fact`](Self::fact). Returns
    /// the key. Never blocks.
    pub fn emit<C: FactCommand>(
        &mut self,
        player: PlayerId,
        input: C,
    ) -> Result<String, EmitError> {
        let fact = self.fact(&input)?;
        let key = fact.key().to_string();
        self.emit_fact_for(player, fact).map(|()| key)
    }

    /// [`emit`](Self::emit) with explicit dedupe parts.
    pub fn emit_keyed<C: FactCommand>(
        &mut self,
        player: PlayerId,
        input: C,
        parts: &[&dyn std::fmt::Display],
    ) -> Result<String, EmitError> {
        let fact = self.fact_with_key(&input, parts)?;
        let key = fact.key().to_string();
        self.emit_fact_for(player, fact).map(|()| key)
    }

    /// Emit a typed zone/system fact (no principal; the sink's system
    /// principal applies). Returns the key.
    pub fn emit_zone<C: FactCommand>(&mut self, input: C) -> Result<String, EmitError> {
        let fact = self.fact(&input)?;
        let key = fact.key().to_string();
        self.emit_fact(fact).map(|()| key)
    }

    /// Emit a zone/system fact (no principal). Never blocks.
    pub fn emit_fact(&mut self, fact: Fact) -> Result<(), EmitError> {
        self.emit_bound(fact, None)
    }

    /// Emit a fact on behalf of `player`; the principal is bound from the
    /// player's verified ticket. Never blocks.
    pub fn emit_fact_for(&mut self, player: PlayerId, fact: Fact) -> Result<(), EmitError> {
        let principal = self
            .core
            .players
            .get(&player)
            .map(|slot| slot.info.principal.clone())
            .ok_or(EmitError::UnknownPlayer)?;
        self.emit_bound(fact, Some(principal))
    }

    fn emit_bound(&mut self, fact: Fact, principal: Option<String>) -> Result<(), EmitError> {
        let stamp = self.core.stamp_fact();
        self.core
            .facts
            .as_mut()
            .ok_or(EmitError::NoSink)?
            .try_send(fact.bind(principal, stamp))
    }

    /// Unsettled facts.
    pub fn facts_in_flight(&self) -> usize {
        self.core.in_flight()
    }

    /// Lag-compensation history (filled by [`Zone::record_rewind`]).
    pub fn rewind(&self) -> &Rewind {
        &self.core.rewind
    }

    /// Reliable event to one viewer (dropped if it has no live connection).
    pub fn send_event(&mut self, to: impl Into<Viewer>, type_id: u32, payload: Vec<u8>) {
        if let Some(client) = self.core.client_of(to.into()) {
            self.core
                .server_message(client, &ServerMessage::Event { type_id, payload });
        }
    }

    /// Reliable event to every connection.
    pub fn broadcast_event(&mut self, type_id: u32, payload: Vec<u8>) {
        let frame = ServerMessage::Event { type_id, payload }
            .encode(&())
            .expect("event encodes");
        let clients: Vec<_> = self.core.clients.keys().copied().collect();
        for client in clients {
            self.core.send(client, frame.clone());
        }
    }

    /// Typed reliable event to one viewer (dropped if it has no live
    /// connection).
    pub fn send<E: GameEvent>(&mut self, to: impl Into<Viewer>, event: &E) {
        self.send_event(to, event.event_id(), event.to_payload());
    }

    /// Typed reliable event to every connection.
    pub fn broadcast<E: GameEvent>(&mut self, event: &E) {
        self.broadcast_event(event.event_id(), event.to_payload());
    }

    /// Set a viewer's area of interest.
    pub fn set_view(&mut self, viewer: impl Into<Viewer>, view: View) {
        match viewer.into() {
            Viewer::Player(p) => {
                if let Some(slot) = self.core.players.get_mut(&p) {
                    slot.view = view;
                }
            }
            Viewer::Observer(o) => {
                if let Some(slot) = self.core.observers.get_mut(&o) {
                    slot.view = view;
                }
            }
        }
    }

    /// Remove a player after the current hook returns.
    pub fn kick(&mut self, player: PlayerId, message: impl Into<String>) {
        self.core.pending_kicks.push((player, message.into()));
    }

    /// Provenance of anything produced now (zone, activation, tick, wall
    /// time, content and config version); embed it in events so clients
    /// and logs can tell which content version produced them.
    pub fn stamp(&self) -> FactStamp {
        self.core.stamp()
    }

    /// Move `player` to `target_zone` with `carried` state. A signed
    /// transfer ticket is issued now; after the current hook returns the
    /// player is removed (`LeaveReason::Handoff`) and its client is told
    /// `Handoff(target, ticket)`. The player can never be in both zones: it
    /// leaves this one before the ticket is usable, and the ticket is
    /// single-use.
    pub fn handoff(
        &mut self,
        player: PlayerId,
        target_zone: &str,
        carried: Vec<u8>,
    ) -> Result<(), HandoffError> {
        if target_zone == self.core.zone_id {
            return Err(HandoffError::SameZone);
        }
        if carried.len() > MAX_CARRIED_BYTES {
            return Err(HandoffError::TooLarge);
        }
        let (principal, name, extensions) = self
            .core
            .players
            .get(&player)
            .map(|slot| {
                (
                    slot.info.principal.clone(),
                    slot.info.name.clone(),
                    slot.info.extensions.clone(),
                )
            })
            .ok_or(HandoffError::UnknownPlayer)?;
        if self
            .core
            .pending_handoffs
            .iter()
            .any(|(queued, _, _)| *queued == player)
        {
            return Err(HandoffError::Refused);
        }
        let departed_at_ms = self.core.stamp_ms();
        let request = HandoffRequest {
            principal,
            name,
            extensions,
            source_zone: self.core.zone_id.clone(),
            target_zone: target_zone.to_string(),
            carried,
            departed_at_ms,
        };
        let issuer = self
            .core
            .transfer_issuer
            .as_mut()
            .ok_or(HandoffError::NoIssuer)?;
        let ticket = issuer(&request).ok_or(HandoffError::Refused)?;
        self.core.pending_handoffs.push((
            player,
            ServerMessage::Handoff {
                target_zone: request.target_zone,
                transfer_ticket: ticket,
            },
            departed_at_ms,
        ));
        Ok(())
    }

    /// Send a tier-up event to the parent zone (and, while `levels` remain,
    /// further up). Delivered asynchronously on a later tick of the parent.
    pub fn emit_upward(&mut self, kind: impl Into<String>, payload: Vec<u8>, levels: u8) {
        if levels == 0 {
            return;
        }
        self.core.upward.push(ChildEvent {
            origin_zone: self.core.zone_id.clone(),
            from_zone: self.core.zone_id.clone(),
            kind: kind.into(),
            payload,
            origin_tick: self.core.tick,
            content_version: self.core.config.content_version.clone(),
            levels,
        });
    }
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
    pending: AdvanceReport,
    hash_ticks: bool,
    /// The last patch's world: the next one shares its unchanged entities
    /// and encodes each change once for every client.
    last_world: Option<EncodedWorld>,
}

macro_rules! hook {
    ($host:ident . $method:ident ( $($arg:expr),* )) => {
        $host.zone.$method(&mut Ctx { core: &mut $host.core } $(, $arg)*)
    };
}

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
            transfer_issuer: None,
            pending_handoffs: Vec::new(),
            upward: Vec::new(),
            child_inbox: Vec::new(),
            dropped_frozen: 0,
            fact_seq: (0, 0),
            stamped: 0,
            stamp_floor_ms: 0,
            pinned_stamp_ms: None,
            lifecycle_backlog: VecDeque::new(),
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
            pending: AdvanceReport::default(),
            hash_ticks: false,
            last_world: None,
        };
        host.process_kicks();
        host
    }

    /// Attach the zone side of a fact channel.
    pub fn with_facts(mut self, facts: FactQueue) -> Self {
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

    /// Hand a hibernated snapshot to the zone ([`Zone::on_restore`]); call
    /// right after construction.
    pub fn restore(&mut self, state: &[u8], saved_version: &str) {
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

    /// Mutable game state (operator tooling; bypasses determinism).
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

    /// Server clock in ms (for Pong).
    pub fn server_time_ms(&self) -> f64 {
        self.core.server_time_ms()
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

    /// Admit a verified ticket holder.
    pub fn join(
        &mut self,
        claims: &TicketClaims,
        capabilities: u32,
        content_version: &str,
    ) -> Result<Joined, JoinError> {
        self.admit(claims, capabilities, content_version, None)
    }

    /// Admit a player arriving with a verified transfer ticket; the zone
    /// sees [`Zone::on_handoff_join`] with the carried state (also when the
    /// principal is still present, e.g. inside its reconnect grace).
    ///
    /// From now on this zone stamps facts later than the source stamped the
    /// departure ([`Transfer::departed_at_ms`]), whatever the two hosts'
    /// clocks say.
    pub fn join_transfer(
        &mut self,
        transfer: &Transfer,
        capabilities: u32,
        content_version: &str,
    ) -> Result<Joined, JoinError> {
        if transfer.claims.role != Role::Player {
            return Err(JoinError::new(
                error::UNAUTHORIZED,
                "transfers are for players",
            ));
        }
        self.core.stamp_floor_ms = self
            .core
            .stamp_floor_ms
            .max(transfer.departed_at_ms.saturating_add(1));
        self.admit(
            &transfer.claims,
            capabilities,
            content_version,
            Some((transfer.source_zone.as_str(), transfer.carried.clone())),
        )
    }

    fn admit(
        &mut self,
        claims: &TicketClaims,
        capabilities: u32,
        content_version: &str,
        handoff: Option<(&str, Vec<u8>)>,
    ) -> Result<Joined, JoinError> {
        if self.disposed {
            return Err(JoinError::new(error::NOT_FOUND, "zone disposed"));
        }
        if claims.zone != self.core.zone_id {
            return Err(JoinError::new(
                error::UNAUTHORIZED,
                "ticket is for another zone",
            ));
        }
        let expected = &self.core.config.content_version;
        let mut reload = false;
        if !expected.is_empty() && content_version != expected {
            let allowed = match self.core.config.stale_policy {
                StalePolicy::Strict => false,
                StalePolicy::SafeRoom { roam } => {
                    let gate = self.zone.is_gate();
                    reload = gate;
                    handoff.is_some() && claims.role == Role::Player && (gate || roam)
                }
            };
            if !allowed {
                return Err(JoinError::new(
                    error::VERSION_MISMATCH,
                    format!("content version {content_version} != {expected}: reload"),
                ));
            }
        }
        let existing = self
            .core
            .players
            .values()
            .find(|slot| slot.info.principal == claims.principal)
            .map(|slot| slot.info.id);
        let capacity_ok = match claims.role {
            Role::Player => {
                existing.is_some() || self.core.players.len() < self.core.config.max_players
            }
            Role::Observer => self.core.observers.len() < self.core.config.max_observers,
        };
        if !capacity_ok {
            return Err(JoinError::new(error::ZONE_FULL, "zone is full"));
        }
        let request = JoinRequest {
            claims,
            capabilities,
            content_version,
        };
        if let Admission::Reject(message) = hook!(self.on_auth(&request)) {
            self.process_kicks();
            return Err(JoinError::new(error::REJECTED, message));
        }
        self.wake();
        let client = ClientId(self.core.next_id());
        let mut handoff = handoff;
        let joined = match (claims.role, existing) {
            (Role::Player, Some(player)) => {
                if let Some(slot) = self.core.players.get_mut(&player) {
                    if claims.name.is_some() {
                        slot.info.name = claims.name.clone();
                    }
                    slot.info.extensions = claims.extensions.clone();
                }
                self.bind_player(player, client, capabilities);
                let zone = self.core.zone_id.clone();
                if let Some((from, carried)) = handoff.take() {
                    self.core.emit_lifecycle(
                        player,
                        PlayerJoined {
                            zone,
                            via: JoinVia::Handoff,
                            from_zone: Some(from.to_string()),
                        },
                    );
                    hook!(self.on_handoff_join(player, from, carried));
                } else {
                    self.core.emit_lifecycle(
                        player,
                        PlayerJoined {
                            zone,
                            via: JoinVia::Rejoin,
                            from_zone: None,
                        },
                    );
                    hook!(self.on_rejoin(player));
                }
                Joined {
                    client,
                    viewer: Viewer::Player(player),
                }
            }
            (Role::Player, None) => {
                let player = PlayerId(self.core.next_id());
                let radius = self.core.config.default_view_radius;
                let capacity = self.core.config.input_capacity;
                self.core.players.insert(
                    player,
                    PlayerSlot {
                        info: PlayerInfo {
                            id: player,
                            principal: claims.principal.clone(),
                            name: claims.name.clone(),
                            extensions: claims.extensions.clone(),
                            capabilities,
                            connected: true,
                        },
                        inputs: InputBuffer::new(capacity),
                        client: None,
                        view: View::new(0.0, 0.0, radius),
                        grace_until: None,
                        reconnect_token: String::new(),
                        session_id: (self.token_source)(),
                        content_version: content_version.to_string(),
                        transitions: 0,
                    },
                );
                self.bind_player(player, client, capabilities);
                let zone = self.core.zone_id.clone();
                match handoff.take() {
                    Some((from, carried)) => {
                        self.core.emit_lifecycle(
                            player,
                            PlayerJoined {
                                zone,
                                via: JoinVia::Handoff,
                                from_zone: Some(from.to_string()),
                            },
                        );
                        hook!(self.on_handoff_join(player, from, carried));
                    }
                    None => {
                        self.core.emit_lifecycle(
                            player,
                            PlayerJoined {
                                zone,
                                via: JoinVia::Ticket,
                                from_zone: None,
                            },
                        );
                        hook!(self.on_join(player));
                    }
                }
                Joined {
                    client,
                    viewer: Viewer::Player(player),
                }
            }
            (Role::Observer, _) => {
                let observer = ObserverId(self.core.next_id());
                let session_id = (self.token_source)();
                self.core.observers.insert(
                    observer,
                    ObserverSlot {
                        info: ObserverInfo {
                            id: observer,
                            principal: claims.principal.clone(),
                            name: claims.name.clone(),
                            extensions: claims.extensions.clone(),
                            capabilities,
                        },
                        client,
                        view: View::new(0.0, 0.0, self.core.config.default_view_radius),
                    },
                );
                self.attach_client(client, Viewer::Observer(observer));
                self.welcome(client, session_id, String::new(), Role::Observer);
                hook!(self.on_observe(observer));
                Joined {
                    client,
                    viewer: Viewer::Observer(observer),
                }
            }
        };
        if let Viewer::Player(player) = joined.viewer {
            if let Some(slot) = self.core.players.get_mut(&player) {
                slot.content_version = content_version.to_string();
            }
            if reload {
                self.reload_player(player);
            }
        }
        self.process_kicks();
        Ok(joined)
    }

    /// Resume a player's session with the token from its last Welcome.
    pub fn reconnect(&mut self, token: &str, capabilities: u32) -> Result<Joined, JoinError> {
        self.reconnect_as(token, capabilities, None)
    }

    /// [`reconnect`](Self::reconnect) presenting the client's (possibly
    /// reloaded) content version. Under [`StalePolicy::Strict`] a stale
    /// version is refused; under `SafeRoom` it is admitted to this zone
    /// (the one the player is in), and told `Kick(RELOAD)` again if this
    /// zone is a gate.
    pub fn reconnect_with_version(
        &mut self,
        token: &str,
        capabilities: u32,
        content_version: &str,
    ) -> Result<Joined, JoinError> {
        self.reconnect_as(token, capabilities, Some(content_version))
    }

    fn reconnect_as(
        &mut self,
        token: &str,
        capabilities: u32,
        content_version: Option<&str>,
    ) -> Result<Joined, JoinError> {
        let player = (!token.is_empty() && !self.disposed)
            .then(|| {
                self.core
                    .players
                    .values()
                    .find(|slot| slot.reconnect_token == token)
                    .map(|slot| slot.info.id)
            })
            .flatten()
            .ok_or_else(|| JoinError::new(error::RECONNECT_FAILED, "unknown or expired token"))?;
        let expected = self.core.config.content_version.clone();
        let mut reload = false;
        if let Some(version) = content_version {
            if !expected.is_empty() && version != expected {
                if self.core.config.stale_policy == StalePolicy::Strict {
                    return Err(JoinError::new(
                        error::VERSION_MISMATCH,
                        format!("content version {version} != {expected}: reload"),
                    ));
                }
                reload = self.zone.is_gate();
            }
        }
        self.wake();
        let client = ClientId(self.core.next_id());
        self.bind_player(player, client, capabilities);
        if let Some(version) = content_version {
            if let Some(slot) = self.core.players.get_mut(&player) {
                slot.content_version = version.to_string();
            }
        }
        let zone = self.core.zone_id.clone();
        self.core.emit_lifecycle(player, PlayerReconnected { zone });
        hook!(self.on_reconnected(player));
        if reload {
            self.reload_player(player);
        }
        self.process_kicks();
        Ok(Joined {
            client,
            viewer: Viewer::Player(player),
        })
    }

    /// Tell a connected player to reload its content (`Kick(RELOAD)`) and
    /// keep it in the reconnect grace window so it can reconnect with its
    /// last Welcome's token after reloading.
    pub fn reload_player(&mut self, player: PlayerId) {
        let now = self.core.clock.now();
        let grace = self.core.config.reconnect_grace;
        let message = ServerMessage::Kick {
            code: kick::RELOAD,
            message: format!("content {} required", self.core.config.content_version),
        };
        let Some(slot) = self.core.players.get_mut(&player) else {
            return;
        };
        let client = slot.client.take();
        slot.info.connected = false;
        slot.inputs.clear();
        slot.grace_until = Some(now + grace);
        if let Some(client) = client {
            self.core.clients.remove(&client);
            self.core.server_message(client, &message);
            self.core.outbound.push(Outbound::Close { client });
            self.connection_lost(player, LostCause::Reload);
        }
    }

    /// Grace started: lifecycle fact, then [`Zone::on_connection_lost`].
    fn connection_lost(&mut self, player: PlayerId, cause: LostCause) {
        let lost = ConnectionLost {
            zone: self.core.zone_id.clone(),
            grace_ms: self.core.config.reconnect_grace.as_millis() as u64,
            cause,
        };
        self.core.emit_lifecycle(player, lost);
        hook!(self.on_connection_lost(player));
    }

    /// [`reload_player`](Self::reload_player) every connected player whose
    /// content version differs from the zone's (hard rollout). Returns the
    /// number reloaded.
    pub fn reload_stale_players(&mut self) -> usize {
        let expected = self.core.config.content_version.clone();
        if expected.is_empty() {
            return 0;
        }
        let stale: Vec<_> = self
            .core
            .players
            .values()
            .filter(|slot| slot.client.is_some() && slot.content_version != expected)
            .map(|slot| slot.info.id)
            .collect();
        for player in &stale {
            self.reload_player(*player);
        }
        stale.len()
    }

    /// A client patched its content live and now runs `version`
    /// (`ClientMessage::Content`): recorded for its player only when it is
    /// the zone's current version (a client can catch up, never get
    /// ahead). Returns whether it was recorded.
    pub fn content_patched(&mut self, client: ClientId, version: &str) -> bool {
        if version != self.core.config.content_version {
            return false;
        }
        let Some(Viewer::Player(player)) = self.viewer_of(client) else {
            return false;
        };
        match self.core.players.get_mut(&player) {
            Some(slot) => {
                slot.content_version = version.to_string();
                true
            }
            None => false,
        }
    }

    /// Content version a player's client reported.
    pub fn player_content_version(&self, player: PlayerId) -> Option<&str> {
        self.core
            .players
            .get(&player)
            .map(|slot| slot.content_version.as_str())
    }

    /// Switch the remote config version ([`HostConfig::config_version`]);
    /// facts and stamps produced afterwards carry it.
    pub fn set_config_version(&mut self, version: impl Into<String>) {
        self.core.config.config_version = version.into();
    }

    /// Switch the zone to a new content version
    /// ([`Zone::on_content_version_changed`]); facts and stamps produced
    /// afterwards carry it.
    pub fn set_content_version(&mut self, version: impl Into<String>) {
        let version = version.into();
        if version == self.core.config.content_version {
            return;
        }
        let previous = std::mem::replace(&mut self.core.config.content_version, version.clone());
        hook!(self.on_content_version_changed(&previous, &version));
        self.process_kicks();
    }

    /// Queue a tier-up event from a child zone; delivered to
    /// [`Zone::on_child_event`] on a later tick (or the next `advance` when
    /// the zone is not ticking), never while frozen.
    pub fn deliver_child_event(&mut self, event: ChildEvent) {
        self.core.child_inbox.push(event);
    }

    /// Tier-up events emitted (or forwarded) by this zone since the last
    /// call; the driver routes them to the parent zone.
    pub fn drain_upward(&mut self) -> Vec<ChildEvent> {
        std::mem::take(&mut self.core.upward)
    }

    /// Inputs dropped because the zone was frozen.
    pub fn inputs_dropped_frozen(&self) -> u64 {
        self.core.dropped_frozen
    }

    fn attach_client(&mut self, client: ClientId, viewer: Viewer) {
        self.core.clients.insert(
            client,
            ClientSlot {
                viewer,
                replica: ClientReplica::new(),
            },
        );
    }

    fn bind_player(&mut self, player: PlayerId, client: ClientId, capabilities: u32) {
        let token = (self.token_source)();
        let slot = self.core.players.get_mut(&player).expect("player exists");
        let previous = slot.client.replace(client);
        slot.grace_until = None;
        slot.info.connected = true;
        slot.info.capabilities = capabilities;
        slot.reconnect_token = token.clone();
        // Every Welcome restarts the client's input sequence (ticket or
        // reconnect token), so forget the previous connection's.
        slot.inputs.reset_sequence();
        let session_id = slot.session_id.clone();
        if let Some(old) = previous {
            self.core.clients.remove(&old);
            self.core.server_message(
                old,
                &ServerMessage::Kick {
                    code: kick::REPLACED,
                    message: "replaced by a newer connection".into(),
                },
            );
            self.core.outbound.push(Outbound::Close { client: old });
        }
        self.attach_client(client, Viewer::Player(player));
        self.welcome(client, session_id, token, Role::Player);
    }

    fn welcome(&mut self, client: ClientId, session_id: String, token: String, role: Role) {
        let welcome = ServerMessage::Welcome(Welcome {
            session_id,
            zone_id: self.core.zone_id.clone(),
            role,
            tick_rate: self.core.config.tick_rate,
            patch_rate: self.core.config.patch_rate,
            server_tick: self.core.tick as u32,
            server_time: self.core.server_time_ms(),
            reconnect_token: token,
            content_version: self.core.config.content_version.clone(),
            schema: self.schema.clone(),
        });
        self.core.server_message(client, &welcome);
    }

    /// A connection ended. Graceful (Leave) removes the player now; otherwise
    /// the player stays (disconnected) for `reconnect_grace`.
    pub fn disconnect(&mut self, client: ClientId, graceful: bool) {
        let Some(slot) = self.core.clients.remove(&client) else {
            return;
        };
        match slot.viewer {
            Viewer::Player(player) => {
                if graceful {
                    self.remove_player(player, LeaveReason::Left, None);
                } else if let Some(p) = self.core.players.get_mut(&player) {
                    p.client = None;
                    p.info.connected = false;
                    p.inputs.clear();
                    p.grace_until = Some(self.core.clock.now() + self.core.config.reconnect_grace);
                    self.connection_lost(player, LostCause::Dropped);
                }
            }
            Viewer::Observer(observer) => self.remove_observer(observer, None),
        }
        self.process_kicks();
    }

    /// Offer a wire input from `client`. Decodes and sanitizes via the zone.
    pub fn push_input(
        &mut self,
        client: ClientId,
        seq: u32,
        client_tick: u32,
        payload: &[u8],
    ) -> PushOutcome {
        let Some(Viewer::Player(player)) = self.viewer_of(client) else {
            return PushOutcome::UnknownClient;
        };
        if self.frozen {
            self.core.dropped_frozen += 1;
            return PushOutcome::Rejected;
        }
        let Some(slot) = self.core.players.get(&player) else {
            return PushOutcome::UnknownClient;
        };
        if let Some(outcome) = slot.inputs.precheck(seq) {
            return outcome;
        }
        let Some(input) = self
            .zone
            .decode_input(player, payload)
            .and_then(|input| self.zone.sanitize_input(player, input))
        else {
            return PushOutcome::Rejected;
        };
        self.wake();
        self.core
            .players
            .get_mut(&player)
            .expect("player exists")
            .inputs
            .push(seq, client_tick, input)
    }

    /// Send a reliable event (operator/server side).
    pub fn send_event(&mut self, to: Viewer, type_id: u32, payload: Vec<u8>) {
        Ctx {
            core: &mut self.core,
        }
        .send_event(to, type_id, payload);
    }

    /// Send a typed event now (see [`Ctx::send`]).
    pub fn send<E: GameEvent>(&mut self, to: Viewer, event: &E) {
        self.send_event(to, event.event_id(), event.to_payload());
    }

    /// Deliver a service message ([`crate::inbox`]): a kick removes every
    /// connection of the principal (players leave with
    /// [`LeaveReason::Kicked`]), a game message runs
    /// [`Zone::on_message`]. Call between ticks (the host is not ticking
    /// while the caller holds `&mut self`).
    pub fn deliver(&mut self, inbound: ZoneInbound<Z::Message>) -> Delivered {
        let mut delivered = Delivered::default();
        match inbound {
            ZoneInbound::Kick { principal, reason } => {
                let players: Vec<_> = self
                    .core
                    .players
                    .values()
                    .filter(|slot| slot.info.principal == principal)
                    .map(|slot| slot.info.id)
                    .collect();
                delivered.players_kicked = players.len();
                for player in players {
                    self.core.pending_kicks.push((player, reason.clone()));
                }
                let observers: Vec<_> = self
                    .core
                    .observers
                    .values()
                    .filter(|slot| slot.info.principal == principal)
                    .map(|slot| slot.info.id)
                    .collect();
                delivered.observers_kicked = observers.len();
                let bye = ServerMessage::Kick {
                    code: kick::BY_ZONE,
                    message: reason,
                };
                for observer in observers {
                    self.remove_observer(observer, Some(&bye));
                }
            }
            ZoneInbound::Message(message) => hook!(self.on_message(message)),
        }
        self.process_kicks();
        delivered
    }

    /// Kick a player now.
    pub fn kick(&mut self, player: PlayerId, message: impl Into<String>) {
        self.core.pending_kicks.push((player, message.into()));
        self.process_kicks();
    }

    /// Freeze: ticks stop and inputs are rejected; clients get Freeze.
    pub fn freeze(&mut self, notice: Notice) {
        self.freeze_request = None;
        self.frozen = true;
        self.broadcast(&ServerMessage::Freeze(notice));
    }

    /// Barrier phase 1: freeze as soon as [`Zone::freeze_ready`] allows (now,
    /// or at the end of a later `advance`; the zone keeps ticking until
    /// then). Returns the resulting state.
    pub fn request_freeze(&mut self, notice: Notice) -> FreezeState {
        if !self.frozen {
            self.freeze_request = Some(notice);
            self.try_freeze();
        }
        self.freeze_state()
    }

    /// Abort a barrier: drop a pending freeze request, and resume with
    /// `notice` if already frozen.
    pub fn cancel_freeze(&mut self, notice: Notice) {
        self.freeze_request = None;
        if self.frozen {
            self.resume(notice);
        }
    }

    /// Freeze progress.
    pub fn freeze_state(&self) -> FreezeState {
        if self.frozen {
            FreezeState::Frozen
        } else if self.freeze_request.is_some() {
            FreezeState::Freezing
        } else {
            FreezeState::Running
        }
    }

    fn try_freeze(&mut self) {
        if self.freeze_request.is_some() && !self.disposed && self.zone.freeze_ready() {
            let notice = self.freeze_request.take().expect("checked");
            self.freeze(notice);
        }
    }

    /// Resume after [`freeze`](Self::freeze); the frozen backlog is not simulated.
    pub fn resume(&mut self, notice: Notice) {
        self.freeze_request = None;
        self.frozen = false;
        self.hot.reset();
        if let Some(idle) = &mut self.idle {
            idle.reset();
        }
        self.broadcast(&ServerMessage::Resume(notice));
    }

    /// Tell a player to move to `target_zone`, then remove it
    /// (`LeaveReason::Handoff`). The transfer saga is the caller's concern.
    pub fn handoff(&mut self, player: PlayerId, target_zone: &str, transfer_ticket: &str) {
        let message = ServerMessage::Handoff {
            target_zone: target_zone.into(),
            transfer_ticket: transfer_ticket.into(),
        };
        self.remove_player(player, LeaveReason::Handoff, Some(message));
        self.process_kicks();
    }

    /// Kick everyone (`on_leave(Disposed)` / `on_unobserve`), call
    /// `on_dispose`, clear timers. The host stops ticking afterwards.
    pub fn dispose(&mut self) {
        if self.disposed {
            return;
        }
        let bye = ServerMessage::Kick {
            code: kick::ZONE_DISPOSED,
            message: "zone disposed".into(),
        };
        let players: Vec<_> = self.core.players.keys().copied().collect();
        for player in players {
            self.remove_player(player, LeaveReason::Disposed, Some(bye.clone()));
        }
        let observers: Vec<_> = self.core.observers.keys().copied().collect();
        for observer in observers {
            self.remove_observer(observer, Some(&bye));
        }
        hook!(self.on_dispose());
        self.core.pending_kicks.clear();
        self.core.timers.clear();
        self.disposed = true;
    }

    fn broadcast(&mut self, message: &ServerMessage) {
        let clients: Vec<_> = self.core.clients.keys().copied().collect();
        for client in clients {
            self.core.server_message(client, message);
        }
    }

    fn remove_player(
        &mut self,
        player: PlayerId,
        reason: LeaveReason,
        last: Option<ServerMessage>,
    ) {
        let Some(slot) = self.core.players.remove(&player) else {
            return;
        };
        if let Some(client) = slot.client {
            self.core.clients.remove(&client);
            if let Some(message) = last {
                self.core.server_message(client, &message);
            }
            self.core.outbound.push(Outbound::Close { client });
        }
        hook!(self.on_leave(player, reason));
        if self.core.config.lifecycle_facts {
            let zone = self.core.zone_id.clone();
            let (principal, n) = (slot.info.principal, slot.transitions + 1);
            match LeftReason::from_leave(reason) {
                Some(reason) => {
                    self.core
                        .lifecycle(player, &principal, n, PlayerLeft { zone, reason })
                }
                None => self
                    .core
                    .lifecycle(player, &principal, n, PlayerDisconnected { zone }),
            }
        }
    }

    fn remove_observer(&mut self, observer: ObserverId, last: Option<&ServerMessage>) {
        let Some(slot) = self.core.observers.remove(&observer) else {
            return;
        };
        self.core.clients.remove(&slot.client);
        if let Some(message) = last {
            self.core.server_message(slot.client, message);
            self.core.outbound.push(Outbound::Close {
                client: slot.client,
            });
        }
        self.core.departing = Some(slot.info);
        hook!(self.on_unobserve(observer));
        self.core.departing = None;
    }

    fn process_kicks(&mut self) {
        while let Some((player, message, departed_at_ms)) = self.core.pending_handoffs.pop() {
            self.core.pinned_stamp_ms = Some(departed_at_ms);
            self.remove_player(player, LeaveReason::Handoff, Some(message));
            self.core.pinned_stamp_ms = None;
        }
        while let Some((player, message)) = self.core.pending_kicks.pop() {
            let kick = ServerMessage::Kick {
                code: kick::BY_ZONE,
                message,
            };
            self.remove_player(player, LeaveReason::Kicked, Some(kick));
        }
    }

    fn wake(&mut self) {
        let now = self.core.clock.now();
        self.set_tier(Tier::Hot, now);
    }

    fn set_tier(&mut self, to: Tier, now: Duration) {
        let from = self.core.tier;
        if from == to {
            return;
        }
        self.core.tier = to;
        self.hot.reset();
        if let Some(idle) = &mut self.idle {
            idle.reset();
        }
        match to {
            Tier::Hot => self.next_patch = now,
            Tier::Idle => self.idle_since = now,
            Tier::Dormant => {}
        }
        self.transitions.push((from, to));
        hook!(self.on_tier_changed(from, to));
        self.process_kicks();
    }

    fn evaluate_tier(&mut self, now: Duration) {
        let present = !self.core.players.is_empty() || !self.core.clients.is_empty();
        if present || self.zone.has_pending_work() {
            self.set_tier(Tier::Hot, now);
            return;
        }
        match self.core.tier {
            Tier::Hot => self.set_tier(Tier::Idle, now),
            Tier::Idle => {
                if now.saturating_sub(self.idle_since) >= self.core.config.dormant_after
                    && self.core.in_flight() == 0
                    && !self.zone.wants_background()
                {
                    self.set_tier(Tier::Dormant, now);
                }
            }
            Tier::Dormant => {}
        }
    }

    fn deliver_settlements(&mut self) {
        let tick = self.core.tick;
        let settled = match &mut self.core.facts {
            Some(queue) => queue.drain_at(tick),
            None => return,
        };
        for settlement in settled {
            self.pending.settled.push((tick, settlement.clone()));
            hook!(self.on_fact_settled(settlement));
        }
        self.process_kicks();
    }

    fn deliver_child_events(&mut self) {
        if self.frozen || self.core.child_inbox.is_empty() {
            return;
        }
        for event in std::mem::take(&mut self.core.child_inbox) {
            hook!(self.on_child_event(&event));
            if event.levels > 1 {
                let mut forward = event;
                forward.levels -= 1;
                forward.from_zone = self.core.zone_id.clone();
                self.core.upward.push(forward);
            }
        }
        self.process_kicks();
    }

    fn tick_once(&mut self, dt: Duration) {
        // wasm32-unknown-unknown (celld) has no `Instant`: measure with the
        // host clock there (the celld zone host).
        #[cfg(not(target_arch = "wasm32"))]
        let started = std::time::Instant::now();
        #[cfg(target_arch = "wasm32")]
        let started = self.core.clock.now();
        self.core.tick += 1;
        self.core.sim_time += dt;
        self.core.dt = dt;
        self.deliver_settlements();
        self.deliver_child_events();
        let due = self.core.timers.due(self.core.sim_time);
        for (id, timer) in due {
            hook!(self.on_timer(id, timer));
        }
        hook!(self.on_tick());
        self.core.dropped_ticks = 0;
        self.process_kicks();
        let mut frame = RewindFrame::default();
        self.zone.record_rewind(&mut frame);
        let entities = frame.into_entities();
        if !entities.is_empty() || !self.core.rewind.is_empty() {
            self.core
                .rewind
                .record(self.core.tick, self.core.sim_time, entities);
        }
        #[cfg(not(target_arch = "wasm32"))]
        self.pending.tick_durations.push(started.elapsed());
        #[cfg(target_arch = "wasm32")]
        self.pending
            .tick_durations
            .push(self.core.clock.now().saturating_sub(started));
        if self.hash_ticks {
            let hash = self.state_hash();
            self.pending.tick_hashes.push((self.core.tick, hash));
        }
    }

    /// This patch's replicated world: every registered entity collection's
    /// projected views ([`Zone::entities`]), then [`Zone::replicate`].
    pub fn world_view(&self) -> WorldView {
        let mut view = WorldView::default();
        self.entities.project(&self.zone, &mut view);
        self.zone.replicate(&mut view);
        view
    }

    fn patch(&mut self) {
        let view = self.world_view();
        let core = &mut self.core;
        self.pending.entities = Some(view.entities().len());
        let interest = &core.config.interest;
        let world = match self.last_world.take() {
            Some(last) => last.next(&self.schema, view, interest),
            None => EncodedWorld::build(&self.schema, view, interest),
        };
        let tick = core.tick as u32;
        let budget = core.config.byte_budget;
        let mut frames = Vec::with_capacity(core.clients.len());
        for (client, slot) in core.clients.iter_mut() {
            let (view, ack) = match slot.viewer {
                Viewer::Player(p) => match core.players.get(&p) {
                    Some(player) => (player.view, player.inputs.last_processed()),
                    None => continue,
                },
                Viewer::Observer(o) => match core.observers.get(&o) {
                    Some(observer) => (observer.view, 0),
                    None => continue,
                },
            };
            let (frame, stats) = slot.replica.build_delta(
                &self.schema,
                &world,
                &core.config.interest,
                &view,
                budget,
                tick,
                ack,
            );
            self.pending.frames.push(FrameStats {
                client: *client,
                bytes: frame.len(),
                stats,
            });
            frames.push(Outbound::Frame {
                client: *client,
                frame,
            });
        }
        core.outbound.extend(frames);
        self.last_world = Some(world);
    }

    /// Run due ticks and patches for the time elapsed on the clock.
    pub fn advance(&mut self) -> AdvanceReport {
        let mut report = std::mem::take(&mut self.pending);
        if self.disposed {
            return report;
        }
        let now = self.core.clock.now();
        let elapsed = now.saturating_sub(self.last_advance);
        self.last_advance = now;
        self.core.flush_lifecycle();

        let expired: Vec<_> = self
            .core
            .players
            .values()
            .filter(|slot| slot.grace_until.is_some_and(|until| until <= now))
            .map(|slot| slot.info.id)
            .collect();
        for player in expired {
            self.remove_player(player, LeaveReason::Disconnected, None);
        }
        self.process_kicks();
        self.evaluate_tier(now);

        let (steps, dt) = match self.core.tier {
            Tier::Hot => (self.hot.advance(elapsed), self.hot.step()),
            Tier::Idle => match &mut self.idle {
                Some(idle) => (idle.advance(elapsed), idle.step()),
                None => Default::default(),
            },
            Tier::Dormant => Default::default(),
        };
        report.dropped = steps.dropped;
        if !dt.is_zero() {
            report.dropped_ticks = (steps.dropped.as_nanos() / dt.as_nanos()) as u32;
        }
        self.core.dropped_ticks = self.core.dropped_ticks.saturating_add(report.dropped_ticks);
        if !self.frozen {
            for _ in 0..steps.steps {
                self.tick_once(dt);
            }
            report.ticks = steps.steps;
        }
        if report.ticks == 0 {
            self.deliver_settlements();
            self.deliver_child_events();
        }
        self.try_freeze();

        let patch_interval = rate_to_step(self.core.config.patch_rate);
        if self.core.tier == Tier::Hot && !self.core.clients.is_empty() && now >= self.next_patch {
            self.patch();
            report.patched = true;
            self.next_patch = if now - self.next_patch >= patch_interval {
                now + patch_interval
            } else {
                self.next_patch + patch_interval
            };
        }

        let mut wake = match self.core.tier {
            Tier::Hot => {
                let mut at = now + self.hot.until_next();
                if !self.core.clients.is_empty() {
                    at = at.min(self.next_patch);
                }
                Some(at)
            }
            Tier::Idle => {
                let dormant_at = self.idle_since + self.core.config.dormant_after;
                let tick_at = self.idle.as_ref().map(|idle| now + idle.until_next());
                Some(tick_at.map_or(dormant_at, |t| t.min(dormant_at)).max(now))
            }
            Tier::Dormant => None,
        };
        for slot in self.core.players.values() {
            if let Some(until) = slot.grace_until {
                wake = Some(wake.map_or(until, |w| w.min(until)));
            }
        }
        report.next_wake = wake;
        report.transitions = std::mem::take(&mut self.transitions);
        let pending = std::mem::take(&mut self.pending);
        report.tick_durations.extend(pending.tick_durations);
        report.entities = pending.entities;
        report.frames.extend(pending.frames);
        report.settled.extend(pending.settled);
        report.tick_hashes.extend(pending.tick_hashes);
        report
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
