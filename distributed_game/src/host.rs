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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::clock::Clock;
use crate::facts::{EmitError, Fact, FactQueue};
use crate::input::{ConsumePolicy, InputBuffer, PushOutcome, SequencedInput};
use crate::interest::{InterestConfig, View};
use crate::protocol::{error, kick, Notice, Role, ServerMessage, Welcome};
use crate::replication::{ClientReplica, EncodedWorld, WorldView};
use crate::rng::ZoneRng;
use crate::schema::EntitySchema;
use crate::stamp::FactStamp;
use crate::timers::{TimerId, Timers};
use crate::timestep::{rate_to_step, FixedTimestep};
use crate::zone::{
    Admission, ClientId, JoinRequest, LeaveReason, ObserverId, PlayerId, PlayerInfo, TicketClaims,
    Tier, Viewer, Zone,
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
}

struct ObserverSlot {
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
    tier: Tier,
    players: BTreeMap<PlayerId, PlayerSlot<Z::Input>>,
    observers: BTreeMap<ObserverId, ObserverSlot>,
    clients: BTreeMap<ClientId, ClientSlot>,
    next_id: u32,
    outbound: Vec<Outbound>,
    facts: Option<FactQueue>,
    pending_kicks: Vec<(PlayerId, String)>,
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
        self.facts.as_ref().map_or(0, FactQueue::in_flight)
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

    /// Emit a zone/system fact (no principal). Never blocks.
    pub fn emit_fact(&mut self, fact: Fact) -> Result<(), EmitError> {
        self.emit(fact, None)
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
        self.emit(fact, Some(principal))
    }

    fn emit(&mut self, fact: Fact, principal: Option<String>) -> Result<(), EmitError> {
        let stamp = FactStamp {
            zone_id: self.core.zone_id.clone(),
            tick: self.core.tick,
            content_version: self.core.config.content_version.clone(),
        };
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
}

/// Runs one zone. See the module docs.
pub struct ZoneHost<Z: Zone> {
    zone: Z,
    core: Core<Z>,
    schema: EntitySchema,
    hot: FixedTimestep,
    idle: Option<FixedTimestep>,
    last_advance: Duration,
    idle_since: Duration,
    next_patch: Duration,
    transitions: Vec<(Tier, Tier)>,
    frozen: bool,
    disposed: bool,
    token_source: Box<dyn FnMut() -> String + Send>,
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
        let mut core = Core {
            rng: ZoneRng::for_zone(config.world_seed, &zone_id),
            zone_id,
            clock,
            timers: Timers::default(),
            tick: 0,
            sim_time: Duration::ZERO,
            dt: rate_to_step(config.tick_rate),
            tier: Tier::Hot,
            players: BTreeMap::new(),
            observers: BTreeMap::new(),
            clients: BTreeMap::new(),
            next_id: 0,
            outbound: Vec::new(),
            facts: None,
            pending_kicks: Vec::new(),
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
            hot,
            idle,
            last_advance: now,
            idle_since: now,
            next_patch: now,
            transitions: Vec::new(),
            frozen: false,
            disposed: false,
            token_source: Box::new(default_token),
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

    /// Live connections.
    pub fn client_count(&self) -> usize {
        self.core.clients.len()
    }

    /// Viewer bound to a connection.
    pub fn viewer_of(&self, client: ClientId) -> Option<Viewer> {
        self.core.clients.get(&client).map(|slot| slot.viewer)
    }

    /// Unsettled facts.
    pub fn facts_in_flight(&self) -> usize {
        self.core.in_flight()
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
        if !expected.is_empty() && content_version != expected {
            return Err(JoinError::new(
                error::VERSION_MISMATCH,
                format!("content version {content_version} != {expected}"),
            ));
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
        let joined = match (claims.role, existing) {
            (Role::Player, Some(player)) => {
                self.bind_player(player, client, capabilities);
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
                            capabilities,
                            connected: true,
                        },
                        inputs: InputBuffer::new(capacity),
                        client: None,
                        view: View::new(0.0, 0.0, radius),
                        grace_until: None,
                        reconnect_token: String::new(),
                        session_id: (self.token_source)(),
                    },
                );
                self.bind_player(player, client, capabilities);
                hook!(self.on_join(player));
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
        self.process_kicks();
        Ok(joined)
    }

    /// Resume a player's session with the token from its last Welcome.
    pub fn reconnect(&mut self, token: &str, capabilities: u32) -> Result<Joined, JoinError> {
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
        self.wake();
        let client = ClientId(self.core.next_id());
        self.bind_player(player, client, capabilities);
        Ok(Joined {
            client,
            viewer: Viewer::Player(player),
        })
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
                }
            }
            Viewer::Observer(observer) => {
                self.core.observers.remove(&observer);
                hook!(self.on_unobserve(observer));
            }
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

    /// Kick a player now.
    pub fn kick(&mut self, player: PlayerId, message: impl Into<String>) {
        self.core.pending_kicks.push((player, message.into()));
        self.process_kicks();
    }

    /// Freeze: ticks stop and inputs are rejected; clients get Freeze.
    pub fn freeze(&mut self, notice: Notice) {
        self.frozen = true;
        self.broadcast(&ServerMessage::Freeze(notice));
    }

    /// Resume after [`freeze`](Self::freeze); the frozen backlog is not simulated.
    pub fn resume(&mut self, notice: Notice) {
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
            if let Some(slot) = self.core.observers.remove(&observer) {
                self.core.clients.remove(&slot.client);
                self.core.server_message(slot.client, &bye);
                self.core.outbound.push(Outbound::Close {
                    client: slot.client,
                });
            }
            hook!(self.on_unobserve(observer));
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
    }

    fn process_kicks(&mut self) {
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
        let settled = match &mut self.core.facts {
            Some(queue) => queue.drain(),
            None => return,
        };
        for settlement in settled {
            hook!(self.on_fact_settled(settlement));
        }
        self.process_kicks();
    }

    fn tick_once(&mut self, dt: Duration) {
        self.core.tick += 1;
        self.core.sim_time += dt;
        self.core.dt = dt;
        self.deliver_settlements();
        let due = self.core.timers.due(self.core.sim_time);
        for (id, timer) in due {
            hook!(self.on_timer(id, timer));
        }
        hook!(self.on_tick());
        self.process_kicks();
    }

    fn patch(&mut self) {
        let mut view = WorldView::default();
        self.zone.replicate(&mut view);
        let core = &mut self.core;
        let world = EncodedWorld::build(&self.schema, view, &core.config.interest);
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
            let (frame, _) = slot.replica.build_delta(
                &self.schema,
                &world,
                &core.config.interest,
                &view,
                budget,
                tick,
                ack,
            );
            frames.push(Outbound::Frame {
                client: *client,
                frame,
            });
        }
        core.outbound.extend(frames);
    }

    /// Run due ticks and patches for the time elapsed on the clock.
    pub fn advance(&mut self) -> AdvanceReport {
        let mut report = AdvanceReport::default();
        if self.disposed {
            return report;
        }
        let now = self.core.clock.now();
        let elapsed = now.saturating_sub(self.last_advance);
        self.last_advance = now;

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
        if !self.frozen {
            for _ in 0..steps.steps {
                self.tick_once(dt);
            }
            report.ticks = steps.steps;
        }
        if report.ticks == 0 {
            self.deliver_settlements();
        }

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
        report
    }
}

fn default_token() -> String {
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
