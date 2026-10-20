//! The [`Zone`] trait a game implements, and the identity types it sees.
//!
//! A zone is one authoritative simulation instance (a dungeon floor, a match,
//! a town shard). The [`ZoneHost`](crate::ZoneHost) owns it and calls these
//! hooks; every hook gets a [`Ctx`] for timers, RNG, inputs,
//! facts, events and views. Hooks are synchronous and must not block: durable
//! work goes through facts, never through awaited storage.
//!
//! Lifecycle: `on_create` → (`on_auth` → `on_join` | `on_observe`)* with
//! `on_tick`/`on_timer`/`on_fact_settled` interleaved → `on_leave` /
//! `on_unobserve` → `on_dispose`.

use crate::claims::TicketExtensions;
use crate::facts::FactSettlement;
use crate::host::Ctx;
use crate::protocol::Role;
use crate::replication::WorldView;
use crate::rewind::RewindFrame;
use crate::schema::EntitySchema;
use crate::timers::TimerId;
use crate::world::ChildEvent;

/// Stable id of a player within a zone (survives reconnects).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PlayerId(pub u32);

/// Id of a read-only observer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObserverId(pub u32);

/// Id of one transport connection (changes on reconnect).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(pub u32);

/// Anything that has a view and receives events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Viewer {
    /// A player.
    Player(PlayerId),
    /// An observer.
    Observer(ObserverId),
}

impl From<PlayerId> for Viewer {
    fn from(id: PlayerId) -> Self {
        Self::Player(id)
    }
}

impl From<ObserverId> for Viewer {
    fn from(id: ObserverId) -> Self {
        Self::Observer(id)
    }
}

/// Claims from a **verified** admission ticket. The principal is the only
/// identity the runtime trusts; nothing in client payloads can change it.
///
/// Built by a `net::TicketVerifier` (the default HS256 `net::TicketKey`, or a
/// game's own verifier for tickets minted by its web backend).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketClaims {
    /// Verified principal (`sub`).
    pub principal: String,
    /// Zone the ticket admits to.
    pub zone: String,
    /// Expiry, Unix seconds.
    pub expires_at: u64,
    /// Single-use replay key (`nonce`, or a JWT `jti`): the server refuses
    /// a second admission with the same key until `expires_at`.
    pub nonce: String,
    /// Player or observer.
    pub role: Role,
    /// Optional display name vouched for by the ticket issuer (never
    /// identity: use `principal` for that). Exposed as
    /// [`PlayerInfo::name`] / [`ObserverInfo::name`].
    pub name: Option<String>,
    /// Game-declared claims the issuer vouches for (match id, team…; the
    /// ticket's `ext`), checked by the `net` server against
    /// `ServerConfig::ticket_extensions`. Never identity or rewards.
    pub extensions: TicketExtensions,
}

impl TicketClaims {
    /// Claims without a display name.
    pub fn new(
        principal: impl Into<String>,
        zone: impl Into<String>,
        expires_at: u64,
        nonce: impl Into<String>,
        role: Role,
    ) -> Self {
        Self {
            principal: principal.into(),
            zone: zone.into(),
            expires_at,
            nonce: nonce.into(),
            role,
            name: None,
            extensions: TicketExtensions::new(),
        }
    }

    /// Attach extension claims.
    pub fn with_extensions(mut self, extensions: TicketExtensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Attach a display name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Key the server's replay cache records (the nonce / `jti`).
    pub fn replay_key(&self) -> &str {
        &self.nonce
    }
}

/// Admission request seen by [`Zone::on_auth`].
#[derive(Debug, Clone, Copy)]
pub struct JoinRequest<'a> {
    /// Verified claims.
    pub claims: &'a TicketClaims,
    /// Hello capabilities (game-defined bits).
    pub capabilities: u32,
    /// Client content version.
    pub content_version: &'a str,
}

/// `on_auth` decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Let them in.
    Admit,
    /// Refuse with a message.
    Reject(String),
}

/// Why a player left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveReason {
    /// Client sent Leave.
    Left,
    /// Connection lost and the reconnect grace window elapsed.
    Disconnected,
    /// Removed by the zone or operator.
    Kicked,
    /// Handed off to another zone.
    Handoff,
    /// Zone disposed.
    Disposed,
    /// A fresh ticket for the same principal started a new client session in
    /// this slot (e.g. a page reload); see [`Zone::on_rejoin`].
    Rejoined,
}

/// Player as seen by the zone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerInfo {
    /// Stable id.
    pub id: PlayerId,
    /// Verified principal from the ticket.
    pub principal: String,
    /// Display name from the ticket ([`TicketClaims::name`]), if any.
    pub name: Option<String>,
    /// Extension claims from the ticket ([`TicketClaims::extensions`]);
    /// replaced by a fresh ticket's on rejoin, carried across handoffs.
    pub extensions: TicketExtensions,
    /// Hello capabilities.
    pub capabilities: u32,
    /// False while inside the reconnect grace window.
    pub connected: bool,
}

/// Observer as seen by the zone ([`Ctx::observer`](crate::Ctx::observer)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserverInfo {
    /// Observer id (address events with [`Ctx::send_event`](crate::Ctx::send_event)).
    pub id: ObserverId,
    /// Verified principal from the observer ticket.
    pub principal: String,
    /// Display name from the ticket, if any.
    pub name: Option<String>,
    /// Extension claims from the ticket.
    pub extensions: TicketExtensions,
    /// Hello capabilities.
    pub capabilities: u32,
}

/// Activity tier of a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    /// Clients connected or work pending: full tick rate, patches flowing.
    Hot,
    /// Empty: optional slow clock for background simulation.
    Idle,
    /// No ticks; eligible for hibernation.
    Dormant,
}

/// A game zone. See the module docs for the lifecycle.
pub trait Zone: Sized + Send + 'static {
    /// Construction parameters.
    type Config;
    /// Decoded, sanitized player input.
    type Input: Send + 'static;
    /// Timer payload.
    type Timer: Clone + Send + 'static;
    /// Messages services deliver into the running zone
    /// ([`on_message`](Self::on_message), [`crate::inbox`]): a
    /// `#[derive(GameEvent)]` struct or enum, or [`NoMessages`](crate::NoMessages).
    type Message: crate::GameEvent + Send + 'static;

    /// Replicated entity schema (called once at creation).
    fn schema() -> EntitySchema;

    /// Build the zone. Timers and RNG are available through `ctx`.
    fn on_create(ctx: &mut Ctx<'_, Self>, config: Self::Config) -> Self;

    /// Decide admission for a verified ticket (capacity is checked first).
    fn on_auth(&mut self, _ctx: &mut Ctx<'_, Self>, _request: &JoinRequest<'_>) -> Admission {
        Admission::Admit
    }

    /// A player joined (not called on reconnect).
    fn on_join(&mut self, _ctx: &mut Ctx<'_, Self>, _player: PlayerId) {}

    /// A player left for `reason` (not called on a disconnect still inside
    /// the grace window: see [`on_connection_lost`](Self::on_connection_lost)).
    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, _player: PlayerId, _reason: LeaveReason) {}

    /// A player's connection dropped (or it was told to reload) and its
    /// reconnect grace window started; the slot and its entity stay.
    /// `on_leave(Disconnected)` follows if it does not come back in time.
    fn on_connection_lost(&mut self, _ctx: &mut Ctx<'_, Self>, _player: PlayerId) {}

    /// A player resumed its slot with a reconnect token within the grace
    /// window.
    fn on_reconnected(&mut self, _ctx: &mut Ctx<'_, Self>, _player: PlayerId) {}

    /// A fresh ticket (not a reconnect token) arrived for a principal that
    /// already holds a player slot here, e.g. the page was reloaded or opened
    /// in another tab with the same identity. The old connection was
    /// replaced (`Kick(REPLACED)`) and the new one is a new client session:
    /// its input sequence starts again and it knows nothing about per-player
    /// state the zone sent on join (own entity id, hello events…).
    ///
    /// Defaults to `on_leave(Rejoined)` then `on_join`, i.e. a fresh join in
    /// the same [`PlayerId`]. Override to keep in-zone state and only resend
    /// what the client needs.
    fn on_rejoin(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        self.on_leave(ctx, player, LeaveReason::Rejoined);
        self.on_join(ctx, player);
    }

    /// An observer subscribed. Its principal and name are available from
    /// [`Ctx::observer`](crate::Ctx::observer); address it with
    /// [`Ctx::send_event`](crate::Ctx::send_event)`(observer, ..)`.
    fn on_observe(&mut self, _ctx: &mut Ctx<'_, Self>, _observer: ObserverId) {}

    /// An observer left. [`Ctx::observer`](crate::Ctx::observer) still
    /// returns it during this hook (it can no longer receive events).
    fn on_unobserve(&mut self, _ctx: &mut Ctx<'_, Self>, _observer: ObserverId) {}

    /// Decode raw input bytes from the wire; `None` drops the input.
    fn decode_input(&self, player: PlayerId, payload: &[u8]) -> Option<Self::Input>;

    /// Validate/clamp an input before buffering; `None` rejects it.
    fn sanitize_input(&self, _player: PlayerId, input: Self::Input) -> Option<Self::Input> {
        Some(input)
    }

    /// One fixed simulation step (`ctx.dt()` is the step length of the tier).
    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>);

    /// A message from a service ([`crate::inbox`]), delivered on the zone's
    /// task between ticks (never during one). The built-in kick is handled
    /// by the host and never reaches this hook.
    fn on_message(&mut self, _ctx: &mut Ctx<'_, Self>, _message: Self::Message) {}

    /// A timer fired (before `on_tick` of the same tick).
    fn on_timer(&mut self, _ctx: &mut Ctx<'_, Self>, _id: TimerId, _timer: Self::Timer) {}

    /// A fact emitted earlier settled.
    fn on_fact_settled(&mut self, _ctx: &mut Ctx<'_, Self>, _settlement: FactSettlement) {}

    /// The activity tier changed.
    fn on_tier_changed(&mut self, _ctx: &mut Ctx<'_, Self>, _from: Tier, _to: Tier) {}

    /// True keeps the zone Hot even with nobody connected.
    fn has_pending_work(&self) -> bool {
        false
    }

    /// True keeps an empty zone Idle (background clock) instead of Dormant.
    fn wants_background(&self) -> bool {
        false
    }

    /// The zone's entity collections, registered once at creation
    /// ([`entity`](crate::entity)): every patch the host pushes each
    /// entity's [`Project`](crate::Project)ed view (in registration order),
    /// then calls [`replicate`](Self::replicate). Default: none.
    fn entities() -> crate::Entities<Self> {
        crate::Entities::new()
    }

    /// Describe replicated entities for this patch that are not projected
    /// from a registered collection ([`entities`](Self::entities)).
    /// Default: nothing.
    fn replicate(&self, _out: &mut WorldView) {}

    /// Record hit volumes for lag compensation after each tick
    /// ([`Ctx::rewind`](crate::Ctx::rewind)). Default: nothing recorded.
    fn record_rewind(&self, _frame: &mut RewindFrame) {}

    /// Digest of the authoritative state for record/replay
    /// ([`replay`](mod@crate::replay)). `None` hashes the replicated
    /// [`WorldView`] instead, which misses unreplicated state.
    fn state_hash(&self) -> Option<u64> {
        None
    }

    /// The zone is being destroyed; timers are cleared afterwards.
    fn on_dispose(&mut self, _ctx: &mut Ctx<'_, Self>) {}

    // ---- World structure (all optional) ----

    /// Durable snapshot taken when the zone hibernates (Dormant long
    /// enough); `None` = nothing worth keeping. Stored in a
    /// [`ZoneStateStore`](crate::world::ZoneStateStore) and handed to
    /// [`on_restore`](Self::on_restore) on the next activation.
    fn hibernate(&self) -> Option<Vec<u8>> {
        None
    }

    /// Restore a snapshot written by [`hibernate`](Self::hibernate) (called
    /// right after `on_create`). `saved_version` is the content version it
    /// was written under.
    fn on_restore(&mut self, _ctx: &mut Ctx<'_, Self>, _state: &[u8], _saved_version: &str) {}

    /// A player arrived through a handoff from `from_zone` with `carried`
    /// state (instead of `on_join`). Also called when the principal was
    /// still present (reconnect grace), and for a handoff that failed and
    /// returned to its source (`from_zone` is then the zone it never
    /// reached). Defaults to `on_join`.
    fn on_handoff_join(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        player: PlayerId,
        _from_zone: &str,
        _carried: Vec<u8>,
    ) {
        self.on_join(ctx, player);
    }

    /// A tier-up event from a descendant zone (see
    /// [`world::topology`](crate::world::topology)).
    fn on_child_event(&mut self, _ctx: &mut Ctx<'_, Self>, _event: &ChildEvent) {}

    /// True marks a safe-room gate: stale clients are told to reload here
    /// (see [`StalePolicy`](crate::world::StalePolicy)).
    fn is_gate(&self) -> bool {
        false
    }

    /// Barrier readiness: while false a requested freeze waits (the zone
    /// keeps ticking), e.g. to finish a boss phase. A world barrier aborts
    /// if a zone is not ready within its timeout.
    fn freeze_ready(&self) -> bool {
        true
    }

    /// The content version changed (world barrier commit).
    fn on_content_version_changed(&mut self, _ctx: &mut Ctx<'_, Self>, _from: &str, _to: &str) {}
}
