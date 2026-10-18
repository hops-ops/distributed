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

use crate::facts::FactSettlement;
use crate::host::Ctx;
use crate::protocol::Role;
use crate::replication::WorldView;
use crate::rewind::RewindFrame;
use crate::schema::EntitySchema;
use crate::timers::TimerId;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketClaims {
    /// Verified principal (`sub`).
    pub principal: String,
    /// Zone the ticket admits to.
    pub zone: String,
    /// Expiry, Unix seconds.
    pub expires_at: u64,
    /// Single-use nonce.
    pub nonce: String,
    /// Player or observer.
    pub role: Role,
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
}

/// Player as seen by the zone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerInfo {
    /// Stable id.
    pub id: PlayerId,
    /// Verified principal from the ticket.
    pub principal: String,
    /// Hello capabilities.
    pub capabilities: u32,
    /// False while inside the reconnect grace window.
    pub connected: bool,
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

    /// A player left for `reason` (not called on a disconnect still inside the grace window).
    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, _player: PlayerId, _reason: LeaveReason) {}

    /// An observer subscribed.
    fn on_observe(&mut self, _ctx: &mut Ctx<'_, Self>, _observer: ObserverId) {}

    /// An observer left.
    fn on_unobserve(&mut self, _ctx: &mut Ctx<'_, Self>, _observer: ObserverId) {}

    /// Decode raw input bytes from the wire; `None` drops the input.
    fn decode_input(&self, player: PlayerId, payload: &[u8]) -> Option<Self::Input>;

    /// Validate/clamp an input before buffering; `None` rejects it.
    fn sanitize_input(&self, _player: PlayerId, input: Self::Input) -> Option<Self::Input> {
        Some(input)
    }

    /// One fixed simulation step (`ctx.dt()` is the step length of the tier).
    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>);

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

    /// Describe replicated entities for this patch.
    fn replicate(&self, out: &mut WorldView);

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
}
