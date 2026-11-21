//! Player lifecycle facts emitted by the host itself.
//!
//! With [`HostConfig::lifecycle_facts`](crate::HostConfig::lifecycle_facts)
//! on, every player transition becomes a durable fact on the zone's
//! [`FactQueue`](crate::FactQueue), bound to the player's **verified
//! principal** (never to anything a zone or client wrote):
//!
//! | Fact | Command | When |
//! |---|---|---|
//! | [`PlayerJoined`] | [`PLAYER_JOINED`] | admitted with a ticket, a fresh ticket for a held slot (rejoin) or a transfer ticket (handoff) |
//! | [`ConnectionLost`] | [`CONNECTION_LOST`] | the connection dropped (or was told to reload); the reconnect grace starts |
//! | [`PlayerReconnected`] | [`PLAYER_RECONNECTED`] | a reconnect token resumed the slot within the grace |
//! | [`PlayerDisconnected`] | [`PLAYER_DISCONNECTED`] | the grace expired; the slot is gone |
//! | [`PlayerLeft`] | [`PLAYER_LEFT`] | the slot was removed for any other [`LeftReason`] |
//!
//! Every slot ends with exactly one of [`PlayerLeft`] / [`PlayerDisconnected`].
//!
//! **Keys** are `"{command}:{zone}:{boot_epoch}:p{slot}:{n}"` where `n`
//! counts the slot's transitions: computed once at emission (a retry keeps
//! it), never repeated within an activation, and distinct after a restart
//! (new boot epoch). **Order** per principal follows emission: the facts
//! travel in the principal's lane of the fact worker (also with
//! `max_concurrent_commits > 1`), and when the queue is full the host keeps
//! them in a backlog and sends them, in order, as capacity frees up — a
//! lifecycle fact is never dropped for backpressure. A `PlayerJoined` is
//! queued before anything the zone emits in `on_join`, a `PlayerLeft` after
//! `on_leave`. A handoff departure is stamped with the time in the transfer
//! ticket, so the target's `PlayerJoined` sorts after the source's
//! `PlayerLeft` ([`FactStamp::emission_order`](crate::FactStamp::emission_order)).

use serde::{Deserialize, Serialize};

use crate::zone::LeaveReason;

/// Command of [`PlayerJoined`].
pub const PLAYER_JOINED: &str = "zone.player_joined";
/// Command of [`PlayerLeft`].
pub const PLAYER_LEFT: &str = "zone.player_left";
/// Command of [`ConnectionLost`].
pub const CONNECTION_LOST: &str = "zone.connection_lost";
/// Command of [`PlayerReconnected`].
pub const PLAYER_RECONNECTED: &str = "zone.player_reconnected";
/// Command of [`PlayerDisconnected`].
pub const PLAYER_DISCONNECTED: &str = "zone.player_disconnected";

/// Every lifecycle command, in the table's order.
pub const LIFECYCLE_COMMANDS: [&str; 5] = [
    PLAYER_JOINED,
    CONNECTION_LOST,
    PLAYER_RECONNECTED,
    PLAYER_DISCONNECTED,
    PLAYER_LEFT,
];

/// How a player entered the zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinVia {
    /// An admission ticket for a new slot.
    Ticket,
    /// A fresh ticket for a slot the principal already held (page reload,
    /// second tab).
    Rejoin,
    /// A handoff transfer ticket.
    Handoff,
}

/// Player admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, crate::FactCommand)]
#[fact(command = PLAYER_JOINED)]
pub struct PlayerJoined {
    /// Zone id.
    pub zone: String,
    /// Ticket, rejoin or handoff.
    pub via: JoinVia,
    /// Source zone of a handoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_zone: Option<String>,
}

/// Why a slot was removed (a grace expiry is [`PlayerDisconnected`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeftReason {
    /// The client left gracefully.
    Left,
    /// Kicked by the zone or the server (incl. the zone inbox).
    Kicked,
    /// Moved to another zone.
    Handoff,
    /// The zone was disposed (shutdown, hibernation with players is
    /// impossible, ownership loss).
    Disposed,
}

impl LeftReason {
    /// The fact reason for a host leave reason (`None` for `Disconnected`,
    /// which is its own fact, and `Rejoined`, which keeps the slot).
    pub fn from_leave(reason: LeaveReason) -> Option<Self> {
        match reason {
            LeaveReason::Left => Some(Self::Left),
            LeaveReason::Kicked => Some(Self::Kicked),
            LeaveReason::Handoff => Some(Self::Handoff),
            LeaveReason::Disposed => Some(Self::Disposed),
            LeaveReason::Disconnected | LeaveReason::Rejoined => None,
        }
    }
}

/// Slot removed (terminal).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, crate::FactCommand)]
#[fact(command = PLAYER_LEFT)]
pub struct PlayerLeft {
    /// Zone id.
    pub zone: String,
    /// Why.
    pub reason: LeftReason,
}

/// What ended the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LostCause {
    /// The transport dropped.
    Dropped,
    /// The server told the client to reload its content (`Kick(RELOAD)`).
    Reload,
}

/// Connection lost; the slot is kept for `grace_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, crate::FactCommand)]
#[fact(command = CONNECTION_LOST)]
pub struct ConnectionLost {
    /// Zone id.
    pub zone: String,
    /// Reconnect grace (ms).
    pub grace_ms: u64,
    /// Dropped or told to reload.
    pub cause: LostCause,
}

/// Slot resumed with a reconnect token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, crate::FactCommand)]
#[fact(command = PLAYER_RECONNECTED)]
pub struct PlayerReconnected {
    /// Zone id.
    pub zone: String,
}

/// The reconnect grace expired; the slot is gone (terminal).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, crate::FactCommand)]
#[fact(command = PLAYER_DISCONNECTED)]
pub struct PlayerDisconnected {
    /// Zone id.
    pub zone: String,
}

/// Any lifecycle fact input, decoded from a [`Fact`](crate::Fact) or
/// [`FactSettlement`](crate::FactSettlement) by command name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lifecycle {
    /// [`PlayerJoined`].
    Joined(PlayerJoined),
    /// [`ConnectionLost`].
    ConnectionLost(ConnectionLost),
    /// [`PlayerReconnected`].
    Reconnected(PlayerReconnected),
    /// [`PlayerDisconnected`].
    Disconnected(PlayerDisconnected),
    /// [`PlayerLeft`].
    Left(PlayerLeft),
}

impl Lifecycle {
    /// Decode `input` of `command`; `None` for other commands or a
    /// malformed input.
    pub fn decode(command: &str, input: &serde_json::Value) -> Option<Self> {
        let input = input.clone();
        Some(match command {
            PLAYER_JOINED => Self::Joined(serde_json::from_value(input).ok()?),
            CONNECTION_LOST => Self::ConnectionLost(serde_json::from_value(input).ok()?),
            PLAYER_RECONNECTED => Self::Reconnected(serde_json::from_value(input).ok()?),
            PLAYER_DISCONNECTED => Self::Disconnected(serde_json::from_value(input).ok()?),
            PLAYER_LEFT => Self::Left(serde_json::from_value(input).ok()?),
            _ => return None,
        })
    }

    /// Decode a fact.
    pub fn of(fact: &crate::Fact) -> Option<Self> {
        Self::decode(fact.command(), fact.input())
    }

    /// Zone id.
    pub fn zone(&self) -> &str {
        match self {
            Self::Joined(f) => &f.zone,
            Self::ConnectionLost(f) => &f.zone,
            Self::Reconnected(f) => &f.zone,
            Self::Disconnected(f) => &f.zone,
            Self::Left(f) => &f.zone,
        }
    }

    /// True for the terminal facts (the slot is gone).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Disconnected(_) | Self::Left(_))
    }
}
