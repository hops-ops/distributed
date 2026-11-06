//! Zone-to-zone messages: one running zone tells another something, e.g. a
//! shot that carries on into the room next door, or a mob that follows a
//! player across a boundary.
//!
//! A zone sends with [`Ctx::send_zone`](crate::Ctx::send_zone) (or
//! [`send_zone_for`](crate::Ctx::send_zone_for), which binds a player's
//! verified principal into the message, as facts do); the target handles
//! it in [`Zone::on_zone_message`](crate::Zone::on_zone_message). Messages
//! use the zone's own [`Zone::Message`](crate::Zone::Message) type and
//! codec.
//!
//! Each message is a [`ZoneEnvelope`] with an id unique to the sending
//! activation (`zone/boot_epoch/seq`). The driver takes them with
//! [`ZoneHost::take_zone_messages`](crate::ZoneHost::take_zone_messages)
//! and hands them to the target's
//! [`ZoneHost::deliver_zone`](crate::ZoneHost::deliver_zone), which drops
//! an id it has seen recently (at most once per activation; at-least-once
//! transports may resend). On celld the object posts envelopes to the
//! target's object with the internal secret (like a cell's push), never on
//! a public route. A message for a zone that is not running is refused: it
//! never wakes a zone. Delivery is best effort; a protocol that must not
//! lose state (a mob crossing over) needs its own acks and timeouts:
//! [`lease`](crate::lease) keeps them for an entity lent to another zone.
//!
//! **Identity.** The receiving zone sees the sender's principal only as a
//! [`VerifiedPrincipal`], which game code cannot construct: it came from a
//! verified player ticket in the sending zone. Facts on behalf of it go
//! through [`Ctx::emit_fact_as`](crate::Ctx::emit_fact_as).

use std::collections::{HashSet, VecDeque};

use serde::{Deserialize, Serialize};

/// A message between zones, as carried by a transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZoneEnvelope {
    /// Unique per sending activation (`zone/boot_epoch/seq`): the target
    /// drops repeats.
    pub id: String,
    /// Sending zone id.
    pub from: String,
    /// Target zone id.
    pub to: String,
    /// The verified principal of the player the message is on behalf of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    /// Event type id of the message ([`GameEvent::event_id`](crate::GameEvent::event_id)).
    pub type_id: u32,
    /// Its encoded payload.
    pub payload: Vec<u8>,
}

/// A principal bound from a verified player ticket (in this zone or in the
/// zone that sent a message). Only the framework makes one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VerifiedPrincipal(String);

impl VerifiedPrincipal {
    pub(crate) fn new(principal: String) -> Self {
        Self(principal)
    }

    /// The principal.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Who sent a zone message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneSender {
    /// The sending zone's id.
    pub zone: String,
    /// The player it was sent on behalf of
    /// ([`send_zone_for`](crate::Ctx::send_zone_for)).
    pub principal: Option<VerifiedPrincipal>,
}

/// Why a zone message was not delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZoneMessageError {
    /// Addressed to another zone.
    WrongZone,
    /// Seen before (a resend): dropped.
    Duplicate,
    /// Not a message of this zone's type.
    Decode(String),
}

impl std::fmt::Display for ZoneMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongZone => f.write_str("addressed to another zone"),
            Self::Duplicate => f.write_str("duplicate message"),
            Self::Decode(e) => write!(f, "bad message: {e}"),
        }
    }
}

impl std::error::Error for ZoneMessageError {}

/// Message ids seen lately (bounded: the oldest are forgotten first).
#[derive(Debug, Default)]
pub(crate) struct SeenIds {
    ids: HashSet<String>,
    order: VecDeque<String>,
}

/// Ids remembered per activation.
const SEEN_CAPACITY: usize = 4096;

impl SeenIds {
    /// True the first time `id` is offered.
    pub(crate) fn first(&mut self, id: &str) -> bool {
        if self.ids.contains(id) {
            return false;
        }
        if self.order.len() >= SEEN_CAPACITY {
            if let Some(old) = self.order.pop_front() {
                self.ids.remove(&old);
            }
        }
        self.ids.insert(id.to_string());
        self.order.push_back(id.to_string());
        true
    }
}
