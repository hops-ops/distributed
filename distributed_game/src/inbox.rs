//! The zone inbox: typed messages from services into a running zone.
//!
//! A zone declares [`Zone::Message`](crate::Zone::Message) — a
//! `#[derive(GameEvent)]` struct or enum, so messages have stable type ids
//! and the same typed binary payload codec as events (and appear in the
//! zone's [`GameSurface`](crate::GameSurface) under `messages`) — and
//! handles them in [`Zone::on_message`](crate::Zone::on_message). Zones that
//! take none use [`NoMessages`].
//!
//! Delivery ([`ZoneHost::deliver`](crate::ZoneHost::deliver)) runs on the
//! zone's own task between ticks, never concurrently with one. The host
//! handles the built-in [`ZoneInbound::Kick`] itself (the player leaves with
//! `LeaveReason::Kicked` and, with lifecycle facts on, a `PlayerLeft`
//! fact). Over the network: `GameServer::send` locally, `POST
//! /admin/zone-message` for peers and services (admin secret), and
//! `net::ZoneInboxClient`, which finds the owning host through the
//! `ZoneDirectory` and follows `wrong_owner` answers. A message for a zone
//! that is not active is refused; it never activates the zone.

use crate::authoring::GameEvent;
use crate::protocol::wire::{DecodeError, Writer};
use crate::surface::{EventSurface, RecordSurface};

/// What a service can deliver into a zone.
#[derive(Debug, Clone, PartialEq)]
pub enum ZoneInbound<M> {
    /// Remove every connection of `principal` (players leave with
    /// `LeaveReason::Kicked`, observers are unsubscribed). Clients get
    /// `Kick(BY_ZONE, reason)`.
    Kick {
        /// Verified principal to remove (a service decision, e.g. a ban).
        principal: String,
        /// Shown to the client.
        reason: String,
    },
    /// A game message for [`Zone::on_message`](crate::Zone::on_message).
    Message(M),
}

/// Outcome of a delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Delivered {
    /// Players removed by a kick.
    pub players_kicked: usize,
    /// Observers removed by a kick.
    pub observers_kicked: usize,
}

/// The message type of a zone that takes none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoMessages {}

impl GameEvent for NoMessages {
    fn event_id(&self) -> u32 {
        match *self {}
    }

    fn encode_payload(&self, _w: &mut Writer) {
        match *self {}
    }

    fn decode_event(_type_id: u32, _payload: &[u8]) -> Result<Option<Self>, DecodeError> {
        Ok(None)
    }

    fn event_surfaces(_records: &mut Vec<RecordSurface>) -> Vec<EventSurface> {
        Vec::new()
    }
}
