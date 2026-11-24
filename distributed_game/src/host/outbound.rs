//! What leaves the zone besides Deltas: events to viewers, zone messages
//! and tier-up events.

use super::*;

impl<Z: Zone> ZoneHost<Z> {
    /// Zone messages sent since the last call ([`crate::zone_message`]);
    /// the driver hands each to its target's
    /// [`deliver_zone`](Self::deliver_zone).
    pub fn take_zone_messages(&mut self) -> Vec<ZoneEnvelope> {
        std::mem::take(&mut self.core.zone_out)
    }

    /// [`Stimulus::ZoneMessage`].
    pub(super) fn deliver_envelope(
        &mut self,
        envelope: ZoneEnvelope,
    ) -> Result<(), ZoneMessageError> {
        if envelope.to != self.core.zone_id {
            return Err(ZoneMessageError::WrongZone);
        }
        let message = Z::Message::decode_event(envelope.type_id, &envelope.payload)
            .map_err(|e| ZoneMessageError::Decode(e.to_string()))?
            .ok_or_else(|| ZoneMessageError::Decode(format!("type {}", envelope.type_id)))?;
        if !self.core.zone_seen.first(&envelope.id) {
            return Err(ZoneMessageError::Duplicate);
        }
        let sender = ZoneSender {
            zone: envelope.from,
            principal: envelope.principal.map(VerifiedPrincipal::new),
        };
        hook!(self.on_zone_message(&sender, message));
        self.process_kicks();
        Ok(())
    }

    /// Tier-up events emitted (or forwarded) by this zone since the last
    /// call; the driver routes them to the parent zone.
    pub fn drain_upward(&mut self) -> Vec<ChildEvent> {
        std::mem::take(&mut self.core.upward)
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

    pub(super) fn broadcast(&mut self, message: &ServerMessage) {
        let clients: Vec<_> = self.core.clients.keys().copied().collect();
        for client in clients {
            self.core.server_message(client, message);
        }
    }
}
