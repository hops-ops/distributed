//! Provider message encoding shared by ingress and scrape.

use distributed::microsvc::HandlerError;
use distributed::OutboxMessage;

use super::map::MappedDelivery;

/// Encode a mapped provider delivery as the outbox message `id`.
///
/// Callers derive `id` from their command id, so one command never stages an
/// id another command committed.
pub fn provider_message(
    id: String,
    mapped: &MappedDelivery,
) -> Result<OutboxMessage, HandlerError> {
    OutboxMessage::encode(id, mapped.message_name.as_str(), &mapped.payload)
        .map_err(|error| HandlerError::Other(Box::new(error)))
}
