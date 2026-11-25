//! Outbox lookups over the public `OutboxStore` surface.

use distributed::{OutboxMessage, OutboxMessageStatus, OutboxStore};

/// Find an outbox message by id by scanning every status bucket — the only
/// by-id lookup the public store surface offers.
pub async fn find_outbox_by_id<S>(outbox: &S, id: &str) -> Option<OutboxMessage>
where
    S: OutboxStore + Send + Sync,
{
    for status in [
        OutboxMessageStatus::Pending,
        OutboxMessageStatus::InFlight,
        OutboxMessageStatus::Published,
        OutboxMessageStatus::Failed,
    ] {
        let messages = outbox
            .messages_by_status(status, usize::MAX)
            .await
            .expect("outbox status lookup should succeed");
        if let Some(message) = messages.into_iter().find(|message| message.id() == id) {
            return Some(message);
        }
    }
    None
}

/// The current status of the outbox message with `id`, if it is stored at all.
pub async fn outbox_status_by_id<S>(outbox: &S, id: &str) -> Option<OutboxMessageStatus>
where
    S: OutboxStore + Send + Sync,
{
    find_outbox_by_id(outbox, id).await.map(|m| m.status)
}
