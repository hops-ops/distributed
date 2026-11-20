#![expect(
    clippy::manual_async_fn,
    reason = "async trait impls return impl Future + Send to preserve public Send bounds"
)]

mod api;
mod in_memory;

#[cfg(test)]
mod tests;

pub use api::{
    ClaimOutboxMessages, OutboxBacklogStats, OutboxClaimRef, OutboxPublishFailureAction,
    OutboxStore, OUTBOX_RETRY,
};

pub(crate) use api::{claim_order_ids, ensure_active_claim, sort_by_claim_order};
