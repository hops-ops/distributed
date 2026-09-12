//! Outbox-backed durable receive.
//!
//! [`OutboxSource`] turns any [`OutboxStore`] into a [`MessageSource`]:
//! it claims durable rows (`FOR UPDATE SKIP LOCKED` + lease in the SQL stores),
//! maps each to a canonical [`Message`], and settles by row status —
//! ack→complete, nack→release-for-retry, dead-letter/park→fail (the terminal
//! DLQ/archive state). `OutboxSource<PostgresOutboxStore>` is the Postgres
//! "starter" durable transport; the same type works over the in-memory and
//! SQLite stores for tests.
//!
//! `recv` drains the currently-claimable rows and then returns `Ok(None)`, so
//! `run_source` processes a finite backlog and stops. A long-running consumer
//! wraps `run_source` in a poll loop (waking on `LISTEN`/`NOTIFY` where the store
//! supports it, with polling authoritative); that daemon is a thin runtime
//! wrapper layered on top of this pure, runtime-agnostic source.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use super::{ClaimOutboxMessages, OutboxClaimRef, OutboxStore};
use crate::bus::{Message, MessageSource, ReceivedMessage, TransportError};
use crate::outbox::OutboxMessage;

/// Default lease held on a claimed row while it is being dispatched.
pub const DEFAULT_OUTBOX_SOURCE_LEASE: Duration = Duration::from_secs(30);
/// Default number of rows claimed per `recv` refill.
pub const DEFAULT_OUTBOX_SOURCE_BATCH: usize = 16;

/// A [`MessageSource`] backed by an [`OutboxStore`].
pub struct OutboxSource<S> {
    store: Arc<S>,
    worker_id: String,
    lease: Duration,
    max_attempts: u32,
    batch_size: usize,
    destination: Option<String>,
    buffer: VecDeque<OutboxMessage>,
}

impl<S> OutboxSource<S>
where
    S: OutboxStore,
{
    /// Create a source. `worker_id` scopes claims; `max_attempts` is the
    /// retryable-failure ceiling before a row is failed.
    pub fn new(store: Arc<S>, worker_id: impl Into<String>, max_attempts: u32) -> Self {
        Self {
            store,
            worker_id: worker_id.into(),
            lease: DEFAULT_OUTBOX_SOURCE_LEASE,
            max_attempts,
            batch_size: DEFAULT_OUTBOX_SOURCE_BATCH,
            destination: None,
            buffer: VecDeque::new(),
        }
    }

    /// Set the claim lease / visibility timeout.
    ///
    /// # Panics
    /// Panics if `lease` is zero: a zero lease makes a claimed row immediately
    /// re-claimable by competing workers, defeating the lease.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        assert!(
            !lease.is_zero(),
            "OutboxSource lease must be greater than zero"
        );
        self.lease = lease;
        self
    }

    /// Set how many rows are claimed per refill.
    ///
    /// # Panics
    /// Panics if `batch_size` is zero: a zero batch claims nothing, so `recv`
    /// would return `Ok(None)` forever even when rows are pending.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        assert!(
            batch_size > 0,
            "OutboxSource batch_size must be greater than zero"
        );
        self.batch_size = batch_size;
        self
    }

    /// Only claim rows bound to this point-to-point destination.
    pub fn with_destination(mut self, destination: impl Into<String>) -> Self {
        self.destination = Some(destination.into());
        self
    }

    fn claim_request(&self) -> ClaimOutboxMessages {
        let mut request =
            ClaimOutboxMessages::new(self.worker_id.clone(), self.batch_size, self.lease);
        if let Some(destination) = &self.destination {
            request = request.to_destination(destination.clone());
        }
        request
    }
}

impl<S> MessageSource for OutboxSource<S>
where
    S: OutboxStore,
{
    type Received = ReceivedOutboxMessage<S>;

    fn transport_name(&self) -> &'static str {
        "outbox"
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        if self.buffer.is_empty() {
            let claimed = self.store.claim(self.claim_request()).await?;
            self.buffer.extend(claimed);
        }
        match self.buffer.pop_front() {
            Some(row) => {
                let claim = OutboxClaimRef::from_message(&row)?;
                Ok(Some(ReceivedOutboxMessage {
                    store: self.store.clone(),
                    message: Message::from(row),
                    claim,
                    max_attempts: self.max_attempts,
                }))
            }
            None => Ok(None),
        }
    }
}

/// A claimed outbox row, settled back to the store on ack/nack.
pub struct ReceivedOutboxMessage<S> {
    store: Arc<S>,
    message: Message,
    claim: OutboxClaimRef,
    max_attempts: u32,
}

impl<S> ReceivedMessage for ReceivedOutboxMessage<S>
where
    S: OutboxStore,
{
    fn message(&self) -> &Message {
        &self.message
    }

    /// Complete the row (transport delivery succeeded).
    async fn ack(self) -> Result<(), TransportError> {
        self.store.complete(&self.claim).await?;
        Ok(())
    }

    /// Release for retry, or fail once the attempt ceiling is reached.
    async fn nack(self, reason: &str) -> Result<(), TransportError> {
        self.store
            .record_failure(&self.claim, reason, self.max_attempts)
            .await?;
        Ok(())
    }

    /// Fail the row terminally (the outbox `Failed` status is the DLQ/archive).
    async fn dead_letter(self, reason: &str) -> Result<(), TransportError> {
        self.store.fail(&self.claim, reason).await?;
        Ok(())
    }

    /// Park terminally for manual inspection (same `Failed` status as dead-letter
    /// in the outbox's state model).
    async fn park(self, reason: &str) -> Result<(), TransportError> {
        self.store.fail(&self.claim, reason).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{run_source, RunOptions};
    use crate::microsvc::Service;
    use crate::outbox_worker::testing::block_on;
    use crate::{
        CommitBatch, InMemoryRepository, OutboxMessage, OutboxMessageStatus, OutboxStore,
        TransactionalCommit,
    };
    use serde_json::json;

    fn store_row(repo: &InMemoryRepository, id: &str, name: &str) {
        let message = OutboxMessage::create(id, name, b"{}".to_vec()).unwrap();
        let mut batch = CommitBatch::empty();
        batch.outbox_messages.push(message);
        block_on(repo.commit_batch(batch)).unwrap();
    }

    fn status(repo: &InMemoryRepository, id: &str) -> Option<OutboxMessageStatus> {
        let store = repo.outbox_store();
        [
            OutboxMessageStatus::Pending,
            OutboxMessageStatus::InFlight,
            OutboxMessageStatus::Published,
            OutboxMessageStatus::Failed,
        ]
        .into_iter()
        .find(|status| {
            block_on(store.messages_by_status(status.clone(), usize::MAX))
                .unwrap()
                .iter()
                .any(|m| m.id() == id)
        })
    }

    fn source(repo: &InMemoryRepository) -> OutboxSource<crate::InMemoryOutboxStore> {
        OutboxSource::new(Arc::new(repo.outbox_store()), "pg-transport", 3)
    }

    #[test]
    #[should_panic(expected = "lease must be greater than zero")]
    fn with_lease_zero_panics() {
        let repo = InMemoryRepository::new();
        let _ = source(&repo).with_lease(Duration::ZERO);
    }

    #[test]
    #[should_panic(expected = "batch_size must be greater than zero")]
    fn with_batch_size_zero_panics() {
        let repo = InMemoryRepository::new();
        let _ = source(&repo).with_batch_size(0);
    }

    #[test]
    fn recv_yields_claimed_rows_then_drains_to_none() {
        let repo = InMemoryRepository::new();
        store_row(&repo, "m1", "evt");
        store_row(&repo, "m2", "evt");
        let mut src = source(&repo);

        let first = block_on(src.recv()).unwrap().expect("first row");
        let second = block_on(src.recv()).unwrap().expect("second row");
        // Both claimed (in-flight) and held by this source; nothing else claimable.
        let third = block_on(src.recv()).unwrap();
        assert!(third.is_none(), "drains to None once nothing is claimable");

        let mut ids = vec![
            first.message().id().unwrap().to_string(),
            second.message().id().unwrap().to_string(),
        ];
        ids.sort();
        assert_eq!(ids, vec!["m1".to_string(), "m2".to_string()]);
    }

    #[test]
    fn ack_completes_the_row() {
        let repo = InMemoryRepository::new();
        store_row(&repo, "m1", "evt");
        let mut src = source(&repo);
        let received = block_on(src.recv()).unwrap().unwrap();
        block_on(received.ack()).unwrap();
        assert_eq!(status(&repo, "m1"), None);
    }

    #[test]
    fn nack_releases_for_retry() {
        let repo = InMemoryRepository::new();
        store_row(&repo, "m1", "evt");
        let mut src = source(&repo);
        let received = block_on(src.recv()).unwrap().unwrap();
        block_on(received.nack("transient")).unwrap();
        assert_eq!(status(&repo, "m1"), Some(OutboxMessageStatus::Pending));
    }

    #[test]
    fn dead_letter_fails_the_row() {
        let repo = InMemoryRepository::new();
        store_row(&repo, "m1", "evt");
        let mut src = source(&repo);
        let received = block_on(src.recv()).unwrap().unwrap();
        block_on(received.dead_letter("poison")).unwrap();
        assert_eq!(status(&repo, "m1"), Some(OutboxMessageStatus::Failed));
    }

    #[test]
    fn run_source_drains_outbox_and_completes() {
        let repo = InMemoryRepository::new();
        store_row(&repo, "m1", "evt");
        store_row(&repo, "m2", "evt");

        let handled = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let h = handled.clone();
        let service = Arc::new(
            Service::new().routes(
                crate::microsvc::Routes::new()
                    .with_dependencies(())
                    .event("evt")
                    .handle(move |ctx: &crate::microsvc::Context<()>| {
                        let h = h.clone();
                        let id = ctx.message().id().unwrap_or_default().to_string();
                        async move {
                            h.lock().unwrap().push(id);
                            Ok(json!({}))
                        }
                    }),
            ),
        );

        block_on(run_source(service, source(&repo), RunOptions::idempotent())).unwrap();

        let mut ids = handled.lock().unwrap().clone();
        ids.sort();
        assert_eq!(ids, vec!["m1".to_string(), "m2".to_string()]);
        assert_eq!(status(&repo, "m1"), None);
        assert_eq!(status(&repo, "m2"), None);
    }

    #[test]
    fn unhandled_outbox_message_is_acked_and_completed() {
        let repo = InMemoryRepository::new();
        store_row(&repo, "m1", "unrelated");
        // Service handles a different event; the unrelated row is acked-ignored,
        // i.e. completed, so it does not loop forever.
        let service: Arc<Service> = Arc::new(
            Service::new().routes(
                crate::microsvc::Routes::new()
                    .with_dependencies(())
                    .event("evt")
                    .handle(|_: &crate::microsvc::Context<()>| async move { Ok(json!({})) }),
            ),
        );
        block_on(run_source(service, source(&repo), RunOptions::idempotent())).unwrap();
        assert_eq!(status(&repo, "m1"), None);
    }
}
