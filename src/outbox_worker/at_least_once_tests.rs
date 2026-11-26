//! At-least-once across the wire: a dispatcher that crashes between publish
//! and complete republishes the row, and the consumer's inbox receipt makes
//! the duplicate harmless.
//!
//! The dispatcher thresholds are proven in `outbox_dispatch`, the inbox in
//! the repository conformance; this composes them.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::bus::{
    run_source, Handlers, Message, MessagePublisher, MessageSource, ReceivedMessage, RunOptions,
    TransportError,
};
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::{ClaimOutboxMessages, OutboxClaimRef, OutboxDispatcher, OutboxStore};
use crate::{
    CommitBatch, InMemoryOutboxStore, InMemoryRepository, InboxReceipt, RepositoryError,
    TransactionalCommit,
};

/// An outbox whose next `complete` fails as a retryable storage error: the
/// dispatcher crashed after the publish succeeded and before the row was
/// marked done.
struct CompleteOnceFailingStore {
    inner: InMemoryOutboxStore,
    fail_next_complete: AtomicBool,
}

impl OutboxStore for CompleteOnceFailingStore {
    async fn messages_by_status(
        &self,
        status: OutboxMessageStatus,
        limit: usize,
    ) -> Result<Vec<OutboxMessage>, RepositoryError> {
        self.inner.messages_by_status(status, limit).await
    }

    async fn claim(
        &self,
        request: ClaimOutboxMessages,
    ) -> Result<Vec<OutboxMessage>, RepositoryError> {
        self.inner.claim(request).await
    }

    async fn complete<'a>(&'a self, claim: &'a OutboxClaimRef) -> Result<(), RepositoryError> {
        if self.fail_next_complete.swap(false, Ordering::SeqCst) {
            return Err(RepositoryError::Storage {
                operation: "complete outbox row (simulated crash)".into(),
                retryable: true,
                source: None,
            });
        }
        self.inner.complete(claim).await
    }

    async fn release<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
        retry_after: Duration,
    ) -> Result<(), RepositoryError> {
        self.inner.release(claim, error, retry_after).await
    }

    async fn requeue_failed<'a>(&'a self, message_id: &'a str) -> Result<(), RepositoryError> {
        self.inner.requeue_failed(message_id).await
    }

    async fn fail<'a>(
        &'a self,
        claim: &'a OutboxClaimRef,
        error: &'a str,
    ) -> Result<(), RepositoryError> {
        self.inner.fail(claim, error).await
    }
}

/// Keeps every published message, as a broker would.
#[derive(Clone, Default)]
struct Wire(Arc<Mutex<Vec<Message>>>);

impl MessagePublisher for Wire {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        self.0.lock().unwrap().push(message);
        Ok(())
    }
}

/// Delivers what the wire carried, in order, and counts acknowledgements.
struct Replay {
    deliveries: VecDeque<Message>,
    acks: Arc<AtomicUsize>,
}

struct Delivery {
    message: Message,
    acks: Arc<AtomicUsize>,
}

impl MessageSource for Replay {
    type Received = Delivery;

    async fn recv(&mut self) -> Result<Option<Delivery>, TransportError> {
        Ok(self.deliveries.pop_front().map(|message| Delivery {
            message,
            acks: self.acks.clone(),
        }))
    }
}

impl ReceivedMessage for Delivery {
    fn message(&self) -> &Message {
        &self.message
    }

    async fn ack(self) -> Result<(), TransportError> {
        self.acks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn nack(self, _delay: Duration) -> Result<(), TransportError> {
        panic!("a duplicate is acknowledged, not handed back");
    }
}

async fn outbox_status(repo: &InMemoryRepository, id: &str) -> Option<OutboxMessageStatus> {
    for status in [
        OutboxMessageStatus::Pending,
        OutboxMessageStatus::InFlight,
        OutboxMessageStatus::Failed,
    ] {
        let rows = repo
            .outbox_store()
            .messages_by_status(status.clone(), usize::MAX)
            .await
            .unwrap();
        if rows.iter().any(|row| row.id() == id) {
            return Some(status);
        }
    }
    None
}

#[tokio::test]
async fn publish_then_crash_republishes_and_consumer_inbox_dedupes() {
    let producer = InMemoryRepository::new();
    let message_id = "evt-crash";
    let mut batch = CommitBatch::empty();
    batch
        .outbox_messages
        .push(OutboxMessage::create(message_id, "order.initialized", b"{}".to_vec()).unwrap());
    producer.commit_batch(batch).await.unwrap();

    let wire = Wire::default();
    let ids = [message_id.to_string()];

    // A worker with a short lease publishes, then the completion write
    // "crashes".
    let crash_lease = Duration::from_millis(100);
    OutboxDispatcher::new(
        CompleteOnceFailingStore {
            inner: producer.outbox_store(),
            fail_next_complete: AtomicBool::new(true),
        },
        wire.clone(),
        "worker-crashed",
        crash_lease,
        5,
    )
    .dispatch_ids(&ids)
    .await
    .expect_err("the failed completion write surfaces (the crash)");

    // The row is still owed. The in-memory outbox measures its lease on the
    // wall clock, so the next worker waits it out before reclaiming.
    tokio::time::sleep(crash_lease + Duration::from_millis(200)).await;
    let outcome = OutboxDispatcher::new(
        producer.outbox_store(),
        wire.clone(),
        "worker-retry",
        Duration::from_secs(60),
        5,
    )
    .dispatch_ids(&ids)
    .await
    .unwrap();
    assert_eq!(outcome.published, 1, "the reclaimed row is republished");
    assert_eq!(outbox_status(&producer, message_id).await, None);

    let deliveries = wire.0.lock().unwrap().clone();
    assert_eq!(
        deliveries.len(),
        2,
        "the crash produced a duplicate delivery"
    );
    assert!(deliveries.iter().all(|d| d.id() == Some(message_id)));

    // The consumer commits its effect with an inbox receipt; a duplicate
    // receipt means the effect is already applied.
    let consumer_repo = InMemoryRepository::new();
    let applied = Arc::new(AtomicUsize::new(0));
    let attempt = Arc::new(AtomicUsize::new(0));
    let consumer = "checkout-consumer";
    let handlers = {
        let (repo, applied, attempt) = (consumer_repo.clone(), applied.clone(), attempt.clone());
        Arc::new(
            Handlers::new().on_event("order.initialized", move |message: &Message| {
                let (repo, applied) = (repo.clone(), applied.clone());
                let id = message.id().unwrap_or_default().to_string();
                // A fresh effect id per attempt: only the receipt fences the
                // duplicate.
                let effect = format!("effect-{}", attempt.fetch_add(1, Ordering::SeqCst));
                async move {
                    let mut batch = CommitBatch::empty();
                    batch.inbox_receipts.push(InboxReceipt::new(consumer, &id));
                    batch.outbox_messages.push(
                        OutboxMessage::create(effect, "effect.applied", b"{}".to_vec()).unwrap(),
                    );
                    match repo.commit_batch(batch).await {
                        Ok(()) => {
                            applied.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                        Err(RepositoryError::DuplicateInboxReceipt { .. }) => Ok(()),
                        Err(other) => Err(TransportError::permanent(other.to_string())),
                    }
                }
            }),
        )
    };
    let acks = Arc::new(AtomicUsize::new(0));
    run_source(
        handlers,
        Replay {
            deliveries: deliveries.into(),
            acks: acks.clone(),
        },
        RunOptions::until_idle(),
    )
    .await
    .unwrap();

    assert_eq!(acks.load(Ordering::SeqCst), 2, "both deliveries are acked");
    assert_eq!(applied.load(Ordering::SeqCst), 1, "the effect applied once");
    assert!(consumer_repo.inbox_contains(consumer, message_id));
    assert!(outbox_status(&consumer_repo, "effect-0").await.is_some());
    assert!(
        outbox_status(&consumer_repo, "effect-1").await.is_none(),
        "the duplicate's effect was fenced by the inbox receipt"
    );
}
