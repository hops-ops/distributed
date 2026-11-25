//! Consumer inbox conformance: identical observable semantics across the
//! in-memory, SQLite, and Postgres backends.

use distributed::{
    CommitBatch, InboxReceipt, InboxStore, OutboxMessage, OutboxStore, RepositoryError,
    TransactionalCommit,
};

use super::scenario::unique_id;
use crate::outbox::find_outbox_by_id;

fn batch_with(outbox: Vec<OutboxMessage>, receipts: Vec<InboxReceipt>) -> CommitBatch<'static> {
    let mut batch = CommitBatch::new(Vec::new());
    batch.outbox_messages = outbox;
    batch.inbox_receipts = receipts;
    batch
}

async fn outbox_present<S: OutboxStore + Send + Sync>(outbox: &S, id: &str) -> bool {
    find_outbox_by_id(outbox, id).await.is_some()
}

/// Records a receipt, dedupes a replay, and — crucially — fences a **real
/// effect**: a batch carrying a duplicate receipt rolls back its outbox write too.
/// Also covers multiple distinct receipts in one batch and cross-consumer
/// independence of the same message id.
pub async fn inbox_records_dedupes_and_fences_with_real_effects<R, S>(repo: R, outbox: S)
where
    R: InboxStore + TransactionalCommit + Clone + Send + Sync + 'static,
    S: OutboxStore + Send + Sync,
{
    let consumer = unique_id("consumer");
    let m1 = unique_id("msg");
    let effect1 = unique_id("effect");
    let effect2 = unique_id("effect");

    // Pre-check is false until the receipt is committed.
    assert!(!repo.inbox_contains(&consumer, &m1).await.unwrap());

    // First delivery: the receipt commits atomically with a real outbox effect.
    repo.commit_batch(batch_with(
        vec![OutboxMessage::create(&effect1, "effect.applied", b"{}".to_vec()).unwrap()],
        vec![InboxReceipt::new(&consumer, &m1)],
    ))
    .await
    .expect("first delivery commits");
    assert!(repo.inbox_contains(&consumer, &m1).await.unwrap());
    assert!(
        outbox_present(&outbox, &effect1).await,
        "first effect landed"
    );

    // Replay: a batch with the duplicate receipt AND a fresh effect must roll back
    // the effect too — proving the receipt fences the whole transaction.
    let err = repo
        .commit_batch(batch_with(
            vec![OutboxMessage::create(&effect2, "effect.applied", b"{}".to_vec()).unwrap()],
            vec![InboxReceipt::new(&consumer, &m1)],
        ))
        .await
        .expect_err("replay is rejected");
    assert!(
        matches!(err, RepositoryError::DuplicateInboxReceipt { ref message_id, .. } if *message_id == m1),
        "got {err:?}"
    );
    assert!(
        !outbox_present(&outbox, &effect2).await,
        "the duplicate rolled the real effect back — effectively-once fence"
    );

    // Multiple distinct receipts commit together.
    let a = unique_id("msg");
    let b = unique_id("msg");
    repo.commit_batch(batch_with(
        Vec::new(),
        vec![
            InboxReceipt::new(&consumer, &a),
            InboxReceipt::new(&consumer, &b),
        ],
    ))
    .await
    .expect("distinct receipts commit");
    assert!(repo.inbox_contains(&consumer, &a).await.unwrap());
    assert!(repo.inbox_contains(&consumer, &b).await.unwrap());

    // The dedupe scope is the consumer: the same message id for a different
    // consumer is independent.
    let other = unique_id("consumer");
    repo.commit_batch(batch_with(Vec::new(), vec![InboxReceipt::new(&other, &m1)]))
        .await
        .expect("a different consumer records the same message id independently");
    assert!(repo.inbox_contains(&other, &m1).await.unwrap());
}

/// Empty `consumer` or `message_id` is rejected with the same typed error on
/// every backend (parity — the relational `CHECK` is only a backstop).
pub async fn inbox_rejects_empty_receipt<R>(repo: R)
where
    R: InboxStore + TransactionalCommit + Clone + Send + Sync + 'static,
{
    for receipt in [
        InboxReceipt::new("", unique_id("msg")),
        InboxReceipt::new(unique_id("consumer"), ""),
    ] {
        let err = repo
            .commit_batch(batch_with(Vec::new(), vec![receipt]))
            .await
            .expect_err("an empty receipt field is rejected");
        assert!(
            matches!(err, RepositoryError::InvalidInboxReceipt { .. }),
            "got {err:?}"
        );
    }
}
