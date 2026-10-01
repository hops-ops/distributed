use super::api::BACKLOG_STATS_SCAN_LIMIT;
use super::*;
use crate::{
    CommitBatch, InMemoryOutboxStore, InMemoryRepository, OutboxMessage, OutboxMessageStatus,
    RepositoryError, TransactionalCommit,
};
use std::future::Future;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime};

async fn store_message(repo: &InMemoryRepository, message: OutboxMessage) -> String {
    let id = message.id().to_string();
    let mut batch = CommitBatch::empty();
    batch.outbox_messages.push(message);
    repo.commit_batch(batch).await.unwrap();
    id
}

fn load_message(repo: &InMemoryRepository, id: &str) -> OutboxMessage {
    repo.outbox_storage()
        .read()
        .unwrap()
        .get(id)
        .unwrap()
        .clone()
}

#[tokio::test]
async fn claim_includes_expired_in_flight_messages() {
    let repo = InMemoryRepository::new();
    let mut message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
    message
        .claim_at("worker-1", Duration::from_secs(1), SystemTime::UNIX_EPOCH)
        .unwrap();
    let id = store_message(&repo, message).await;

    let store = repo.outbox_store();
    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-2",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].worker_id.as_deref(), Some("worker-2"));
    assert_eq!(claimed[0].attempts, 2);

    let stored = load_message(&repo, &id);
    assert_eq!(stored.worker_id.as_deref(), Some("worker-2"));
    assert_eq!(stored.attempts, 2);
    assert!(stored.is_in_flight());
}

#[tokio::test]
async fn claim_skips_unexpired_in_flight_messages() {
    let repo = InMemoryRepository::new();
    let mut message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
    message
        .claim_for("worker-1", Duration::from_secs(60))
        .unwrap();
    let id = store_message(&repo, message).await;

    let store = repo.outbox_store();
    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-2",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();

    assert!(claimed.is_empty());
    let stored = load_message(&repo, &id);
    assert_eq!(stored.worker_id.as_deref(), Some("worker-1"));
    assert_eq!(stored.attempts, 1);
}

#[tokio::test]
async fn claim_uses_created_at_before_message_id_order() {
    let repo = InMemoryRepository::new();
    let mut newer = OutboxMessage::create("msg-a", "Event", b"{}".to_vec()).unwrap();
    newer.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let mut older = OutboxMessage::create("msg-z", "Event", b"{}".to_vec()).unwrap();
    older.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    store_message(&repo, newer).await;
    store_message(&repo, older).await;

    let claimed = repo
        .outbox_store()
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();

    assert_eq!(claimed[0].id(), "msg-z");
}

#[tokio::test]
async fn claim_by_explicit_ids_claims_only_requested() {
    let repo = InMemoryRepository::new();
    store_message(
        &repo,
        OutboxMessage::create("msg-a", "Event", b"{}".to_vec()).unwrap(),
    )
    .await;
    store_message(
        &repo,
        OutboxMessage::create("msg-b", "Event", b"{}".to_vec()).unwrap(),
    )
    .await;
    store_message(
        &repo,
        OutboxMessage::create("msg-c", "Event", b"{}".to_vec()).unwrap(),
    )
    .await;

    let claimed = repo
        .outbox_store()
        .claim(ClaimOutboxMessages::for_ids(
            "worker-1",
            vec!["msg-b".to_string(), "msg-c".to_string()],
            Duration::from_secs(60),
        ))
        .await
        .unwrap();

    let mut claimed_ids = claimed
        .iter()
        .map(|m| m.id().to_string())
        .collect::<Vec<_>>();
    claimed_ids.sort();
    assert_eq!(claimed_ids, vec!["msg-b".to_string(), "msg-c".to_string()]);
    // The unrequested row stays pending.
    assert!(load_message(&repo, "msg-a").is_pending());
}

#[tokio::test]
async fn claim_by_ids_skips_unclaimable_without_error() {
    let repo = InMemoryRepository::new();
    let mut leased = OutboxMessage::create("msg-a", "Event", b"{}".to_vec()).unwrap();
    leased
        .claim_for("other-worker", Duration::from_secs(60))
        .unwrap();
    store_message(&repo, leased).await;

    // Requesting a currently-leased id (and a missing id) yields no claim,
    // not an error.
    let claimed = repo
        .outbox_store()
        .claim(ClaimOutboxMessages::for_ids(
            "worker-1",
            vec!["msg-a".to_string(), "missing".to_string()],
            Duration::from_secs(60),
        ))
        .await
        .unwrap();

    assert!(claimed.is_empty());
}

#[test]
fn sort_by_claim_order_uses_message_id_tiebreaker() {
    let mut later = OutboxMessage::create("msg-c", "Event", b"{}".to_vec()).unwrap();
    later.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let mut second = OutboxMessage::create("msg-b", "Event", b"{}".to_vec()).unwrap();
    second.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let mut first = OutboxMessage::create("msg-a", "Event", b"{}".to_vec()).unwrap();
    first.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let mut messages = vec![later, second, first];

    sort_by_claim_order(&mut messages);

    assert_eq!(
        messages
            .iter()
            .map(|message| message.id())
            .collect::<Vec<_>>(),
        vec!["msg-a", "msg-b", "msg-c"]
    );
}

#[tokio::test]
async fn backlog_stats_counts_pending_and_tracks_oldest_created_at() {
    let repo = InMemoryRepository::new();
    let mut older = OutboxMessage::create("msg-a", "Event", b"{}".to_vec()).unwrap();
    older.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let mut newer = OutboxMessage::create("msg-b", "Event", b"{}".to_vec()).unwrap();
    newer.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let mut settled = OutboxMessage::create("msg-c", "Event", b"{}".to_vec()).unwrap();
    settled.fail("done".to_string()).unwrap();
    store_message(&repo, newer).await;
    store_message(&repo, settled).await;
    store_message(&repo, older).await;

    let stats = repo.outbox_store().backlog_stats().await.unwrap();

    assert_eq!(
        stats,
        OutboxBacklogStats {
            pending: 2,
            oldest_created_at: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)),
        }
    );
}

/// The default `backlog_stats` must honor the mandatory listing bound —
/// a store that only implements the required methods must never see an
/// unbounded (`usize::MAX`) page-in from the gauge path.
#[tokio::test]
async fn default_backlog_stats_scan_is_bounded() {
    struct LimitProbeStore;

    impl OutboxStore for LimitProbeStore {
        fn messages_by_status(
            &self,
            _status: OutboxMessageStatus,
            limit: usize,
        ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + '_ {
            async move {
                assert_eq!(limit, BACKLOG_STATS_SCAN_LIMIT);
                Ok(Vec::new())
            }
        }

        fn claim<'a>(
            &'a self,
            _request: ClaimOutboxMessages,
        ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + 'a {
            async move { unimplemented!("probe store only lists") }
        }

        fn complete<'a>(
            &'a self,
            _claim: &'a OutboxClaimRef,
        ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
            async move { unimplemented!("probe store only lists") }
        }

        fn release<'a>(
            &'a self,
            _claim: &'a OutboxClaimRef,
            _error: &'a str,
        ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
            async move { unimplemented!("probe store only lists") }
        }

        fn fail<'a>(
            &'a self,
            _claim: &'a OutboxClaimRef,
            _error: &'a str,
        ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
            async move { unimplemented!("probe store only lists") }
        }
    }

    let stats = LimitProbeStore.backlog_stats().await.unwrap();
    assert_eq!(stats, OutboxBacklogStats::default());
}

#[tokio::test]
async fn competing_workers_only_claim_message_once() {
    let repo = InMemoryRepository::new();
    let message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
    let id = store_message(&repo, message).await;

    let barrier = Arc::new(Barrier::new(3));
    let store_a = repo.outbox_store();
    let store_b = repo.outbox_store();
    let barrier_a = Arc::clone(&barrier);
    let barrier_b = Arc::clone(&barrier);

    let worker_a = thread::spawn(move || {
        barrier_a.wait();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store_a.claim(ClaimOutboxMessages::new(
            "worker-a",
            1,
            Duration::from_secs(60),
        )))
        .unwrap()
        .len()
    });
    let worker_b = thread::spawn(move || {
        barrier_b.wait();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(store_b.claim(ClaimOutboxMessages::new(
            "worker-b",
            1,
            Duration::from_secs(60),
        )))
        .unwrap()
        .len()
    });

    barrier.wait();
    let total_claimed = worker_a.join().unwrap() + worker_b.join().unwrap();

    assert_eq!(total_claimed, 1);
    let stored = load_message(&repo, &id);
    assert!(stored.is_in_flight());
    assert_eq!(stored.attempts, 1);
}

#[tokio::test]
async fn publish_failure_releases_until_retry_ceiling_then_fails() {
    let repo = InMemoryRepository::new();
    let message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
    let id = store_message(&repo, message).await;

    let store = repo.outbox_store();
    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();
    let action = store
        .record_failure(&claim, "first failure", 2)
        .await
        .unwrap();
    assert_eq!(action, OutboxPublishFailureAction::Released);

    let stored = load_message(&repo, &id);
    assert!(stored.is_pending());
    assert_eq!(stored.attempts, 1);
    assert_eq!(stored.last_error.as_deref(), Some("first failure"));

    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();
    let action = store
        .record_failure(&claim, "second failure", 2)
        .await
        .unwrap();
    assert_eq!(action, OutboxPublishFailureAction::Failed);

    let stored = load_message(&repo, &id);
    assert!(stored.is_failed());
    assert_eq!(stored.attempts, 2);
    assert_eq!(stored.last_error.as_deref(), Some("second failure"));
}

#[tokio::test]
async fn missing_message_updates_return_not_found() {
    let store = InMemoryOutboxStore {
        storage: Default::default(),
    };
    let claim = OutboxClaimRef {
        message_id: "missing".into(),
        worker_id: "worker-1".into(),
        leased_until: SystemTime::now(),
        attempt: 1,
    };

    let is_missing =
        |err: RepositoryError| matches!(&err, RepositoryError::NotFound { id } if id == "missing");
    assert!(is_missing(store.complete(&claim).await.unwrap_err()));
    assert!(is_missing(
        store.release(&claim, "error").await.unwrap_err()
    ));
    assert!(is_missing(store.fail(&claim, "error").await.unwrap_err()));
}

#[tokio::test]
async fn stale_or_mismatched_claims_cannot_be_completed() {
    let repo = InMemoryRepository::new();
    let message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
    let _id = store_message(&repo, message).await;

    let store = repo.outbox_store();
    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let mut claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();
    claim.worker_id = "worker-2".into();
    let err = store.complete(&claim).await.unwrap_err();
    assert!(matches!(err, RepositoryError::InvalidState { .. }));

    let mut expired = OutboxMessage::create("msg-2", "Event", b"{}".to_vec()).unwrap();
    expired
        .claim_at("worker-1", Duration::from_secs(1), SystemTime::UNIX_EPOCH)
        .unwrap();
    let expired_id = store_message(&repo, expired).await;
    let expired = load_message(&repo, &expired_id);
    let claim = OutboxClaimRef::from_message(&expired).unwrap();
    let err = store.complete(&claim).await.unwrap_err();
    assert!(matches!(err, RepositoryError::InvalidState { .. }));
}

#[tokio::test]
async fn stale_attempt_claims_cannot_complete_later_claims() {
    let repo = InMemoryRepository::new();
    let message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
    let _id = store_message(&repo, message).await;

    let store = repo.outbox_store();
    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let stale_claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();
    store.release(&stale_claim, "retry").await.unwrap();

    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let current_claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();

    let err = store.complete(&stale_claim).await.unwrap_err();
    assert!(matches!(err, RepositoryError::InvalidState { .. }));
    store.complete(&current_claim).await.unwrap();
}

#[tokio::test]
async fn complete_many_completes_the_whole_batch() {
    let repo = InMemoryRepository::new();
    for id in ["msg-1", "msg-2", "msg-3"] {
        store_message(
            &repo,
            OutboxMessage::create(id, "Event", b"{}".to_vec()).unwrap(),
        )
        .await;
    }

    let store = repo.outbox_store();
    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            3,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let claims = claimed
        .iter()
        .map(OutboxClaimRef::from_message)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    store.complete_many(&claims).await.unwrap();

    for id in ["msg-1", "msg-2", "msg-3"] {
        assert!(!repo.outbox_storage().read().unwrap().contains_key(id));
    }
}

#[tokio::test]
async fn complete_many_rejects_stale_and_missing_claims() {
    let repo = InMemoryRepository::new();
    store_message(
        &repo,
        OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap(),
    )
    .await;

    let store = repo.outbox_store();
    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let claims = vec![OutboxClaimRef::from_message(&claimed[0]).unwrap()];

    store.complete_many(&claims).await.unwrap();
    // Re-settling a deleted delivery row is NotFound, same as `complete`.
    let err = store.complete_many(&claims).await.unwrap_err();
    assert!(matches!(err, RepositoryError::NotFound { .. }));

    let missing = vec![OutboxClaimRef {
        message_id: "missing".into(),
        worker_id: "worker-1".into(),
        leased_until: SystemTime::now(),
        attempt: 1,
    }];
    let err = store.complete_many(&missing).await.unwrap_err();
    assert!(matches!(err, RepositoryError::NotFound { id } if id == "missing"));
}

#[tokio::test]
async fn already_published_message_is_not_completed_again() {
    let repo = InMemoryRepository::new();
    let message = OutboxMessage::create("msg-1", "Event", b"{}".to_vec()).unwrap();
    let _id = store_message(&repo, message).await;

    let store = repo.outbox_store();
    let claimed = store
        .claim(ClaimOutboxMessages::new(
            "worker-1",
            1,
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
    let claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();
    store.complete(&claim).await.unwrap();

    let err = store.complete(&claim).await.unwrap_err();
    assert!(matches!(err, RepositoryError::NotFound { .. }));
}
