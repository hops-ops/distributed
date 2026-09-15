use std::sync::Mutex;
use std::time::Duration;

use distributed::bus::{Message, MessagePublisher, TransportError};
use distributed::{
    Aggregate, AggregateBuilder, ClaimOutboxMessages, GetStream, OutboxClaimRef, OutboxDispatcher,
    OutboxMessage, OutboxMessageStatus, OutboxPublishFailureAction, OutboxStore, RepositoryError,
    StreamIdentity, TransactionalCommit,
};

use super::outbox_support::find_outbox_by_id;
use super::scenario::unique_id;
use super::seat::Seat;

struct FlakyPublisher {
    fail_first: usize,
    attempts: Mutex<usize>,
    delivered: Mutex<Vec<String>>,
}

impl FlakyPublisher {
    fn new(fail_first: usize) -> Self {
        Self {
            fail_first,
            attempts: Mutex::new(0),
            delivered: Mutex::new(Vec::new()),
        }
    }

    fn delivered(&self) -> Vec<String> {
        self.delivered.lock().expect("delivered lock").clone()
    }

    fn attempts(&self) -> usize {
        *self.attempts.lock().expect("attempts lock")
    }
}

impl MessagePublisher for FlakyPublisher {
    async fn publish(&self, message: Message) -> Result<(), TransportError> {
        let attempt = {
            let mut attempts = self.attempts.lock().expect("attempts lock");
            *attempts += 1;
            *attempts
        };
        if attempt <= self.fail_first {
            return Err(TransportError::retryable(
                "simulated transient publish failure",
            ));
        }
        self.delivered
            .lock()
            .expect("delivered lock")
            .push(message.id().unwrap_or_default().to_string());
        Ok(())
    }
}

pub async fn high_level_outbox_commit_persists_row_without_stream<R, S>(repo: R, outbox: S)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
    S: OutboxStore + Send + Sync,
{
    let seat_id = unique_id("outbox-seat");
    let message_id = unique_id("outbox-message");
    commit_outbox_for_seat(&repo, seat_id.clone(), &message_id, "seat.added").await;

    let stored = find_outbox_by_id(&outbox, &message_id)
        .await
        .expect("outbox message should be stored");
    assert_eq!(stored.status, OutboxMessageStatus::Pending);
    assert_eq!(
        stored.source_aggregate_type.as_deref(),
        Some(Seat::aggregate_type())
    );
    assert_eq!(
        stored.source_aggregate_id.as_deref(),
        Some(seat_id.as_str())
    );
    assert_eq!(stored.source_sequence, Some(1));

    let old_outbox_stream =
        StreamIdentity::new("distributed::outbox::message::OutboxMessage", stored.id())
            .expect("old outbox stream identity should be syntactically valid");
    assert!(repo
        .get_stream(&old_outbox_stream)
        .await
        .expect("old outbox stream lookup should succeed")
        .is_none());
}

pub async fn duplicate_outbox_insert_rolls_back_aggregate<R>(repo: R)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
{
    let duplicate_message_id = unique_id("duplicate-outbox");
    commit_outbox_for_seat(
        &repo,
        unique_id("existing-seat"),
        &duplicate_message_id,
        "seat.added",
    )
    .await;

    let rollback_seat_id = unique_id("rollback-seat");
    let rollback_identity = StreamIdentity::new(Seat::aggregate_type(), &rollback_seat_id)
        .expect("seat stream identity should be valid");
    let mut rollback_seat = added_seat(&rollback_seat_id);
    let duplicate_message =
        OutboxMessage::create(&duplicate_message_id, "seat.added_again", b"{}".to_vec())
            .expect("duplicate outbox message should be valid");

    let err = repo
        .clone()
        .aggregate::<Seat>()
        .outbox(duplicate_message)
        .commit(&mut rollback_seat)
        .await
        .expect_err("duplicate outbox id should reject the batch");

    assert!(matches!(
        err,
        RepositoryError::DuplicateOutboxMessageInBatch { .. }
    ));
    assert!(repo
        .get_stream(&rollback_identity)
        .await
        .expect("rollback stream lookup should succeed")
        .is_none());
    assert_eq!(rollback_seat.entity.committed_version(), 0);
}

pub async fn aggregate_conflict_rolls_back_outbox<R, S>(repo: R, outbox: S)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
    S: OutboxStore + Send + Sync,
{
    let seat_id = unique_id("conflict-outbox-seat");
    let seat_repo = repo.clone().aggregate::<Seat>();
    let mut original = added_seat(&seat_id);
    seat_repo
        .commit(&mut original)
        .await
        .expect("initial seat commit should succeed");

    let mut stale = seat_repo
        .get(&seat_id)
        .await
        .expect("stale load should succeed")
        .expect("stale seat should exist");
    let mut winner = seat_repo
        .get(&seat_id)
        .await
        .expect("winner load should succeed")
        .expect("winner seat should exist");
    stale
        .reserve(
            unique_id("stale-checkout"),
            seat_id.clone(),
            stale.category.clone(),
        )
        .expect("stale reservation should be valid locally");
    winner
        .reserve(
            unique_id("winner-checkout"),
            seat_id.clone(),
            winner.category.clone(),
        )
        .expect("winner reservation should be valid locally");
    seat_repo
        .commit(&mut winner)
        .await
        .expect("winner commit should succeed");

    let message_id = unique_id("rollback-outbox-message");
    let err = repo
        .clone()
        .aggregate::<Seat>()
        .outbox(outbox_message(&message_id, "seat.reserved"))
        .commit(&mut stale)
        .await
        .expect_err("stale aggregate should reject the batch");

    assert!(matches!(err, RepositoryError::ConcurrentWrite { .. }));
    assert!(find_outbox_by_id(&outbox, &message_id).await.is_none());
}

pub async fn worker_claim_complete_and_retry_lifecycle<R, S>(repo: R, outbox: S)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
    S: OutboxStore + Send + Sync,
{
    let complete_message_id = unique_id("complete-outbox");
    commit_outbox_for_seat(
        &repo,
        unique_id("complete-seat"),
        &complete_message_id,
        "seat.added",
    )
    .await;

    let claimed = claim_one(
        &outbox,
        ClaimOutboxMessages::new("worker-a", 1, Duration::from_secs(60)),
    )
    .await;
    assert_eq!(claimed.id(), complete_message_id);

    let wrong_claim = OutboxClaimRef {
        message_id: claimed.id().to_string(),
        worker_id: "worker-b".into(),
        leased_until: claimed.leased_until.expect("claim should have lease"),
        attempt: claimed.attempts,
    };
    let stale_err = outbox
        .complete(&wrong_claim)
        .await
        .expect_err("wrong worker should not complete a claim");
    assert!(matches!(stale_err, RepositoryError::InvalidState { .. }));

    let claim = OutboxClaimRef::from_message(&claimed).expect("claim should be valid");
    outbox
        .complete(&claim)
        .await
        .expect("owning worker should complete the claim");
    assert!(find_outbox_by_id(&outbox, &complete_message_id)
        .await
        .is_none());

    let retry_message_id = unique_id("retry-outbox");
    commit_outbox_for_seat(
        &repo,
        unique_id("retry-seat"),
        &retry_message_id,
        "seat.added",
    )
    .await;

    let claimed = claim_one(
        &outbox,
        ClaimOutboxMessages::new("worker-r", 1, Duration::from_secs(60)),
    )
    .await;
    let claim = OutboxClaimRef::from_message(&claimed).expect("claim should be valid");
    let action = outbox
        .record_failure(&claim, "first failure", 2)
        .await
        .expect("first failure should be recorded");
    assert_eq!(action, OutboxPublishFailureAction::Released);

    let released = find_outbox_by_id(&outbox, &retry_message_id)
        .await
        .expect("released message should exist");
    assert_eq!(released.status, OutboxMessageStatus::Pending);
    assert_eq!(released.attempts, 1);
    assert_eq!(released.last_error.as_deref(), Some("first failure"));

    let claimed = claim_one(
        &outbox,
        ClaimOutboxMessages::new("worker-r", 1, Duration::from_secs(60)),
    )
    .await;
    let stale_err = outbox
        .complete(&claim)
        .await
        .expect_err("stale attempt should not complete a later claim");
    assert!(matches!(stale_err, RepositoryError::InvalidState { .. }));
    let claim = OutboxClaimRef::from_message(&claimed).expect("claim should be valid");
    let action = outbox
        .record_failure(&claim, "second failure", 2)
        .await
        .expect("second failure should be recorded");
    assert_eq!(action, OutboxPublishFailureAction::Failed);

    let failed = find_outbox_by_id(&outbox, &retry_message_id)
        .await
        .expect("failed message should exist");
    assert_eq!(failed.status, OutboxMessageStatus::Failed);
    assert_eq!(failed.attempts, 2);
    assert_eq!(failed.last_error.as_deref(), Some("second failure"));
}

pub async fn worker_claim_by_ids_claims_only_requested<R, S>(repo: R, outbox: S)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
    S: OutboxStore + Send + Sync,
{
    let wanted_id = unique_id("wanted-outbox");
    let other_id = unique_id("other-outbox");
    for (message_id, seat_id) in [(&wanted_id, "wanted-seat"), (&other_id, "other-seat")] {
        commit_outbox_for_seat(&repo, unique_id(seat_id), message_id, "seat.added").await;
    }

    let claimed = claim_one(
        &outbox,
        ClaimOutboxMessages::for_ids(
            "immediate-worker",
            vec![wanted_id.clone()],
            Duration::from_secs(60),
        ),
    )
    .await;
    assert_eq!(claimed.id(), wanted_id);

    let other = find_outbox_by_id(&outbox, &other_id)
        .await
        .expect("other message should exist");
    assert_eq!(other.status, OutboxMessageStatus::Pending);

    let empty = outbox
        .claim(ClaimOutboxMessages::for_ids(
            "immediate-worker",
            vec![unique_id("never-stored")],
            Duration::from_secs(60),
        ))
        .await
        .expect("claiming a missing id should not error");
    assert!(empty.is_empty());

    let leased = outbox
        .claim(ClaimOutboxMessages::for_ids(
            "worker-b",
            vec![wanted_id.clone()],
            Duration::from_secs(60),
        ))
        .await
        .expect("claiming a live-leased id should not error");
    assert!(
        leased.is_empty(),
        "by-id claim must not steal a row already leased by another worker"
    );
    let still_owned = find_outbox_by_id(&outbox, &wanted_id)
        .await
        .expect("leased message should exist");
    assert_eq!(still_owned.status, OutboxMessageStatus::InFlight);
    assert_eq!(still_owned.worker_id.as_deref(), Some("immediate-worker"));
}

pub async fn worker_completes_claims_in_one_batch<R, S>(repo: R, outbox: S)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
    S: OutboxStore + Send + Sync,
{
    let message_ids: Vec<String> = (0..3)
        .map(|index| unique_id(&format!("batch-complete-{index}")))
        .collect();
    for (index, message_id) in message_ids.iter().enumerate() {
        commit_outbox_for_seat(
            &repo,
            unique_id(&format!("batch-complete-seat-{index}")),
            message_id,
            "seat.added",
        )
        .await;
    }

    let claimed = outbox
        .claim(ClaimOutboxMessages::for_ids(
            "batch-worker",
            message_ids.clone(),
            Duration::from_secs(60),
        ))
        .await
        .expect("claiming the staged rows should succeed");
    assert_eq!(claimed.len(), message_ids.len());
    let claims = claimed
        .iter()
        .map(OutboxClaimRef::from_message)
        .collect::<Result<Vec<_>, _>>()
        .expect("claims should be valid");

    outbox
        .complete_many(&claims)
        .await
        .expect("batched complete should settle every active claim");
    for message_id in &message_ids {
        assert!(find_outbox_by_id(&outbox, message_id).await.is_none());
    }

    // Re-settling deleted delivery rows is NotFound, as with serial completion.
    let stale_err = outbox
        .complete_many(&claims)
        .await
        .expect_err("stale batch should not complete again");
    assert!(matches!(stale_err, RepositoryError::NotFound { .. }));

    // An empty batch is a no-op, not an error.
    outbox
        .complete_many(&[])
        .await
        .expect("empty batch should be a no-op");
}

pub async fn expired_outbox_lease_is_reclaimed_by_second_worker<R, S>(repo: R, outbox: S)
where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
    S: OutboxStore + Send + Sync,
{
    let message_id = unique_id("crash-lease-outbox");
    commit_outbox_for_seat(
        &repo,
        unique_id("crash-lease-seat"),
        &message_id,
        "seat.added",
    )
    .await;

    let lease = Duration::from_secs(1);
    let claimed_a = claim_one(&outbox, ClaimOutboxMessages::new("worker-a", 1, lease)).await;
    assert_eq!(claimed_a.id(), message_id);
    let stale_claim_a = OutboxClaimRef::from_message(&claimed_a).expect("A's claim is valid");

    let live = outbox
        .claim(ClaimOutboxMessages::new("worker-b", 1, lease))
        .await
        .expect("worker B poll should not error while the lease is live");
    assert!(
        live.is_empty(),
        "an unexpired lease must not be reclaimable by another worker"
    );

    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let claimed_b = claim_one(
        &outbox,
        ClaimOutboxMessages::new("worker-b", 1, Duration::from_secs(60)),
    )
    .await;
    assert_eq!(claimed_b.id(), message_id);
    assert_eq!(claimed_b.worker_id.as_deref(), Some("worker-b"));
    assert!(
        claimed_b.attempts > claimed_a.attempts,
        "reclaim increments the attempt counter (fences A's stale claim)"
    );
    let claim_b = OutboxClaimRef::from_message(&claimed_b).expect("B's claim is valid");

    let late_complete = outbox
        .complete(&stale_claim_a)
        .await
        .expect_err("A's late complete must be fenced");
    assert!(
        matches!(late_complete, RepositoryError::InvalidState { .. }),
        "stale-worker complete should be InvalidState, got {late_complete:?}"
    );
    let late_release = outbox
        .release(&stale_claim_a, "A woke up late")
        .await
        .expect_err("A's late release must be fenced");
    assert!(
        matches!(late_release, RepositoryError::InvalidState { .. }),
        "stale-worker release should be InvalidState, got {late_release:?}"
    );

    outbox
        .complete(&claim_b)
        .await
        .expect("the reclaiming worker should complete the row");
    assert!(find_outbox_by_id(&outbox, &message_id).await.is_none());
}

pub async fn publish_failure_after_commit_retains_outbox_row_until_delivered<R, S>(
    repo: R,
    outbox: S,
) where
    R: GetStream + TransactionalCommit + Clone + Send + Sync + 'static,
    S: OutboxStore + Send + Sync + Clone,
{
    let message_id = unique_id("publish-retry-outbox");
    commit_outbox_for_seat(
        &repo,
        unique_id("publish-retry-seat"),
        &message_id,
        "seat.added",
    )
    .await;

    let dispatcher = OutboxDispatcher::new(
        outbox.clone(),
        FlakyPublisher::new(2),
        "immediate:publish-retry",
        Duration::from_secs(60),
        10,
    );
    let ids = [message_id.clone()];

    let pass1 = dispatcher
        .dispatch_ids(&ids)
        .await
        .expect("dispatch pass should not surface a transport error");
    assert_eq!(pass1.published, 0);
    assert_eq!(pass1.released, 1);
    assert_eq!(pass1.failed, 0);
    let after_1 = find_outbox_by_id(&outbox, &message_id)
        .await
        .expect("row survives the first failure");
    assert_eq!(
        after_1.status,
        OutboxMessageStatus::Pending,
        "a failed publish releases the row back to Pending (still owed)"
    );
    assert_eq!(after_1.attempts, 1);

    let pass2 = dispatcher.dispatch_ids(&ids).await.expect("second pass");
    assert_eq!(pass2.published, 0);
    assert_eq!(pass2.released, 1);
    let after_2 = find_outbox_by_id(&outbox, &message_id)
        .await
        .expect("row survives the second failure");
    assert_eq!(after_2.status, OutboxMessageStatus::Pending);
    assert_eq!(after_2.attempts, 2);

    let pass3 = dispatcher.dispatch_ids(&ids).await.expect("third pass");
    assert_eq!(pass3.published, 1);
    assert_eq!(pass3.released, 0);
    assert_eq!(pass3.failed, 0);
    assert!(find_outbox_by_id(&outbox, &message_id).await.is_none());

    assert_eq!(
        dispatcher.publisher().attempts(),
        3,
        "publisher was invoked once per pass (two failures + one success)"
    );
    assert_eq!(
        dispatcher.publisher().delivered(),
        vec![message_id],
        "the message was delivered exactly once, only on the successful pass"
    );
}

async fn commit_outbox_for_seat<R>(repo: &R, seat_id: String, message_id: &str, event_type: &str)
where
    R: TransactionalCommit + Clone + Send + Sync + 'static,
{
    let mut seat = added_seat(&seat_id);
    repo.clone()
        .aggregate::<Seat>()
        .outbox(outbox_message(message_id, event_type))
        .commit(&mut seat)
        .await
        .expect("aggregate and outbox should commit atomically");
}

fn outbox_message(id: &str, event_type: &str) -> OutboxMessage {
    OutboxMessage::create(id, event_type, b"{}".to_vec()).expect("outbox message should be valid")
}

async fn claim_one<S>(outbox: &S, request: ClaimOutboxMessages) -> OutboxMessage
where
    S: OutboxStore + Send + Sync,
{
    let claimed = outbox.claim(request).await.expect("claim should succeed");
    assert_eq!(claimed.len(), 1);
    claimed.into_iter().next().expect("one claimed message")
}

fn added_seat(id: &str) -> Seat {
    let mut seat = Seat::default();
    seat.add(id.to_string(), "floor".to_string())
        .expect("seat should be valid");
    seat
}
