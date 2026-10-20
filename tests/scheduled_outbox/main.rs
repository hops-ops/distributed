//! Durable timers: `OutboxMessage::deliver_at` is honoured by every outbox
//! store's claim query and survives restart (SQLite file database).
//! Postgres skips without `DATABASE_URL`.

#[cfg(feature = "sqlite")]
#[path = "../support/sqlite.rs"]
mod sqlite_support;

use std::time::{Duration, SystemTime};

use distributed::{
    ClaimOutboxMessages, CommitBatch, InMemoryRepository, OutboxMessage, OutboxStore,
    TransactionalCommit,
};

const LEASE: Duration = Duration::from_secs(60);

fn messages(now: SystemTime) -> Vec<OutboxMessage> {
    vec![
        OutboxMessage::create_to("immediate", "Ping", "q", vec![1]).unwrap(),
        OutboxMessage::create_to("past", "Ping", "q", vec![2])
            .unwrap()
            .deliver_at(now - Duration::from_secs(5)),
        OutboxMessage::create_to("future", "Ping", "q", vec![3])
            .unwrap()
            .deliver_after(now, Duration::from_secs(3600)),
    ]
}

async fn commit_messages<R: TransactionalCommit>(repo: &R, messages: Vec<OutboxMessage>) {
    let mut batch = CommitBatch::empty();
    batch.outbox_messages = messages;
    repo.commit_batch(batch).await.unwrap();
}

/// Due rows are claimable (by poll and by explicit id); the future row is
/// listed as pending with its schedule but never claimed.
async fn scenario_schedule_honoured<S: OutboxStore>(store: &S) {
    let pending = store.pending(10).await.unwrap();
    assert_eq!(pending.len(), 3);
    let future = pending.iter().find(|m| m.id() == "future").unwrap();
    assert!(future.deliver_at.is_some());
    assert!(pending
        .iter()
        .find(|m| m.id() == "immediate")
        .unwrap()
        .deliver_at
        .is_none());

    let by_id = store
        .claim(ClaimOutboxMessages::for_ids(
            "w",
            vec!["future".into()],
            LEASE,
        ))
        .await
        .unwrap();
    assert!(
        by_id.is_empty(),
        "explicit-id claim must not bypass the schedule"
    );

    let mut claimed: Vec<String> = store
        .claim(ClaimOutboxMessages::new("w", 10, LEASE).to_destination("q"))
        .await
        .unwrap()
        .iter()
        .map(|m| m.id().to_string())
        .collect();
    claimed.sort();
    assert_eq!(claimed, vec!["immediate".to_string(), "past".to_string()]);
}

#[tokio::test]
async fn in_memory_store_honours_deliver_at() {
    let repo = InMemoryRepository::new();
    commit_messages(&repo, messages(SystemTime::now())).await;
    scenario_schedule_honoured(&repo.outbox_store()).await;
}

#[test]
fn deliver_at_round_trips_through_serde_and_defaults_to_none() {
    let message = OutboxMessage::create("m", "T", vec![])
        .unwrap()
        .deliver_after(SystemTime::now(), Duration::from_secs(1));
    let json = serde_json::to_value(&message).unwrap();
    let back: OutboxMessage = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(back.deliver_at, message.deliver_at);
    let mut legacy = json;
    legacy.as_object_mut().unwrap().remove("deliver_at");
    let old: OutboxMessage = serde_json::from_value(legacy).unwrap();
    assert_eq!(
        old.deliver_at, None,
        "rows without the field stay immediate"
    );
}

#[cfg(feature = "sqlite")]
mod sqlite_backend {
    use super::*;
    use crate::sqlite_support::TempDb;
    use distributed::{OutboxClaimRef, SqliteRepository};

    #[tokio::test]
    async fn sqlite_store_honours_deliver_at() {
        let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
            .await
            .unwrap();
        commit_messages(&repo, messages(SystemTime::now())).await;
        scenario_schedule_honoured(&repo.outbox_store()).await;
    }

    #[tokio::test]
    async fn scheduled_message_survives_restart_and_fires_when_due() {
        let db = TempDb::new("scheduled_outbox_restart");
        let delay = Duration::from_millis(400);
        let due_at;
        {
            let repo = SqliteRepository::new(db.pool().await);
            repo.migrate().await.unwrap();
            let timer = OutboxMessage::create_to("timer-1", "Timeout", "saga", b"t".to_vec())
                .unwrap()
                .deliver_after(SystemTime::now(), delay);
            due_at = timer.deliver_at.unwrap();
            commit_messages(&repo, vec![timer]).await;
            repo.pool().close().await;
        } // process "crashes"

        let repo = SqliteRepository::new(db.pool().await);
        let store = repo.outbox_store();
        let pending = store.pending(10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].deliver_at, Some(due_at), "schedule persisted");
        let early = store
            .claim(ClaimOutboxMessages::new("w", 10, LEASE))
            .await
            .unwrap();
        assert!(early.is_empty(), "not claimable before it is due");

        // The one unavoidable wait: the SQL claim compares against wall time.
        let remaining = due_at.duration_since(SystemTime::now()).unwrap_or_default();
        tokio::time::sleep(remaining + Duration::from_millis(50)).await;

        let fired = store
            .claim(ClaimOutboxMessages::new("w", 10, LEASE))
            .await
            .unwrap();
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].payload, b"t");
        store
            .complete(&OutboxClaimRef::from_message(&fired[0]).unwrap())
            .await
            .unwrap();

        // Released (retried) rows are immediately claimable again: the
        // release bumps `next_available_at` to now, not back to the schedule.
        let retry = OutboxMessage::create_to("timer-2", "Timeout", "saga", vec![])
            .unwrap()
            .deliver_at(SystemTime::now() - Duration::from_millis(1));
        commit_messages(&repo, vec![retry]).await;
        let claimed = store
            .claim(ClaimOutboxMessages::new("w", 10, LEASE))
            .await
            .unwrap();
        let claim = OutboxClaimRef::from_message(&claimed[0]).unwrap();
        store.release(&claim, "transient").await.unwrap();
        let again = store
            .claim(ClaimOutboxMessages::new("w", 10, LEASE))
            .await
            .unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].attempts, 2);
    }
}

#[cfg(feature = "postgres")]
#[path = "../support/postgres.rs"]
mod postgres_support;

#[cfg(feature = "postgres")]
mod postgres_backend {
    use super::*;
    use crate::postgres_support::PostgresTestSchema;

    #[tokio::test]
    async fn postgres_store_honours_deliver_at() {
        let Some(schema) =
            PostgresTestSchema::create_from_env("sched", "skipping postgres schedule test").await
        else {
            return;
        };
        let repo = schema.repository().await;
        commit_messages(&repo, messages(SystemTime::now())).await;
        scenario_schedule_honoured(&repo.outbox_store()).await;
    }
}
