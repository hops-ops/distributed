//! SQLite bus transport integration tests.
//!
//! Exercises `SqliteBus` over a local SQLite file: command work-queue claims,
//! event log offsets, retry/nack behavior, and corrupt-row handling.

// Shared broker-test helpers (recording_for, message fixtures, bus scenarios).
use common::sqlite as sqlite_support;
use common::transport as conformance;
use conformance::{command, event, recorded_ids, recording_for, COMMAND_NAME, EVENT_NAME, PAYLOAD};
use sqlite_support::TempDb;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    Bus, BusConsumer, Handlers, Message, MessageKind, MessageRouter, OrderedDelivery, RunOptions,
    SqliteBus, SubscriptionPlan, TransportError,
};
use distributed::projection_protocol::ProjectionEpoch;
use distributed::{CAUSATION_ID, TRACEPARENT};
use sqlx::SqlitePool;
use tokio::sync::Notify;

async fn bus() -> (TempDb, SqlitePool, SqliteBus) {
    let db = TempDb::new("distributed_sqlite_bus_test");
    let pool = db.pool().await;
    let bus = SqliteBus::new(pool.clone()).group("orders");
    bus.ensure_tables().await.expect("ensure tables");
    (db, pool, bus)
}

/// Build a `SqliteBus` over `pool` for `group` (empty `group` = no group).
/// Tables are already ensured by [`bus`].
fn sqlite_bus(pool: &SqlitePool, group: &str) -> SqliteBus {
    let bus = SqliteBus::new(pool.clone());
    if group.is_empty() {
        bus
    } else {
        bus.group(group)
    }
}

/// One temporary database per test; every bus shares its pool.
struct Sqlite {
    _db: TempDb,
    pool: SqlitePool,
}

impl conformance::bus::BusHarness for Sqlite {
    type Bus = SqliteBus;

    async fn start() -> Option<Self> {
        let (db, pool, _bus) = bus().await;
        Some(Self { _db: db, pool })
    }

    async fn bus(&self, group: &str) -> SqliteBus {
        sqlite_bus(&self.pool, group)
    }
}

common::bus_conformance_tests!(Sqlite);

#[derive(Default)]
struct OrderedEvidenceRecorder {
    observed: Mutex<Vec<(String, u64)>>,
}

impl MessageRouter for OrderedEvidenceRecorder {
    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        kind == MessageKind::Event && name == EVENT_NAME
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        SubscriptionPlan {
            commands: Vec::new(),
            events: vec![EVENT_NAME.to_string()],
        }
    }

    async fn dispatch(&self, _message: &Message) -> Result<(), TransportError> {
        Ok(())
    }

    async fn dispatch_ordered(
        &self,
        _message: &Message,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<(), TransportError> {
        let ordered =
            ordered.ok_or_else(|| TransportError::permanent("missing SQL ordering evidence"))?;
        self.observed
            .lock()
            .unwrap()
            .push((ordered.epoch().as_str().to_string(), ordered.position()));
        Ok(())
    }
}

#[derive(Default)]
struct RotationRouter {
    attempts: AtomicUsize,
    seen: Mutex<Vec<String>>,
    first_started: Notify,
    release_first: Notify,
}

impl MessageRouter for RotationRouter {
    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        kind == MessageKind::Event && name == EVENT_NAME
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        SubscriptionPlan {
            commands: Vec::new(),
            events: vec![EVENT_NAME.to_string()],
        }
    }

    async fn dispatch(&self, _message: &Message) -> Result<(), TransportError> {
        Ok(())
    }

    async fn dispatch_ordered(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<(), TransportError> {
        ordered.ok_or_else(|| TransportError::permanent("missing SQL ordering evidence"))?;
        self.seen
            .lock()
            .unwrap()
            .push(message.id().unwrap_or_default().to_string());
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_started.notify_one();
            self.release_first.notified().await;
        }
        Ok(())
    }
}

async fn recreate_nullable_queue_table(pool: &SqlitePool) {
    sqlx::query("DROP TABLE IF EXISTS bus_queue")
        .execute(pool)
        .await
        .expect("drop bus_queue");
    sqlx::query(
        r#"
        CREATE TABLE bus_queue (
            seq          INTEGER PRIMARY KEY AUTOINCREMENT,
            claim_token  TEXT,
            name         TEXT,
            message_id   TEXT,
            kind         TEXT NOT NULL,
            payload      BLOB NOT NULL,
            content_type TEXT NOT NULL DEFAULT 'application/json',
            metadata     TEXT NOT NULL DEFAULT '[]',
            available_at REAL NOT NULL DEFAULT (unixepoch('now','subsec')),
            locked_until REAL,
            attempts     INTEGER NOT NULL DEFAULT 0
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("create nullable bus_queue");
    sqlx::query(
        "CREATE INDEX bus_queue_claim_idx ON bus_queue (name, available_at, locked_until, seq)",
    )
    .execute(pool)
    .await
    .expect("create queue index");
}

async fn recreate_nullable_log_table(pool: &SqlitePool) {
    sqlx::query("DROP TABLE IF EXISTS bus_log")
        .execute(pool)
        .await
        .expect("drop bus_log");
    sqlx::query(
        r#"
        CREATE TABLE bus_log (
            seq          INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT,
            message_id   TEXT,
            kind         TEXT NOT NULL,
            payload      BLOB NOT NULL,
            content_type TEXT DEFAULT 'application/json',
            metadata     TEXT NOT NULL DEFAULT '[]',
            appended_at  REAL NOT NULL DEFAULT (unixepoch('now','subsec'))
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("create nullable bus_log");
    sqlx::query("CREATE INDEX bus_log_name_seq_idx ON bus_log (name, seq)")
        .execute(pool)
        .await
        .expect("create log index");
    sqlx::query(
        "CREATE UNIQUE INDEX bus_log_message_id_unique_idx \
         ON bus_log (message_id) WHERE message_id IS NOT NULL",
    )
    .execute(pool)
    .await
    .expect("create stable message ID index");
}

async fn corrupt_latest_queue_kind(pool: &SqlitePool) {
    sqlx::query("UPDATE bus_queue SET kind = 'bogus' WHERE seq = (SELECT max(seq) FROM bus_queue)")
        .execute(pool)
        .await
        .expect("corrupt queue kind");
}

async fn corrupt_latest_log_kind(pool: &SqlitePool) {
    sqlx::query("UPDATE bus_log SET kind = 'bogus' WHERE seq = (SELECT max(seq) FROM bus_log)")
        .execute(pool)
        .await
        .expect("corrupt log kind");
}

async fn corrupt_latest_log_metadata(pool: &SqlitePool) {
    sqlx::query(
        "UPDATE bus_log SET metadata = 'not-json' WHERE seq = (SELECT max(seq) FROM bus_log)",
    )
    .execute(pool)
    .await
    .expect("corrupt log metadata");
}

async fn corrupt_latest_log_content_type(pool: &SqlitePool) {
    sqlx::query(
        "UPDATE bus_log SET content_type = NULL WHERE seq = (SELECT max(seq) FROM bus_log)",
    )
    .execute(pool)
    .await
    .expect("corrupt log content type");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bus_send_listen_is_point_to_point_across_a_group() {
    let (_db, pool, _bus) = bus().await;
    conformance::bus_send_listen_is_point_to_point_across_a_group(|group| {
        let bus = sqlite_bus(&pool, group);
        async move { bus }
    })
    .await;
}

#[tokio::test]
async fn bus_publish_subscribe_fans_out_across_groups() {
    let (_db, pool, _bus) = bus().await;
    conformance::bus_publish_subscribe_fans_out_across_groups(|group| {
        let bus = sqlite_bus(&pool, group);
        async move { bus }
    })
    .await;
}

#[tokio::test]
async fn bus_subscribe_uses_named_service_as_consumer_group() {
    let (_db, pool, _bus) = bus().await;
    conformance::bus_subscribe_uses_named_service_as_consumer_group(|| {
        let bus = sqlite_bus(&pool, "");
        async move { bus }
    })
    .await;
}

#[tokio::test]
async fn retryable_event_failure_does_not_advance_offset() {
    let (_db, pool, bus) = bus().await;
    bus.publish_message(event("e1"))
        .await
        .expect("publish event");

    let attempts = Arc::new(AtomicUsize::new(0));
    let seen = attempts.clone();
    let handlers = Arc::new(Handlers::new().named("projections").on_event(
        EVENT_NAME,
        move |_: &Message| {
            let seen = seen.clone();
            async move {
                let previous = seen.fetch_add(1, Ordering::SeqCst);
                if previous == 0 {
                    Err(TransportError::retryable("transient"))
                } else {
                    Ok(())
                }
            }
        },
    ));

    SqliteBus::new(pool.clone())
        .subscribe(handlers, RunOptions::until_idle())
        .await
        .expect("subscriber drains after retry");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "event was reread once because nack left the offset unmoved"
    );
    let offset: Option<i64> =
        sqlx::query_scalar("SELECT last_seq FROM bus_offset WHERE consumer = 'projections'")
            .fetch_optional(&pool)
            .await
            .expect("read offset");
    assert_eq!(offset, Some(1), "offset advanced only after success");
}

#[tokio::test]
async fn bus_schema_rejects_unsupported_message_kind() {
    let (_db, pool, _bus) = bus().await;

    let queue_err = sqlx::query("INSERT INTO bus_queue (name, kind, payload) VALUES (?, ?, ?)")
        .bind(COMMAND_NAME)
        .bind("bogus")
        .bind(PAYLOAD.to_vec())
        .execute(&pool)
        .await
        .expect_err("queue kind check rejects unsupported message kind");
    assert!(
        queue_err.to_string().contains("CHECK"),
        "unexpected queue kind error: {queue_err}"
    );

    let log_err = sqlx::query("INSERT INTO bus_log (name, kind, payload) VALUES (?, ?, ?)")
        .bind(EVENT_NAME)
        .bind("bogus")
        .bind(PAYLOAD.to_vec())
        .execute(&pool)
        .await
        .expect_err("log kind check rejects unsupported message kind");
    assert!(
        log_err.to_string().contains("CHECK"),
        "unexpected log kind error: {log_err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_or_locked_writer_contention_is_retryable() {
    let db = TempDb::new("distributed_sqlite_bus_test");
    let setup_pool = db.pool().await;
    SqliteBus::new(setup_pool.clone())
        .ensure_tables()
        .await
        .expect("ensure tables");

    let lock_pool = db.pool_with_timeout(Duration::from_millis(0)).await;
    let send_pool = db.pool_with_timeout(Duration::from_millis(0)).await;
    let mut conn = lock_pool.acquire().await.expect("lock connection");
    sqlx::query("BEGIN EXCLUSIVE")
        .execute(&mut *conn)
        .await
        .expect("begin exclusive");

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        SqliteBus::new(send_pool).send(COMMAND_NAME, PAYLOAD.to_vec()),
    )
    .await
    .expect("send should not hang behind busy lock");
    let err = result.expect_err("send should fail while database is locked");
    assert!(
        err.is_retryable(),
        "busy/locked contention must be retryable, got {err}"
    );

    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("rollback exclusive lock");
}

#[tokio::test]
async fn bus_listen_dead_letters_corrupt_queue_row_not_silently() {
    let (_db, pool, bus) = bus().await;
    recreate_nullable_queue_table(&pool).await;

    bus.send_message(command("poison"))
        .await
        .expect("send poison");
    corrupt_latest_queue_kind(&pool).await;
    bus.send_message(command("poison-kind"))
        .await
        .expect("send poison kind");
    corrupt_latest_queue_kind(&pool).await;
    bus.send_message(command("ok")).await.expect("send ok");

    let rec = Arc::new(Mutex::new(Vec::new()));
    bus.listen(
        recording_for(COMMAND_NAME, MessageKind::Command, rec.clone()),
        RunOptions::until_idle(),
    )
    .await
    .expect("listen drains without surfacing corrupt row as fatal");

    assert_eq!(
        recorded_ids(&rec),
        vec!["ok".to_string()],
        "only the valid row handled"
    );
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM bus_queue")
        .fetch_one(&pool)
        .await
        .expect("count queue");
    assert_eq!(
        remaining, 0,
        "corrupt row routed through policy, not redelivered forever"
    );
    let dead_letters: Vec<Vec<u8>> = sqlx::query_scalar(
        "SELECT payload FROM bus_log WHERE name = 'distributed.dead_letter' ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .expect("read dead letters");
    assert_eq!(dead_letters.len(), 2, "every corrupt row was stored");
    let dead: distributed::bus::DeadLetter =
        serde_json::from_slice(&dead_letters[0]).expect("dead letter payload");
    assert!(dead.reason.contains("corrupt row"), "{}", dead.reason);
    assert_eq!(dead.attempts, 1);
}

#[tokio::test]
async fn bus_subscribe_dead_letters_corrupt_log_row_not_silently() {
    let (_db, pool, bus) = bus().await;
    recreate_nullable_log_table(&pool).await;

    bus.publish_message(event("poison"))
        .await
        .expect("publish leading poison");
    corrupt_latest_log_kind(&pool).await;
    bus.publish_message(event("ok")).await.expect("publish ok");
    bus.publish_message(event("poison-tail"))
        .await
        .expect("publish trailing poison");
    corrupt_latest_log_kind(&pool).await;
    bus.publish_message(event("poison-kind"))
        .await
        .expect("publish corrupt kind");
    corrupt_latest_log_kind(&pool).await;
    bus.publish_message(event("poison-metadata"))
        .await
        .expect("publish corrupt metadata");
    corrupt_latest_log_metadata(&pool).await;
    bus.publish_message(event("poison-content-type"))
        .await
        .expect("publish corrupt content type");
    corrupt_latest_log_content_type(&pool).await;

    let rec = Arc::new(Mutex::new(Vec::new()));
    SqliteBus::new(pool.clone())
        .group("projections")
        .subscribe(
            recording_for(EVENT_NAME, MessageKind::Event, rec.clone()),
            RunOptions::until_idle(),
        )
        .await
        .expect("subscribe drains past corrupt entries");

    assert_eq!(
        recorded_ids(&rec),
        vec!["ok".to_string()],
        "only the valid event handled"
    );
    let offset: Option<i64> =
        sqlx::query_scalar("SELECT last_seq FROM bus_offset WHERE consumer = 'projections'")
            .fetch_optional(&pool)
            .await
            .expect("read offset");
    let max_seq: i64 =
        sqlx::query_scalar("SELECT max(seq) FROM bus_log WHERE name <> 'distributed.dead_letter'")
            .fetch_one(&pool)
            .await
            .expect("max seq");
    assert_eq!(
        offset,
        Some(max_seq),
        "offset advanced past corrupt entries through the failure policy"
    );
    let dead_letters: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bus_log WHERE name = 'distributed.dead_letter'")
            .fetch_one(&pool)
            .await
            .expect("count dead letters");
    assert_eq!(
        dead_letters, 5,
        "every corrupt entry was stored, not dropped"
    );
}

#[tokio::test]
async fn stable_log_retry_keeps_the_original_cursor_and_rejects_conflicts() {
    let (_db, pool, bus) = bus().await;
    let first = event("stable-event")
        .with_metadata(CAUSATION_ID, "command-1")
        .with_metadata(TRACEPARENT, "first-attempt");
    bus.publish_message(first)
        .await
        .expect("first append commits");

    let original_seq: i64 =
        sqlx::query_scalar("SELECT seq FROM bus_log WHERE message_id = 'stable-event'")
            .fetch_one(&pool)
            .await
            .expect("read original cursor");
    let retry = event("stable-event")
        .with_metadata(CAUSATION_ID, "command-1")
        .with_metadata(TRACEPARENT, "retry-attempt");
    bus.publish_message(retry)
        .await
        .expect("same causal envelope is an idempotent retry");

    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bus_log WHERE message_id = 'stable-event'")
            .fetch_one(&pool)
            .await
            .expect("count stable ID rows");
    let retained_seq: i64 =
        sqlx::query_scalar("SELECT seq FROM bus_log WHERE message_id = 'stable-event'")
            .fetch_one(&pool)
            .await
            .expect("read retained cursor");
    let retained_metadata: String =
        sqlx::query_scalar("SELECT metadata FROM bus_log WHERE message_id = 'stable-event'")
            .fetch_one(&pool)
            .await
            .expect("read authoritative metadata");
    assert_eq!(rows, 1, "ambiguous retry did not append a second row");
    assert_eq!(
        retained_seq, original_seq,
        "ambiguous retry retained the original ordered cursor"
    );
    assert!(
        retained_metadata.contains("first-attempt"),
        "the first committed envelope remains authoritative"
    );
    assert!(!retained_metadata.contains("retry-attempt"));

    let payload_conflict = Message::new(
        EVENT_NAME,
        MessageKind::Event,
        br#"{"different":true}"#.to_vec(),
    )
    .with_id("stable-event")
    .with_metadata(CAUSATION_ID, "command-1");
    let error = bus
        .publish_message(payload_conflict)
        .await
        .expect_err("same stable ID cannot identify a different payload");
    assert!(error.is_permanent(), "conflict is deterministic corruption");

    let causation_conflict = event("stable-event").with_metadata(CAUSATION_ID, "different-command");
    let error = bus
        .publish_message(causation_conflict)
        .await
        .expect_err("same stable ID cannot identify a different causation");
    assert!(error.is_permanent(), "causation conflict is permanent");

    let rows_after_conflicts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bus_log WHERE message_id = 'stable-event'")
            .fetch_one(&pool)
            .await
            .expect("count rows after conflicts");
    assert_eq!(rows_after_conflicts, 1, "conflicts leave the log unchanged");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_stable_log_retries_share_one_original_cursor() {
    let (_db, pool, bus) = bus().await;
    let first = event("concurrent-stable")
        .with_metadata(CAUSATION_ID, "command-concurrent")
        .with_metadata(TRACEPARENT, "attempt-a");
    let retry = event("concurrent-stable")
        .with_metadata(CAUSATION_ID, "command-concurrent")
        .with_metadata(TRACEPARENT, "attempt-b");

    let (first_result, retry_result) =
        tokio::join!(bus.publish_message(first), bus.publish_message(retry));
    first_result.expect("one concurrent append wins");
    retry_result.expect("the equivalent concurrent append is idempotent");

    let rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bus_log WHERE message_id = 'concurrent-stable'")
            .fetch_one(&pool)
            .await
            .expect("count concurrent stable ID rows");
    let seq: i64 =
        sqlx::query_scalar("SELECT seq FROM bus_log WHERE message_id = 'concurrent-stable'")
            .fetch_one(&pool)
            .await
            .expect("read stable cursor");
    assert_eq!(rows, 1);
    assert_eq!(seq, 1, "the only allocated cursor remains authoritative");
}

#[tokio::test]
async fn nonempty_log_identity_adoption_must_be_explicit_and_retires_offsets() {
    let (_db, pool, bus) = bus().await;
    bus.publish_message(event("identity-loss"))
        .await
        .expect("append before identity loss");
    let retired_epoch: String =
        sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .expect("read retired epoch");
    sqlx::query(
        "INSERT INTO bus_offset (consumer, source_epoch, last_seq) \
         VALUES ('stale-consumer', ?, 1)",
    )
    .bind(&retired_epoch)
    .execute(&pool)
    .await
    .expect("seed stale bound offset");
    sqlx::query("DROP TABLE bus_log_identity")
        .execute(&pool)
        .await
        .expect("simulate independently lost identity");

    let error = bus
        .ensure_tables()
        .await
        .expect_err("a retained log cannot receive a random replacement epoch");
    assert!(error.is_permanent());
    let identities: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_log_identity")
        .fetch_one(&pool)
        .await
        .expect("count rolled-back identity");
    let offsets_before_adoption: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_offset")
        .fetch_one(&pool)
        .await
        .expect("count still-bound offsets");
    assert_eq!(
        identities, 0,
        "failed default adoption installs no identity"
    );
    assert_eq!(
        offsets_before_adoption, 1,
        "failed default adoption does not clear offsets"
    );

    let adopted_epoch = ProjectionEpoch::new("operator-adopted-retained-log").unwrap();
    SqliteBus::new(pool.clone())
        .with_source_epoch(adopted_epoch.clone())
        .ensure_tables()
        .await
        .expect("explicitly adopt the retained log");
    let replacement_epoch: String =
        sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .expect("read replacement epoch");
    let offsets: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_offset")
        .fetch_one(&pool)
        .await
        .expect("count retired offsets");
    assert_ne!(replacement_epoch, retired_epoch);
    assert_eq!(replacement_epoch, adopted_epoch.as_str());
    assert_eq!(
        offsets, 0,
        "identity creation and offset invalidation commit together"
    );
}

#[tokio::test]
async fn configured_epoch_initializes_but_cannot_relabel_an_existing_log() {
    let db = TempDb::new("distributed_sqlite_bus_epoch_override");
    let pool = db.pool().await;
    let initial_epoch = ProjectionEpoch::new("operator-generation-1").unwrap();
    let bus = SqliteBus::new(pool.clone())
        .group("epoch-observer")
        .with_source_epoch(initial_epoch.clone());
    bus.ensure_tables()
        .await
        .expect("configured epoch initializes a new log");
    let persisted: String =
        sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .expect("read initialized epoch");
    assert_eq!(persisted, initial_epoch.as_str());
    bus.publish_message(event("delivered-epoch"))
        .await
        .expect("append under configured epoch");
    let recorder = Arc::new(OrderedEvidenceRecorder::default());
    bus.subscribe(recorder.clone(), RunOptions::until_idle())
        .await
        .expect("deliver ordered row");
    assert_eq!(
        *recorder.observed.lock().unwrap(),
        vec![(persisted.clone(), 1)],
        "delivery exposes the durable row's epoch and original SQL position"
    );

    let mismatched = SqliteBus::new(pool.clone())
        .with_source_epoch(ProjectionEpoch::new("operator-generation-2").unwrap());
    let error = mismatched
        .ensure_tables()
        .await
        .expect_err("a builder override cannot relabel existing positions");
    assert!(error.is_permanent());
    let error = mismatched
        .publish_message(event("must-not-append"))
        .await
        .expect_err("producer mismatch fails before append");
    assert!(error.is_permanent());
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM bus_log")
        .fetch_one(&pool)
        .await
        .expect("count log rows");
    let still_persisted: String =
        sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .expect("read unchanged epoch");
    assert_eq!(rows, 1, "mismatched producer did not append");
    assert_eq!(still_persisted, initial_epoch.as_str());
}

#[tokio::test]
async fn log_rewind_requires_an_explicit_fenced_reset() {
    let (_db, pool, bus) = bus().await;
    bus.publish_message(event("before-reset"))
        .await
        .expect("append before reset");

    let (before_epoch, before_generation, before_high_water): (String, i64, i64) = sqlx::query_as(
        "SELECT source_epoch, generation, high_water \
             FROM bus_log_identity WHERE singleton = 1",
    )
    .fetch_one(&pool)
    .await
    .expect("read initial log identity");
    assert_eq!(before_generation, 1);
    assert_eq!(before_high_water, 1);
    sqlx::query(
        "INSERT INTO bus_offset (consumer, source_epoch, last_seq) \
         VALUES ('projector', ?, 1)",
    )
    .bind(&before_epoch)
    .execute(&pool)
    .await
    .expect("seed old generation offset");

    sqlx::query("DROP TABLE bus_log")
        .execute(&pool)
        .await
        .expect("simulate independently rebuilt log");
    let error = SqliteBus::new(pool.clone())
        .ensure_tables()
        .await
        .expect_err("ordinary startup cannot authorize cursor-domain reuse");
    assert!(error.is_permanent());
    let error = bus
        .publish_message(event("must-not-rotate"))
        .await
        .expect_err("ordinary publish cannot authorize cursor-domain reuse");
    assert!(error.is_permanent());
    let replacement_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_log")
        .fetch_one(&pool)
        .await
        .expect("count replacement rows before reset");
    assert_eq!(replacement_rows, 0);
    let unchanged: (String, i64, i64) = sqlx::query_as(
        "SELECT source_epoch, generation, high_water \
         FROM bus_log_identity WHERE singleton = 1",
    )
    .fetch_one(&pool)
    .await
    .expect("read unchanged rewind fence");
    assert_eq!(
        unchanged,
        (before_epoch.clone(), before_generation, before_high_water)
    );
    let offsets_before_reset: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_offset")
        .fetch_one(&pool)
        .await
        .expect("count fenced offsets");
    assert_eq!(offsets_before_reset, 1);

    let expected_epoch = ProjectionEpoch::new(before_epoch.clone()).unwrap();
    let wrong_epoch = ProjectionEpoch::new("wrong-current-generation").unwrap();
    let next_epoch = ProjectionEpoch::new("operator-reset-generation-2").unwrap();
    let error = bus
        .reset_ordered_log(&wrong_epoch, &next_epoch)
        .await
        .expect_err("compare-and-swap reset rejects a stale expected epoch");
    assert!(error.is_permanent());
    let error = bus
        .reset_ordered_log(&expected_epoch, &expected_epoch)
        .await
        .expect_err("a reset cannot reuse the retired epoch");
    assert!(error.is_permanent());
    bus.reset_ordered_log(&expected_epoch, &next_epoch)
        .await
        .expect("operator-authorized reset");

    let (after_epoch, after_generation, after_high_water): (String, i64, i64) = sqlx::query_as(
        "SELECT source_epoch, generation, high_water \
             FROM bus_log_identity WHERE singleton = 1",
    )
    .fetch_one(&pool)
    .await
    .expect("read rotated log identity");
    assert_eq!(after_epoch, next_epoch.as_str());
    assert_eq!(after_generation, before_generation + 1);
    assert_eq!(after_high_water, 0);
    let offsets: i64 = sqlx::query_scalar("SELECT count(*) FROM bus_offset")
        .fetch_one(&pool)
        .await
        .expect("count retired offsets");
    assert_eq!(offsets, 0, "old-generation offsets cannot skip the new log");

    let rebuilt = SqliteBus::new(pool.clone());
    rebuilt
        .publish_message(event("after-reset"))
        .await
        .expect("append in new generation");
    let new_seq: i64 =
        sqlx::query_scalar("SELECT seq FROM bus_log WHERE message_id = 'after-reset'")
            .fetch_one(&pool)
            .await
            .expect("read rebuilt cursor");
    let persisted_epoch: String =
        sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .expect("read persisted epoch");
    assert_eq!(new_seq, 1, "the rebuilt log reused a numeric position");
    assert_eq!(
        persisted_epoch, after_epoch,
        "ordinary appends retain the rotated epoch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_subscriber_stops_before_cached_rows_after_epoch_rotation() {
    let (_db, pool, producer) = bus().await;
    for index in 0..17 {
        producer
            .publish_message(event(format!("old-{index}")))
            .await
            .expect("append old-generation event");
    }
    let router = Arc::new(RotationRouter::default());
    let subscriber = SqliteBus::new(pool.clone()).group("rotation-observer");
    let running = tokio::spawn({
        let router = router.clone();
        async move { subscriber.subscribe(router, RunOptions::until_idle()).await }
    });
    tokio::time::timeout(Duration::from_secs(2), router.first_started.notified())
        .await
        .expect("first buffered delivery started");

    let current_epoch: String =
        sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .expect("read current epoch");
    let current_epoch = ProjectionEpoch::new(current_epoch).unwrap();
    let next_epoch = ProjectionEpoch::new("live-reset-generation-2").unwrap();
    producer
        .reset_ordered_log(&current_epoch, &next_epoch)
        .await
        .expect("reset log while a handler is in flight");
    let replacement = SqliteBus::new(pool.clone());
    replacement
        .publish_message(event("new-0"))
        .await
        .expect("append replacement-generation event");
    router.release_first.notify_one();

    let error = tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .expect("subscriber stopped")
        .expect("subscriber task joined")
        .expect_err("retired subscriber fails closed");
    assert!(error.is_permanent(), "epoch mismatch is not retryable");
    assert_eq!(
        *router.seen.lock().unwrap(),
        vec!["old-0".to_string()],
        "cached old rows and replacement rows were never dispatched"
    );
    let offsets: i64 =
        sqlx::query_scalar("SELECT count(*) FROM bus_offset WHERE consumer = 'rotation-observer'")
            .fetch_one(&pool)
            .await
            .expect("count stale offsets");
    assert_eq!(offsets, 0, "retired delivery did not settle into new epoch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_queue_claim_cannot_be_settled_by_stale_worker() {
    let (_db, pool, bus) = bus().await;
    let bus = bus.with_lease(Duration::from_millis(250));
    bus.send_message(command("c1")).await.expect("send command");

    let attempts = Arc::new(AtomicUsize::new(0));
    let first_claimed = Arc::new(Notify::new());
    let second_claimed = Arc::new(Notify::new());
    let allow_first_finish = Arc::new(Notify::new());
    let allow_second_finish = Arc::new(Notify::new());

    let handlers = Arc::new({
        let attempts = attempts.clone();
        let first_claimed = first_claimed.clone();
        let second_claimed = second_claimed.clone();
        let allow_first_finish = allow_first_finish.clone();
        let allow_second_finish = allow_second_finish.clone();
        Handlers::new().on_command(COMMAND_NAME, move |_: &Message| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            let first_claimed = first_claimed.clone();
            let second_claimed = second_claimed.clone();
            let allow_first_finish = allow_first_finish.clone();
            let allow_second_finish = allow_second_finish.clone();
            async move {
                match attempt {
                    0 => {
                        first_claimed.notify_one();
                        allow_first_finish.notified().await;
                        Ok(())
                    }
                    1 => {
                        second_claimed.notify_one();
                        allow_second_finish.notified().await;
                        Err(TransportError::retryable("second claim releases for retry"))
                    }
                    _ => Ok(()),
                }
            }
        })
    });

    let first = tokio::spawn({
        let bus = bus.clone();
        let handlers = handlers.clone();
        async move { bus.listen(handlers, RunOptions::until_idle()).await }
    });
    tokio::time::timeout(Duration::from_secs(2), first_claimed.notified())
        .await
        .expect("first worker claimed the command");

    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = tokio::spawn({
        let bus = bus.clone();
        let handlers = handlers.clone();
        async move { bus.listen(handlers, RunOptions::until_idle()).await }
    });
    tokio::time::timeout(Duration::from_secs(2), second_claimed.notified())
        .await
        .expect("second worker reclaimed the expired lease");

    allow_first_finish.notify_one();
    tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("stale first worker finished")
        .expect("first worker joined")
        .expect("first listener drains");

    allow_second_finish.notify_waiters();
    tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("second worker finished")
        .expect("second worker joined")
        .expect("second listener drains");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "stale ack did not delete the newer claim before it could be retried"
    );
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM bus_queue")
        .fetch_one(&pool)
        .await
        .expect("count queue");
    assert_eq!(remaining, 0, "retried command was eventually acked");
}
