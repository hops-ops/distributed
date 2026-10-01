//! Postgres transport adapter integration tests.
//!
//! Exercises `OutboxSource<PostgresOutboxStore>` — the Postgres "starter"
//! durable transport — against a real Postgres: claim (`FOR UPDATE SKIP LOCKED`
//! with a lease), dispatch, and settle by row status. Skips when `DATABASE_URL`
//! is unset.
#![cfg(feature = "postgres")]

#[path = "../support/postgres.rs"]
mod postgres;

// Shared broker-test helpers (recording_for, bus scenarios).
#[path = "../transport_conformance/mod.rs"]
mod conformance;
use conformance::{outbox_support, recording_for};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    run_source, Bus, BusConsumer, Handlers, MessageRouter, MessageSource, OrderedDelivery,
    PostgresBus, ReceivedMessage, RunOptions, SubscriptionPlan, TransportError,
};
use distributed::microsvc::{Context, Message, MessageKind, Routes, Service};
use distributed::projection_protocol::ProjectionEpoch;
use distributed::OutboxSource;
use distributed::{
    CommitBatch, OutboxMessage, OutboxMessageStatus, PostgresOutboxStore, PostgresRepository,
    TransactionalCommit, CAUSATION_ID, TRACEPARENT,
};
use serde_json::json;
use tokio::sync::Notify;

const SKIP: &str = "skipping postgres transport test";

async fn enqueue(repo: &PostgresRepository, id: &str, name: &str) {
    let mut batch = CommitBatch::empty();
    batch
        .outbox_messages
        .push(OutboxMessage::create(id, name, b"{}".to_vec()).unwrap());
    repo.commit_batch(batch)
        .await
        .expect("outbox row should commit");
}

async fn status(store: &PostgresOutboxStore, id: &str) -> Option<OutboxMessageStatus> {
    outbox_support::outbox_status_by_id(store, id).await
}

fn recording_service(handled: Arc<Mutex<Vec<String>>>) -> Arc<Service> {
    Arc::new(
        Service::new().routes(
            Routes::new()
                .with_dependencies(())
                .event("order.initialized")
                .handle(move |ctx: &Context<()>| {
                    handled
                        .lock()
                        .unwrap()
                        .push(ctx.message().id().unwrap_or_default().to_string());
                    async move { Ok(json!({})) }
                }),
        ),
    )
}

#[tokio::test]
async fn outbox_source_run_drains_and_completes() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("pg_tx_drain", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    enqueue(&repo, "m1", "order.initialized").await;
    enqueue(&repo, "m2", "order.initialized").await;
    let store = Arc::new(repo.outbox_store());

    let handled = Arc::new(Mutex::new(Vec::new()));
    let service = recording_service(handled.clone());
    run_source(
        service,
        OutboxSource::new(store.clone(), "pg-drain", 3),
        RunOptions::idempotent(),
    )
    .await
    .unwrap();

    let mut ids = handled.lock().unwrap().clone();
    ids.sort();
    assert_eq!(ids, vec!["m1".to_string(), "m2".to_string()]);
    assert_eq!(status(&store, "m1").await, None);
    assert_eq!(status(&store, "m2").await, None);
}

#[tokio::test]
async fn concurrent_sources_process_each_row_once() {
    let Some(schema) =
        postgres::PostgresTestSchema::create_from_env("pg_tx_concurrent", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let ids: Vec<String> = (0..20).map(|i| format!("c{i}")).collect();
    for id in &ids {
        enqueue(&repo, id, "order.initialized").await;
    }
    let store = Arc::new(repo.outbox_store());

    let handled = Arc::new(Mutex::new(Vec::new()));
    let run = |worker: &'static str| {
        run_source(
            recording_service(handled.clone()),
            OutboxSource::new(store.clone(), worker, 3),
            RunOptions::idempotent(),
        )
    };
    // Two competing consumers drain concurrently; SKIP LOCKED guarantees each
    // row is claimed (and handled) exactly once.
    let (a, b) = tokio::join!(run("worker-a"), run("worker-b"));
    a.unwrap();
    b.unwrap();

    let mut got = handled.lock().unwrap().clone();
    got.sort();
    let unique = {
        let mut u = got.clone();
        u.dedup();
        u
    };
    assert_eq!(got, unique, "no row handled more than once");
    assert_eq!(unique.len(), ids.len(), "every row handled");
}

#[tokio::test]
async fn nack_releases_then_a_later_claim_completes() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("pg_tx_retry", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    enqueue(&repo, "m1", "order.initialized").await;
    let store = Arc::new(repo.outbox_store());

    // First claim, nack -> released to pending (attempts incremented).
    let mut source = OutboxSource::new(store.clone(), "pg-retry", 5);
    let received = source.recv().await.unwrap().expect("a claimable row");
    received.nack("transient").await.unwrap();
    assert_eq!(
        status(&store, "m1").await,
        Some(OutboxMessageStatus::Pending)
    );

    // A later claim completes it.
    let mut source2 = OutboxSource::new(store.clone(), "pg-retry-2", 5);
    let received2 = source2.recv().await.unwrap().expect("a re-claimable row");
    received2.ack().await.unwrap();
    assert_eq!(status(&store, "m1").await, None);
}

#[tokio::test]
async fn dead_letter_marks_row_failed() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("pg_tx_dlq", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    enqueue(&repo, "m1", "order.initialized").await;
    let store = Arc::new(repo.outbox_store());

    let mut source = OutboxSource::new(store.clone(), "pg-dlq", 3);
    let received = source.recv().await.unwrap().expect("a claimable row");
    received.dead_letter("poison").await.unwrap();
    assert_eq!(
        status(&store, "m1").await,
        Some(OutboxMessageStatus::Failed)
    );
}

// ---- PostgresBus: send/listen (work queue) + publish/subscribe (log+offsets) ----

/// Build a `PostgresBus` over `pool` for `group` (empty `group` = no group),
/// with the bus tables ensured.
async fn pg_bus(pool: &sqlx::PgPool, group: &str) -> PostgresBus {
    let bus = PostgresBus::new(pool.clone());
    let bus = if group.is_empty() {
        bus
    } else {
        bus.group(group)
    };
    bus.ensure_tables().await.expect("ensure tables");
    bus
}

#[derive(Default)]
struct OrderedEvidenceRecorder {
    observed: Mutex<Vec<(String, u64)>>,
}

impl MessageRouter for OrderedEvidenceRecorder {
    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        kind == MessageKind::Event && name == "order.initialized"
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        SubscriptionPlan {
            commands: Vec::new(),
            events: vec!["order.initialized".to_string()],
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
        kind == MessageKind::Event && name == "order.initialized"
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        SubscriptionPlan {
            commands: Vec::new(),
            events: vec!["order.initialized".to_string()],
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

/// `send` + `listen`: the work queue is claimed `FOR UPDATE SKIP LOCKED`, so two
/// replicas sharing a `group` compete — each command handled exactly once.
#[tokio::test]
async fn bus_send_listen_is_point_to_point_across_a_group() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_pp", SKIP).await else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    conformance::bus_send_listen_is_point_to_point_across_a_group(|group| pg_bus(&pool, group))
        .await;
}

/// `publish` + `subscribe`: each `group` has its own log offset, so every group
/// reads the full log — fan-out.
#[tokio::test]
async fn bus_publish_subscribe_fans_out_across_groups() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_fan", SKIP).await else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    conformance::bus_publish_subscribe_fans_out_across_groups(|group| pg_bus(&pool, group)).await;
}

#[tokio::test]
async fn bus_subscribe_uses_named_service_as_consumer_group() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_named_group", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    conformance::bus_subscribe_uses_named_service_as_consumer_group(|| pg_bus(&pool, "")).await;
}

// ---- corrupt-row handling: a row that fails to decode must NOT vanish ----

/// Recreate `bus_queue` without the schema's NOT NULL / CHECK guards so tests
/// can simulate corruption (a migration mishap, a manual edit, a driver/type
/// mismatch) that a hardened schema would otherwise reject at write time.
async fn recreate_permissive_queue_table(pool: &sqlx::PgPool) {
    sqlx::query("DROP TABLE IF EXISTS bus_queue")
        .execute(pool)
        .await
        .expect("drop bus_queue");
    sqlx::query(
        r#"
        CREATE TABLE bus_queue (
            seq          BIGSERIAL PRIMARY KEY,
            claim_token  TEXT,
            name         TEXT,
            message_id   TEXT,
            kind         TEXT NOT NULL,
            payload      BYTEA NOT NULL,
            content_type TEXT NOT NULL DEFAULT 'application/json',
            metadata     TEXT NOT NULL DEFAULT '[]',
            available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            locked_until TIMESTAMPTZ,
            attempts     INTEGER NOT NULL DEFAULT 0
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("create permissive bus_queue");
    sqlx::query(
        "CREATE INDEX bus_queue_claim_idx ON bus_queue (name, available_at, locked_until, seq)",
    )
    .execute(pool)
    .await
    .expect("create queue index");
}

/// Recreate `bus_log` without the schema's NOT NULL / CHECK guards (see
/// [`recreate_permissive_queue_table`]).
async fn recreate_permissive_log_table(pool: &sqlx::PgPool) {
    sqlx::query("DROP TABLE IF EXISTS bus_log")
        .execute(pool)
        .await
        .expect("drop bus_log");
    sqlx::query(
        r#"
        CREATE TABLE bus_log (
            seq          BIGSERIAL PRIMARY KEY,
            name         TEXT,
            message_id   TEXT,
            kind         TEXT NOT NULL,
            payload      BYTEA NOT NULL,
            content_type TEXT DEFAULT 'application/json',
            metadata     TEXT NOT NULL DEFAULT '[]',
            appended_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("create permissive bus_log");
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

async fn corrupt_latest_queue_name(pool: &sqlx::PgPool) {
    sqlx::query("UPDATE bus_queue SET name = NULL WHERE seq = (SELECT max(seq) FROM bus_queue)")
        .execute(pool)
        .await
        .expect("null out queue name");
}

async fn corrupt_latest_queue_kind(pool: &sqlx::PgPool) {
    sqlx::query("UPDATE bus_queue SET kind = 'bogus' WHERE seq = (SELECT max(seq) FROM bus_queue)")
        .execute(pool)
        .await
        .expect("corrupt queue kind");
}

async fn corrupt_latest_log_name(pool: &sqlx::PgPool) {
    sqlx::query("UPDATE bus_log SET name = NULL WHERE seq = (SELECT max(seq) FROM bus_log)")
        .execute(pool)
        .await
        .expect("null out log name");
}

async fn corrupt_latest_log_kind(pool: &sqlx::PgPool) {
    sqlx::query("UPDATE bus_log SET kind = 'bogus' WHERE seq = (SELECT max(seq) FROM bus_log)")
        .execute(pool)
        .await
        .expect("corrupt log kind");
}

async fn corrupt_latest_log_metadata(pool: &sqlx::PgPool) {
    sqlx::query(
        "UPDATE bus_log SET metadata = 'not-json' WHERE seq = (SELECT max(seq) FROM bus_log)",
    )
    .execute(pool)
    .await
    .expect("corrupt log metadata");
}

async fn corrupt_latest_log_content_type(pool: &sqlx::PgPool) {
    sqlx::query(
        "UPDATE bus_log SET content_type = NULL WHERE seq = (SELECT max(seq) FROM bus_log)",
    )
    .execute(pool)
    .await
    .expect("corrupt log content type");
}

/// The hardened schema rejects unsupported message kinds at write time, so a
/// `kind` CHECK violation never reaches a consumer as a corrupt row.
#[tokio::test]
async fn bus_schema_rejects_unsupported_message_kind() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_kind_check", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone());
    bus.ensure_tables().await.expect("ensure tables");

    let queue_err = sqlx::query("INSERT INTO bus_queue (name, kind, payload) VALUES ($1, $2, $3)")
        .bind("order.initialize")
        .bind("bogus")
        .bind(b"{}".to_vec())
        .execute(&pool)
        .await
        .expect_err("queue kind check rejects unsupported message kind");
    assert!(
        queue_err.to_string().contains("check"),
        "unexpected queue kind error: {queue_err}"
    );

    let log_err = sqlx::query("INSERT INTO bus_log (name, kind, payload) VALUES ($1, $2, $3)")
        .bind("order.initialized")
        .bind("bogus")
        .bind(b"{}".to_vec())
        .execute(&pool)
        .await
        .expect_err("log kind check rejects unsupported message kind");
    assert!(
        log_err.to_string().contains("check"),
        "unexpected log kind error: {log_err}"
    );
}

/// A corrupt `bus_queue` row is routed through the failure policy (dead-letter by
/// default → the row is deleted) rather than being decoded into an empty-named
/// message and silently ack-and-ignored. The valid row beside it is still
/// handled, and the run drains to completion.
#[tokio::test]
async fn bus_listen_dead_letters_corrupt_queue_row_not_silently() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_corrupt_q", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone()).group("orders");
    bus.ensure_tables().await.expect("ensure tables");
    recreate_permissive_queue_table(&pool).await;

    // Poison rows (nulled name, bogus kind) and a healthy row.
    bus.send_message(
        Message::new("order.initialize", MessageKind::Command, b"{}".to_vec()).with_id("poison"),
    )
    .await
    .expect("send poison");
    corrupt_latest_queue_name(&pool).await;
    bus.send_message(
        Message::new("order.initialize", MessageKind::Command, b"{}".to_vec())
            .with_id("poison-kind"),
    )
    .await
    .expect("send poison kind");
    corrupt_latest_queue_kind(&pool).await;
    bus.send_message(
        Message::new("order.initialize", MessageKind::Command, b"{}".to_vec()).with_id("ok"),
    )
    .await
    .expect("send ok");

    let rec = Arc::new(Mutex::new(Vec::new()));
    bus.listen(
        recording_for("order.initialize", MessageKind::Command, rec.clone()),
        RunOptions::idempotent(),
    )
    .await
    .expect("listen drains without surfacing the corrupt row as a fatal error");

    // The healthy command was handled; the corrupt row was never dispatched as
    // an empty-named message.
    let handled = rec.lock().unwrap().clone();
    assert_eq!(
        handled,
        vec!["ok".to_string()],
        "only the valid row handled"
    );

    // The corrupt row did not vanish into ack-and-ignore *and* did not get stuck
    // redelivering forever: under the default dead-letter policy it leaves the
    // queue. The queue is fully drained.
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM bus_queue")
        .fetch_one(&pool)
        .await
        .expect("count queue");
    assert_eq!(
        remaining, 0,
        "corrupt row routed through policy, not redelivered forever"
    );
}

/// A corrupt `bus_log` row is routed through the failure policy (dead-letter by
/// default → the consumer offset advances past it) rather than silently
/// ack-and-ignored. The consumer makes progress to the healthy entry after it.
#[tokio::test]
async fn bus_subscribe_dead_letters_corrupt_log_row_not_silently() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_corrupt_l", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let producer = PostgresBus::new(pool.clone());
    producer.ensure_tables().await.expect("ensure tables");
    recreate_permissive_log_table(&pool).await;

    // Layout (by seq): poison, ok, then trailing poison entries. The trailing
    // poisons are the highest seqs, so a consumer that *silently skips* corrupt
    // entries (matching only by name) would stop its offset at the healthy `ok`
    // entry and never reach the last seq — the offset would fall short of max_seq
    // and this test would fail. Reaching max_seq proves the corrupt entries were
    // settled through the policy (offset advanced past them), not skipped because
    // their name no longer matched.
    producer
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec()).with_id("poison"),
        )
        .await
        .expect("publish leading poison");
    corrupt_latest_log_name(&pool).await;
    producer
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec()).with_id("ok"),
        )
        .await
        .expect("publish ok");
    producer
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                .with_id("poison-tail"),
        )
        .await
        .expect("publish trailing poison");
    corrupt_latest_log_name(&pool).await;
    producer
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                .with_id("poison-kind"),
        )
        .await
        .expect("publish corrupt kind");
    corrupt_latest_log_kind(&pool).await;
    producer
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                .with_id("poison-metadata"),
        )
        .await
        .expect("publish corrupt metadata");
    corrupt_latest_log_metadata(&pool).await;
    producer
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                .with_id("poison-content-type"),
        )
        .await
        .expect("publish corrupt content type");
    corrupt_latest_log_content_type(&pool).await;

    let rec = Arc::new(Mutex::new(Vec::new()));
    PostgresBus::new(pool.clone())
        .group("projections")
        .subscribe(
            recording_for("order.initialized", MessageKind::Event, rec.clone()),
            RunOptions::idempotent(),
        )
        .await
        .expect("subscribe drains past the corrupt entries");

    // The healthy event between the poison entries was handled — the consumer did
    // not get stuck on a corrupt row, and no corrupt row was dispatched as an
    // empty-named message.
    let handled = rec.lock().unwrap().clone();
    assert_eq!(
        handled,
        vec!["ok".to_string()],
        "only the valid event handled"
    );

    // The offset advanced past every entry, including the trailing corrupt one
    // (dead-letter advances the log offset). If the corrupt entries were skipped
    // silently by name, the offset would stop at the `ok` entry, short of max_seq.
    let offset: Option<i64> =
        sqlx::query_scalar("SELECT last_seq FROM bus_offset WHERE consumer = 'projections'")
            .fetch_optional(&pool)
            .await
            .expect("read offset");
    let max_seq: i64 = sqlx::query_scalar("SELECT max(seq) FROM bus_log")
        .fetch_one(&pool)
        .await
        .expect("max seq");
    assert_eq!(
        offset,
        Some(max_seq),
        "offset advanced past the trailing corrupt entry, not stuck or skipped-silently"
    );
}

#[tokio::test]
async fn stable_log_retry_keeps_the_original_cursor_and_rejects_conflicts() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_log_dedupe", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone());
    bus.ensure_tables().await.expect("ensure tables");

    let first = Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
        .with_id("stable-event")
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
    let retry = Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
        .with_id("stable-event")
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
        "order.initialized",
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

    let causation_conflict = Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
        .with_id("stable-event")
        .with_metadata(CAUSATION_ID, "different-command");
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
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_log_race", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone());
    bus.ensure_tables().await.expect("ensure tables");
    let first = Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
        .with_id("concurrent-stable")
        .with_metadata(CAUSATION_ID, "command-concurrent")
        .with_metadata(TRACEPARENT, "attempt-a");
    let retry = Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
        .with_id("concurrent-stable")
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
async fn legacy_equivalent_stable_id_duplicates_retain_the_minimum_cursor() {
    let Some(schema) =
        postgres::PostgresTestSchema::create_from_env("bus_log_legacy_ok", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone());
    bus.ensure_tables().await.expect("ensure tables");
    sqlx::query("DROP INDEX bus_log_message_id_unique_idx")
        .execute(&pool)
        .await
        .expect("simulate legacy schema without uniqueness");
    let first_metadata = serde_json::to_string(&vec![
        (CAUSATION_ID, "legacy-command"),
        (TRACEPARENT, "legacy-first"),
    ])
    .unwrap();
    let retry_metadata = serde_json::to_string(&vec![
        (CAUSATION_ID, "legacy-command"),
        (TRACEPARENT, "legacy-retry"),
    ])
    .unwrap();
    for metadata in [&first_metadata, &retry_metadata] {
        sqlx::query(
            "INSERT INTO bus_log \
                 (name, message_id, kind, payload, content_type, metadata) \
             VALUES ('order.initialized', 'legacy-stable', 'event', $1, \
                     'application/json', $2)",
        )
        .bind(b"{}".as_slice())
        .bind(metadata)
        .execute(&pool)
        .await
        .expect("seed equivalent legacy duplicate");
    }

    bus.ensure_tables()
        .await
        .expect("equivalent duplicates are migrated safely");
    let retained: (i64, String, i64) = sqlx::query_as(
        "SELECT MIN(seq), MIN(metadata), COUNT(*) \
         FROM bus_log WHERE message_id = 'legacy-stable'",
    )
    .fetch_one(&pool)
    .await
    .expect("read migrated legacy row");
    assert_eq!(retained.0, 1, "the minimum legacy cursor is authoritative");
    assert_eq!(retained.2, 1);
    assert!(
        retained.1.contains("legacy-first"),
        "the first committed envelope is retained"
    );
    let unique_index: bool =
        sqlx::query_scalar("SELECT to_regclass('bus_log_message_id_unique_idx') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("inspect stable ID index");
    assert!(unique_index);
}

#[tokio::test]
async fn legacy_conflicting_stable_id_duplicates_fail_preflight_without_mutation() {
    let Some(schema) =
        postgres::PostgresTestSchema::create_from_env("bus_log_legacy_bad", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone());
    bus.ensure_tables().await.expect("ensure tables");
    sqlx::query("DROP INDEX bus_log_message_id_unique_idx")
        .execute(&pool)
        .await
        .expect("simulate legacy schema without uniqueness");
    for payload in [br#"{}"#.as_slice(), br#"{"different":true}"#.as_slice()] {
        sqlx::query(
            "INSERT INTO bus_log \
                 (name, message_id, kind, payload, content_type, metadata) \
             VALUES ('order.initialized', 'legacy-conflict', 'event', $1, \
                     'application/json', '[]')",
        )
        .bind(payload)
        .execute(&pool)
        .await
        .expect("seed conflicting legacy duplicate");
    }

    let error = bus
        .ensure_tables()
        .await
        .expect_err("conflicting legacy duplicates fail closed");
    assert!(error.is_permanent());
    let rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM bus_log WHERE message_id = 'legacy-conflict'")
            .fetch_one(&pool)
            .await
            .expect("count untouched conflicting rows");
    let unique_index: bool =
        sqlx::query_scalar("SELECT to_regclass('bus_log_message_id_unique_idx') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("inspect absent stable ID index");
    assert_eq!(rows, 2, "failed preflight rolls back deduplication");
    assert!(!unique_index, "no unsafe uniqueness fence was installed");
}

#[tokio::test]
async fn nonempty_log_identity_adoption_must_be_explicit_and_retires_offsets() {
    let Some(schema) =
        postgres::PostgresTestSchema::create_from_env("bus_identity_loss", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone());
    bus.ensure_tables().await.expect("ensure tables");
    bus.publish_message(
        Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
            .with_id("identity-loss"),
    )
    .await
    .expect("append before identity loss");
    let retired_epoch: String =
        sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .expect("read retired epoch");
    sqlx::query(
        "INSERT INTO bus_offset (consumer, source_epoch, last_seq) \
         VALUES ('stale-consumer', $1, 1)",
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
    PostgresBus::new(pool.clone())
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
    let Some(schema) =
        postgres::PostgresTestSchema::create_from_env("bus_epoch_override", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let initial_epoch = ProjectionEpoch::new("operator-generation-1").unwrap();
    let bus = PostgresBus::new(pool.clone())
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
    bus.publish_message(
        Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
            .with_id("delivered-epoch"),
    )
    .await
    .expect("append under configured epoch");
    let recorder = Arc::new(OrderedEvidenceRecorder::default());
    bus.subscribe(recorder.clone(), RunOptions::idempotent())
        .await
        .expect("deliver ordered row");
    assert_eq!(
        *recorder.observed.lock().unwrap(),
        vec![(persisted.clone(), 1)],
        "delivery exposes the durable row's epoch and original SQL position"
    );

    let mismatched = PostgresBus::new(pool.clone())
        .with_source_epoch(ProjectionEpoch::new("operator-generation-2").unwrap());
    let error = mismatched
        .ensure_tables()
        .await
        .expect_err("a builder override cannot relabel existing positions");
    assert!(error.is_permanent());
    let error = mismatched
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                .with_id("must-not-append"),
        )
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
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_log_epoch", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone());
    bus.ensure_tables().await.expect("ensure tables");
    bus.publish_message(
        Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
            .with_id("before-reset"),
    )
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
         VALUES ('projector', $1, 1)",
    )
    .bind(&before_epoch)
    .execute(&pool)
    .await
    .expect("seed old generation offset");

    sqlx::query("DROP TABLE bus_log")
        .execute(&pool)
        .await
        .expect("simulate independently rebuilt log");
    let error = PostgresBus::new(pool.clone())
        .ensure_tables()
        .await
        .expect_err("ordinary startup cannot authorize cursor-domain reuse");
    assert!(error.is_permanent());
    let error = bus
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                .with_id("must-not-rotate"),
        )
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

    let rebuilt = PostgresBus::new(pool.clone());
    rebuilt
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                .with_id("after-reset"),
        )
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
    let Some(schema) =
        postgres::PostgresTestSchema::create_from_env("bus_live_rotation", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let producer = PostgresBus::new(pool.clone());
    producer.ensure_tables().await.expect("ensure tables");
    for index in 0..17 {
        producer
            .publish_message(
                Message::new("order.initialized", MessageKind::Event, b"{}".to_vec())
                    .with_id(format!("old-{index}")),
            )
            .await
            .expect("append old-generation event");
    }
    let router = Arc::new(RotationRouter::default());
    let subscriber = PostgresBus::new(pool.clone()).group("rotation-observer");
    let running = tokio::spawn({
        let router = router.clone();
        async move { subscriber.subscribe(router, RunOptions::idempotent()).await }
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
    let replacement = PostgresBus::new(pool.clone());
    replacement
        .publish_message(
            Message::new("order.initialized", MessageKind::Event, b"{}".to_vec()).with_id("new-0"),
        )
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

/// Claim-token fencing: after a lease expires and the command is reclaimed by a
/// second worker (new claim token), the stale first worker's ack must not settle
/// the row out from under the newer claim. Mirrors the sqlite_transport test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_queue_claim_cannot_be_settled_by_stale_worker() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env("bus_stale", SKIP).await
    else {
        return;
    };
    let repo = schema.repository().await;
    let pool = repo.pool().clone();
    let bus = PostgresBus::new(pool.clone())
        .group("orders")
        .with_lease(Duration::from_millis(250));
    bus.ensure_tables().await.expect("ensure tables");
    bus.send_message(
        Message::new("order.initialize", MessageKind::Command, b"{}".to_vec()).with_id("c1"),
    )
    .await
    .expect("send command");

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
        Handlers::new().on_command("order.initialize", move |_: &distributed::bus::Message| {
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
        async move { bus.listen(handlers, RunOptions::idempotent()).await }
    });
    tokio::time::timeout(Duration::from_secs(2), first_claimed.notified())
        .await
        .expect("first worker claimed the command");

    tokio::time::sleep(Duration::from_millis(300)).await;
    let second = tokio::spawn({
        let bus = bus.clone();
        let handlers = handlers.clone();
        async move { bus.listen(handlers, RunOptions::idempotent()).await }
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
