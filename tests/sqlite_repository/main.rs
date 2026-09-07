#![cfg(feature = "sqlite")]
use distributed::read_model::ReadModelWritePlanBuilder;

#[path = "../support/outbox.rs"]
mod outbox_support;

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use outbox_support::find_outbox_by_id;

use distributed::table::TableSchemaRegistry;
use distributed::{
    sourced, Aggregate, AggregateBuilder, CommitBatch, Entity, GetStream, OutboxMessage,
    OutboxMessageStatus, OutboxStore, ReadModel, ReadModelWritePlanCommitExt, RepositoryError,
    RowKey, RowPatch, RowValue, SqliteRepository, StreamIdentity, StreamWrite, TransactionalCommit,
    OUTBOX_MESSAGES_TABLE,
};
use serde::{Deserialize, Serialize};
use sqlx::migrate::{Migration, MigrationType, Migrator};

fn legacy_sqlite_migrator() -> Migrator {
    Migrator::with_migrations(vec![
        Migration::new(
            1,
            "initial".into(),
            MigrationType::Simple,
            sqlx::SqlSafeStr::into_sql_str(include_str!(
                "../../migrations/sqlite/0001_initial.sql"
            )),
            false,
        ),
        Migration::new(
            2,
            "command ledger".into(),
            MigrationType::Simple,
            sqlx::SqlSafeStr::into_sql_str(include_str!(
                "../../migrations/sqlite/0002_command_ledger.sql"
            )),
            false,
        ),
        Migration::new(
            3,
            "projection protocol".into(),
            MigrationType::Simple,
            sqlx::SqlSafeStr::into_sql_str(include_str!(
                "../../migrations/sqlite/0003_projection_protocol.sql"
            )),
            false,
        ),
    ])
}

#[derive(Default)]
struct Counter {
    entity: Entity,
    value: i32,
}

#[sourced(entity, aggregate_type = "sqlite.counter")]
impl Counter {
    #[event("incremented")]
    fn increment(&mut self, id: String, by: i32) {
        self.entity.set_id(&id);
        self.value += by;
    }
}

#[derive(Default)]
struct CounterProjection {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "sqlite.counter_projection")]
impl CounterProjection {
    #[event("touched")]
    fn touch(&mut self, id: String) {
        self.entity.set_id(&id);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("local_relational_counter_views")]
struct RelationalCounterView {
    #[id]
    id: String,
    value: i64,
    #[readmodel(jsonb)]
    counts: HashMap<String, i64>,
}

async fn repository() -> SqliteRepository {
    SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .unwrap()
}

#[tokio::test]
async fn outbox_backlog_stats_order_sqlite_text_timestamps_numerically() {
    let repo = repository().await;
    let mut newer = OutboxMessage::create("backlog-newer", "counter.touched", b"{}".to_vec())
        .expect("newer outbox message should be valid");
    newer.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let mut older = OutboxMessage::create("backlog-older", "counter.touched", b"{}".to_vec())
        .expect("older outbox message should be valid");
    older.created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(2);

    repo.commit_batch(CommitBatch {
        streams: Vec::new(),
        outbox_messages: vec![newer, older],
        read_model_plans: Vec::new(),
        snapshots: Vec::new(),
        inbox_receipts: Vec::new(),
    })
    .await
    .expect("outbox-only batch should commit");

    let stats = repo
        .outbox_store()
        .backlog_stats()
        .await
        .expect("backlog stats should load");

    assert_eq!(stats.pending, 2);
    assert_eq!(
        stats.oldest_created_at,
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2))
    );
}

// Consumer inbox semantics are covered for all backends by the shared
// `persistent_repository_conformance::inbox` scenarios.

async fn bootstrap_relational_counter_table(repo: &SqliteRepository) {
    let mut registry = TableSchemaRegistry::new();
    registry.register::<RelationalCounterView>().unwrap();
    repo.bootstrap_table_schema_for_dev(&registry)
        .await
        .unwrap();
}

fn relational_counter_key(id: &str) -> RowKey {
    RowKey::new([("id", RowValue::String(id.into()))])
}

#[tokio::test]
async fn migration_is_idempotent_and_aggregate_stream_round_trips() {
    let repo = repository().await;
    repo.migrate().await.unwrap();
    let counter_repo = repo.clone().aggregate::<Counter>();

    let mut counter = Counter::default();
    counter.entity.set_correlation_id("corr-1");
    counter.increment("counter-1".into(), 2).unwrap();
    counter.increment("counter-1".into(), 3).unwrap();

    counter_repo.commit(&mut counter).await.unwrap();

    let loaded = counter_repo.get("counter-1").await.unwrap().unwrap();
    assert_eq!(loaded.value, 5);
    assert_eq!(loaded.entity().events().len(), 2);
    assert_eq!(loaded.entity().events()[0].sequence, 1);
    assert_eq!(loaded.entity().events()[1].sequence, 2);
    assert_eq!(loaded.entity().events()[0].correlation_id(), Some("corr-1"));
}

#[tokio::test]
async fn projected_command_ledger_rows_upgrade_to_atomic_without_schema_drift() {
    let repo = SqliteRepository::connect("sqlite::memory:").await.unwrap();
    legacy_sqlite_migrator().run(repo.pool()).await.unwrap();

    sqlx::query(
        r#"
        INSERT INTO command_ledger (
            service_id, principal_partition, command_id, command_name,
            command_contract_hash, input_hash, state, causation_id,
            attempt_number, outcome, completed_at, retention_expires_at
        ) VALUES (?, ?, ?, ?, zeroblob(32), zeroblob(32), 'projected', ?, 1, '{}', 1.0, 2.0)
        "#,
    )
    .bind("service")
    .bind("principal")
    .bind("command")
    .bind("todo.complete")
    .bind("cause")
    .execute(repo.pool())
    .await
    .unwrap();

    repo.migrate().await.unwrap();

    let state: String = sqlx::query_scalar(
        "SELECT state FROM command_ledger WHERE service_id = 'service' AND command_id = 'command'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(state, "atomic");

    let latest_version: i64 = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
        .fetch_one(repo.pool())
        .await
        .unwrap();
    assert_eq!(latest_version, 5);

    let created_at_type: String = sqlx::query_scalar(
        "SELECT typeof(created_at) FROM command_ledger WHERE service_id = 'service'",
    )
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(created_at_type, "real");

    let invalid_json = sqlx::query(
        r#"
        INSERT INTO command_ledger (
            service_id, principal_partition, command_id, command_name,
            command_contract_hash, input_hash, state, causation_id,
            attempt_number, outcome, completed_at, retention_expires_at
        ) VALUES ('service', 'principal', 'bad-json', 'todo.complete',
                  zeroblob(32), zeroblob(32), 'atomic', 'bad-json-cause',
                  1, '{', 1.0, 2.0)
        "#,
    )
    .execute(repo.pool())
    .await;
    assert!(
        invalid_json.is_err(),
        "JSON validity CHECK must survive migration"
    );
}

#[tokio::test]
async fn dev_bootstrap_applies_registered_table_schemas() {
    let repo = SqliteRepository::connect("sqlite::memory:").await.unwrap();
    let mut registry = TableSchemaRegistry::new();
    registry
        .register_schema(distributed::outbox_message_schema().clone())
        .unwrap();

    let artifacts = repo.generate_table_migration_artifacts(&registry).unwrap();
    assert!(artifacts[0]
        .statements
        .iter()
        .any(|statement| statement.contains("CREATE TABLE IF NOT EXISTS \"outbox_messages\"")));

    let bootstrap = repo
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .unwrap();
    assert_eq!(
        bootstrap.bootstrapped_tables,
        vec![OUTBOX_MESSAGES_TABLE.to_string()]
    );

    let row = sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
        .bind(OUTBOX_MESSAGES_TABLE)
        .fetch_one(repo.pool())
        .await
        .unwrap();
    let table_name: String = sqlx::Row::try_get(&row, "name").unwrap();
    assert_eq!(table_name, OUTBOX_MESSAGES_TABLE);
}

#[tokio::test]
async fn aggregate_stream_identity_separates_same_id_across_types() {
    let repo = repository().await;
    let counter_repo = repo.clone().aggregate::<Counter>();
    let projection_repo = repo.clone().aggregate::<CounterProjection>();

    let mut counter = Counter::default();
    counter.increment("shared-id".into(), 7).unwrap();
    let mut projection = CounterProjection::default();
    projection.touch("shared-id".into()).unwrap();

    counter_repo.commit(&mut counter).await.unwrap();
    projection_repo.commit(&mut projection).await.unwrap();

    let loaded_counter = counter_repo.get("shared-id").await.unwrap().unwrap();
    let loaded_projection = projection_repo.get("shared-id").await.unwrap().unwrap();

    assert_eq!(loaded_counter.value, 7);
    assert_eq!(loaded_counter.entity().events().len(), 1);
    assert_eq!(loaded_projection.entity().events().len(), 1);
}

#[tokio::test]
async fn optimistic_conflict_rolls_back_other_stream_and_read_model_plan() {
    let repo = repository().await;
    bootstrap_relational_counter_table(&repo).await;
    let counter_repo = repo.clone().aggregate::<Counter>();

    let mut original = Counter::default();
    original.increment("conflict-1".into(), 1).unwrap();
    counter_repo.commit(&mut original).await.unwrap();

    let mut stale = counter_repo.get("conflict-1").await.unwrap().unwrap();
    let mut winner = counter_repo.get("conflict-1").await.unwrap().unwrap();
    stale.increment("conflict-1".into(), 10).unwrap();
    winner.increment("conflict-1".into(), 20).unwrap();
    counter_repo.commit(&mut winner).await.unwrap();

    let mut other = CounterProjection::default();
    other.touch("should-not-commit".into()).unwrap();

    let view = RelationalCounterView {
        id: "should-not-commit".into(),
        value: 99,
        counts: HashMap::new(),
    };
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models.upsert(&view).unwrap();

    let stale_identity = StreamIdentity::new(Counter::aggregate_type(), "conflict-1").unwrap();
    let other_identity =
        StreamIdentity::new(CounterProjection::aggregate_type(), "should-not-commit").unwrap();
    let err = repo
        .commit_batch(CommitBatch {
            inbox_receipts: Vec::new(),
            streams: vec![
                StreamWrite::new(stale_identity.clone(), stale.entity_mut()),
                StreamWrite::new(other_identity.clone(), other.entity_mut()),
            ],
            outbox_messages: Vec::new(),
            read_model_plans: vec![read_models.into_write_plan().unwrap()],
            snapshots: Vec::new(),
        })
        .await
        .unwrap_err();

    assert!(matches!(err, RepositoryError::ConcurrentWrite { .. }));
    assert!(repo.get_stream(&other_identity).await.unwrap().is_none());
    let remaining: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM "local_relational_counter_views"
        WHERE "id" = ?
        "#,
    )
    .bind("should-not-commit")
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(remaining, 0);
    assert_eq!(stale.entity().committed_version(), 1);
    assert_eq!(stale.entity().new_events().len(), 1);
}

#[tokio::test]
async fn read_model_failure_mid_plan_rolls_back_events_and_outbox() {
    // A single commit carries: an aggregate event, an outbox row, and a
    // read-model plan with TWO mutations. The first mutation is valid; the second
    // violates a real database constraint (a BEFORE-INSERT trigger that ABORTs on
    // a negative value — a genuine engine-level failure mid-plan, not an
    // application pre-check). The whole transaction must roll back: the event,
    // the outbox row, AND the first (already-applied) read-model mutation must
    // ALL be absent afterward.
    let repo = repository().await;
    bootstrap_relational_counter_table(&repo).await;

    // Install a real DB constraint the read-model schema is unaware of: reject
    // any row whose `value` is negative. SQLite triggers are enforced by the
    // engine inside the transaction.
    sqlx::query(
        r#"
        CREATE TRIGGER reject_negative_counter_value
        BEFORE INSERT ON "local_relational_counter_views"
        WHEN NEW."value" < 0
        BEGIN
            SELECT RAISE(ABORT, 'value must be non-negative');
        END;
        "#,
    )
    .execute(repo.pool())
    .await
    .unwrap();

    let good_id = "midplan-good";
    let bad_id = "midplan-bad";
    let mut read_models = ReadModelWritePlanBuilder::new();
    // Mutation 1: valid, applied first within the tx.
    read_models
        .upsert(&RelationalCounterView {
            id: good_id.into(),
            value: 1,
            counts: HashMap::new(),
        })
        .unwrap();
    // Mutation 2: violates the trigger — must abort the whole transaction.
    read_models
        .upsert(&RelationalCounterView {
            id: bad_id.into(),
            value: -1,
            counts: HashMap::new(),
        })
        .unwrap();

    let aggregate_id = "midplan-aggregate";
    let mut projection = CounterProjection::default();
    projection.touch(aggregate_id.into()).unwrap();
    let identity = StreamIdentity::new(CounterProjection::aggregate_type(), aggregate_id).unwrap();

    let outbox_id = "midplan-outbox";
    let outbox_message =
        OutboxMessage::create(outbox_id, "counter.touched", b"{}".to_vec()).unwrap();

    let err = repo
        .commit_batch(CommitBatch {
            inbox_receipts: Vec::new(),
            streams: vec![StreamWrite::new(identity.clone(), projection.entity_mut())],
            outbox_messages: vec![outbox_message],
            read_model_plans: vec![read_models.into_write_plan().unwrap()],
            snapshots: Vec::new(),
        })
        .await
        .expect_err("a mid-plan constraint violation must fail the commit");
    // A read-model CHECK-constraint violation is a deterministic, non-retryable
    // fault, so it surfaces as the permanent `Storage` variant (not `Model`):
    // re-running the identical write cannot change the outcome.
    assert!(
        matches!(
            &err,
            RepositoryError::Storage {
                retryable: false,
                ..
            }
        ),
        "expected a permanent Storage error from the constraint violation, got {err:?}"
    );
    assert!(!err.is_retryable());

    // 1. The aggregate event must be absent.
    assert!(
        repo.get_stream(&identity).await.unwrap().is_none(),
        "the aggregate stream must roll back with the failed read-model plan"
    );

    // 2. The outbox row must be absent.
    assert!(
        find_outbox_by_id(&repo.outbox_store(), outbox_id)
            .await
            .is_none(),
        "the outbox row must roll back with the failed read-model plan"
    );

    // 3. The first (already-applied) read-model mutation must be absent.
    let good_rows: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM "local_relational_counter_views" WHERE "id" = ?"#,
    )
    .bind(good_id)
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(
        good_rows, 0,
        "the first read-model mutation must roll back when a later one fails"
    );
    let bad_rows: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM "local_relational_counter_views" WHERE "id" = ?"#,
    )
    .bind(bad_id)
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(bad_rows, 0, "the violating row must never be persisted");
}

#[tokio::test]
async fn commit_batch_lowers_relational_read_model_plan_into_registered_table() {
    let repo = repository().await;
    bootstrap_relational_counter_table(&repo).await;
    let mut counts = HashMap::new();
    counts.insert("wins".to_string(), 2);
    let view = RelationalCounterView {
        id: "relational-batch-1".into(),
        value: 7,
        counts,
    };
    let mut session = ReadModelWritePlanBuilder::new();
    session.upsert(&view).unwrap();
    let mut projection = CounterProjection::default();
    projection.touch("relational-batch-1".into()).unwrap();
    let identity =
        StreamIdentity::new(CounterProjection::aggregate_type(), "relational-batch-1").unwrap();

    repo.read_models(session)
        .commit(&mut projection)
        .await
        .unwrap();

    assert!(repo.get_stream(&identity).await.unwrap().is_some());
    let row = sqlx::query(
        r#"
        SELECT "id", "value", "counts", "_sourced_version"
        FROM "local_relational_counter_views"
        WHERE "id" = ?
        "#,
    )
    .bind("relational-batch-1")
    .fetch_one(repo.pool())
    .await
    .unwrap();
    let stored_counts: String = sqlx::Row::try_get(&row, "counts").unwrap();
    let stored_counts: serde_json::Value = serde_json::from_str(&stored_counts).unwrap();

    assert_eq!(
        sqlx::Row::try_get::<String, _>(&row, "id").unwrap(),
        "relational-batch-1"
    );
    assert_eq!(sqlx::Row::try_get::<i64, _>(&row, "value").unwrap(), 7);
    assert_eq!(stored_counts["wins"].as_i64(), Some(2));
    assert_eq!(
        sqlx::Row::try_get::<i64, _>(&row, "_sourced_version").unwrap(),
        1
    );
}

#[tokio::test]
async fn read_model_session_patches_and_deletes_relational_rows() {
    let repo = repository().await;
    bootstrap_relational_counter_table(&repo).await;
    let mut counts = HashMap::new();
    counts.insert("wins".to_string(), 2);
    let view = RelationalCounterView {
        id: "relational-session-1".into(),
        value: 7,
        counts,
    };
    let mut setup = ReadModelWritePlanBuilder::new();
    setup.upsert(&view).unwrap();
    setup.commit(&repo).await.unwrap();

    let mut patched_counts = HashMap::new();
    patched_counts.insert("wins".to_string(), 3);
    patched_counts.insert("losses".to_string(), 1);
    let patch = RowPatch::new()
        .set("value", RowValue::I64(11))
        .set_serde("counts", &patched_counts)
        .unwrap();
    let mut patch_session = ReadModelWritePlanBuilder::new();
    patch_session
        .patch::<RelationalCounterView>(relational_counter_key("relational-session-1"), patch)
        .unwrap();
    patch_session.commit(&repo).await.unwrap();

    let row = sqlx::query(
        r#"
        SELECT "value", "counts", "_sourced_version"
        FROM "local_relational_counter_views"
        WHERE "id" = ?
        "#,
    )
    .bind("relational-session-1")
    .fetch_one(repo.pool())
    .await
    .unwrap();
    let stored_counts: String = sqlx::Row::try_get(&row, "counts").unwrap();
    let stored_counts: serde_json::Value = serde_json::from_str(&stored_counts).unwrap();
    assert_eq!(sqlx::Row::try_get::<i64, _>(&row, "value").unwrap(), 11);
    assert_eq!(stored_counts["wins"].as_i64(), Some(3));
    assert_eq!(stored_counts["losses"].as_i64(), Some(1));
    assert_eq!(
        sqlx::Row::try_get::<i64, _>(&row, "_sourced_version").unwrap(),
        2
    );

    let mut delete_session = ReadModelWritePlanBuilder::new();
    delete_session
        .delete::<RelationalCounterView>(relational_counter_key("relational-session-1"))
        .unwrap();
    delete_session.commit(&repo).await.unwrap();

    let remaining: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM "local_relational_counter_views"
        WHERE "id" = ?
        "#,
    )
    .bind("relational-session-1")
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn read_model_session_persists_relational_rows() {
    let repo = repository().await;
    bootstrap_relational_counter_table(&repo).await;
    let view = RelationalCounterView {
        id: "view-1".into(),
        value: 42,
        counts: HashMap::new(),
    };
    let mut session = ReadModelWritePlanBuilder::new();
    session.upsert(&view).unwrap();

    let outcome = session.commit(&repo).await.unwrap();
    let row = sqlx::query(
        r#"
        SELECT "value", "_sourced_version"
        FROM "local_relational_counter_views"
        WHERE "id" = ?
        "#,
    )
    .bind("view-1")
    .fetch_one(repo.pool())
    .await
    .unwrap();

    assert!(outcome.was_applied());
    assert_eq!(sqlx::Row::try_get::<i64, _>(&row, "value").unwrap(), 42);
    assert_eq!(
        sqlx::Row::try_get::<i64, _>(&row, "_sourced_version").unwrap(),
        1
    );
}

// Snapshot identity semantics are covered for all backends by the shared
// `persistent_repository_conformance` scenarios; this main keeps only the
// SQLite-dialect raw-SQL assertions.

#[tokio::test]
async fn unsupported_codec_rows_fail_on_read() {
    let repo = repository().await;
    sqlx::query(
        r#"
        INSERT INTO aggregate_events (
          aggregate_type,
          aggregate_id,
          sequence,
          event_name,
          event_version,
          payload,
          payload_codec,
          payload_codec_version,
          metadata,
          recorded_at
        )
        VALUES (?, ?, 1, 'BadEvent', 1, x'00', 'json', 1, '{}', '0.000000000')
        "#,
    )
    .bind("sqlite.counter")
    .bind("bad-codec")
    .execute(repo.pool())
    .await
    .unwrap();

    let identity = StreamIdentity::new("sqlite.counter", "bad-codec").unwrap();
    let err = repo.get_stream(&identity).await.unwrap_err();

    assert!(
        matches!(
            &err,
            RepositoryError::Storage {
                retryable: false,
                ..
            }
        ),
        "unexpected error: {err}"
    );
    assert!(
        err.to_string().contains("unsupported payload codec"),
        "unexpected error: {err}"
    );
    assert!(!err.is_retryable());
}

#[tokio::test]
async fn outbox_metadata_columns_round_trip_into_message_metadata() {
    let repo = repository().await;
    let message_id = "outbox-column-metadata";
    sqlx::query(
        r#"
        INSERT INTO outbox_messages (
          message_id,
          event_type,
          payload,
          payload_codec,
          payload_codec_version,
          metadata,
          status,
          created_at,
          next_available_at,
          attempts,
          correlation_id,
          causation_id
        )
        VALUES (?, 'OutboxColumns', x'00', 'bytes', 1, '{}', 'pending',
                '0.000000000', '0.000000000', 0, 'corr-column', 'cause-column')
        "#,
    )
    .bind(message_id)
    .execute(repo.pool())
    .await
    .unwrap();

    let stored = repo
        .outbox_store()
        .messages_by_status(OutboxMessageStatus::Pending, usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .find(|message| message.id() == message_id)
        .unwrap();

    assert_eq!(stored.correlation_id(), Some("corr-column"));
    assert_eq!(stored.causation_id(), Some("cause-column"));
}

#[tokio::test]
async fn commit_batch_with_1000_events_round_trips() {
    // SQLite's historical bound-parameter ceiling is 999, so a 1000-event batch
    // must be inserted across multiple multi-row INSERT chunks. Prove the
    // chunking boundary preserves count, order, and payloads.
    let repo = repository().await;
    let identity = StreamIdentity::new("sqlite.counter", "big-batch").unwrap();

    let mut entity = Entity::with_id("big-batch");
    for i in 0..1000 {
        entity
            .digest(format!("bulk_recorded_{i}"), &(i as u64))
            .unwrap();
    }
    repo.commit_batch(CommitBatch::new(vec![StreamWrite::new(
        identity.clone(),
        &mut entity,
    )]))
    .await
    .expect("a 1000-event batch should commit across bind-param chunks");

    let loaded = repo
        .get_stream(&identity)
        .await
        .unwrap()
        .expect("stream should exist");
    assert_eq!(loaded.committed_version(), 1000);
    assert_eq!(loaded.events().len(), 1000);
    let sequences: Vec<u64> = loaded.events().iter().map(|event| event.sequence).collect();
    assert_eq!(
        sequences,
        (1..=1000).collect::<Vec<u64>>(),
        "sequences must be contiguous across chunk boundaries"
    );
    // Spot-check payloads at the chunk seams and ends.
    for index in [0usize, 98, 99, 100, 998, 999] {
        let event = &loaded.events()[index];
        assert_eq!(event.event_name, format!("bulk_recorded_{index}"));
        let value: u64 = bitcode::deserialize(&event.payload).expect("payload decodes");
        assert_eq!(value, index as u64);
    }
}
