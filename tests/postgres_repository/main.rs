#![cfg(feature = "postgres")]
use distributed::read_model::ReadModelWritePlanBuilder;

#[path = "../support/ids.rs"]
mod ids;
#[path = "../support/outbox.rs"]
mod outbox_support;
#[path = "../support/postgres.rs"]
mod postgres;

use std::collections::HashMap;

use ids::unique_id;
use outbox_support::find_outbox_by_id;

use distributed::{
    sourced, Aggregate, AggregateBuilder, CommitBatch, Entity, GetStream, OutboxMessage,
    OutboxMessageStatus, OutboxStore, PostgresRepository, ReadModel, ReadModelWritePlanCommitExt,
    RepositoryError, RowKey, RowPatch, RowValue, StreamIdentity, StreamWrite, TableSchemaRegistry,
    TransactionalCommit,
};
use serde::{Deserialize, Serialize};
use sqlx::migrate::{Migration, MigrationType, Migrator};

fn legacy_postgres_migrator() -> Migrator {
    Migrator::with_migrations(vec![
        Migration::new(
            1,
            "initial".into(),
            MigrationType::Simple,
            sqlx::SqlSafeStr::into_sql_str(include_str!(
                "../../migrations/postgres/0001_initial.sql"
            )),
            false,
        ),
        Migration::new(
            2,
            "command ledger".into(),
            MigrationType::Simple,
            sqlx::SqlSafeStr::into_sql_str(include_str!(
                "../../migrations/postgres/0002_command_ledger.sql"
            )),
            false,
        ),
        Migration::new(
            3,
            "projection protocol".into(),
            MigrationType::Simple,
            sqlx::SqlSafeStr::into_sql_str(include_str!(
                "../../migrations/postgres/0003_projection_protocol.sql"
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

#[sourced(entity, aggregate_type = "postgres.counter")]
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

#[sourced(entity, aggregate_type = "postgres.counter_projection")]
impl CounterProjection {
    #[event("touched")]
    fn touch(&mut self, id: String) {
        self.entity.set_id(&id);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("postgres_relational_counter_views")]
struct RelationalCounterView {
    #[id]
    id: String,
    value: i64,
    #[readmodel(jsonb)]
    counts: HashMap<String, i64>,
}

async fn repository() -> Option<(postgres::PostgresTestSchema, PostgresRepository)> {
    let schema = postgres::PostgresTestSchema::create_from_env(
        "postgres_repo",
        "skipping Postgres integration test",
    )
    .await?;
    let repo = schema.repository().await;
    Some((schema, repo))
}

// Consumer inbox semantics are covered for all backends by the shared
// `persistent_repository_conformance::inbox` scenarios.

async fn bootstrap_relational_counter_table(repo: &PostgresRepository) {
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
async fn migration_is_idempotent_and_uses_postgres_column_types() {
    let Some((schema, repo)) = repository().await else {
        return;
    };
    repo.migrate().await.unwrap();

    let rows = sqlx::query(
        r#"
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = 'aggregate_events'
          AND column_name IN ('payload', 'metadata', 'recorded_at')
        "#,
    )
    .bind(schema.schema_name())
    .fetch_all(repo.pool())
    .await
    .unwrap();

    let mut columns = rows
        .into_iter()
        .map(|row| {
            (
                sqlx::Row::try_get::<String, _>(&row, "column_name").unwrap(),
                sqlx::Row::try_get::<String, _>(&row, "udt_name").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    columns.sort();

    assert_eq!(
        columns,
        vec![
            ("metadata".into(), "jsonb".into()),
            ("payload".into(), "bytea".into()),
            ("recorded_at".into(), "timestamptz".into()),
        ]
    );

    let document_table: Option<String> = sqlx::query_scalar(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_name = 'transactional_read_models'
        "#,
    )
    .bind(schema.schema_name())
    .fetch_optional(repo.pool())
    .await
    .unwrap();
    assert!(document_table.is_none());

    let processed_messages_table: Option<String> = sqlx::query_scalar(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
          AND table_name = 'read_model_processed_messages'
        "#,
    )
    .bind(schema.schema_name())
    .fetch_optional(repo.pool())
    .await
    .unwrap();
    assert!(processed_messages_table.is_none());
}

#[tokio::test]
async fn projected_command_ledger_rows_upgrade_to_atomic_and_preserve_checks() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env(
        "postgres_ledger_upgrade",
        "skipping Postgres command-ledger upgrade test",
    )
    .await
    else {
        return;
    };
    let repo = schema.repository_unmigrated().await;
    legacy_postgres_migrator().run(repo.pool()).await.unwrap();

    sqlx::query(
        r#"
        INSERT INTO command_ledger (
            service_id, principal_partition, command_id, command_name,
            command_contract_hash, input_hash, state, causation_id,
            attempt_number, outcome, completed_at, retention_expires_at
        ) VALUES ($1, $2, $3, $4,
                  decode(repeat('00', 32), 'hex'), decode(repeat('00', 32), 'hex'),
                  'projected', $5, 1, '{}'::jsonb, now(), now() + interval '1 day')
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
    assert_eq!(latest_version, 6);

    let invalid_service = sqlx::query(
        r#"
        INSERT INTO command_ledger (
            service_id, principal_partition, command_id, command_name,
            command_contract_hash, input_hash, state, causation_id,
            attempt_number, outcome, completed_at, retention_expires_at
        ) VALUES ('', 'principal', 'invalid-service', 'todo.complete',
                  decode(repeat('00', 32), 'hex'), decode(repeat('00', 32), 'hex'),
                  'atomic', 'invalid-service-cause', 1, '{}'::jsonb, now(), now() + interval '1 day')
        "#,
    )
    .execute(repo.pool())
    .await;
    assert!(
        invalid_service.is_err(),
        "non-empty service CHECK must survive migration"
    );
}

#[tokio::test]
async fn aggregate_stream_round_trips_with_metadata() {
    let Some((_schema, repo)) = repository().await else {
        return;
    };
    let counter_repo = repo.clone().aggregate::<Counter>();
    let id = unique_id("counter");

    let mut counter = Counter::default();
    counter.entity.set_correlation_id("corr-postgres");
    counter.increment(id.clone(), 2).unwrap();
    counter.increment(id.clone(), 3).unwrap();

    counter_repo.commit(&mut counter).await.unwrap();

    let loaded = counter_repo.get(&id).await.unwrap().unwrap();
    assert_eq!(loaded.value, 5);
    assert_eq!(loaded.entity().events().len(), 2);
    assert_eq!(loaded.entity().events()[0].sequence, 1);
    assert_eq!(loaded.entity().events()[1].sequence, 2);
    assert_eq!(
        loaded.entity().events()[0].correlation_id(),
        Some("corr-postgres")
    );
}

// Optimistic-conflict rollback, duplicate-stream rejection, and snapshot
// identity semantics are covered for all backends by the shared
// `persistent_repository_conformance` scenarios; this main keeps only the
// Postgres-dialect raw-SQL assertions.

#[tokio::test]
async fn read_model_failure_mid_plan_rolls_back_events_and_outbox() {
    // Postgres variant of the SQLite mid-plan rollback test: a commit carrying an
    // aggregate event, an outbox row, and a two-mutation read-model plan whose
    // second mutation violates a real CHECK constraint must roll the WHOLE
    // transaction back — event, outbox row, and the first (already-applied)
    // read-model mutation all absent. Skips locally without DATABASE_URL.
    let Some((_schema, repo)) = repository().await else {
        return;
    };
    bootstrap_relational_counter_table(&repo).await;

    // A real engine-level constraint the read-model schema is unaware of.
    sqlx::query(
        r#"ALTER TABLE "postgres_relational_counter_views"
           ADD CONSTRAINT reject_negative_value CHECK ("value" >= 0)"#,
    )
    .execute(repo.pool())
    .await
    .unwrap();

    let good_id = unique_id("midplan-good");
    let bad_id = unique_id("midplan-bad");
    let mut read_models = ReadModelWritePlanBuilder::new();
    read_models
        .upsert(&RelationalCounterView {
            id: good_id.clone(),
            value: 1,
            counts: HashMap::new(),
        })
        .unwrap();
    read_models
        .upsert(&RelationalCounterView {
            id: bad_id.clone(),
            value: -1,
            counts: HashMap::new(),
        })
        .unwrap();

    let aggregate_id = unique_id("midplan-aggregate");
    let mut projection = CounterProjection::default();
    projection.touch(aggregate_id.clone()).unwrap();
    let identity = StreamIdentity::new(CounterProjection::aggregate_type(), &aggregate_id).unwrap();

    let outbox_id = unique_id("midplan-outbox");
    let outbox_message =
        OutboxMessage::create(&outbox_id, "counter.touched", b"{}".to_vec()).unwrap();

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

    // 1. Aggregate event absent.
    assert!(
        repo.get_stream(&identity).await.unwrap().is_none(),
        "the aggregate stream must roll back"
    );

    // 2. Outbox row absent.
    assert!(
        find_outbox_by_id(&repo.outbox_store(), &outbox_id)
            .await
            .is_none(),
        "the outbox row must roll back"
    );

    // 3. First read-model mutation absent.
    let good_rows: i64 = sqlx::query_scalar(
        r#"SELECT COUNT(*) FROM "postgres_relational_counter_views" WHERE "id" = $1"#,
    )
    .bind(&good_id)
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(good_rows, 0, "the first read-model mutation must roll back");
}

#[tokio::test]
async fn commit_batch_lowers_relational_read_model_plan_into_registered_table() {
    let Some((_schema, repo)) = repository().await else {
        return;
    };
    bootstrap_relational_counter_table(&repo).await;
    let id = unique_id("relational-batch");
    let mut counts = HashMap::new();
    counts.insert("wins".to_string(), 2);
    let view = RelationalCounterView {
        id: id.clone(),
        value: 7,
        counts,
    };
    let mut session = ReadModelWritePlanBuilder::new();
    session.upsert(&view).unwrap();
    let mut projection = CounterProjection::default();
    projection.touch(id.clone()).unwrap();
    let identity = StreamIdentity::new(CounterProjection::aggregate_type(), &id).unwrap();

    repo.read_models(session)
        .commit(&mut projection)
        .await
        .unwrap();

    assert!(repo.get_stream(&identity).await.unwrap().is_some());
    let row = sqlx::query(
        r#"
        SELECT "id", "value", "counts"::text AS counts, "_sourced_version"
        FROM "postgres_relational_counter_views"
        WHERE "id" = $1
        "#,
    )
    .bind(&id)
    .fetch_one(repo.pool())
    .await
    .unwrap();
    let stored_counts: String = sqlx::Row::try_get(&row, "counts").unwrap();
    let stored_counts: serde_json::Value = serde_json::from_str(&stored_counts).unwrap();

    assert_eq!(sqlx::Row::try_get::<String, _>(&row, "id").unwrap(), id);
    assert_eq!(sqlx::Row::try_get::<i64, _>(&row, "value").unwrap(), 7);
    assert_eq!(stored_counts["wins"].as_i64(), Some(2));
    assert_eq!(
        sqlx::Row::try_get::<i64, _>(&row, "_sourced_version").unwrap(),
        1
    );
}

#[tokio::test]
async fn read_model_session_patches_and_deletes_relational_rows() {
    let Some((_schema, repo)) = repository().await else {
        return;
    };
    bootstrap_relational_counter_table(&repo).await;
    let id = unique_id("relational-session");
    let mut counts = HashMap::new();
    counts.insert("wins".to_string(), 2);
    let view = RelationalCounterView {
        id: id.clone(),
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
        .patch::<RelationalCounterView>(relational_counter_key(&id), patch)
        .unwrap();
    patch_session.commit(&repo).await.unwrap();

    let row = sqlx::query(
        r#"
        SELECT "value", "counts"::text AS counts, "_sourced_version"
        FROM "postgres_relational_counter_views"
        WHERE "id" = $1
        "#,
    )
    .bind(&id)
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
        .delete::<RelationalCounterView>(relational_counter_key(&id))
        .unwrap();
    delete_session.commit(&repo).await.unwrap();

    let remaining: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM "postgres_relational_counter_views"
        WHERE "id" = $1
        "#,
    )
    .bind(&id)
    .fetch_one(repo.pool())
    .await
    .unwrap();
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn read_model_session_persists_relational_rows() {
    let Some((_schema, repo)) = repository().await else {
        return;
    };
    bootstrap_relational_counter_table(&repo).await;
    let id = unique_id("view");
    let view = RelationalCounterView {
        id: id.clone(),
        value: 42,
        counts: HashMap::new(),
    };
    let mut session = ReadModelWritePlanBuilder::new();
    session.upsert(&view).unwrap();

    let outcome = session.commit(&repo).await.unwrap();
    let row = sqlx::query(
        r#"
        SELECT "value", "_sourced_version"
        FROM "postgres_relational_counter_views"
        WHERE "id" = $1
        "#,
    )
    .bind(&id)
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

#[tokio::test]
async fn unsupported_codec_rows_fail_on_read() {
    let Some((_schema, repo)) = repository().await else {
        return;
    };
    let id = unique_id("bad-codec");
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
        VALUES ($1, $2, 1, 'BadEvent', 1, $3, 'json', 1, '{}'::jsonb, now())
        "#,
    )
    .bind("postgres.counter")
    .bind(&id)
    .bind(vec![0_u8])
    .execute(repo.pool())
    .await
    .unwrap();

    let identity = StreamIdentity::new("postgres.counter", &id).unwrap();
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
    let Some((_schema, repo)) = repository().await else {
        return;
    };
    let message_id = unique_id("outbox-column-metadata");
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
        VALUES ($1, 'OutboxColumns', decode('00', 'hex'), 'bytes', 1, '{}'::jsonb,
                'pending', now(), now(), 0, 'corr-column', 'cause-column')
        "#,
    )
    .bind(&message_id)
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
async fn backend_termination_mid_commit_rolls_back_and_nothing_persists() {
    // Fault injection: a commit_batch is killed mid-transaction (the backend is
    // terminated while the commit waits on a table lock). The write must roll
    // back completely and surface as a storage error rather than hanging or
    // partially persisting.
    let Some((schema, repo)) = repository().await else {
        return;
    };
    let database_url = std::env::var("DATABASE_URL").expect("repository() checked DATABASE_URL");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .expect("admin pool connects");

    // Take an ACCESS EXCLUSIVE lock on the events table from a second
    // connection so the commit blocks mid-transaction at its first table touch.
    let mut locker = admin.acquire().await.expect("locker connection");
    let locker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *locker)
        .await
        .expect("locker pid");
    sqlx::query("BEGIN")
        .execute(&mut *locker)
        .await
        .expect("begin lock tx");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE \"{}\".aggregate_events IN ACCESS EXCLUSIVE MODE",
        schema.schema_name().replace('"', "\"\"")
    )))
    .execute(&mut *locker)
    .await
    .expect("lock aggregate_events");

    // The commit now parks on the lock inside its transaction.
    let id = unique_id("terminated");
    let commit = {
        let repo = repo.clone();
        let id = id.clone();
        tokio::spawn(async move {
            let mut counter = Counter::default();
            counter.increment(id, 1).unwrap();
            repo.aggregate::<Counter>().commit(&mut counter).await
        })
    };

    // Find the backend our lock is blocking (precisely: blocked BY locker_pid),
    // then terminate it mid-commit.
    let mut victim_pid: Option<i32> = None;
    for _ in 0..100 {
        let blocked: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE pg_blocking_pids(pid) @> ARRAY[$1] LIMIT 1",
        )
        .bind(locker_pid)
        .fetch_optional(&admin)
        .await
        .expect("pg_stat_activity lookup");
        if let Some(pid) = blocked {
            victim_pid = Some(pid);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let victim_pid = victim_pid.expect("the commit should block on the table lock");
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(victim_pid)
        .fetch_one(&admin)
        .await
        .expect("pg_terminate_backend");
    assert!(terminated, "the blocked commit backend was terminated");

    let err = commit
        .await
        .expect("commit task joins")
        .expect_err("a terminated backend must fail the commit");
    assert!(
        matches!(&err, RepositoryError::Storage { .. }),
        "the terminated commit surfaces as a storage error, got {err:?}"
    );
    // The termination surfaces as SQLSTATE 57P01; `is_sqlx_transient` now
    // classifies 57P01/57P02/57P03 and class-08 connection failures as
    // transient (losing the connection is an infrastructure hiccup), so the
    // error is retryable.
    assert!(
        err.is_retryable(),
        "57P01 (admin shutdown) must be retryable; got {err:?}"
    );

    // Release the lock and prove the whole transaction rolled back.
    sqlx::query("ROLLBACK")
        .execute(&mut *locker)
        .await
        .expect("release lock");
    let identity = StreamIdentity::new(Counter::aggregate_type(), &id).unwrap();
    assert!(
        repo.get_stream(&identity).await.unwrap().is_none(),
        "nothing from the killed commit may persist"
    );
}
