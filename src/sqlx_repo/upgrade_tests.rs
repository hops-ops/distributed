//! An existing database upgrades in place.
//!
//! - Migrations 0016 and 0017 convert populated framework tables to
//!   integer-microsecond timestamps. Each test applies the migrations before
//!   0016, writes rows in every timestamp form an earlier release stored,
//!   upgrades, and reads the converted values back — directly and through
//!   the outbox store.
//! - The read-model bootstrap brings Postgres text columns an earlier release
//!   created with the database's default collation to `COLLATE "C"`, rows
//!   kept.

use std::time::{Duration, UNIX_EPOCH};

use sqlx::{Database, Pool};

use super::repo::embedded_migrator;
use crate::outbox::OutboxMessageStatus;
use crate::outbox_worker::OutboxStore;
use crate::repository::migrations::EmbeddedMigration;

/// One `(sql, expected)` per converted column: `sql` selects a single
/// `BIGINT` value from the upgraded database.
type Expectations = &'static [(&'static str, i64)];

async fn apply_before_0016<DB>(pool: &Pool<DB>, migrations: &[EmbeddedMigration])
where
    DB: Database,
    DB::Connection: sqlx::migrate::Migrate,
    for<'c> &'c mut DB::Connection: sqlx::Executor<'c, Database = DB>,
{
    let before: Vec<_> = migrations
        .iter()
        .copied()
        .filter(|migration| migration.version < 16)
        .collect();
    assert_eq!(before.last().map(|migration| migration.version), Some(15));
    embedded_migrator(&before).run(pool).await.unwrap();
}

async fn assert_converted<DB>(pool: &Pool<DB>, expectations: Expectations)
where
    DB: Database,
    for<'c> &'c Pool<DB>: sqlx::Executor<'c, Database = DB>,
    for<'q> <DB as Database>::Arguments: sqlx::IntoArguments<DB>,
    (i64,): for<'r> sqlx::FromRow<'r, DB::Row>,
{
    for (sql, expected) in expectations {
        let (value,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(*sql))
            .fetch_one(pool)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(value, *expected, "{sql}");
    }
}

/// The upgraded outbox store reads the converted rows as the times they were.
async fn assert_outbox_reads(store: impl OutboxStore) {
    let pending = store
        .messages_by_status(OutboxMessageStatus::Pending, 10)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0].created_at,
        UNIX_EPOCH + Duration::from_micros(1_700_000_002_000_001)
    );
    let backlog = store.backlog_stats().await.unwrap();
    assert_eq!(backlog.pending, 1);
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use crate::repository::migrations::SQLITE_MIGRATIONS;

    /// Rows in the forms a v15 SQLite database holds: repository-written
    /// `"secs.nanos"` text, `CURRENT_TIMESTAMP` default text, REAL epoch
    /// seconds, and integer milliseconds.
    const V15_ROWS: &str = "
        INSERT INTO aggregate_events (aggregate_type, aggregate_id, sequence, event_name, payload, payload_codec, payload_codec_version, recorded_at)
        VALUES ('order', 'o-1', 1, 'Placed', x'00', 'json', 1, '1700000000.123456789'),
               ('order', 'o-1', 2, 'Paid', x'00', 'json', 1, '2023-11-14 22:13:20');
        INSERT INTO aggregate_snapshots (aggregate_type, aggregate_id, version, snapshot_version, payload, payload_codec, payload_codec_version, recorded_at, updated_at)
        VALUES ('order', 'o-1', 2, 1, x'00', 'json', 1, '1700000001.000000500', '2023-11-14 22:13:21');
        INSERT INTO outbox_messages (message_id, event_type, payload, payload_codec, payload_codec_version, status, created_at, next_available_at, updated_at)
        VALUES ('m-1', 'Placed', x'00', 'json', 1, 'pending', '1700000002.000001000', '1700000002.000001000', '2023-11-14 22:13:22');
        INSERT INTO outbox_messages (message_id, event_type, payload, payload_codec, payload_codec_version, status, created_at, next_available_at, claimed_by, claimed_until, attempts, updated_at)
        VALUES ('m-2', 'Paid', x'00', 'json', 1, 'in_flight', '1700000003.000000000', '1700000003.000000000', 'worker', '1700000003.999999999', 1, '1700000003.000000000');
        INSERT INTO consumer_inbox (consumer, message_id, processed_at) VALUES ('c', 'm-0', '2023-11-14 22:13:20');
        INSERT INTO aggregate_locks (lock_key, owner_token, acquired_at, expires_at) VALUES ('order:o-1', 'sq-1', 1700000000.25, 1700000030.5);
        INSERT INTO command_ledger (service_id, principal_partition, command_id, command_name, command_contract_hash, input_hash, state, causation_id, attempt_number, outcome, created_at, updated_at, completed_at, retention_expires_at)
        VALUES ('svc', 'p', 'done', 'Place', zeroblob(32), zeroblob(32), 'succeeded', 'cause-1', 1, '{}', 1700000000.001, 1700000000.002, 1700000000.003, 1700086400.0);
        INSERT INTO command_ledger (service_id, principal_partition, command_id, command_name, command_contract_hash, input_hash, state, causation_id, attempt_token, attempt_number, lease_expires_at, created_at, updated_at, retention_expires_at)
        VALUES ('svc', 'p', 'running', 'Place', zeroblob(32), zeroblob(32), 'in_progress', 'cause-2', 'attempt', 1, 1700000010.5, 1700000000.0, 1700000000.0, 1700086400.0);
        INSERT INTO coordination_leases (lease_key, holder, fencing_token, acquired_at, expires_at)
        VALUES ('held', 'h', 3, 1700000000.5, 1700000015.75), ('released', 'h', 1, 1700000000.5, 0);
        INSERT INTO aggregate_schedules (aggregate_type, aggregate_id, name, command, input, due_at_ms, generation, armed, claimed_until_ms)
        VALUES ('order', 'o-1', 'expire', 'Expire', '{}', 1700000000123, 1, 1, 1700000000999),
               ('order', 'o-2', 'expire', 'Expire', '{}', 1700000000124, 1, 1, NULL);
        INSERT INTO cell_alarms (name, command, input, due_at_ms, generation, armed)
        VALUES ('tick', 'Tick', '{}', 1700000000456, 1, 1);
    ";

    const EXPECTED: Expectations = &[
        (
            "SELECT recorded_at FROM aggregate_events WHERE sequence = 1",
            1_700_000_000_123_456,
        ),
        (
            "SELECT recorded_at FROM aggregate_events WHERE sequence = 2",
            1_700_000_000_000_000,
        ),
        (
            "SELECT recorded_at FROM aggregate_snapshots",
            1_700_000_001_000_000,
        ),
        (
            "SELECT updated_at FROM aggregate_snapshots",
            1_700_000_001_000_000,
        ),
        (
            "SELECT created_at FROM outbox_messages WHERE message_id = 'm-1'",
            1_700_000_002_000_001,
        ),
        (
            "SELECT next_available_at FROM outbox_messages WHERE message_id = 'm-1'",
            1_700_000_002_000_001,
        ),
        (
            "SELECT updated_at FROM outbox_messages WHERE message_id = 'm-1'",
            1_700_000_002_000_000,
        ),
        (
            "SELECT claimed_until IS NULL FROM outbox_messages WHERE message_id = 'm-1'",
            1,
        ),
        (
            "SELECT claimed_until FROM outbox_messages WHERE message_id = 'm-2'",
            1_700_000_003_999_999,
        ),
        (
            "SELECT processed_at FROM consumer_inbox",
            1_700_000_000_000_000,
        ),
        (
            "SELECT acquired_at FROM aggregate_locks",
            1_700_000_000_250_000,
        ),
        (
            "SELECT expires_at FROM aggregate_locks",
            1_700_000_030_500_000,
        ),
        (
            "SELECT created_at FROM command_ledger WHERE command_id = 'done'",
            1_700_000_000_001_000,
        ),
        (
            "SELECT updated_at FROM command_ledger WHERE command_id = 'done'",
            1_700_000_000_002_000,
        ),
        (
            "SELECT completed_at FROM command_ledger WHERE command_id = 'done'",
            1_700_000_000_003_000,
        ),
        (
            "SELECT retention_expires_at FROM command_ledger WHERE command_id = 'done'",
            1_700_086_400_000_000,
        ),
        (
            "SELECT lease_expires_at FROM command_ledger WHERE command_id = 'running'",
            1_700_000_010_500_000,
        ),
        (
            "SELECT expires_at FROM coordination_leases WHERE lease_key = 'held'",
            1_700_000_015_750_000,
        ),
        (
            "SELECT expires_at FROM coordination_leases WHERE lease_key = 'released'",
            0,
        ),
        (
            "SELECT due_at FROM aggregate_schedules WHERE aggregate_id = 'o-1'",
            1_700_000_000_123_000,
        ),
        (
            "SELECT claimed_until FROM aggregate_schedules WHERE aggregate_id = 'o-1'",
            1_700_000_000_999_000,
        ),
        (
            "SELECT claimed_until IS NULL FROM aggregate_schedules WHERE aggregate_id = 'o-2'",
            1,
        ),
        ("SELECT due_at FROM cell_alarms", 1_700_000_000_456_000),
        // Every converted value is stored as an integer, not text or real.
        (
            "SELECT COUNT(*) FROM (
                SELECT typeof(recorded_at) AS t FROM aggregate_events
                UNION ALL SELECT typeof(updated_at) FROM aggregate_snapshots
                UNION ALL SELECT typeof(created_at) FROM outbox_messages
                UNION ALL SELECT typeof(processed_at) FROM consumer_inbox
                UNION ALL SELECT typeof(expires_at) FROM aggregate_locks
                UNION ALL SELECT typeof(retention_expires_at) FROM command_ledger
                UNION ALL SELECT typeof(acquired_at) FROM coordination_leases
             ) WHERE t <> 'integer'",
            0,
        ),
    ];

    #[tokio::test]
    async fn upgrade_converts_every_stored_timestamp_form() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        apply_before_0016(&pool, SQLITE_MIGRATIONS).await;
        sqlx::raw_sql(V15_ROWS).execute(&pool).await.unwrap();

        crate::SqliteRepository::migrate_pool(&pool).await.unwrap();

        assert_converted(&pool, EXPECTED).await;
        assert_outbox_reads(crate::SqliteRepository::new(pool).outbox_store()).await;
    }

    #[tokio::test]
    async fn outbox_claim_scan_uses_the_claim_index() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        crate::SqliteRepository::migrate_pool(&pool).await.unwrap();
        // The candidate scan of `claim_sqlite`, with its binds inlined.
        let plan: Vec<(i64, i64, i64, String)> = sqlx::query_as(
            "EXPLAIN QUERY PLAN SELECT message_id FROM outbox_messages WHERE \
             status IN ('pending', 'in_flight') AND ((status = 'pending' AND next_available_at <= 1) \
             OR (status = 'in_flight' AND (claimed_until IS NULL OR claimed_until <= 1))) \
             ORDER BY created_at ASC, message_id ASC LIMIT 10",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let detail: Vec<_> = plan.into_iter().map(|row| row.3).collect();
        assert!(
            detail
                .iter()
                .any(|step| step.contains("USING INDEX outbox_messages_claim_idx")),
            "{detail:?}"
        );
        assert!(
            !detail.iter().any(|step| step.contains("TEMP B-TREE")),
            "the index order serves ORDER BY: {detail:?}"
        );
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    use crate::repository::migrations::POSTGRES_MIGRATIONS;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;

    /// Rows in the forms a v15 Postgres database holds: `timestamptz`
    /// columns, double-precision epoch seconds, integer milliseconds.
    const V15_ROWS: &str = "
        INSERT INTO aggregate_events (aggregate_type, aggregate_id, sequence, event_name, payload, payload_codec, payload_codec_version, recorded_at)
        VALUES ('order', 'o-1', 1, 'Placed', '\\x00', 'json', 1, to_timestamp(0) + interval '1700000000123456 microseconds');
        INSERT INTO aggregate_snapshots (aggregate_type, aggregate_id, version, snapshot_version, payload, payload_codec, payload_codec_version, recorded_at, updated_at)
        VALUES ('order', 'o-1', 1, 1, '\\x00', 'json', 1, to_timestamp(1700000001), to_timestamp(1700000001));
        INSERT INTO outbox_messages (message_id, event_type, payload, payload_codec, payload_codec_version, status, created_at, next_available_at, updated_at)
        VALUES ('m-1', 'Placed', '\\x00', 'json', 1, 'pending', to_timestamp(0) + interval '1700000002000001 microseconds', to_timestamp(0) + interval '1700000002000001 microseconds', to_timestamp(1700000002));
        INSERT INTO outbox_messages (message_id, event_type, payload, payload_codec, payload_codec_version, status, created_at, next_available_at, claimed_by, claimed_until, attempts, updated_at)
        VALUES ('m-2', 'Paid', '\\x00', 'json', 1, 'in_flight', to_timestamp(1700000003), to_timestamp(1700000003), 'worker', to_timestamp(0) + interval '1700000003999999 microseconds', 1, to_timestamp(1700000003));
        INSERT INTO consumer_inbox (consumer, message_id, processed_at) VALUES ('c', 'm-0', to_timestamp(1700000000));
        INSERT INTO aggregate_locks (lock_key, owner_token, acquired_at, expires_at) VALUES ('order:o-1', 'pg-1', 1700000000.25, 1700000030.5);
        INSERT INTO command_ledger (service_id, principal_partition, command_id, command_name, command_contract_hash, input_hash, state, causation_id, attempt_number, outcome, created_at, updated_at, completed_at, retention_expires_at)
        VALUES ('svc', 'p', 'done', 'Place', decode(repeat('00', 32), 'hex'), decode(repeat('00', 32), 'hex'), 'succeeded', 'cause-1', 1, '{}', to_timestamp(1700000000.001), to_timestamp(1700000000.002), to_timestamp(1700000000.003), to_timestamp(1700086400));
        INSERT INTO coordination_leases (lease_key, holder, fencing_token, acquired_at, expires_at)
        VALUES ('held', 'h', 3, 1700000000.5, 1700000015.75), ('released', 'h', 1, 1700000000.5, 0);
        INSERT INTO aggregate_schedules (aggregate_type, aggregate_id, name, command, input, due_at_ms, generation, armed, claimed_until_ms)
        VALUES ('order', 'o-1', 'expire', 'Expire', '{}', 1700000000123, 1, 1, 1700000000999);
        INSERT INTO cell_alarms (name, command, input, due_at_ms, generation, armed)
        VALUES ('tick', 'Tick', '{}', 1700000000456, 1, 1);
    ";

    const EXPECTED: Expectations = &[
        (
            "SELECT recorded_at FROM aggregate_events",
            1_700_000_000_123_456,
        ),
        (
            "SELECT updated_at FROM aggregate_snapshots",
            1_700_000_001_000_000,
        ),
        (
            "SELECT created_at FROM outbox_messages WHERE message_id = 'm-1'",
            1_700_000_002_000_001,
        ),
        (
            "SELECT claimed_until FROM outbox_messages WHERE message_id = 'm-2'",
            1_700_000_003_999_999,
        ),
        (
            "SELECT processed_at FROM consumer_inbox",
            1_700_000_000_000_000,
        ),
        (
            "SELECT acquired_at FROM aggregate_locks",
            1_700_000_000_250_000,
        ),
        (
            "SELECT expires_at FROM aggregate_locks",
            1_700_000_030_500_000,
        ),
        (
            "SELECT created_at FROM command_ledger",
            1_700_000_000_001_000,
        ),
        (
            "SELECT completed_at FROM command_ledger",
            1_700_000_000_003_000,
        ),
        (
            "SELECT retention_expires_at FROM command_ledger",
            1_700_086_400_000_000,
        ),
        (
            "SELECT expires_at FROM coordination_leases WHERE lease_key = 'held'",
            1_700_000_015_750_000,
        ),
        (
            "SELECT expires_at FROM coordination_leases WHERE lease_key = 'released'",
            0,
        ),
        (
            "SELECT due_at FROM aggregate_schedules",
            1_700_000_000_123_000,
        ),
        (
            "SELECT claimed_until FROM aggregate_schedules",
            1_700_000_000_999_000,
        ),
        ("SELECT due_at FROM cell_alarms", 1_700_000_000_456_000),
        // No framework column is left as timestamptz or double precision
        // (sqlx's own migration ledger is not a framework table).
        (
            "SELECT COUNT(*) FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name <> '_sqlx_migrations'
               AND data_type IN ('timestamp with time zone', 'double precision')",
            0,
        ),
    ];

    #[tokio::test]
    async fn upgrade_converts_every_stored_timestamp_form() {
        let Some(url) = crate::test_env::require_service("DATABASE_URL", "Postgres upgrade test")
        else {
            return;
        };
        let schema = format!("timestamp_upgrade_{}", uuid::Uuid::now_v7().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let options = PgConnectOptions::from_str(&url)
            .unwrap()
            .options([("search_path", schema.as_str())]);
        // Run in a task so the schema is dropped even when an assertion fails.
        let upgrade = tokio::spawn(async move {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .unwrap();
            apply_before_0016(&pool, POSTGRES_MIGRATIONS).await;
            sqlx::raw_sql(V15_ROWS).execute(&pool).await.unwrap();
            crate::PostgresRepository::migrate_pool(&pool)
                .await
                .unwrap();

            assert_converted(&pool, EXPECTED).await;
            assert_outbox_reads(crate::PostgresRepository::new(pool.clone()).outbox_store()).await;
            pool.close().await;
        })
        .await;

        sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&admin)
            .await
            .unwrap();
        if let Err(error) = upgrade {
            std::panic::resume_unwind(error.into_panic());
        }
    }

    /// A read-model table as an earlier release generated it: text columns
    /// in the database's default collation.
    #[tokio::test]
    async fn bootstrap_moves_existing_text_columns_to_c_collation() {
        use crate::table::{
            ColumnType, PrimaryKey, TableColumn, TableIndex, TableKind, TableSchema,
            TableSchemaRegistry, TableSqlDialect,
        };
        let Some((_schema, url)) = crate::sqlx_repo::postgres_test_schema::postgres_test_schema(
            "DATABASE_URL",
            "Postgres read-model collation upgrade test",
            "collation_upgrade",
        )
        .await
        else {
            return;
        };
        let mut registry = TableSchemaRegistry::new();
        registry
            .register_schema(TableSchema {
                model_name: "Label".into(),
                table_name: "labels".into(),
                columns: vec![
                    TableColumn::new("id", "id", ColumnType::Text),
                    TableColumn::new("title", "title", ColumnType::Text),
                    TableColumn::new("rank", "rank", ColumnType::Integer),
                ],
                primary_key: PrimaryKey::new(["id"]),
                version_column: None,
                indexes: vec![TableIndex::new(["title"])],
                relationships: Vec::new(),
                kind: TableKind::ReadModel,
            })
            .unwrap();
        let repository = crate::PostgresRepository::connect_and_migrate(&url)
            .await
            .unwrap();
        let pool = repository.pool();
        for statement in
            crate::table::table_schema_statements(&registry, TableSqlDialect::Postgres).unwrap()
        {
            let earlier = statement.replace(" COLLATE \"C\"", "");
            sqlx::raw_sql(sqlx::AssertSqlSafe(earlier))
                .execute(pool)
                .await
                .unwrap();
        }
        sqlx::raw_sql(
            "INSERT INTO labels (id, title, rank) VALUES ('b', 'b', 1), ('B', 'B', 2), ('a', 'a', 3)",
        )
        .execute(pool)
        .await
        .unwrap();
        let collations = || async {
            sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT column_name::text, collation_name::text FROM information_schema.columns \
                 WHERE table_schema = current_schema() AND table_name = 'labels' \
                 AND data_type = 'text' ORDER BY column_name",
            )
            .fetch_all(pool)
            .await
            .unwrap()
        };
        assert!(collations()
            .await
            .iter()
            .all(|(_, collation)| collation.as_deref() != Some("C")));

        // Twice: the second bootstrap finds nothing to alter.
        for _ in 0..2 {
            repository
                .bootstrap_table_schema_for_dev(&registry)
                .await
                .unwrap();
            assert_eq!(
                collations().await,
                vec![
                    ("id".to_string(), Some("C".to_string())),
                    ("title".to_string(), Some("C".to_string())),
                ]
            );
        }
        let ids: Vec<(String,)> = sqlx::query_as("SELECT id FROM labels ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap();
        assert_eq!(
            ids.into_iter().map(|(id,)| id).collect::<Vec<_>>(),
            ["B", "a", "b"],
            "bytewise order, rows kept"
        );
        // The title index still serves the column after the change.
        let (valid,): (bool,) = sqlx::query_as(
            "SELECT bool_and(indisvalid) FROM pg_index WHERE indrelid = 'labels'::regclass",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert!(valid);
    }
}
