//! SQLite bus: the bus contract, the SQL bus contract, and what only SQLite
//! does (one writer at a time).

use std::time::Duration;

use common::sqlite::TempDb;
use common::transport::bus::{BusCapabilities, BusHarness, KindCapabilities};
use distributed::bus::{Bus, SqliteBus, TransportError};
use distributed::projection_protocol::ProjectionEpoch;
use sqlx::SqlitePool;

use crate::sql_bus_conformance::{sql_bus_conformance_tests, SqlBackend};

/// One temporary database per test; every bus shares its pool.
struct Sqlite {
    _db: TempDb,
    pool: SqlitePool,
}

impl BusHarness for Sqlite {
    type Bus = SqliteBus;

    const CAPABILITIES: BusCapabilities = BusCapabilities {
        // The queue keeps every send; the log stores a stable id once.
        commands: KindCapabilities {
            dedupes_ids: false,
            ..KindCapabilities::LOG
        },
        events: KindCapabilities::LOG,
        rebalance_may_redeliver: false,
        added_metadata: &[],
    };

    async fn start() -> Option<Self> {
        let sqlite = Self::start_empty().await?;
        SqliteBus::new(sqlite.pool.clone())
            .ensure_tables()
            .await
            .expect("ensure tables");
        Some(sqlite)
    }

    async fn bus(&self) -> SqliteBus {
        SqliteBus::new(self.pool.clone())
    }
}

impl SqlBackend for Sqlite {
    const PERMISSIVE_QUEUE: &'static [&'static str] = &[
        "DROP TABLE IF EXISTS bus_queue",
        "CREATE TABLE bus_queue (
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
        )",
        "CREATE INDEX bus_queue_claim_idx ON bus_queue (name, available_at, locked_until, seq)",
    ];
    const PERMISSIVE_LOG: &'static [&'static str] = &[
        "DROP TABLE IF EXISTS bus_log",
        "CREATE TABLE bus_log (
            seq          INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT,
            message_id   TEXT,
            kind         TEXT NOT NULL,
            payload      BLOB NOT NULL,
            content_type TEXT DEFAULT 'application/json',
            metadata     TEXT NOT NULL DEFAULT '[]',
            appended_at  REAL NOT NULL DEFAULT (unixepoch('now','subsec'))
        )",
        "CREATE INDEX bus_log_name_seq_idx ON bus_log (name, seq)",
        "CREATE UNIQUE INDEX bus_log_message_id_unique_idx \
         ON bus_log (message_id) WHERE message_id IS NOT NULL",
    ];
    const PAYLOAD: &'static str = "X'7b7d'";

    async fn start_empty() -> Option<Self> {
        let db = TempDb::new("distributed_sqlite_bus_test");
        let pool = db.pool().await;
        Some(Self { _db: db, pool })
    }

    async fn execute(&self, sql: &str) -> Result<(), String> {
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_owned()))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn ensure_tables(bus: &SqliteBus) -> Result<(), TransportError> {
        bus.ensure_tables().await
    }

    fn with_source_epoch(bus: SqliteBus, epoch: ProjectionEpoch) -> SqliteBus {
        bus.with_source_epoch(epoch)
    }

    fn with_lease(bus: SqliteBus, lease: Duration) -> SqliteBus {
        bus.with_lease(lease)
    }

    async fn reset_ordered_log(
        bus: &SqliteBus,
        expected: &ProjectionEpoch,
        next: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        bus.reset_ordered_log(expected, next).await
    }
}

common::bus_conformance_tests!(Sqlite);
sql_bus_conformance_tests!(Sqlite);

/// A write that finds the database locked fails at once as retryable, so
/// the caller retries instead of hanging behind the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn busy_or_locked_writer_contention_is_retryable() {
    let db = TempDb::new("distributed_sqlite_bus_test");
    SqliteBus::new(db.pool().await)
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
        SqliteBus::new(send_pool).send("sql.ship", b"{}".to_vec()),
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
