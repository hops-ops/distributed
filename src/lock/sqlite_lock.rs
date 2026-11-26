//! SQLite dialect for the durable SQLx lease lock. Drop a
//! [`SqliteLockManager`] into a `QueuedRepository` with
//! `.queued_with(SqliteLockManager::new(pool))`. See [`super::sqlx_common`]
//! for the lease model — all machinery is the shared
//! [`SqlxLockManager`]/[`SqlxLock`]; this file only supplies the SQLite SQL
//! and the `SQLITE_BUSY` contention mapping.
//!
//! Cross-process serialization requires the managers to share one database —
//! use a file or `cache=shared` URL, not a per-connection `:memory:` pool.

use sqlx::sqlite::SqliteQueryResult;
use sqlx::Sqlite;

use crate::repository::sql::sqlite_now;
use crate::sqlx_repo::is_sqlite_busy;

use super::sqlx_common::{LockDialect, SqlxLock, SqlxLockManager};

/// SQLite [`LockDialect`]: `?n` placeholders, `unixepoch('now','subsec')` in
/// microseconds as the authoritative clock.
#[derive(Debug, Clone, Copy)]
pub struct SqliteDialect;

impl LockDialect for SqliteDialect {
    type Db = Sqlite;

    const OWNER_PREFIX: &'static str = "sqlite";

    /// Inline lease-table DDL for standalone [`SqlxLockManager::migrate`]. The
    /// same table is created by `SqliteRepository`'s migrations; both are
    /// idempotent.
    const DDL: &'static str = "\
CREATE TABLE IF NOT EXISTS aggregate_locks (\
  lock_key TEXT NOT NULL PRIMARY KEY,\
  owner_token TEXT NOT NULL,\
  acquired_at INTEGER NOT NULL,\
  expires_at INTEGER NOT NULL,\
  CHECK (lock_key <> ''),\
  CHECK (owner_token <> '')\
);\
CREATE INDEX IF NOT EXISTS aggregate_locks_expires_at_idx ON aggregate_locks (expires_at);";

    /// Same conditional upsert as Postgres (insert / steal-expired /
    /// re-acquire-own-token), one atomic statement.
    const ACQUIRE_SQL: &'static str = concat!(
        "INSERT INTO aggregate_locks (lock_key, owner_token, acquired_at, expires_at) VALUES (?1, ?2, ",
        sqlite_now!(),
        ", ",
        sqlite_now!(),
        " + ?3) ON CONFLICT (lock_key) DO UPDATE SET owner_token = excluded.owner_token, \
         acquired_at = excluded.acquired_at, expires_at = excluded.expires_at \
         WHERE aggregate_locks.expires_at <= ",
        sqlite_now!(),
        " OR aggregate_locks.owner_token = excluded.owner_token RETURNING owner_token"
    );

    const RELEASE_SQL: &'static str =
        "DELETE FROM aggregate_locks WHERE lock_key = ?1 AND owner_token = ?2";

    const SWEEP_SQL: &'static str = concat!(
        "DELETE FROM aggregate_locks WHERE expires_at <= ",
        sqlite_now!()
    );

    /// SQLite serializes writers, so a colliding writer may get `SQLITE_BUSY` —
    /// treat that as contention (retry), not failure, since the pool sets no
    /// `busy_timeout`.
    fn busy_is_contention(err: &sqlx::Error) -> bool {
        is_sqlite_busy(err)
    }

    fn rows_affected(result: SqliteQueryResult) -> u64 {
        result.rows_affected()
    }
}

/// SQLite-backed [`LockManager`](super::LockManager). Hands out one cached
/// [`SqliteLock`] per key (like `InMemoryLockManager`).
pub type SqliteLockManager = SqlxLockManager<SqliteDialect>;

/// A single SQLite lease lock for one stream key.
pub type SqliteLock = SqlxLock<SqliteDialect>;
