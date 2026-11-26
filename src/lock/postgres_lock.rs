//! Postgres dialect for the durable SQLx lease lock. Drop a
//! [`PostgresLockManager`] into a `QueuedRepository` with
//! `.queued_with(PostgresLockManager::new(pool))` to serialize aggregate
//! access across processes. See [`super::sqlx_common`] for the lease model —
//! all machinery is the shared [`SqlxLockManager`]/[`SqlxLock`]; this file only
//! supplies the Postgres SQL.

use sqlx::postgres::PgQueryResult;
use sqlx::Postgres;

use super::sqlx_common::{LockDialect, SqlxLock, SqlxLockManager};
use crate::repository::sql::postgres_now;

/// Postgres [`LockDialect`]: `$n` placeholders, `clock_timestamp()` in
/// microseconds as the authoritative clock.
#[derive(Debug, Clone, Copy)]
pub struct PostgresDialect;

impl LockDialect for PostgresDialect {
    type Db = Postgres;

    const OWNER_PREFIX: &'static str = "pg";

    /// Inline lease-table DDL for standalone [`SqlxLockManager::migrate`]. The
    /// same table is created by `PostgresRepository`'s migrations; both are
    /// idempotent.
    const DDL: &'static str = "\
CREATE TABLE IF NOT EXISTS aggregate_locks (\
  lock_key text NOT NULL PRIMARY KEY,\
  owner_token text NOT NULL,\
  acquired_at bigint NOT NULL,\
  expires_at bigint NOT NULL,\
  CHECK (lock_key <> ''),\
  CHECK (owner_token <> '')\
);\
CREATE INDEX IF NOT EXISTS aggregate_locks_expires_at_idx ON aggregate_locks (expires_at);";

    /// Atomic conditional upsert: insert when absent, or steal when the lease
    /// expired, or re-acquire our own token (idempotent on a lost-response
    /// retry). A holder that is present and unexpired makes the DO UPDATE a
    /// no-op, so RETURNING yields no row — without raising a unique violation.
    const ACQUIRE_SQL: &'static str = concat!(
        "INSERT INTO aggregate_locks (lock_key, owner_token, acquired_at, expires_at) VALUES ($1, $2, ",
        postgres_now!(),
        ", ",
        postgres_now!(),
        " + $3) ON CONFLICT (lock_key) DO UPDATE SET owner_token = EXCLUDED.owner_token, \
         acquired_at = EXCLUDED.acquired_at, expires_at = EXCLUDED.expires_at \
         WHERE aggregate_locks.expires_at <= ",
        postgres_now!(),
        " OR aggregate_locks.owner_token = EXCLUDED.owner_token RETURNING owner_token"
    );

    const RELEASE_SQL: &'static str =
        "DELETE FROM aggregate_locks WHERE lock_key = $1 AND owner_token = $2";

    const SWEEP_SQL: &'static str = concat!(
        "DELETE FROM aggregate_locks WHERE expires_at <= ",
        postgres_now!()
    );

    fn rows_affected(result: PgQueryResult) -> u64 {
        result.rows_affected()
    }
}

/// Postgres-backed [`LockManager`](super::LockManager). Hands out one cached
/// [`PostgresLock`] per key (like `InMemoryLockManager`).
pub type PostgresLockManager = SqlxLockManager<PostgresDialect>;

/// A single Postgres lease lock for one stream key.
pub type PostgresLock = SqlxLock<PostgresDialect>;
