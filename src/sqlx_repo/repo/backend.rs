use super::*;

pub(crate) use crate::repository::migrations::EmbeddedMigration;
#[cfg(feature = "postgres")]
pub(crate) use crate::repository::migrations::POSTGRES_MIGRATIONS;
#[cfg(feature = "sqlite")]
pub(crate) use crate::repository::migrations::SQLITE_MIGRATIONS;

/// Build an embedded migrator from the validated, generated migration inventory.
pub(crate) fn embedded_migrator(files: &[EmbeddedMigration]) -> Migrator {
    Migrator::with_migrations(
        files
            .iter()
            .map(|migration| {
                Migration::new(
                    migration.version,
                    migration.description.into(),
                    MigrationType::Simple,
                    sqlx::SqlSafeStr::into_sql_str(migration.sql),
                    false,
                )
            })
            .collect(),
    )
}

/// Group stream identities by aggregate type so each type is one id-list
/// round trip instead of a query per identity. Callers issue single-type
/// batches in the common case, so this usually yields one group; the grouping
/// only exists to keep arbitrary mixed-type inputs correct.
pub(super) fn ids_by_type(identities: &[StreamIdentity]) -> BTreeMap<&str, Vec<&str>> {
    let mut groups: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for identity in identities {
        groups
            .entry(identity.aggregate_type())
            .or_default()
            .push(identity.aggregate_id());
    }
    groups
}

/// Dialect surface for the shared repository path (event store, snapshots,
/// outbox lifecycle, consumer inbox, schema bootstrap).
///
/// Everything the two SQL backends genuinely disagree on is an item here; the
/// free functions and the [`SqlxRepository`]/[`SqlxOutboxStore`] impls in this
/// module are the single copy of everything they agree on.
pub trait SqlxRepoBackend: SqlxReadModelBackend {
    /// Embedded migrations applied by `migrate_pool`. Runs through
    /// `sqlx::migrate::Migrator`, which keeps a `_sqlx_migrations` ledger and
    /// executes each migration file whole (the previous hand-rolled runner
    /// split files on `;`, which breaks on function bodies and string
    /// literals, and kept no record of what had been applied).
    fn migrator() -> &'static Migrator;
    /// Maximum bound parameters per statement. Multi-row inserts are chunked to
    /// stay under this; Postgres is effectively unlimited so chunking collapses
    /// to a single statement and both backends share one code path.
    const MAX_BIND_PARAMS: usize;
    /// Whether event-insert conflict recovery re-reads stream versions inside
    /// the failed transaction. SQLite does not abort a transaction on a
    /// constraint error, so the re-read can (and must, to see this tx's own
    /// earlier chunks) happen in-tx. A failed Postgres statement aborts the
    /// transaction, so the re-read runs on a separate pool connection.
    const CONFLICT_REREAD_IN_TX: bool;
    /// The database clock as a stored timestamp, integer microseconds since
    /// the UNIX epoch like every framework timestamp column
    /// (`crate::repository::sql::{sqlite_now, postgres_now}`).
    const NOW: &'static str;
    /// Statement that opens a transaction which reads before it writes.
    /// SQLite must take the write lock up front: a deferred transaction that
    /// read and then writes is refused outright when another connection
    /// committed in between, where waiting for the lock is what
    /// `busy_timeout` is for.
    const BEGIN_READ_THEN_WRITE: &'static str;
    /// Statement that opens a read-only transaction whose statements all see
    /// one snapshot. PostgreSQL must ask for it: under its default READ
    /// COMMITTED each statement sees a newer commit than the last. A SQLite
    /// read transaction is one snapshot from its first read.
    const BEGIN_READ_SNAPSHOT: &'static str;
    /// Row-lock suffix a raw table writer takes on the ownership fences of the
    /// tables it writes. Writers share it with each other; it excludes
    /// [`OWNERSHIP_FENCE_REGISTRATION_LOCK`](Self::OWNERSHIP_FENCE_REGISTRATION_LOCK).
    /// SQLite has one writer at a time and no row locks.
    const OWNERSHIP_FENCE_WRITER_LOCK: &'static str;
    /// Row-lock suffix causal registration takes on the fences of the tables
    /// it claims, excluding every writer and every other registration.
    const OWNERSHIP_FENCE_REGISTRATION_LOCK: &'static str;
    /// Command-ledger projection with JSON surfaced as text.
    const COMMAND_LEDGER_SELECT: &'static str;
    /// See [`SqlExecutor::SCHEDULE_CLAIM_LOCK_SUFFIX`](crate::repository::sql::SqlExecutor).
    const SCHEDULE_CLAIM_LOCK_SUFFIX: &'static str;
    /// Row-lock suffix for a reservation/status transaction.
    const COMMAND_LEDGER_LOCK_SUFFIX: &'static str;
    /// Row-lock suffix for a bounded compaction scan.
    const COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX: &'static str;
    /// `SELECT` list for `aggregate_events` rows (JSON surfaced as text).
    const EVENT_SELECT: &'static str;
    /// `SELECT` list for `aggregate_snapshots` rows (JSON surfaced as text).
    const SNAPSHOT_SELECT: &'static str;
    /// `SELECT` list for `outbox_messages` rows (JSON surfaced as text).
    const OUTBOX_SELECT: &'static str;
    /// Dialect for table/read-model schema artifact generation.
    const TABLE_DIALECT: TableSqlDialect;

    /// Pool size when connecting from a database URL.
    fn default_pool_size(database_url: &str) -> u32 {
        let _ = database_url;
        5
    }

    /// Backend-specific settings for a pool opened from a database URL.
    fn pool_options(options: sqlx::pool::PoolOptions<Self>) -> sqlx::pool::PoolOptions<Self> {
        options
    }

    /// Whether a `sqlx::Error` is this backend's unique-violation error.
    fn is_unique_violation(err: &sqlx::Error) -> bool;

    /// Push a JSON value bind, adding the backend's native JSON cast if needed.
    fn push_command_ledger_json(builder: &mut QueryBuilder<Self>, json: &str);

    /// Push a metadata JSON bind (Postgres casts to `::jsonb`).
    fn push_metadata(sep: &mut Separated<'_, Self, &'static str>, json: &str);

    /// Push an `aggregate_id` filter for an id list (Postgres: `= ANY($n)`
    /// array bind; SQLite: `IN (?, ?, ...)`).
    fn push_id_filter(builder: &mut QueryBuilder<Self>, ids: &[&str]);

    /// Claim up to `batch_size` outbox messages. This is the one genuinely
    /// divergent operation: Postgres uses a CTE with `FOR UPDATE SKIP LOCKED`;
    /// SQLite (no row locks) scans candidates and claims them with per-id
    /// conditional updates.
    fn claim_outbox<'a>(
        pool: &'a Pool<Self>,
        request: ClaimOutboxMessages,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + 'a;
}
