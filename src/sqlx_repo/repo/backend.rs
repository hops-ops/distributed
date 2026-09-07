use super::*;

/// One migration registration emitted by the root build script.
#[derive(Clone, Copy)]
pub(crate) struct EmbeddedMigration {
    pub(crate) version: i64,
    pub(crate) description: &'static str,
    pub(crate) sql: &'static str,
}

include!(concat!(env!("OUT_DIR"), "/migration_inventory.rs"));

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

#[expect(
    clippy::items_after_test_module,
    reason = "migration parity tests stay beside generated registration data"
)]
#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "sqlite")]
    #[test]
    fn generated_sqlite_inventory_preserves_order_descriptions_and_bytes() {
        let versions = SQLITE_MIGRATIONS
            .iter()
            .map(|migration| migration.version)
            .collect::<Vec<_>>();
        let descriptions = SQLITE_MIGRATIONS
            .iter()
            .map(|migration| migration.description)
            .collect::<Vec<_>>();
        let sql = SQLITE_MIGRATIONS
            .iter()
            .map(|migration| migration.sql)
            .collect::<Vec<_>>();
        assert_eq!(versions, vec![1, 2, 3, 4, 5]);
        assert_eq!(
            descriptions,
            vec![
                "initial",
                "command ledger",
                "projection protocol",
                "command ledger atomic state",
                "projection source snapshots"
            ]
        );
        assert_eq!(
            sql,
            vec![
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/sqlite/0001_initial.sql"
                )),
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/sqlite/0002_command_ledger.sql"
                )),
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/sqlite/0003_projection_protocol.sql"
                )),
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/sqlite/0004_command_ledger_atomic_state.sql"
                )),
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/sqlite/0005_projection_source_snapshots.sql"
                )),
            ]
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn generated_postgres_inventory_preserves_order_descriptions_and_bytes() {
        let versions = POSTGRES_MIGRATIONS
            .iter()
            .map(|migration| migration.version)
            .collect::<Vec<_>>();
        let descriptions = POSTGRES_MIGRATIONS
            .iter()
            .map(|migration| migration.description)
            .collect::<Vec<_>>();
        let sql = POSTGRES_MIGRATIONS
            .iter()
            .map(|migration| migration.sql)
            .collect::<Vec<_>>();
        assert_eq!(versions, vec![1, 2, 3, 4, 5]);
        assert_eq!(
            descriptions,
            vec![
                "initial",
                "command ledger",
                "projection protocol",
                "command ledger atomic state",
                "projection source snapshots"
            ]
        );
        assert_eq!(
            sql,
            vec![
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/postgres/0001_initial.sql"
                )),
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/postgres/0002_command_ledger.sql"
                )),
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/postgres/0003_projection_protocol.sql"
                )),
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/postgres/0004_command_ledger_atomic_state.sql"
                )),
                include_str!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/migrations/postgres/0005_projection_source_snapshots.sql"
                )),
            ]
        );
    }
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

/// Bound parameters per `aggregate_events` row.
pub(super) const EVENT_BIND_COLUMNS: usize = 10;

/// Bound parameters per `outbox_messages` row.
pub(super) const OUTBOX_BIND_COLUMNS: usize = 19;

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
    /// SQL expression producing the database's current timestamp, used for
    /// server-side `updated_at` maintenance.
    const NOW: &'static str;
    /// Command-ledger projection with timestamp columns normalized for
    /// [`decode_timestamp`](Self::decode_timestamp) and JSON surfaced as text.
    const COMMAND_LEDGER_SELECT: &'static str;
    /// Row-lock suffix for a reservation/status transaction.
    const COMMAND_LEDGER_LOCK_SUFFIX: &'static str;
    /// Row-lock suffix for a bounded compaction scan.
    const COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX: &'static str;
    /// `SELECT` list for `aggregate_events` rows. The recorded-at column must
    /// surface as `recorded_at` in whatever representation
    /// [`decode_timestamp`](Self::decode_timestamp) reads.
    const EVENT_SELECT: &'static str;
    /// `SELECT` list for `aggregate_snapshots` rows (recorded-at as above).
    const SNAPSHOT_SELECT: &'static str;
    /// `SELECT` list for `outbox_messages` rows (`created_at`/`claimed_until`
    /// in the representation `decode_timestamp` reads).
    const OUTBOX_SELECT: &'static str;
    /// ORDER BY expression for outbox `created_at` ordering (SQLite stores
    /// timestamps as text and must cast for numeric ordering).
    const ORDER_BY_CREATED_AT: &'static str;
    /// `SELECT` expression for `MIN(created_at)` surfaced as
    /// `oldest_created_at` in this backend's timestamp representation.
    const OUTBOX_OLDEST_CREATED_AT_SELECT: &'static str;
    /// Dialect for table/read-model schema artifact generation.
    const TABLE_DIALECT: TableSqlDialect;

    /// Owned bind value for a stored timestamp (Postgres: epoch seconds `f64`;
    /// SQLite: `"secs.nanos"` text).
    type TimestampValue: Send + Sync + 'static;

    /// Pool size when connecting from a database URL.
    fn default_pool_size(database_url: &str) -> u32 {
        let _ = database_url;
        5
    }

    /// Whether a `sqlx::Error` is this backend's unique-violation error.
    fn is_unique_violation(err: &sqlx::Error) -> bool;

    /// Encode a [`SystemTime`] into this backend's stored representation.
    fn timestamp_value(timestamp: SystemTime) -> Result<Self::TimestampValue, RepositoryError>;

    /// Push a timestamp value into a separated bind list (Postgres wraps the
    /// bind in `to_timestamp(...)`).
    fn push_timestamp(sep: &mut Separated<'_, Self, &'static str>, value: &Self::TimestampValue);

    /// Push an optional timestamp value into a separated bind list, binding a
    /// typed `NULL` when absent.
    fn push_optional_timestamp(
        sep: &mut Separated<'_, Self, &'static str>,
        value: Option<&Self::TimestampValue>,
    );

    /// Push `<assignment target> = <timestamp>` right-hand side into a builder.
    fn push_timestamp_assign(builder: &mut QueryBuilder<Self>, value: &Self::TimestampValue);

    /// Push `<column> <op> <now>` comparing a stored timestamp column against
    /// epoch seconds (Postgres: `column op to_timestamp($n)`; SQLite:
    /// `CAST(column AS REAL) op ?`).
    fn push_timestamp_cmp(
        builder: &mut QueryBuilder<Self>,
        column: &'static str,
        op: &'static str,
        epoch_secs: f64,
    );

    /// Push a non-transaction-start database clock expression. PostgreSQL uses
    /// `clock_timestamp()` so time spent waiting for a row lock counts against
    /// leases; SQLite uses its subsecond unix epoch clock.
    fn push_command_ledger_now(builder: &mut QueryBuilder<Self>);

    /// Push the same database clock as epoch seconds for decoding into
    /// [`SystemTime`]. This is distinct from [`Self::push_command_ledger_now`]
    /// because PostgreSQL write/comparison expressions require `timestamptz`
    /// while this framework's portable timestamp decoder consumes `f64`.
    fn push_command_ledger_now_epoch(builder: &mut QueryBuilder<Self>);

    /// Push database-now plus a caller-validated positive duration.
    fn push_command_ledger_deadline(builder: &mut QueryBuilder<Self>, duration: Duration);

    /// Push an exact encoded deadline comparison against the same
    /// non-transaction-start database clock used by command leases.
    fn push_command_ledger_deadline_is_live(
        builder: &mut QueryBuilder<Self>,
        deadline: &Self::TimestampValue,
    );

    /// Push a JSON value bind, adding the backend's native JSON cast if needed.
    fn push_command_ledger_json(builder: &mut QueryBuilder<Self>, json: &str);

    /// Decode a stored timestamp column into a [`SystemTime`].
    fn decode_timestamp(
        row: &Self::Row,
        column: &'static str,
    ) -> Result<SystemTime, RepositoryError>;

    /// Decode a nullable stored timestamp column.
    fn decode_optional_timestamp(
        row: &Self::Row,
        column: &'static str,
    ) -> Result<Option<SystemTime>, RepositoryError>;

    /// Push a metadata JSON bind (Postgres casts to `::jsonb`).
    fn push_metadata(sep: &mut Separated<'_, Self, &'static str>, json: &str);

    /// Push an `aggregate_id` filter for an id list (Postgres: `= ANY($n)`
    /// array bind; SQLite: `IN (?, ?, ...)`).
    fn push_id_filter(builder: &mut QueryBuilder<Self>, ids: &[&str]);

    /// Build the consumer-inbox retention `DELETE` for a cutoff age, evaluated
    /// against the database clock.
    fn inbox_purge_query(age: Duration) -> QueryBuilder<Self>;

    /// Claim up to `batch_size` outbox messages. This is the one genuinely
    /// divergent operation: Postgres uses a CTE with `FOR UPDATE SKIP LOCKED`;
    /// SQLite (no row locks) scans candidates and claims them with per-id
    /// conditional updates.
    fn claim_outbox<'a>(
        pool: &'a Pool<Self>,
        request: ClaimOutboxMessages,
    ) -> impl Future<Output = Result<Vec<OutboxMessage>, RepositoryError>> + Send + 'a;
}
