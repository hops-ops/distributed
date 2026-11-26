//! SQL statements per store operation, pinned so a round trip cannot be added
//! to a hot path unnoticed.
//!
//! SQLx reports every statement it executes as a `sqlx::query` tracing event
//! at the connection's `log_statements` level. A counted pool logs at TRACE,
//! which nothing else in this test binary does, and one process-wide
//! subscriber records exactly those events. Counted pools hold one
//! connection: a statement is reported when it finishes, so a marker
//! statement on the same connection proves every earlier one was recorded.
//!
//! `BEGIN`, `COMMIT` and `ROLLBACK` are not counted (SQLite runs them without
//! the logger), nor is the query the Postgres pool runs when a connection is
//! released. Each operation therefore costs two or three round trips more
//! than its count on Postgres.

use std::future::Future;
use std::sync::{Mutex, Once};
use std::time::Duration;

use sqlx::ConnectOptions;
use tracing::field::{Field, Visit};

use crate::command_ledger::{
    CanonicalInputHash, CausalCommitBatch, CausalTransactionalCommit, CommandContractFingerprint,
    CommandId, CommandLedgerKey, CommandLedgerStore, CommandReservation, PrincipalPartitionId,
    ReservationOutcome, TerminalCommandState,
};
use crate::entity::Entity;
use crate::outbox::OutboxMessage;
use crate::projection_protocol::scenario_tests::scenario_schema;
use crate::projection_protocol::{
    PartitionId, ProjectionCommitBatch, ProjectionEpoch, ProjectionExecutionSnapshotBatchRequest,
    ProjectionGeneration, ProjectionInputCursor, ProjectionInputFingerprint,
    ProjectionModelOwnership, ProjectionObservationKind, ProjectionObservationRequest,
    ProjectionObservationTarget, ProjectionProtocolStore, ProjectionQuerySnapshotRequest,
    ProjectionRecordExpectation, ProjectionRecordMutation, ProjectionScopeCodec, ProjectionSource,
    ProjectorTopologyId, RecordTransition, SameTransactionProjectionBatch, TrustedProjectionInput,
};
use crate::repository::{CommitBatch, ReadModelWritePlanStore, StreamIdentity, StreamWrite};
use crate::table::{
    ExpectedVersion, RowKey, RowValue, RowValues, RowWriteMode, TableMutation, TableRowMutation,
    TableSchema, TableSchemaRegistry, TableWritePlan,
};

static RECORDED: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Held by a test for as long as it counts: the record is process-wide, and
/// a counted pool records everything it runs, its own migration included.
/// It serializes counting tests of one process only; what they count must
/// not depend on other tests, so a Postgres counted pool has a schema of its
/// own.
pub(crate) static COUNTING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
/// Proof that the caller holds [`COUNTING`]; a counted pool cannot be made
/// without it.
pub(crate) type Counting<'a> = tokio::sync::MutexGuard<'a, ()>;

const MARKER: &str = "SELECT 'statement count marker'";

struct StatementRecorder;

#[derive(Default)]
struct StatementText {
    summary: String,
    statement: String,
}

impl Visit for StatementText {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "summary" => self.summary = value.to_string(),
            "db.statement" => self.statement = value.to_string(),
            _ => {}
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

impl tracing::Subscriber for StatementRecorder {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "sqlx::query" && *metadata.level() == tracing::Level::TRACE
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut text = StatementText::default();
        event.record(&mut text);
        // SQLx puts a statement it had to shorten in `db.statement`.
        let sql = if text.statement.trim().is_empty() {
            text.summary
        } else {
            text.statement
        };
        RECORDED
            .lock()
            .unwrap()
            .push(sql.split_whitespace().collect::<Vec<_>>().join(" "));
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

/// Connection options whose statements are counted.
pub(crate) fn counted<O: ConnectOptions>(options: O) -> O {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        tracing::subscriber::set_global_default(StatementRecorder)
            .expect("the statement recorder is this test binary's only global subscriber");
    });
    options
        .log_statements(log::LevelFilter::Trace)
        .log_slow_statements(log::LevelFilter::Trace, Duration::from_secs(3600))
}

/// The statements `operation` runs on `pool`, a one-connection counted pool.
/// The caller holds [`COUNTING`].
pub(crate) async fn statements<DB, T>(
    pool: &sqlx::Pool<DB>,
    operation: impl Future<Output = T>,
) -> (T, Vec<String>)
where
    DB: sqlx::Database,
    for<'c> &'c sqlx::Pool<DB>: sqlx::Executor<'c, Database = DB>,
{
    let marker = || async {
        sqlx::raw_sql(MARKER).execute(pool).await.unwrap();
    };
    marker().await;
    RECORDED.lock().unwrap().clear();
    let output = operation.await;
    marker().await;
    let recorded = std::mem::take(&mut *RECORDED.lock().unwrap());
    let statements = recorded
        .into_iter()
        .take_while(|sql| sql != MARKER)
        .filter(|sql| {
            !["BEGIN", "COMMIT", "ROLLBACK"]
                .iter()
                .any(|control| sql.starts_with(control))
                && sql != "SELECT now() <> statement_timestamp()"
        })
        .collect();
    (output, statements)
}

/// Fail with the whole breakdown when `statements` exceeds `bound`.
#[track_caller]
pub(crate) fn assert_at_most(operation: &str, statements: &[String], bound: usize) {
    eprintln!("{operation}: {} statements", statements.len());
    for (index, sql) in statements.iter().enumerate() {
        let short: String = sql.chars().take(150).collect();
        eprintln!("  {:>2}. {short}", index + 1);
    }
    assert!(
        statements.len() <= bound,
        "{operation} ran {} statements, more than the pinned {bound}:\n{}",
        statements.len(),
        statements.join("\n")
    );
}

/// Pinned counts for one backend: the measured count of each operation.
///
/// SQLite's counts are below. Postgres adds `pg_notify` to every commit that
/// changes a read model, and its read-model commits lock the table fence and
/// read the causal marker as two statements where SQLite needs no lock.
struct Bounds {
    /// One event projected into one model: one row upsert and one record
    /// observation. 22 = partition lock upsert, generation check, source
    /// capability, input identity by cursor and by message, receipt by cursor
    /// and by message, current cursor, inbox check, partition ownership,
    /// record read, observation read, physical row version, row upsert,
    /// change insert, record upsert, observation insert, cursor upsert,
    /// identity insert, receipt insert, inbox insert, partition head update.
    /// More rows add a version read and an upsert each; nothing else grows.
    projection_steady: usize,
    /// The first event of a store and partition adds five: the registered
    /// topology and model (each read once per store), the first generation,
    /// the source capability and the partition's model ownership.
    projection_first: usize,
    /// One event, one outbox message and the ledger completion. 7 = ledger
    /// fence, ledger read, ledger clock, stream version, event insert, outbox
    /// insert, ledger completion.
    command: usize,
    /// One raw read-model row on its own: marker read and row upsert, plus
    /// the fence lock and `pg_notify` on Postgres.
    read_model_plan: usize,
    /// The command with a same-transaction projection of one row instead.
    /// 15 = the 3 ledger reads, partition lock upsert, generation check,
    /// partition ownership, record read, physical row version, observation
    /// read, row upsert, change insert, record upsert, observation insert,
    /// partition head update, ledger completion; no event or outbox message.
    direct_projection: usize,
    /// One statement however many rows of one table the batch names.
    snapshot_batch_1: usize,
    snapshot_batch_10: usize,
}

struct Fixture {
    topology: ProjectorTopologyId,
    owned: &'static TableSchema,
    raw: &'static TableSchema,
}

impl Fixture {
    fn new() -> Self {
        // A topology is identified by its digest, and Postgres keeps earlier
        // runs.
        let id = uuid::Uuid::now_v7();
        let mut digest = [23; 32];
        digest[..16].copy_from_slice(id.as_bytes());
        let unique = id.simple();
        Self {
            topology: ProjectorTopologyId::new(1, "statement_count", digest).unwrap(),
            owned: scenario_schema("StatementCountView", format!("statement_count_{unique}")),
            raw: scenario_schema("StatementCountRaw", format!("statement_count_raw_{unique}")),
        }
    }

    fn registry(&self) -> TableSchemaRegistry {
        let mut registry = TableSchemaRegistry::new();
        registry.register_schema(self.owned.clone()).unwrap();
        registry.register_schema(self.raw.clone()).unwrap();
        registry
    }

    fn ownership(&self) -> ProjectionModelOwnership {
        ProjectionModelOwnership::new(&self.owned.model_name, &self.owned.table_name).unwrap()
    }

    fn codec(&self) -> ProjectionScopeCodec {
        ProjectionScopeCodec::with_models(
            self.topology.clone(),
            [(self.owned.model_name.as_str(), self.owned)],
        )
        .unwrap()
    }

    fn partition(&self) -> PartitionId {
        self.codec()
            .encode_partition(Some(&serde_json::json!("tenant")))
            .unwrap()
    }

    fn upsert(schema: &'static TableSchema, id: &str, value: &str) -> TableMutation {
        let mut values = RowValues::new();
        values.insert("id", RowValue::String(id.into()));
        values.insert("value", RowValue::String(value.into()));
        TableMutation::UpsertRow(TableRowMutation {
            schema,
            key: key(id),
            values,
            expected_version: ExpectedVersion::Any,
            mode: RowWriteMode::Upsert,
        })
    }

    /// One projected event: one row upsert of one model and the record
    /// observation the projector stages with it.
    fn projection_batch(
        &self,
        position: u64,
        id: &str,
        expectation: ProjectionRecordExpectation,
    ) -> ProjectionCommitBatch {
        let scope = self
            .codec()
            .encode_row_scope_in_partition(&self.owned.model_name, self.partition(), &key(id))
            .unwrap();
        ProjectionCommitBatch {
            input: TrustedProjectionInput::mint(
                ProjectionInputCursor::new(
                    self.topology.clone(),
                    self.partition(),
                    ProjectionSource::new("todo_stream", b"todo".to_vec()).unwrap(),
                    ProjectionEpoch::new("source-v1").unwrap(),
                    position,
                )
                .unwrap(),
                ProjectionInputFingerprint::from_canonical_bytes(&position.to_be_bytes()),
                format!("message-{position}"),
                format!("cause-{position}"),
                ProjectionGeneration::initial(),
                true,
            )
            .unwrap(),
            ownership: vec![self.ownership()],
            mutations: vec![ProjectionRecordMutation::new(
                scope.clone(),
                Self::upsert(self.owned, id, &format!("value-{position}")),
                expectation,
                RecordTransition::Upsert,
            )
            .unwrap()],
            observations: vec![ProjectionObservationRequest {
                kind: ProjectionObservationKind::Record,
                target: ProjectionObservationTarget::StagedRecord(scope),
            }],
        }
    }

    fn snapshot_request(&self, ids: &[String]) -> ProjectionExecutionSnapshotBatchRequest {
        ProjectionExecutionSnapshotBatchRequest::new(
            ids.iter()
                .map(|id| {
                    ProjectionQuerySnapshotRequest::new(
                        &self.codec(),
                        Some(&serde_json::json!("tenant")),
                        &self.owned.model_name,
                        key(id),
                        Vec::new(),
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap()
    }
}

fn key(id: &str) -> RowKey {
    RowKey::new([("id", RowValue::String(id.into()))])
}

async fn reserve<R: CommandLedgerStore>(store: &R) -> crate::command_ledger::CommandAttempt {
    let reservation = CommandReservation::new(
        CommandLedgerKey::new(
            "statement-count",
            PrincipalPartitionId::new("tenant:count").unwrap(),
            CommandId::parse(uuid::Uuid::now_v7().hyphenated().to_string()).unwrap(),
        )
        .unwrap(),
        "count",
        CommandContractFingerprint::new([61; 32]),
        CanonicalInputHash::new([62; 32]),
        Duration::from_secs(30),
        Duration::from_secs(3600),
    )
    .unwrap();
    match store.reserve_command(reservation).await.unwrap() {
        ReservationOutcome::Acquired(attempt) => attempt,
        _ => panic!("a fresh command reservation acquires its first attempt"),
    }
}

fn succeeded(
    attempt: &crate::command_ledger::CommandAttempt,
) -> crate::command_ledger::CommandCompletion<'_> {
    attempt
        .complete(
            TerminalCommandState::Succeeded,
            serde_json::json!({"counted": true}),
            Duration::from_secs(3600),
        )
        .unwrap()
}

/// One event of a new stream and one outbox message.
fn domain_write(entity: &mut Entity) -> CommitBatch<'_> {
    let identity = StreamIdentity::new("statement-count", entity.id()).unwrap();
    entity.digest_empty("StatementCounted").unwrap();
    let mut domain = CommitBatch::new(vec![StreamWrite::new(identity, entity)]);
    domain.outbox_messages.push(
        OutboxMessage::create(
            format!("outbox-{}", uuid::Uuid::now_v7()),
            "StatementCounted",
            b"{}".to_vec(),
        )
        .unwrap(),
    );
    domain
}

async fn measure<DB, R>(pool: &sqlx::Pool<DB>, store: &R, fixture: &Fixture, bounds: &Bounds)
where
    DB: sqlx::Database,
    for<'c> &'c sqlx::Pool<DB>: sqlx::Executor<'c, Database = DB>,
    R: ProjectionProtocolStore
        + CommandLedgerStore
        + CausalTransactionalCommit
        + ReadModelWritePlanStore,
{
    // The partition's first event also creates the partition, its first
    // generation, its model ownership and the source capability.
    let (first, counted) = statements(
        pool,
        store.commit_projection(fixture.projection_batch(
            1,
            "todo-1",
            ProjectionRecordExpectation::Missing,
        )),
    )
    .await;
    let first = first.unwrap();
    assert_at_most(
        "projection commit, first event",
        &counted,
        bounds.projection_first,
    );

    let (second, counted) = statements(
        pool,
        store.commit_projection(fixture.projection_batch(
            2,
            "todo-1",
            ProjectionRecordExpectation::Exact(first.records[0].revision.clone()),
        )),
    )
    .await;
    second.unwrap();
    assert_at_most(
        "projection commit, steady state",
        &counted,
        bounds.projection_steady,
    );

    let mut entity = Entity::with_id(format!("stream-{}", uuid::Uuid::now_v7()));
    let attempt = reserve(store).await;
    let completion = succeeded(&attempt);
    let (committed, counted) = statements(
        pool,
        store.commit_causal_batch(CausalCommitBatch::new(
            domain_write(&mut entity),
            completion,
        )),
    )
    .await;
    committed.unwrap();
    assert_at_most("command commit", &counted, bounds.command);

    // A table's first raw write also creates its ownership fence.
    store
        .commit_write_plan(TableWritePlan::new(vec![Fixture::upsert(
            fixture.raw,
            "raw-0",
            "zero",
        )]))
        .await
        .unwrap();

    let (committed, counted) = statements(
        pool,
        store.commit_write_plan(TableWritePlan::new(vec![Fixture::upsert(
            fixture.raw,
            "raw-2",
            "two",
        )])),
    )
    .await;
    committed.unwrap();
    assert_at_most("read-model write plan", &counted, bounds.read_model_plan);

    let attempt = reserve(store).await;
    let direct = SameTransactionProjectionBatch::single_upsert(
        fixture.topology.clone(),
        fixture.partition(),
        fixture.ownership(),
        fixture
            .codec()
            .encode_row_scope_in_partition(
                &fixture.owned.model_name,
                fixture.partition(),
                &key("direct-1"),
            )
            .unwrap(),
        Fixture::upsert(fixture.owned, "direct-1", "direct"),
        attempt.causation_id().as_str(),
    )
    .unwrap();
    let completion = attempt
        .complete(
            TerminalCommandState::Atomic,
            serde_json::json!({"counted": true}),
            Duration::from_secs(3600),
        )
        .unwrap();
    let (committed, counted) = statements(
        pool,
        store.commit_causal_batch(CausalCommitBatch::with_direct_projection(
            CommitBatch::empty(),
            completion,
            direct,
        )),
    )
    .await;
    committed.unwrap();
    assert_at_most(
        "command commit with a same-transaction projection",
        &counted,
        bounds.direct_projection,
    );

    let ids = (0..10)
        .map(|index| format!("snapshot-{index}"))
        .collect::<Vec<_>>();
    for (index, id) in ids.iter().enumerate() {
        store
            .commit_projection(fixture.projection_batch(
                3 + index as u64,
                id,
                ProjectionRecordExpectation::Missing,
            ))
            .await
            .unwrap();
    }
    for (rows, bound) in [(1, bounds.snapshot_batch_1), (10, bounds.snapshot_batch_10)] {
        let request = fixture.snapshot_request(&ids[..rows]);
        let (batch, counted) =
            statements(pool, store.projection_execution_snapshot_batch(&request)).await;
        let batch = batch.unwrap();
        assert_eq!(batch.snapshots.len(), rows);
        assert!(batch
            .snapshots
            .iter()
            .all(|snapshot| snapshot.row.is_some() && snapshot.record.is_some()));
        assert_at_most(
            &format!("execution snapshot batch of {rows}"),
            &counted,
            bound,
        );
    }
}

#[cfg(feature = "sqlite")]
pub(crate) async fn counted_sqlite_pool(_counting: &Counting<'_>) -> sqlx::SqlitePool {
    use std::str::FromStr;
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(counted(
            sqlx::sqlite::SqliteConnectOptions::from_str("sqlite::memory:").unwrap(),
        ))
        .await
        .unwrap();
    crate::SqliteRepository::migrate_pool(&pool).await.unwrap();
    pool
}

/// A counted pool on a fresh schema (`prefix`) of the Postgres test database,
/// or `None` without one. Keep the schema alive as long as the pool.
///
/// A schema of its own makes the count independent of what other tests,
/// in this process or another, did to the shared database: every count is a
/// first run on empty tables.
#[cfg(feature = "postgres")]
pub(crate) async fn counted_postgres_pool(
    _counting: &Counting<'_>,
    var: &str,
    prefix: &str,
) -> Option<(common::postgres::PostgresTestSchema, sqlx::PgPool)> {
    use crate::sqlx_repo::repo::SqlxRepoBackend;
    use std::str::FromStr;
    let (schema, url) = crate::sqlx_repo::postgres_test_schema::postgres_test_schema(
        var,
        "Postgres statement count",
        prefix,
    )
    .await?;
    let pool =
        sqlx::Postgres::pool_options(sqlx::postgres::PgPoolOptions::new().max_connections(1))
            .connect_with(counted(
                sqlx::postgres::PgConnectOptions::from_str(&url).unwrap(),
            ))
            .await
            .unwrap();
    crate::PostgresRepository::migrate_pool(&pool)
        .await
        .unwrap();
    Some((schema, pool))
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_statements_per_operation_are_pinned() {
    let counting = COUNTING.lock().await;
    let pool = counted_sqlite_pool(&counting).await;
    let fixture = Fixture::new();
    let store = crate::SqliteRepository::new(pool.clone());
    store
        .bootstrap_table_schema_for_dev(&fixture.registry())
        .await
        .unwrap();
    store
        .register_projection_models(&fixture.topology, &[fixture.ownership()])
        .await
        .unwrap();
    measure(
        &pool,
        &store,
        &fixture,
        &Bounds {
            projection_first: 27,
            projection_steady: 22,
            command: 7,
            read_model_plan: 3,
            direct_projection: 15,
            snapshot_batch_1: 1,
            snapshot_batch_10: 1,
        },
    )
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_statements_per_operation_are_pinned() {
    let counting = COUNTING.lock().await;
    let Some((_schema, pool)) = counted_postgres_pool(
        &counting,
        "DISTRIBUTED_TEST_POSTGRES_URL",
        "statement_count",
    )
    .await
    else {
        return;
    };
    let fixture = Fixture::new();
    let store = crate::PostgresRepository::new(pool.clone());
    store
        .bootstrap_table_schema_for_dev(&fixture.registry())
        .await
        .unwrap();
    store
        .register_projection_models(&fixture.topology, &[fixture.ownership()])
        .await
        .unwrap();
    measure(
        &pool,
        &store,
        &fixture,
        &Bounds {
            projection_first: 28,
            projection_steady: 23,
            command: 7,
            read_model_plan: 4,
            direct_projection: 16,
            snapshot_batch_1: 1,
            snapshot_batch_10: 1,
        },
    )
    .await;
}

/// A store reads a registration once and remembers it; it never remembers a
/// refusal, and another store reads for itself.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_registration_is_verified_once_per_store_and_a_refusal_is_not_remembered() {
    let counting = COUNTING.lock().await;
    let pool = counted_sqlite_pool(&counting).await;
    let fixture = Fixture::new();
    let store = crate::SqliteRepository::new(pool.clone());
    store
        .bootstrap_table_schema_for_dev(&fixture.registry())
        .await
        .unwrap();
    let batch = |position, expectation| fixture.projection_batch(position, "todo-1", expectation);
    let registration_reads = |statements: &[String]| {
        statements
            .iter()
            .filter(|sql| sql.contains("FROM projection_registered_models"))
            .count()
    };

    let refused = store
        .commit_projection(batch(1, ProjectionRecordExpectation::Missing))
        .await;
    assert!(
        matches!(
            refused,
            Err(crate::projection_protocol::ProjectionProtocolError::InvalidBatch(_))
        ),
        "{refused:?}"
    );
    store
        .register_projection_models(&fixture.topology, &[fixture.ownership()])
        .await
        .unwrap();

    let (first, counted) = statements(
        &pool,
        store.commit_projection(batch(1, ProjectionRecordExpectation::Missing)),
    )
    .await;
    let first = first.unwrap();
    assert_eq!(registration_reads(&counted), 2, "topology and model");
    let (second, counted) = statements(
        &pool,
        store.clone().commit_projection(batch(
            2,
            ProjectionRecordExpectation::Exact(first.records[0].revision.clone()),
        )),
    )
    .await;
    let second = second.unwrap();
    assert_eq!(registration_reads(&counted), 0);

    let other = crate::SqliteRepository::new(pool.clone());
    let (third, counted) = statements(
        &pool,
        other.commit_projection(batch(
            3,
            ProjectionRecordExpectation::Exact(second.records[0].revision.clone()),
        )),
    )
    .await;
    third.unwrap();
    assert_eq!(registration_reads(&counted), 2);
}
