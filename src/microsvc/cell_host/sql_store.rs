//! Cell-local execution of the ordinary SQL event/snapshot/receipt repository.
//! No in-memory working copy and no whole-cell export are used here.
//!
//! Shared by both cell executors: Durable Object `storage.sql` on
//! `workers-rs`/wasm32 and a synchronous rusqlite file on native
//! `cell-sqlite`. Each executor supplies only `read` and a synchronous
//! all-or-nothing `transaction`; schema bootstrap, the command commit, ledger
//! fencing and outbox leases below are identical on both targets.

use std::future::Future;
use std::task::{Context, Poll, Waker};

use sha2::{Digest, Sha256};
#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
use worker::State;

use super::sql_executor::CellSqlConnection;
use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalGetStream, CausalRepositoryIdentity,
    CausalStorageIdentity, CausalTransactionalCommit, CommandCompletion, CommandLedgerError,
    CommandLedgerKey, CommandLedgerStore, CommandLookup, CommandLookupScope, CommandReservation,
    ExternalCommandCompletion, ReservationOutcome,
};
use crate::entity::Entity;
use crate::microsvc::HasOutboxStore;
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::{ClaimOutboxMessages, OutboxBacklogStats, OutboxClaimRef, OutboxStore};
use crate::repository::sql::{self, ledger, outbox, SqlExecutor, SqlRow, Statement};
use crate::repository::{
    validate_commit_batch, CommitBatch, GetStream, RepositoryError, SnapshotStore, SnapshotWrite,
    StreamIdentity, TransactionalCommit,
};
use crate::snapshot::SnapshotRecord;

use super::alarm::{AlarmSettlement, ArmedCellAlarm};

// Durable Object SQL limits bindings per statement, not per transaction.
// Both shared insert planners split large commands within the same transaction.
const MAX_SQL_BIND_PARAMS: usize = 100;

/// Shared SQL operations are async for SQLx, but every call through a cell
/// executor completes synchronously. Refuse accidental async work; never spin
/// or let a pending future outlive the transaction callback.
pub(super) fn finish_sql<T, E: From<RepositoryError>>(
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => Err(RepositoryError::Model(
            "cell SQL transaction attempted to await asynchronous work".into(),
        )
        .into()),
    }
}

#[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
use super::store::SealedRowReducer;

#[derive(Clone)]
pub(super) struct CellSqlRepository {
    connection: CellSqlConnection,
    identity: CausalStorageIdentity,
    #[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
    sealed_row_reducer: Option<SealedRowReducer>,
}

impl CellSqlRepository {
    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    pub fn from_state(state: State) -> Result<Self, RepositoryError> {
        let connection = CellSqlConnection::from_state(state)?;
        Self::initialize(&connection)?;
        Ok(Self {
            connection,
            identity: CausalStorageIdentity::new(),
        })
    }

    /// Open one native cell file. The file is bound to `instance_name` on
    /// first open; reopening it as another cell fails closed.
    #[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
    pub fn open(path: &std::path::Path, instance_name: &str) -> Result<Self, RepositoryError> {
        let connection = CellSqlConnection::open(path)?;
        Self::initialize(&connection)?;
        connection
            .transaction(|executor| finish_sql(native::bind_instance(executor, instance_name)))?;
        Ok(Self {
            connection,
            identity: CausalStorageIdentity::new(),
            sealed_row_reducer: None,
        })
    }

    #[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
    pub fn set_sealed_row_reducer(&mut self, reducer: SealedRowReducer) {
        self.sealed_row_reducer = Some(reducer);
    }

    #[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
    pub fn sealed_row(&self) -> Result<Option<serde_json::Value>, RepositoryError> {
        self.connection
            .read(|executor| finish_sql(native::sealed_row(executor)))
    }

    #[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
    pub fn replace_sealed_row(&self, row: &serde_json::Value) -> Result<(), RepositoryError> {
        self.connection
            .transaction(|executor| finish_sql(native::write_sealed_row(executor, row)))
    }

    fn initialize(connection: &CellSqlConnection) -> Result<(), RepositoryError> {
        connection.transaction(|executor| finish_sql(async {
            let tables = executor.query(Statement::new("SELECT name FROM sqlite_master WHERE type = 'table'")).await?;
            let tables = tables.iter().map(|row| row.text("name")).collect::<Result<Vec<_>, _>>()?;
            if tables.iter().any(|name| name == "cell_state") {
                return Err(RepositoryError::Model("whole-state cell storage requires an explicit migration before opening this version".into()));
            }
            let registered = tables.iter().any(|name| name == "__distributed_cell_migrations");
            if !registered && tables.iter().any(|name| matches!(name.as_str(), "aggregate_events" | "aggregate_snapshots" | "command_ledger" | "outbox_messages")) {
                return Err(RepositoryError::Model("unregistered cell SQL schema requires an explicit migration".into()));
            }
            executor.execute(Statement::new("CREATE TABLE IF NOT EXISTS __distributed_cell_migrations (version INTEGER PRIMARY KEY, checksum TEXT NOT NULL)")).await?;
            let applied = executor.query(Statement::new("SELECT version, checksum FROM __distributed_cell_migrations ORDER BY version")).await?;
            let migrations = crate::repository::migrations::cell_migrations().collect::<Vec<_>>();
            if applied.len() > migrations.len() {
                return Err(RepositoryError::Model("cell SQL schema is newer than this runtime".into()));
            }
            for (row, migration) in applied.iter().zip(&migrations) {
                let checksum = format!("{:x}", Sha256::digest(migration.sql.as_bytes()));
                if row.integer("version")? != migration.version || row.text("checksum")? != checksum {
                    return Err(RepositoryError::Model("cell SQL migration history differs from this runtime".into()));
                }
            }
            for migration in migrations.iter().skip(applied.len()) {
                // Execute the original validated migration as a whole; never
                // split SQL on semicolons or maintain a parallel schema copy.
                executor.execute(Statement::new(migration.sql)).await?;
                let checksum = format!("{:x}", Sha256::digest(migration.sql.as_bytes()));
                executor.execute(Statement::new("INSERT INTO __distributed_cell_migrations (version, checksum) VALUES (")
                    .bind(migration.version.into()).sql(", ").bind(checksum.as_str().into()).sql(")")).await?;
            }
            Ok::<_, RepositoryError>(())
        }))
    }

    fn commit(
        &self,
        batch: CommitBatch<'_>,
        completion: Option<CommandCompletion>,
    ) -> Result<(), CommandLedgerError> {
        if !batch.read_model_plans.is_empty() || !batch.inbox_receipts.is_empty() {
            return Err(CommandLedgerError::Invalid(
                "aggregate cells accept command effects, not projection or consumer-inbox writes"
                    .into(),
            ));
        }
        let prepared = validate_commit_batch(&batch)?;
        super::alarm::validate_alarm_writes(&batch.alarms)?;
        self.connection.transaction(|executor| {
            finish_sql(async {
                if let Some(completion) = &completion {
                    ledger::preflight(executor, completion).await?;
                }
                for append in &prepared {
                    let actual = sql::stream_version(executor, &append.identity).await?;
                    if actual != append.expected_version {
                        return Err(RepositoryError::ConcurrentWrite {
                            id: append.identity.to_string(),
                            expected: append.expected_version,
                            actual,
                        }
                        .into());
                    }
                }
                for insert in sql::event_inserts(&prepared, MAX_SQL_BIND_PARAMS)? {
                    executor.execute(insert.statement).await?;
                }
                // SQL binding errors in this runtime have no structured constraint
                // code. Check identities within the same synchronous transaction,
                // where another request cannot insert between this read and write.
                for message in &batch.outbox_messages {
                    let existing = executor
                        .query(
                            Statement::new(
                                "SELECT message_id FROM outbox_messages WHERE message_id = ",
                            )
                            .bind(message.id().into()),
                        )
                        .await?;
                    if !existing.is_empty() {
                        return Err(RepositoryError::DuplicateOutboxMessageInBatch {
                            id: message.id().into(),
                        }
                        .into());
                    }
                }
                for insert in outbox::inserts(&batch.outbox_messages, MAX_SQL_BIND_PARAMS)? {
                    executor.execute(insert.statement).await?;
                }
                for snapshot in &batch.snapshots {
                    let SnapshotWrite::Save { identity, record } = snapshot;
                    sql::save_snapshot(executor, identity, record).await?;
                }
                // Alarm rows join the same transaction: a failed command (or a
                // lost ledger lease below) leaves no alarm armed.
                if !batch.alarms.is_empty() {
                    alarms::apply(executor, &batch.alarms).await?;
                }
                // Native sealed read-model row: same transaction, so a reducer
                // error or lost ledger lease rolls the row back with the events.
                #[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
                if let Some(reducer) = &self.sealed_row_reducer {
                    if prepared.iter().any(|append| !append.events.is_empty()) {
                        let current = native::sealed_row(executor).await?;
                        if let Some(next) = reducer(current.as_ref(), &prepared)? {
                            if current.as_ref() != Some(&next) {
                                native::write_sealed_row(executor, &next).await?;
                            }
                        }
                    }
                }
                // Same final fenced write as SQLx. A lost lease rolls back every
                // participant, and entities are marked committed only afterwards.
                if let Some(completion) = &completion {
                    ledger::complete(executor, completion).await?;
                }
                Ok::<_, CommandLedgerError>(())
            })
        })?;
        for stream in batch.streams {
            stream.entity.mark_committed();
        }
        Ok(())
    }
}

impl CellSqlRepository {
    /// Armed alarms, earliest first.
    pub(super) fn armed_alarms(&self) -> Result<Vec<ArmedCellAlarm>, RepositoryError> {
        self.connection
            .read(|executor| finish_sql(alarms::armed(executor, None, usize::MAX)))
    }

    /// Armed alarms due at or before `now_ms`, earliest first.
    pub(super) fn due_alarms(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<Vec<ArmedCellAlarm>, RepositoryError> {
        self.connection
            .read(|executor| finish_sql(alarms::armed(executor, Some(now_ms), limit)))
    }

    /// Due time of the earliest armed alarm.
    pub(super) fn next_alarm_due_ms(&self) -> Result<Option<i64>, RepositoryError> {
        self.connection
            .read(|executor| finish_sql(alarms::next_due(executor)))
    }

    /// Settle the fired generation of `name`; a newer arm is left untouched.
    pub(super) fn settle_alarm(
        &self,
        name: &str,
        generation: u64,
        settlement: &AlarmSettlement,
    ) -> Result<(), RepositoryError> {
        self.connection.transaction(|executor| {
            finish_sql(alarms::settle(executor, name, generation, settlement))
        })
    }
}

impl GetStream for CellSqlRepository {
    async fn get_stream(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<Entity>, RepositoryError> {
        self.connection
            .read(|executor| finish_sql(sql::load_stream(executor, identity, None)))
    }
    async fn get_stream_tail(
        &self,
        identity: &StreamIdentity,
        after_version: u64,
    ) -> Result<Option<Entity>, RepositoryError> {
        self.connection
            .read(|executor| finish_sql(sql::load_stream(executor, identity, Some(after_version))))
    }
}
impl CausalGetStream for CellSqlRepository {
    fn get_causal_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl std::future::Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a
    {
        GetStream::get_stream_tail(self, identity, after_version)
    }
    async fn get_causal_stream(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<Entity>, RepositoryError> {
        self.get_stream(identity).await
    }
}
impl SnapshotStore for CellSqlRepository {
    async fn get_snapshot(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<SnapshotRecord>, RepositoryError> {
        self.connection
            .read(|executor| finish_sql(sql::load_snapshot(executor, identity)))
    }
    async fn save_snapshot(
        &self,
        identity: &StreamIdentity,
        record: SnapshotRecord,
    ) -> Result<(), RepositoryError> {
        self.connection
            .transaction(|executor| finish_sql(sql::save_snapshot(executor, identity, &record)))
    }
    async fn delete_snapshot(&self, identity: &StreamIdentity) -> Result<bool, RepositoryError> {
        self.connection
            .transaction(|executor| finish_sql(sql::delete_snapshot(executor, identity)))
    }
}
impl CausalRepositoryIdentity for CellSqlRepository {
    fn causal_storage_identity(&self) -> CausalStorageIdentity {
        self.identity
    }
}
impl CommandLedgerStore for CellSqlRepository {
    async fn reserve_command(
        &self,
        reservation: CommandReservation,
    ) -> Result<ReservationOutcome, CommandLedgerError> {
        self.connection
            .transaction(|executor| finish_sql(ledger::reserve(executor, &reservation)))
    }
    async fn lookup_command(
        &self,
        key: &CommandLedgerKey,
        scope: CommandLookupScope<'_>,
    ) -> Result<CommandLookup, CommandLedgerError> {
        self.connection
            .transaction(|executor| finish_sql(ledger::lookup(executor, key, scope)))
    }
    async fn mark_retryable_unknown(
        &self,
        attempt: AttemptFence,
    ) -> Result<(), CommandLedgerError> {
        self.connection
            .transaction(|executor| finish_sql(ledger::mark_retryable(executor, &attempt)))
    }

    async fn complete_external_command(
        &self,
        completion: ExternalCommandCompletion,
    ) -> Result<(), CommandLedgerError> {
        self.connection
            .transaction(|executor| finish_sql(ledger::complete_external(executor, &completion)))
    }
    async fn compact_expired_commands(&self, limit: usize) -> Result<u64, CommandLedgerError> {
        self.connection
            .transaction(|executor| finish_sql(ledger::compact(executor, limit)))
    }
}
impl TransactionalCommit for CellSqlRepository {
    async fn commit_batch(&self, batch: CommitBatch<'_>) -> Result<(), RepositoryError> {
        self.commit(batch, None).map_err(|error| match error {
            CommandLedgerError::Storage(error) => error,
            error => RepositoryError::Model(error.to_string()),
        })
    }
}
impl CausalTransactionalCommit for CellSqlRepository {
    async fn commit_causal_batch(
        &self,
        batch: CausalCommitBatch<'_>,
    ) -> Result<(), CommandLedgerError> {
        if batch.direct_projection.is_some() {
            return Err(CommandLedgerError::Invalid(
                "aggregate cells do not execute read-model projections".into(),
            ));
        }
        self.commit(batch.domain, Some(batch.completion))
    }
}

#[derive(Clone)]
pub struct CellSqlOutboxStore {
    connection: CellSqlConnection,
}

impl HasOutboxStore for CellSqlRepository {
    type OutboxStore = CellSqlOutboxStore;
    fn outbox_store(&self) -> Self::OutboxStore {
        CellSqlOutboxStore {
            connection: self.connection.clone(),
        }
    }
}

impl OutboxStore for CellSqlOutboxStore {
    async fn messages_by_status(
        &self,
        status: OutboxMessageStatus,
        limit: usize,
    ) -> Result<Vec<OutboxMessage>, RepositoryError> {
        // Match the native no-practical-bound convention within JS's exact
        // integer range; never round a bound crossing the runtime binding.
        let limit = (limit as u64).min(9_007_199_254_740_991) as i64;
        let rows = self.connection.read(|executor| {
            finish_sql(
                executor.query(
                    Statement::new("SELECT ")
                        .sql(crate::repository::sqlite_codec::OUTBOX_SELECT)
                        .sql(" FROM outbox_messages WHERE status = ")
                        .bind(status.as_str().into())
                        .sql(" ORDER BY CAST(created_at AS REAL), message_id LIMIT ")
                        .bind(limit.into()),
                ),
            )
        })?;
        rows.into_iter().map(outbox::from_row).collect()
    }
    async fn backlog_stats(&self) -> Result<OutboxBacklogStats, RepositoryError> {
        let rows = self.connection.read(|executor| finish_sql(executor.query(Statement::new("SELECT COUNT(*) AS pending, MIN(CAST(created_at AS REAL)) AS oldest FROM outbox_messages WHERE status = 'pending'"))))?;
        let row = rows
            .first()
            .ok_or_else(|| RepositoryError::Model("outbox count returned no row".into()))?;
        Ok(OutboxBacklogStats {
            pending: usize::try_from(row.integer("pending")?)
                .map_err(|_| RepositoryError::Model("outbox count is not representable".into()))?,
            oldest_created_at: row.optional_timestamp("oldest")?,
        })
    }
    async fn claim(
        &self,
        request: ClaimOutboxMessages,
    ) -> Result<Vec<OutboxMessage>, RepositoryError> {
        self.connection.transaction(|executor| {
            finish_sql(outbox::claim_sqlite(executor, request, crate::time::now()))
        })
    }
    async fn complete(&self, claim: &OutboxClaimRef) -> Result<(), RepositoryError> {
        self.settle(claim, outbox::Transition::Complete)
    }
    async fn release(&self, claim: &OutboxClaimRef, error: &str) -> Result<(), RepositoryError> {
        self.settle(claim, outbox::Transition::Release(error))
    }
    async fn fail(&self, claim: &OutboxClaimRef, error: &str) -> Result<(), RepositoryError> {
        self.settle(claim, outbox::Transition::Fail(error))
    }
}
impl CellSqlOutboxStore {
    fn settle(
        &self,
        claim: &OutboxClaimRef,
        transition: outbox::Transition<'_>,
    ) -> Result<(), RepositoryError> {
        self.connection.transaction(|executor| {
            finish_sql(outbox::transition(
                executor,
                claim,
                transition,
                crate::time::now(),
            ))
        })
    }
}

/// Native-only cell metadata: the instance binding and the sealed row. Kept
/// out of the shared migration inventory so Durable Object schemas (and their
/// checksummed history) are unchanged by the native store.
#[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
mod native {
    use super::*;
    use serde_json::Value;

    const CREATE: &str = "CREATE TABLE IF NOT EXISTS __distributed_native_cell (id INTEGER PRIMARY KEY CHECK (id = 1), instance_name TEXT NOT NULL, sealed_row TEXT)";

    pub(super) async fn bind_instance(
        executor: &mut impl SqlExecutor,
        instance_name: &str,
    ) -> Result<(), RepositoryError> {
        executor.execute(Statement::new(CREATE)).await?;
        let rows = executor
            .query(Statement::new(
                "SELECT instance_name FROM __distributed_native_cell WHERE id = 1",
            ))
            .await?;
        match rows.first() {
            Some(row) => {
                let bound = row.text("instance_name")?;
                if bound != instance_name {
                    return Err(RepositoryError::Model(format!(
                        "cell SQLite file belongs to `{bound}`, not `{instance_name}`"
                    )));
                }
            }
            None => {
                executor
                    .execute(
                        Statement::new(
                            "INSERT INTO __distributed_native_cell (id, instance_name) VALUES (1, ",
                        )
                        .bind(instance_name.into())
                        .sql(")"),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn sealed_row(
        executor: &mut impl SqlExecutor,
    ) -> Result<Option<Value>, RepositoryError> {
        let rows = executor
            .query(Statement::new(
                "SELECT sealed_row FROM __distributed_native_cell WHERE id = 1",
            ))
            .await?;
        match rows
            .first()
            .map(|row| row.optional_text("sealed_row"))
            .transpose()?
            .flatten()
        {
            Some(json) => serde_json::from_str(&json).map(Some).map_err(|error| {
                RepositoryError::Model(format!("cell sealed row is invalid JSON: {error}"))
            }),
            None => Ok(None),
        }
    }

    pub(super) async fn write_sealed_row(
        executor: &mut impl SqlExecutor,
        row: &Value,
    ) -> Result<(), RepositoryError> {
        let json = serde_json::to_string(row)
            .map_err(|error| RepositoryError::Model(format!("cell sealed row: {error}")))?;
        let written = executor
            .execute(
                Statement::new("UPDATE __distributed_native_cell SET sealed_row = ")
                    .bind(json.as_str().into())
                    .sql(" WHERE id = 1"),
            )
            .await?;
        if written != 1 {
            return Err(RepositoryError::Model(
                "cell SQLite file has no instance binding".into(),
            ));
        }
        Ok(())
    }
}

/// Cell alarm rows (`cell_alarms`, migration 11). Shared by Durable Object and
/// native SQLite cells; the reference host mirrors these semantics in memory.
mod alarms {
    use super::*;
    use crate::microsvc::cell_host::alarm::{
        CellAlarmError, CellAlarmWrite, MAX_ARMED_CELL_ALARMS,
    };

    const SELECT: &str =
        "SELECT name, command, input, due_at_ms, generation, attempts FROM cell_alarms WHERE armed = 1";

    pub(super) async fn apply(
        executor: &mut impl SqlExecutor,
        writes: &[CellAlarmWrite],
    ) -> Result<(), RepositoryError> {
        for write in writes {
            match write {
                CellAlarmWrite::Arm(alarm) => {
                    let input = alarm.input_json()?;
                    executor
                        .execute(
                            Statement::new("INSERT INTO cell_alarms (name, command, input, due_at_ms, generation, attempts, armed) VALUES (")
                                .bind(alarm.name().into())
                                .sql(", ")
                                .bind(alarm.command().into())
                                .sql(", ")
                                .bind(input.as_str().into())
                                .sql(", ")
                                .bind(alarm.due_at_ms()?.into())
                                .sql(", 1, 0, 1) ON CONFLICT (name) DO UPDATE SET command = excluded.command, input = excluded.input, due_at_ms = excluded.due_at_ms, generation = cell_alarms.generation + 1, attempts = 0, armed = 1"),
                        )
                        .await?;
                }
                CellAlarmWrite::Cancel(name) => {
                    executor
                        .execute(
                            Statement::new(
                                "UPDATE cell_alarms SET armed = 0, attempts = 0 WHERE name = ",
                            )
                            .bind(name.as_str().into()),
                        )
                        .await?;
                }
            }
        }
        let rows = executor
            .query(Statement::new(
                "SELECT COUNT(*) AS armed FROM cell_alarms WHERE armed = 1",
            ))
            .await?;
        let armed = rows
            .first()
            .map(|row| row.integer("armed"))
            .transpose()?
            .unwrap_or(0);
        if armed > MAX_ARMED_CELL_ALARMS as i64 {
            return Err(CellAlarmError::TooManyArmed.into());
        }
        Ok(())
    }

    pub(super) async fn armed(
        executor: &mut impl SqlExecutor,
        due_by_ms: Option<i64>,
        limit: usize,
    ) -> Result<Vec<ArmedCellAlarm>, RepositoryError> {
        let limit = (limit as u64).min(9_007_199_254_740_991) as i64;
        let mut statement = Statement::new(SELECT);
        if let Some(due_by_ms) = due_by_ms {
            statement = statement.sql(" AND due_at_ms <= ").bind(due_by_ms.into());
        }
        let rows = executor
            .query(
                statement
                    .sql(" ORDER BY due_at_ms, name LIMIT ")
                    .bind(limit.into()),
            )
            .await?;
        rows.iter()
            .map(|row| {
                let input = row.text("input")?;
                Ok(ArmedCellAlarm {
                    name: row.text("name")?,
                    command: row.text("command")?,
                    input: serde_json::from_str(&input).map_err(|error| {
                        RepositoryError::Model(format!("cell alarm input is invalid JSON: {error}"))
                    })?,
                    due_at_ms: row.integer("due_at_ms")?,
                    generation: u64::try_from(row.integer("generation")?).map_err(|_| {
                        RepositoryError::Model("cell alarm generation is negative".into())
                    })?,
                    attempts: u32::try_from(row.integer("attempts")?).unwrap_or(u32::MAX),
                })
            })
            .collect()
    }

    pub(super) async fn next_due(
        executor: &mut impl SqlExecutor,
    ) -> Result<Option<i64>, RepositoryError> {
        let rows = executor
            .query(Statement::new(
                "SELECT MIN(due_at_ms) AS due FROM cell_alarms WHERE armed = 1",
            ))
            .await?;
        Ok(rows
            .first()
            .map(|row| row.optional_integer("due"))
            .transpose()?
            .flatten())
    }

    pub(super) async fn settle(
        executor: &mut impl SqlExecutor,
        name: &str,
        generation: u64,
        settlement: &AlarmSettlement,
    ) -> Result<(), RepositoryError> {
        let generation = i64::try_from(generation)
            .map_err(|_| RepositoryError::Model("cell alarm generation overflow".into()))?;
        let statement = match settlement {
            AlarmSettlement::Done => {
                Statement::new("UPDATE cell_alarms SET armed = 0, attempts = 0 WHERE name = ")
            }
            AlarmSettlement::Retry { due_at_ms } => {
                Statement::new("UPDATE cell_alarms SET attempts = attempts + 1, due_at_ms = ")
                    .bind((*due_at_ms).into())
                    .sql(" WHERE name = ")
            }
        };
        executor
            .execute(
                statement
                    .bind(name.into())
                    .sql(" AND armed = 1 AND generation = ")
                    .bind(generation.into()),
            )
            .await?;
        Ok(())
    }
}
