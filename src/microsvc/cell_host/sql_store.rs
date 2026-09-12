//! Cell-local execution of the ordinary SQL event/snapshot/receipt repository.
//! No in-memory working copy and no whole-cell export are used here.

use sha2::{Digest, Sha256};
use worker::State;

use super::sql_executor::{finish_sql, CellSqlConnection};
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

// Durable Object SQL limits bindings per statement, not per transaction.
// Both shared insert planners split large commands within the same transaction.
const MAX_SQL_BIND_PARAMS: usize = 100;

#[derive(Clone)]
pub(super) struct CellSqlRepository {
    connection: CellSqlConnection,
    identity: CausalStorageIdentity,
}

impl CellSqlRepository {
    pub fn from_state(state: State) -> Result<Self, RepositoryError> {
        let connection = CellSqlConnection::from_state(state)?;
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
        }))?;
        Ok(Self {
            connection,
            identity: CausalStorageIdentity::new(),
        })
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

impl GetStream for CellSqlRepository {
    async fn get_stream(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<Entity>, RepositoryError> {
        finish_sql(sql::load_stream(
            &mut self.connection.executor(),
            identity,
            None,
        ))
    }
    async fn get_stream_tail(
        &self,
        identity: &StreamIdentity,
        after_version: u64,
    ) -> Result<Option<Entity>, RepositoryError> {
        finish_sql(sql::load_stream(
            &mut self.connection.executor(),
            identity,
            Some(after_version),
        ))
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
        finish_sql(sql::load_snapshot(
            &mut self.connection.executor(),
            identity,
        ))
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
        let rows = finish_sql(
            self.connection.executor().query(
                Statement::new("SELECT ")
                    .sql(crate::repository::sqlite_codec::OUTBOX_SELECT)
                    .sql(" FROM outbox_messages WHERE status = ")
                    .bind(status.as_str().into())
                    .sql(" ORDER BY CAST(created_at AS REAL), message_id LIMIT ")
                    .bind(limit.into()),
            ),
        )?;
        rows.into_iter().map(outbox::from_row).collect()
    }
    async fn backlog_stats(&self) -> Result<OutboxBacklogStats, RepositoryError> {
        let rows = finish_sql(self.connection.executor().query(Statement::new("SELECT COUNT(*) AS pending, MIN(CAST(created_at AS REAL)) AS oldest FROM outbox_messages WHERE status = 'pending'")))?;
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
