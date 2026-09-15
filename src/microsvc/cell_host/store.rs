//! One cell's private command-side repository, with enforced stream ownership.
//! Workers use runtime-owned SQLite; native tests use the in-process reference host.

use std::future::Future;
use std::sync::Arc;
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
use std::sync::Mutex;

#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
use serde_json::Value;

#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
use super::sql_store::CellSqlRepository;
use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalGetStream, CausalRepositoryIdentity,
    CausalStorageIdentity, CausalTransactionalCommit, CommandLedgerError, CommandLedgerKey,
    CommandLedgerStore, CommandLookup, CommandLookupScope, CommandReservation,
    ExternalCommandCompletion, ReservationOutcome,
};
use crate::entity::Entity;
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
use crate::entity::EventRecord;
use crate::microsvc::HasOutboxStore;
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
use crate::outbox::OutboxMessage;
use crate::repository::{
    CommitBatch, GetStream, RepositoryError, SnapshotStore, SnapshotWrite, StreamIdentity,
    TransactionalCommit,
};
use crate::snapshot::SnapshotRecord;
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
use crate::InMemoryRepository;
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
type CellRepository = InMemoryRepository;
#[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
type CellRepository = CellSqlRepository;
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
use serde::{Deserialize, Serialize};

#[derive(Clone)]
enum CellOwnership {
    /// One stream identity (Todo, BlobGame). Foreign streams are rejected.
    Exclusive(StreamIdentity),
    /// Parent game cell: map/player/bomb/explosion/saga streams share this
    /// cell's private SQLite. There is no API to commit across two cells.
    Parent {
        name: StreamIdentity,
        owns: Arc<dyn Fn(&StreamIdentity) -> bool + Send + Sync>,
    },
}

/// One stream's event records in a native restart-test export.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
pub struct DurableCellEvents {
    pub stream: String,
    pub events: Vec<EventRecord>,
}

/// Snapshot cache record in a native restart-test export.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
pub struct DurableCellSnapshot {
    pub stream: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub version: u64,
    pub snapshot_version: u64,
    pub payload_codec: String,
    pub payload_codec_version: u16,
    pub payload: Vec<u8>,
}

/// One versioned command-ledger row in a native restart-test export.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
pub struct DurableCellCommand {
    pub id: String,
    pub body: String,
}

/// Current native restart-test export version.
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
pub const DURABLE_AGGREGATE_CELL_STATE_VERSION: u16 = 1;

/// Export of the native in-process reference host for restart conformance tests.
/// This is not a production cell persistence API; Workers use SQLite rows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
pub struct DurableAggregateCellState {
    pub version: u16,
    pub events: Vec<DurableCellEvents>,
    pub snapshots: Vec<DurableCellSnapshot>,
    pub commands: Vec<DurableCellCommand>,
    pub outbox: Vec<OutboxMessage>,
    pub sealed_row: Option<Value>,
}

/// Private command-side repository for one cell instance (`{aggregate_type}:{shard}`).
///
/// Exclusive cells reject any stream that is not this cell's shard. Parent
/// cells hold sibling streams and commit them in one [`CommitBatch`].
///
/// ```compile_fail
/// fn two_cell_transaction_does_not_exist(
///     left: &distributed::cell_host::CellStreamStore,
///     right: &distributed::cell_host::CellStreamStore,
///     batch: distributed::CommitBatch<'_>,
/// ) {
///     let _ = left.commit_across(right, batch);
/// }
/// ```
#[derive(Clone)]
pub struct CellStreamStore {
    ownership: CellOwnership,
    inner: CellRepository,
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    sealed_row: Arc<Mutex<Option<Value>>>,
}

impl CellStreamStore {
    /// Open this runtime-owned SQLite database; no storage export callback exists.
    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    pub fn from_state(
        state: worker::State,
        identity: StreamIdentity,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            ownership: CellOwnership::Exclusive(identity),
            inner: CellSqlRepository::from_state(state)?,
        })
    }

    /// Open a parent-shard SQL cell. All owned sibling streams share this one
    /// database; the predicate never grants access to another cell's storage.
    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    pub fn from_parent_state(
        state: worker::State,
        parent_type: impl Into<String>,
        parent_id: impl Into<String>,
        owns: impl Fn(&StreamIdentity) -> bool + Send + Sync + 'static,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            ownership: CellOwnership::Parent {
                name: StreamIdentity::new(parent_type, parent_id)?,
                owns: Arc::new(owns),
            },
            inner: CellSqlRepository::from_state(state)?,
        })
    }

    /// Bind a store to one exact stream identity.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn for_identity(identity: StreamIdentity) -> Self {
        Self {
            ownership: CellOwnership::Exclusive(identity),
            inner: InMemoryRepository::new(),
            sealed_row: Arc::new(Mutex::new(None)),
        }
    }

    /// Parent-shard cell: `"{parent_type}:{parent_id}"` (bomberman `game:{id}`).
    ///
    /// Child streams of any aggregate type live in this cell's SQLite. A
    /// transaction across two parent cells does not exist.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn for_parent_shard(
        parent_type: impl Into<String>,
        parent_id: impl Into<String>,
        owns: impl Fn(&StreamIdentity) -> bool + Send + Sync + 'static,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            ownership: CellOwnership::Parent {
                name: StreamIdentity::new(parent_type, parent_id)?,
                owns: Arc::new(owns),
            },
            inner: InMemoryRepository::new(),
            sealed_row: Arc::new(Mutex::new(None)),
        })
    }

    /// Named exclusive-cell constructor used by [`super::AggregateCell`].
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn new(
        aggregate_type: impl Into<String>,
        shard_id: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        Ok(Self::for_identity(StreamIdentity::new(
            aggregate_type,
            shard_id,
        )?))
    }

    /// Cell instance name (`type:id`).
    pub fn instance_name(&self) -> String {
        match &self.ownership {
            CellOwnership::Exclusive(identity) | CellOwnership::Parent { name: identity, .. } => {
                identity.to_string()
            }
        }
    }

    /// Stream this exclusive cell owns. Parent cells have no single stream.
    pub fn identity(&self) -> Option<&StreamIdentity> {
        match &self.ownership {
            CellOwnership::Exclusive(identity) => Some(identity),
            CellOwnership::Parent { .. } => None,
        }
    }

    fn ensure_identity(&self, identity: &StreamIdentity) -> Result<(), RepositoryError> {
        match &self.ownership {
            CellOwnership::Parent { owns, .. } if owns(identity) => Ok(()),
            CellOwnership::Parent { name, .. } => Err(RepositoryError::Model(format!(
                "parent cell {name} does not own stream {identity}"
            ))),
            CellOwnership::Exclusive(owned) if identity == owned => Ok(()),
            CellOwnership::Exclusive(owned) => Err(RepositoryError::Model(format!(
                "cell `{owned}` cannot access stream `{identity}`"
            ))),
        }
    }

    /// Sealed read-model row for GET on this cell instance.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn sealed_row(&self) -> Result<Option<Value>, RepositoryError> {
        self.sealed_row
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| RepositoryError::Model("cell sealed row lock poisoned".into()))
    }

    /// Replace the sealed read-model row (Atomic board / Todo view).
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn replace_sealed_row(&self, row: Value) -> Result<(), RepositoryError> {
        let mut guard = self
            .sealed_row
            .lock()
            .map_err(|_| RepositoryError::Model("cell sealed row lock poisoned".into()))?;
        *guard = Some(row);
        Ok(())
    }

    /// Event log for Durable Object SQLite. Memory remains the working copy.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_events(&self) -> Result<Vec<DurableCellEvents>, RepositoryError> {
        Ok(self
            .inner
            .clone_events()?
            .into_iter()
            .map(|(stream, events)| DurableCellEvents { stream, events })
            .collect())
    }

    /// Outbox rows committed with this cell's events (same private SQLite).
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_outbox(&self) -> Result<Vec<OutboxMessage>, RepositoryError> {
        self.inner.clone_outbox()
    }

    /// Restore outbox rows from Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_outbox(
        &self,
        messages: Vec<OutboxMessage>,
    ) -> Result<(), RepositoryError> {
        self.inner.replace_outbox(messages)
    }

    /// Replace the working event log from Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_events(
        &self,
        events: Vec<DurableCellEvents>,
    ) -> Result<(), RepositoryError> {
        self.inner.replace_events(
            events
                .into_iter()
                .map(|row| (row.stream, row.events))
                .collect(),
        )
    }

    /// Snapshot cache for Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_snapshots(&self) -> Result<Vec<DurableCellSnapshot>, RepositoryError> {
        Ok(self
            .inner
            .clone_snapshots()?
            .into_iter()
            .map(|(stream, record)| DurableCellSnapshot {
                stream,
                aggregate_type: record.aggregate_type,
                aggregate_id: record.aggregate_id,
                version: record.version,
                snapshot_version: record.snapshot_version,
                payload_codec: record.payload_codec,
                payload_codec_version: record.payload_codec_version,
                payload: record.payload,
            })
            .collect())
    }

    /// Replace the working snapshot cache from Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_snapshots(
        &self,
        snapshots: Vec<DurableCellSnapshot>,
    ) -> Result<(), RepositoryError> {
        self.inner.replace_snapshots(
            snapshots
                .into_iter()
                .map(|row| {
                    (
                        row.stream,
                        SnapshotRecord {
                            aggregate_type: row.aggregate_type,
                            aggregate_id: row.aggregate_id,
                            version: row.version,
                            snapshot_version: row.snapshot_version,
                            payload_codec: row.payload_codec,
                            payload_codec_version: row.payload_codec_version,
                            payload: row.payload,
                            metadata: Default::default(),
                            recorded_at: crate::time::now(),
                        },
                    )
                })
                .collect(),
        )
    }

    /// Fenced command rows committed with this cell's domain effects.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_commands(&self) -> Result<Vec<DurableCellCommand>, RepositoryError> {
        self.inner
            .clone_command_ledger()?
            .into_iter()
            .map(|record| {
                let id = record.durable_cell_key();
                let body = record
                    .durable_cell_json()
                    .map_err(|error| RepositoryError::Model(error.to_string()))?;
                Ok(DurableCellCommand { id, body })
            })
            .collect()
    }

    /// Restore the complete command ledger before accepting another request.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_commands(
        &self,
        commands: Vec<DurableCellCommand>,
    ) -> Result<(), RepositoryError> {
        let mut records = Vec::with_capacity(commands.len());
        for command in commands {
            let record =
                crate::command_ledger::CommandLedgerRecord::from_durable_cell_json(&command.body)
                    .map_err(|error| RepositoryError::Model(error.to_string()))?;
            if record.durable_cell_key() != command.id {
                return Err(RepositoryError::Model(
                    "cell command ledger row key does not match its body".into(),
                ));
            }
            records.push(record);
        }
        self.inner.replace_command_ledger(records)
    }

    /// Export every durable concern as one versioned persistence envelope.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_state(&self) -> Result<DurableAggregateCellState, RepositoryError> {
        Ok(DurableAggregateCellState {
            version: DURABLE_AGGREGATE_CELL_STATE_VERSION,
            events: self.durable_events()?,
            snapshots: self.durable_snapshots()?,
            commands: self.durable_commands()?,
            outbox: self.durable_outbox()?,
            sealed_row: self.sealed_row()?,
        })
    }

    /// Replace the complete working copy from one persisted envelope.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_state(
        &self,
        state: DurableAggregateCellState,
    ) -> Result<(), RepositoryError> {
        if state.version != DURABLE_AGGREGATE_CELL_STATE_VERSION {
            return Err(RepositoryError::Model(format!(
                "unsupported durable aggregate cell state version {}",
                state.version
            )));
        }

        // Parse and validate the command ledger before replacing the other
        // working-copy components; malformed persisted JSON must fail closed.
        self.restore_durable_commands(state.commands)?;
        self.restore_durable_events(state.events)?;
        self.restore_durable_snapshots(state.snapshots)?;
        self.restore_durable_outbox(state.outbox)?;
        let mut sealed = self
            .sealed_row
            .lock()
            .map_err(|_| RepositoryError::Model("cell sealed row lock poisoned".into()))?;
        *sealed = state.sealed_row;
        Ok(())
    }

    fn ensure_batch(&self, batch: &CommitBatch<'_>) -> Result<(), RepositoryError> {
        for stream in &batch.streams {
            self.ensure_identity(&stream.identity)?;
        }
        for snapshot in &batch.snapshots {
            match snapshot {
                SnapshotWrite::Save { identity, .. } => self.ensure_identity(identity)?,
            }
        }
        Ok(())
    }
}

impl CausalGetStream for CellStreamStore {
    fn get_causal_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl std::future::Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a
    {
        async move {
            self.ensure_identity(identity)?;
            self.inner
                .get_causal_stream_tail(identity, after_version)
                .await
        }
    }
    fn get_causal_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        async move {
            self.ensure_identity(identity)?;
            CausalGetStream::get_causal_stream(&self.inner, identity).await
        }
    }
}

impl GetStream for CellStreamStore {
    fn get_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        async move {
            self.ensure_identity(identity)?;
            GetStream::get_stream(&self.inner, identity).await
        }
    }

    fn get_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        async move {
            self.ensure_identity(identity)?;
            GetStream::get_stream_tail(&self.inner, identity, after_version).await
        }
    }
}

impl SnapshotStore for CellStreamStore {
    fn get_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, RepositoryError>> + Send + 'a {
        async move {
            self.ensure_identity(identity)?;
            SnapshotStore::get_snapshot(&self.inner, identity).await
        }
    }

    fn get_snapshots<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<SnapshotRecord>, RepositoryError>> + Send + 'a {
        async move {
            for identity in identities {
                self.ensure_identity(identity)?;
            }
            SnapshotStore::get_snapshots(&self.inner, identities).await
        }
    }

    fn save_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        record: SnapshotRecord,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            self.ensure_identity(identity)?;
            SnapshotStore::save_snapshot(&self.inner, identity, record).await
        }
    }

    fn delete_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a {
        async move {
            self.ensure_identity(identity)?;
            SnapshotStore::delete_snapshot(&self.inner, identity).await
        }
    }
}

impl CausalRepositoryIdentity for CellStreamStore {
    fn causal_storage_identity(&self) -> CausalStorageIdentity {
        CausalRepositoryIdentity::causal_storage_identity(&self.inner)
    }
}

impl CommandLedgerStore for CellStreamStore {
    fn reserve_command(
        &self,
        reservation: CommandReservation,
    ) -> impl Future<Output = Result<ReservationOutcome, CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::reserve_command(&self.inner, reservation)
    }

    fn lookup_command<'a>(
        &'a self,
        key: &'a CommandLedgerKey,
        scope: CommandLookupScope<'a>,
    ) -> impl Future<Output = Result<CommandLookup, CommandLedgerError>> + Send + 'a {
        CommandLedgerStore::lookup_command(&self.inner, key, scope)
    }

    fn mark_retryable_unknown(
        &self,
        attempt: AttemptFence,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::mark_retryable_unknown(&self.inner, attempt)
    }

    fn complete_external_command(
        &self,
        completion: ExternalCommandCompletion,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::complete_external_command(&self.inner, completion)
    }

    fn compact_expired_commands(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<u64, CommandLedgerError>> + Send + '_ {
        CommandLedgerStore::compact_expired_commands(&self.inner, limit)
    }
}

impl TransactionalCommit for CellStreamStore {
    fn commit_batch<'a>(
        &'a self,
        batch: CommitBatch<'a>,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            self.ensure_batch(&batch)?;
            TransactionalCommit::commit_batch(&self.inner, batch).await
        }
    }
}

impl CausalTransactionalCommit for CellStreamStore {
    fn commit_causal_batch<'a>(
        &'a self,
        batch: CausalCommitBatch<'a>,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + 'a {
        async move {
            self.ensure_batch(&batch.domain)
                .map_err(CommandLedgerError::Storage)?;
            CausalTransactionalCommit::commit_causal_batch(&self.inner, batch).await
        }
    }
}

impl HasOutboxStore for CellStreamStore {
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    type OutboxStore = crate::InMemoryOutboxStore;
    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    type OutboxStore = super::sql_store::CellSqlOutboxStore;

    fn outbox_store(&self) -> Self::OutboxStore {
        self.inner.outbox_store()
    }
}

// A command-only cell cannot bootstrap same-transaction read-model projections.
impl crate::microsvc::dependencies::CausalHostProjections for CellStreamStore {
    async fn __register_direct_projection_models(
        &self,
        _topology: &crate::projection_protocol::ProjectorTopologyId,
        _ownership: &[crate::projection_protocol::ProjectionModelOwnership],
    ) -> Result<(), crate::projection_protocol::ProjectionProtocolError> {
        Err(
            crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                "aggregate cells do not execute read-model projections".into(),
            ),
        )
    }
}
