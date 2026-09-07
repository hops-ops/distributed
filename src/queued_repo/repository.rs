#![expect(
    clippy::manual_async_fn,
    reason = "async trait impls return impl Future + Send to preserve public Send bounds"
)]

use std::future::Future;
use std::sync::Arc;

use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalGetStream, CausalRepositoryIdentity,
    CausalStorageIdentity, CausalTransactionalCommit, CommandLedgerError, CommandLedgerKey,
    CommandLedgerStore, CommandLookup, CommandLookupScope, CommandReservation, ReservationOutcome,
};
use crate::entity::Entity;
use crate::lock::{InMemoryLockManager, Lock, LockManager};
use crate::projection_protocol::{
    ProjectionCausationEvidenceBatch, ProjectionCausationEvidenceRequest, ProjectionChangeCursor,
    ProjectionChangeRead, ProjectionCheckpoint, ProjectionCommitBatch, ProjectionCommitResult,
    ProjectionFailure, ProjectionFailureBatch, ProjectionFailureLocation, ProjectionGeneration,
    ProjectionInputCursor, ProjectionInputDisposition, ProjectionLiveRecordBatch,
    ProjectionLiveRecordBatchRequest, ProjectionModelOwnership, ProjectionObligationEvidenceBatch,
    ProjectionObligationEvidenceBatchRequest, ProjectionObservation, ProjectionObservationKind,
    ProjectionPartition, ProjectionPartitionRuntimeState, ProjectionProtocolError,
    ProjectionProtocolStore, ProjectionQuerySnapshot, ProjectionQuerySnapshotBatch,
    ProjectionQuerySnapshotBatchRequest, ProjectionQuerySnapshotRequest, ProjectionRecordMetadata,
    ProjectionRecordScope, ProjectorTopologyId, TrustedProjectionInput,
};
use crate::read_model::{ReadModelLoadGraph, ReadModelLoadRequest, ReadModelQueryCapabilities};
use crate::repository::{
    CommitBatch, GetStream, InboxStore, ReadModelWritePlanStore, RelationalReadModelQueryStore,
    RepositoryError, SnapshotStore, StreamIdentity, TransactionalCommit,
};
use crate::snapshot::SnapshotRecord;
use crate::table::{TableAdapterCapabilities, TableCommitOutcome, TableStoreError, TableWritePlan};

/// Options for read operations.
#[derive(Debug, Clone, Copy)]
pub struct ReadOpts {
    /// Whether to acquire a lock on the entity/entities.
    pub lock: bool,
}

impl Default for ReadOpts {
    fn default() -> Self {
        Self { lock: true }
    }
}

impl ReadOpts {
    /// Create options that skip locking.
    pub fn no_lock() -> Self {
        Self { lock: false }
    }
}

/// Repository wrapper that serializes access with process-local per-stream locks.
///
/// Locking reads (`get` and `get_many`) intentionally keep matching locks held
/// after returning. Call `commit` to release those locks after a successful
/// write, or call `abort`/`unlock` when the loaded entity is no longer being
/// written. Dropping a loaded entity without `commit` or `abort` leaves its
/// in-memory lock held until an explicit unlock.
///
/// Commit releases held locks only after the inner repository succeeds. On
/// commit errors, locks remain held so callers can inspect state, retry, or
/// explicitly abort.
pub struct QueuedRepository<R, L = InMemoryLockManager> {
    inner: R,
    lock_manager: Arc<L>,
}

impl<R: Clone, L> Clone for QueuedRepository<R, L> {
    fn clone(&self) -> Self {
        QueuedRepository {
            inner: self.inner.clone(),
            lock_manager: Arc::clone(&self.lock_manager),
        }
    }
}

impl<R> QueuedRepository<R> {
    pub fn new(inner: R) -> Self {
        QueuedRepository {
            inner,
            lock_manager: Arc::new(InMemoryLockManager::new()),
        }
    }
}

impl<R, L> QueuedRepository<R, L> {
    /// Access the inner repository.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// Access the lock manager.
    pub fn lock_manager(&self) -> &L {
        &self.lock_manager
    }
}

// ============================================================================
// Queue-locking variant over the repository trait surface. `.queued().aggregate::<T>()`
// serializes per-aggregate `get`/`commit` with the configured async lock manager.
// ============================================================================

impl<R, L: LockManager> QueuedRepository<R, L> {
    /// Create a `QueuedRepository` with a custom async lock manager.
    pub fn with_lock_manager(inner: R, lock_manager: L) -> Self {
        QueuedRepository {
            inner,
            lock_manager: Arc::new(lock_manager),
        }
    }

    fn ensure_lock(&self, id: &str) -> Result<Arc<L::Lock>, RepositoryError> {
        Ok(self.lock_manager.get_lock(id)?)
    }

    async fn lock_ids_in_order(&self, ids: &[&str]) -> Result<Vec<Arc<L::Lock>>, RepositoryError> {
        let mut unique: Vec<&str> = ids.to_vec();
        unique.sort_unstable();
        unique.dedup();

        let mut locks = Vec::with_capacity(unique.len());
        for id in unique {
            let lock = self.ensure_lock(id)?;
            lock.lock().await?;
            locks.push(lock);
        }

        Ok(locks)
    }
}

impl<R, L> GetStream for QueuedRepository<R, L>
where
    R: GetStream,
    L: LockManager,
{
    fn get_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        async move {
            // Acquire and HOLD the per-stream lock across the load, like the
            // synchronous `GetOne`. It is released by `commit_batch` on
            // success, or by an explicit `unlock`/`abort`.
            let lock = self.ensure_lock(&identity.storage_key())?;
            lock.lock().await?;
            self.inner.get_stream(identity).await
        }
    }

    fn get_streams<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<Entity>, RepositoryError>> + Send + 'a {
        async move {
            let keys: Vec<String> = identities.iter().map(StreamIdentity::storage_key).collect();
            let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            // Sorted-order acquire prevents deadlock; locks held after return.
            let _locks = self.lock_ids_in_order(&key_refs).await?;
            self.inner.get_streams(identities).await
        }
    }

    fn get_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        async move {
            // Acquire and HOLD the per-stream lock across the load, exactly like
            // `get_stream` — the snapshot tail load is still a load and must
            // serialize against concurrent writers. Released by `commit_batch`
            // on success, or by `unlock`/`abort`.
            let lock = self.ensure_lock(&identity.storage_key())?;
            lock.lock().await?;
            self.inner.get_stream_tail(identity, after_version).await
        }
    }
}

impl<R, L> CausalGetStream for QueuedRepository<R, L>
where
    R: CausalGetStream,
    L: LockManager,
{
    fn get_causal_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        // Deliberately bypass queue locking: a causal workspace may await user
        // handler code between load and commit. Optimistic stream versions and
        // the durable command attempt fence provide the authoritative safety.
        self.inner.get_causal_stream(identity)
    }
}

impl<R, L> CausalRepositoryIdentity for QueuedRepository<R, L>
where
    R: CausalRepositoryIdentity,
    L: LockManager,
{
    fn causal_storage_identity(&self) -> CausalStorageIdentity {
        self.inner.causal_storage_identity()
    }
}

impl<R, L> CommandLedgerStore for QueuedRepository<R, L>
where
    R: CommandLedgerStore,
    L: LockManager,
{
    fn reserve_command(
        &self,
        reservation: CommandReservation,
    ) -> impl Future<Output = Result<ReservationOutcome, CommandLedgerError>> + Send + '_ {
        self.inner.reserve_command(reservation)
    }

    fn lookup_command<'a>(
        &'a self,
        key: &'a CommandLedgerKey,
        scope: CommandLookupScope<'a>,
    ) -> impl Future<Output = Result<CommandLookup, CommandLedgerError>> + Send + 'a {
        self.inner.lookup_command(key, scope)
    }

    fn mark_retryable_unknown(
        &self,
        attempt: AttemptFence,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_ {
        self.inner.mark_retryable_unknown(attempt)
    }

    fn compact_expired_commands(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<u64, CommandLedgerError>> + Send + '_ {
        self.inner.compact_expired_commands(limit)
    }
}

impl<R, L> TransactionalCommit for QueuedRepository<R, L>
where
    R: TransactionalCommit,
    L: LockManager,
{
    fn commit_batch<'a>(
        &'a self,
        batch: CommitBatch<'a>,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            // Resolve the lock handles for the committed streams. Like the sync
            // `commit_batch`, this does not acquire (a prior locking load owns
            // them) and releases only after the inner commit succeeds, leaving
            // them held on error so callers can retry or `abort`.
            let mut locks = Vec::with_capacity(batch.streams.len());
            for stream in &batch.streams {
                locks.push(self.ensure_lock(&stream.identity.storage_key())?);
            }

            let result = self.inner.commit_batch(batch).await;

            if result.is_ok() {
                // Best-effort release: the inner commit already succeeded, so a
                // lock-cleanup failure must not fail the committed write (a durable
                // lease is reclaimed by its TTL regardless). Mirrors the
                // best-effort release in the lock layer itself.
                for lock in locks {
                    let _ = lock.unlock().await;
                }
            }

            result
        }
    }
}

impl<R, L> CausalTransactionalCommit for QueuedRepository<R, L>
where
    R: CausalTransactionalCommit,
    L: LockManager,
{
    fn commit_causal_batch<'a>(
        &'a self,
        batch: CausalCommitBatch<'a>,
    ) -> impl Future<Output = Result<(), CommandLedgerError>> + Send + 'a {
        // Causal loads deliberately bypass this wrapper's queue locks, so a
        // causal commit never owns one to release. Delegating without touching
        // the lock manager is essential: a matching lock may belong to an
        // unrelated legacy load/commit flow that is still in progress.
        self.inner.commit_causal_batch(batch)
    }
}

impl<R, L> ProjectionProtocolStore for QueuedRepository<R, L>
where
    R: ProjectionProtocolStore,
    L: LockManager,
{
    #[cfg(feature = "graphql")]
    async fn projection_rebuild_records(
        &self,
        context: &crate::projection::rebuild::RebuildContext,
    ) -> Result<Vec<crate::projection_protocol::ProjectionRecordMetadata>, ProjectionProtocolError>
    {
        self.inner.projection_rebuild_records(context).await
    }

    #[cfg(feature = "graphql")]
    async fn commit_projection_rebuild(
        &self,
        plan: crate::projection::rebuild::SnapshotProjectionRebuildPlan,
    ) -> Result<usize, ProjectionProtocolError> {
        self.inner.commit_projection_rebuild(plan).await
    }

    fn register_projection_models<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        ownership: &'a [ProjectionModelOwnership],
    ) -> impl Future<Output = Result<(), ProjectionProtocolError>> + Send + 'a {
        self.inner.register_projection_models(topology, ownership)
    }

    fn commit_projection(
        &self,
        batch: ProjectionCommitBatch,
    ) -> impl Future<Output = Result<ProjectionCommitResult, ProjectionProtocolError>> + Send + '_
    {
        self.inner.commit_projection(batch)
    }

    fn record_projection_failure(
        &self,
        batch: ProjectionFailureBatch,
    ) -> impl Future<Output = Result<ProjectionFailure, ProjectionProtocolError>> + Send + '_ {
        self.inner.record_projection_failure(batch)
    }

    fn projection_checkpoint<'a>(
        &'a self,
        cursor_scope: &'a ProjectionInputCursor,
        generation: ProjectionGeneration,
    ) -> impl Future<Output = Result<Option<ProjectionCheckpoint>, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_checkpoint(cursor_scope, generation)
    }

    fn projection_record<'a>(
        &'a self,
        scope: &'a ProjectionRecordScope,
    ) -> impl Future<Output = Result<Option<ProjectionRecordMetadata>, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_record(scope)
    }

    fn projection_input_disposition<'a>(
        &'a self,
        input: &'a TrustedProjectionInput,
    ) -> impl Future<Output = Result<ProjectionInputDisposition, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_input_disposition(input)
    }

    fn projection_query_snapshot<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshot, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_query_snapshot(request)
    }

    fn projection_query_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshotBatch, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_query_snapshot_batch(request)
    }

    fn projection_obligation_evidence_batch<'a>(
        &'a self,
        request: &'a ProjectionObligationEvidenceBatchRequest,
    ) -> impl Future<Output = Result<ProjectionObligationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_obligation_evidence_batch(request)
    }

    fn projection_causation_evidence<'a>(
        &'a self,
        request: &'a ProjectionCausationEvidenceRequest,
    ) -> impl Future<Output = Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_causation_evidence(request)
    }

    fn projection_live_record_batch<'a>(
        &'a self,
        request: &'a ProjectionLiveRecordBatchRequest,
    ) -> impl Future<Output = Result<ProjectionLiveRecordBatch, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_live_record_batch(request)
    }

    fn projection_partition_runtime_state<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
    ) -> impl Future<Output = Result<Option<ProjectionPartitionRuntimeState>, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner
            .projection_partition_runtime_state(topology, partition)
    }

    fn projection_observation<'a>(
        &'a self,
        causation_id: &'a str,
        scope: &'a ProjectionRecordScope,
        kind: ProjectionObservationKind,
    ) -> impl Future<Output = Result<Option<ProjectionObservation>, ProjectionProtocolError>> + Send + 'a
    {
        self.inner.projection_observation(causation_id, scope, kind)
    }

    fn projection_changes<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        after: Option<&'a ProjectionChangeCursor>,
        limit: usize,
    ) -> impl Future<Output = Result<ProjectionChangeRead, ProjectionProtocolError>> + Send + 'a
    {
        self.inner
            .projection_changes(topology, partition, after, limit)
    }

    fn repair_projection<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<ProjectionGeneration, ProjectionProtocolError>> + Send + 'a
    {
        self.inner
            .repair_projection(topology, partition, failure_id)
    }

    fn compact_projection_changes<'a>(
        &'a self,
        through: &'a ProjectionChangeCursor,
    ) -> impl Future<Output = Result<u64, ProjectionProtocolError>> + Send + 'a {
        self.inner.compact_projection_changes(through)
    }

    fn projection_failure<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailure>, ProjectionProtocolError>> + Send + 'a
    {
        self.inner
            .projection_failure(topology, partition, failure_id)
    }

    fn projection_failure_location<'a>(
        &'a self,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailureLocation>, ProjectionProtocolError>>
           + Send
           + 'a {
        self.inner.projection_failure_location(failure_id)
    }
}

// Non-locking forwards: read models, snapshots, and the consumer inbox are not
// gated by aggregate locks (matching the sync `SnapshotStore` delegation), so a
// queued repository stays a complete drop-in for its inner async repository.

impl<R, L> SnapshotStore for QueuedRepository<R, L>
where
    R: SnapshotStore,
    L: LockManager,
{
    fn get_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, RepositoryError>> + Send + 'a {
        self.inner.get_snapshot(identity)
    }

    fn get_snapshots<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<SnapshotRecord>, RepositoryError>> + Send + 'a {
        self.inner.get_snapshots(identities)
    }

    fn save_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        record: SnapshotRecord,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        self.inner.save_snapshot(identity, record)
    }

    fn delete_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a {
        self.inner.delete_snapshot(identity)
    }
}

impl<R, L> ReadModelWritePlanStore for QueuedRepository<R, L>
where
    R: ReadModelWritePlanStore,
    L: LockManager,
{
    fn read_model_capabilities(&self) -> TableAdapterCapabilities {
        self.inner.read_model_capabilities()
    }

    fn commit_write_plan(
        &self,
        plan: TableWritePlan,
    ) -> impl Future<Output = Result<TableCommitOutcome, TableStoreError>> + Send + '_ {
        self.inner.commit_write_plan(plan)
    }
}

impl<R, L> RelationalReadModelQueryStore for QueuedRepository<R, L>
where
    R: RelationalReadModelQueryStore,
    L: LockManager,
{
    fn read_model_query_capabilities(&self) -> ReadModelQueryCapabilities {
        self.inner.read_model_query_capabilities()
    }

    fn load_graph(
        &self,
        request: ReadModelLoadRequest,
    ) -> impl Future<Output = Result<ReadModelLoadGraph, TableStoreError>> + Send + '_ {
        self.inner.load_graph(request)
    }
}

impl<R, L> InboxStore for QueuedRepository<R, L>
where
    R: InboxStore,
    L: LockManager,
{
    fn inbox_contains<'a>(
        &'a self,
        consumer: &'a str,
        message_id: &'a str,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a {
        self.inner.inbox_contains(consumer, message_id)
    }

    fn purge_inbox_older_than(
        &self,
        age: std::time::Duration,
    ) -> impl Future<Output = Result<u64, RepositoryError>> + Send {
        // Inbox maintenance is a direct store operation; the queue/lock layer adds
        // nothing, so delegate straight through to the inner store.
        self.inner.purge_inbox_older_than(age)
    }
}

/// Opt-out reads for a queued repository — the counterpart to
/// [`GetWithOpts`]. `ReadOpts::no_lock()` reads without acquiring the lock.
pub trait GetWithOpts {
    fn get_stream_with<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        opts: ReadOpts,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a;
}

/// Opt-out multi-reads — the counterpart to [`GetAllWithOpts`].
pub trait GetAllWithOpts {
    fn get_streams_with<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
        opts: ReadOpts,
    ) -> impl Future<Output = Result<Vec<Entity>, RepositoryError>> + Send + 'a;
}

impl<R, L> GetWithOpts for QueuedRepository<R, L>
where
    R: GetStream,
    L: LockManager,
{
    fn get_stream_with<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        opts: ReadOpts,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        async move {
            if opts.lock {
                let lock = self.ensure_lock(&identity.storage_key())?;
                lock.lock().await?;
            }
            self.inner.get_stream(identity).await
        }
    }
}

impl<R, L> GetAllWithOpts for QueuedRepository<R, L>
where
    R: GetStream,
    L: LockManager,
{
    fn get_streams_with<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
        opts: ReadOpts,
    ) -> impl Future<Output = Result<Vec<Entity>, RepositoryError>> + Send + 'a {
        async move {
            if opts.lock {
                let keys: Vec<String> =
                    identities.iter().map(StreamIdentity::storage_key).collect();
                let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
                let _locks = self.lock_ids_in_order(&key_refs).await?;
            }
            self.inner.get_streams(identities).await
        }
    }
}

/// Releasing a held lock for an aborted load over the async repository surface.
///
/// Release is `async` because a durable [`Lock`] (e.g. the SQLx lease-table
/// locks) releases by talking to its backing store; the in-memory lock resolves
/// immediately. It is a separate trait because coherence cannot prove a type is
/// not both several lock-manager kinds at once.
pub trait UnlockableRepository: Send + Sync {
    /// Release the lock held for a stream.
    ///
    /// Keyed by [`StreamIdentity`] — the same key the locking `GetStream`
    /// reads acquire — so an aborted load releases exactly the lock it took.
    fn unlock<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a;

    /// Release a lock for an aborted load (alias for [`unlock`](Self::unlock)).
    fn abort<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        self.unlock(identity)
    }
}

impl<R, L: LockManager> UnlockableRepository for QueuedRepository<R, L>
where
    R: Send + Sync,
{
    fn unlock<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a {
        async move {
            self.ensure_lock(&identity.storage_key())?.unlock().await?;
            Ok(())
        }
    }
}

/// Builder trait for wrapping a repository with queue locking.
pub trait Queueable: Sized {
    /// Wrap with the default async lock manager. Pair with
    /// `.aggregate::<T>()` for per-aggregate serialization over the async
    /// repository surface.
    fn queued(self) -> QueuedRepository<Self, InMemoryLockManager> {
        QueuedRepository::with_lock_manager(self, InMemoryLockManager::new())
    }

    /// Wrap with a custom async lock manager.
    fn queued_with<L: LockManager>(self, lock_manager: L) -> QueuedRepository<Self, L> {
        QueuedRepository::with_lock_manager(self, lock_manager)
    }
}

impl<T> Queueable for T {}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use uuid::Uuid;

    use super::*;
    use crate::command_ledger::{
        CanonicalInputHash, CommandContractFingerprint, CommandId, PrincipalPartitionId,
        TerminalCommandState,
    };
    use crate::in_memory_repo::InMemoryRepository;
    use crate::repository::StreamWrite;

    #[tokio::test]
    async fn causal_commit_does_not_release_lock_owned_by_legacy_load() {
        let repository = QueuedRepository::new(InMemoryRepository::new());
        let aggregate_id = format!("queued-causal-lock-{}", Uuid::now_v7());
        let identity = StreamIdentity::new("queued-causal-test", &aggregate_id).unwrap();

        // The ordinary read API acquires and intentionally retains this lock
        // until its legacy transaction commits or aborts.
        assert!(repository.get_stream(&identity).await.unwrap().is_none());
        let held_lock = repository
            .lock_manager()
            .get_lock(&identity.storage_key())
            .unwrap();
        assert!(!held_lock.try_lock().await.unwrap());

        let command_id = Uuid::now_v7().to_string();
        let reservation = CommandReservation::new(
            CommandLedgerKey::new(
                "queued-causal-test",
                PrincipalPartitionId::new("v1:sha256:test-principal").unwrap(),
                CommandId::parse(&command_id).unwrap(),
            )
            .unwrap(),
            "test.create",
            CommandContractFingerprint::new([1; 32]),
            CanonicalInputHash::new([2; 32]),
            Duration::from_secs(30),
            Duration::from_secs(300),
        )
        .unwrap();
        let attempt = match repository.reserve_command(reservation).await.unwrap() {
            ReservationOutcome::Acquired(attempt) => attempt,
            other => panic!("expected an acquired command attempt, got {other:?}"),
        };
        let completion = attempt
            .complete(
                TerminalCommandState::Succeeded,
                serde_json::json!({"accepted": true}),
                Duration::from_secs(300),
            )
            .unwrap();
        let mut entity = Entity::with_id(&aggregate_id);
        entity.digest_empty("Created").unwrap();
        let domain = CommitBatch::new(vec![StreamWrite::new(identity.clone(), &mut entity)]);

        repository
            .commit_causal_batch(CausalCommitBatch::new(domain, completion))
            .await
            .unwrap();

        assert!(
            !held_lock.try_lock().await.unwrap(),
            "causal commit must not release a lock owned by a legacy load"
        );
        repository.abort(&identity).await.unwrap();
    }
}
