#![expect(
    clippy::manual_async_fn,
    reason = "async trait impls return impl Future + Send to preserve public Send bounds"
)]

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalGetStream, CausalRepositoryIdentity,
    CausalStorageIdentity, CausalTransactionalCommit, CommandCompletion, CommandLedgerError,
    CommandLedgerKey, CommandLedgerStore, CommandLookup, CommandLookupScope, CommandReservation,
    ExternalCommandCompletion, ReservationOutcome,
};
use crate::entity::Entity;
use crate::lock::{InMemoryLockManager, LockGuard, LockManager};
use crate::microsvc::CausalHostProjections;
use crate::projection::rebuild::{RebuildContext, SnapshotProjectionRebuildPlan};
use crate::projection_protocol::{
    PartitionId, ProjectionCausationEvidenceBatch, ProjectionCausationEvidenceRequest,
    ProjectionChangeCursor, ProjectionChangeRead, ProjectionCheckpoint, ProjectionCommitBatch,
    ProjectionCommitResult, ProjectionExecutionSnapshotBatch,
    ProjectionExecutionSnapshotBatchRequest, ProjectionFailure, ProjectionFailureBatch,
    ProjectionFailureLocation, ProjectionGeneration, ProjectionInputCursor,
    ProjectionInputDisposition, ProjectionModelOwnership, ProjectionObservation,
    ProjectionObservationKind, ProjectionPartitionRuntimeState, ProjectionProtocolError,
    ProjectionProtocolStore, ProjectionQuerySnapshot, ProjectionQuerySnapshotRequest,
    ProjectionRecordMetadata, ProjectionRecordScope, ProjectorTopologyId, TrustedProjectionInput,
};
use crate::read_model::{
    ReadModelLoadGraph, ReadModelLoadRequest, ReadModelScanPage, ReadModelScanRequest,
};
use crate::repository::{
    CommitBatch, GetStream, InboxStore, ReadModelWritePlanStore, RelationalReadModelQueryStore,
    RepositoryError, SnapshotStore, StreamIdentity, StreamWrite, TransactionalCommit,
};
use crate::schedule::{BackendHost, Capabilities};
use crate::snapshot::SnapshotRecord;
use crate::table::{TableStoreError, TableWritePlan};
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

/// Repository wrapper that serializes read-modify-write per stream.
///
/// A locking read (`get`, `get_all`) waits for the stream's lock and returns
/// an aggregate that owns it. The lock is released when that aggregate is
/// committed, or when it is dropped: an early return cannot leak it, and a
/// commit releases only the lock its own load took. A read that finds no
/// stream holds nothing.
///
/// A failed commit keeps the lock, so the caller can retry; dropping the
/// aggregate releases it.
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

    async fn hold(&self, key: &str) -> Result<LockGuard<L::Lock>, RepositoryError> {
        Ok(LockGuard::acquire(self.lock_manager.get_lock(key)?).await?)
    }

    /// Load with the stream lock held, and hand the lock to the entity.
    async fn locked<'a, F>(
        &'a self,
        identity: &'a StreamIdentity,
        load: F,
    ) -> Result<Option<Entity>, RepositoryError>
    where
        F: Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a,
    {
        let guard = self.hold(&identity.storage_key()).await?;
        // A miss or an error drops the guard here, which releases the lock.
        Ok(load.await?.map(|mut entity| {
            entity.hold_with(Box::new(guard));
            entity
        }))
    }

    /// Load several streams, each returned entity owning its stream's lock.
    /// Locks are taken in key order so two batches cannot deadlock.
    async fn locked_many<'a>(
        &self,
        identities: impl Iterator<Item = &'a StreamIdentity> + Send,
        load: impl Future<Output = Result<Vec<Entity>, RepositoryError>> + Send,
    ) -> Result<Vec<Entity>, RepositoryError> {
        let mut keyed: Vec<(String, &StreamIdentity)> = identities
            .map(|identity| (identity.storage_key(), identity))
            .collect();
        keyed.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        keyed.dedup_by(|a, b| a.0 == b.0);

        let mut guards = Vec::with_capacity(keyed.len());
        for (key, identity) in keyed {
            guards.push((identity.aggregate_id(), self.hold(&key).await?));
        }
        let mut entities = load.await?;
        for entity in &mut entities {
            if let Some(at) = guards.iter().position(|(id, _)| *id == entity.id()) {
                entity.hold_with(Box::new(guards.swap_remove(at).1));
            }
        }
        Ok(entities)
    }
}

/// Implements trait methods that hand the call to the wrapped repository
/// unchanged. Every method a capability trait declares is listed, defaults
/// included, so a backend's own implementation is never replaced by a
/// trait default here.
macro_rules! delegate {
    ($(
        $(#[$meta:meta])*
        fn $name:ident $(<$lt:lifetime>)? (&$($self_lt:lifetime)? self $(, $arg:ident: $arg_ty:ty)* $(,)?) -> $ret:ty;
    )*) => {$(
        $(#[$meta])*
        fn $name $(<$lt>)? (&$($self_lt)? self $(, $arg: $arg_ty)*) -> $ret {
            self.inner.$name($($arg),*)
        }
    )*};
}

// Locking reads: each returned entity owns its stream's lock.
impl<R, L> GetStream for QueuedRepository<R, L>
where
    R: GetStream,
    L: LockManager,
{
    fn get_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        self.locked(identity, self.inner.get_stream(identity))
    }

    fn get_streams<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<Entity>, RepositoryError>> + Send + 'a {
        self.locked_many(identities.iter(), self.inner.get_streams(identities))
    }

    fn get_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        self.locked(
            identity,
            self.inner.get_stream_tail(identity, after_version),
        )
    }

    fn get_stream_tails<'a>(
        &'a self,
        requests: &'a [(StreamIdentity, u64)],
    ) -> impl Future<Output = Result<Vec<Entity>, RepositoryError>> + Send + 'a {
        self.locked_many(
            requests.iter().map(|(identity, _)| identity),
            self.inner.get_stream_tails(requests),
        )
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
            let CommitBatch {
                mut streams,
                outbox_messages,
                snapshots,
                inbox_receipts,
                schedules,
            } = batch;
            // Lend the entities to the inner commit so the locks they own can
            // be released afterwards.
            let lent = CommitBatch {
                streams: streams
                    .iter_mut()
                    .map(|stream| StreamWrite::new(stream.identity.clone(), &mut *stream.entity))
                    .collect(),
                outbox_messages,
                snapshots,
                inbox_receipts,
                schedules,
            };
            self.inner.commit_batch(lent).await?;

            // Release only what these entities' own loads took. The write is
            // committed, so a release failure must not fail it (a durable
            // lease expires by its TTL).
            for stream in streams {
                if let Some(hold) = stream.entity.take_hold() {
                    if let Ok(guard) = hold.downcast::<LockGuard<L::Lock>>() {
                        let _ = guard.release().await;
                    }
                }
            }
            Ok(())
        }
    }
}

// Causal reads and commits deliberately bypass the queue locks: a causal
// workspace may await handler code between load and commit, and optimistic
// stream versions plus the durable command attempt fence are the
// authoritative safety. A causal commit therefore never owns a lock to
// release, and must not touch one: a matching lock may belong to an unrelated
// locking load that is still in progress.
impl<R: CausalGetStream, L: LockManager> CausalGetStream for QueuedRepository<R, L> {
    delegate! {
        fn get_causal_stream<'a>(&'a self, identity: &'a StreamIdentity)
            -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a;
        fn get_causal_stream_tail<'a>(&'a self, identity: &'a StreamIdentity, after_version: u64)
            -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a;
    }
}

impl<R: CausalTransactionalCommit, L: LockManager> CausalTransactionalCommit
    for QueuedRepository<R, L>
{
    delegate! {
        fn commit_causal_batch<'a>(&'a self, batch: CausalCommitBatch<'a>)
            -> impl Future<Output = Result<CommandCompletion<'a>, CommandLedgerError>> + Send + 'a;
    }
}

impl<R: CausalRepositoryIdentity, L: LockManager> CausalRepositoryIdentity
    for QueuedRepository<R, L>
{
    delegate! {
        fn causal_storage_identity(&self) -> CausalStorageIdentity;
    }
}

impl<R: CommandLedgerStore, L: LockManager> CommandLedgerStore for QueuedRepository<R, L> {
    delegate! {
        fn reserve_command(&self, reservation: CommandReservation)
            -> impl Future<Output = Result<ReservationOutcome, CommandLedgerError>> + Send + '_;
        fn lookup_command<'a>(&'a self, key: &'a CommandLedgerKey, scope: CommandLookupScope<'a>)
            -> impl Future<Output = Result<CommandLookup, CommandLedgerError>> + Send + 'a;
        fn mark_retryable_unknown(&self, attempt: AttemptFence)
            -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_;
        fn complete_external_command(&self, completion: ExternalCommandCompletion)
            -> impl Future<Output = Result<(), CommandLedgerError>> + Send + '_;
        fn compact_expired_commands(&self, limit: usize)
            -> impl Future<Output = Result<u64, CommandLedgerError>> + Send + '_;
    }
}

impl<R: CausalHostProjections, L: LockManager> CausalHostProjections for QueuedRepository<R, L> {
    delegate! {
        #[cfg(feature = "graphql")]
        fn command_causation_evidence<'a>(&'a self, request: &'a ProjectionCausationEvidenceRequest)
            -> impl Future<Output = Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>> + Send + 'a;
        fn __register_direct_projection_models<'a>(
            &'a self,
            topology: &'a ProjectorTopologyId,
            ownership: &'a [ProjectionModelOwnership],
        ) -> impl Future<Output = Result<(), ProjectionProtocolError>> + Send + 'a;
    }
}

impl<R: ProjectionProtocolStore, L: LockManager> ProjectionProtocolStore
    for QueuedRepository<R, L>
{
    delegate! {
        fn projection_rebuild_records<'a>(&'a self, context: &'a RebuildContext)
            -> impl Future<Output = Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError>> + Send + 'a;
        fn commit_projection_rebuild(&self, plan: SnapshotProjectionRebuildPlan)
            -> impl Future<Output = Result<usize, ProjectionProtocolError>> + Send + '_;
        fn register_projection_models<'a>(
            &'a self,
            topology: &'a ProjectorTopologyId,
            ownership: &'a [ProjectionModelOwnership],
        ) -> impl Future<Output = Result<(), ProjectionProtocolError>> + Send + 'a;
        fn commit_projection(&self, batch: ProjectionCommitBatch)
            -> impl Future<Output = Result<ProjectionCommitResult, ProjectionProtocolError>> + Send + '_;
        fn record_projection_failure(&self, batch: ProjectionFailureBatch)
            -> impl Future<Output = Result<ProjectionFailure, ProjectionProtocolError>> + Send + '_;
        fn projection_checkpoint<'a>(
            &'a self,
            cursor_scope: &'a ProjectionInputCursor,
            generation: ProjectionGeneration,
        ) -> impl Future<Output = Result<Option<ProjectionCheckpoint>, ProjectionProtocolError>> + Send + 'a;
        fn projection_record<'a>(&'a self, scope: &'a ProjectionRecordScope)
            -> impl Future<Output = Result<Option<ProjectionRecordMetadata>, ProjectionProtocolError>> + Send + 'a;
        fn projection_input_disposition<'a>(&'a self, input: &'a TrustedProjectionInput)
            -> impl Future<Output = Result<ProjectionInputDisposition, ProjectionProtocolError>> + Send + 'a;
        fn projection_query_snapshot<'a>(&'a self, request: &'a ProjectionQuerySnapshotRequest)
            -> impl Future<Output = Result<ProjectionQuerySnapshot, ProjectionProtocolError>> + Send + 'a;
        fn projection_execution_snapshot_batch<'a>(
            &'a self,
            request: &'a ProjectionExecutionSnapshotBatchRequest,
        ) -> impl Future<Output = Result<ProjectionExecutionSnapshotBatch, ProjectionProtocolError>> + Send + 'a;
        fn projection_causation_evidence<'a>(&'a self, request: &'a ProjectionCausationEvidenceRequest)
            -> impl Future<Output = Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>> + Send + 'a;
        fn projection_partition_runtime_state<'a>(
            &'a self,
            topology: &'a ProjectorTopologyId,
            partition: &'a PartitionId,
        ) -> impl Future<Output = Result<Option<ProjectionPartitionRuntimeState>, ProjectionProtocolError>> + Send + 'a;
        fn projection_observation<'a>(
            &'a self,
            causation_id: &'a str,
            scope: &'a ProjectionRecordScope,
            kind: ProjectionObservationKind,
        ) -> impl Future<Output = Result<Option<ProjectionObservation>, ProjectionProtocolError>> + Send + 'a;
        fn projection_changes<'a>(
            &'a self,
            topology: &'a ProjectorTopologyId,
            partition: &'a PartitionId,
            after: Option<&'a ProjectionChangeCursor>,
            limit: usize,
        ) -> impl Future<Output = Result<ProjectionChangeRead, ProjectionProtocolError>> + Send + 'a;
        fn repair_projection<'a>(
            &'a self,
            topology: &'a ProjectorTopologyId,
            partition: &'a PartitionId,
            failure_id: &'a str,
        ) -> impl Future<Output = Result<ProjectionGeneration, ProjectionProtocolError>> + Send + 'a;
        fn compact_projection_changes<'a>(&'a self, through: &'a ProjectionChangeCursor)
            -> impl Future<Output = Result<u64, ProjectionProtocolError>> + Send + 'a;
        fn projection_failure<'a>(
            &'a self,
            topology: &'a ProjectorTopologyId,
            partition: &'a PartitionId,
            failure_id: &'a str,
        ) -> impl Future<Output = Result<Option<ProjectionFailure>, ProjectionProtocolError>> + Send + 'a;
        fn projection_failure_location<'a>(&'a self, failure_id: &'a str)
            -> impl Future<Output = Result<Option<ProjectionFailureLocation>, ProjectionProtocolError>> + Send + 'a;
    }
}

// Read models, snapshots, the consumer inbox, schedules and the backend host
// are not gated by aggregate locks, so a queued repository stays a complete
// drop-in for the repository it wraps.

impl<R: SnapshotStore, L: LockManager> SnapshotStore for QueuedRepository<R, L> {
    delegate! {
        fn get_snapshot<'a>(&'a self, identity: &'a StreamIdentity)
            -> impl Future<Output = Result<Option<SnapshotRecord>, RepositoryError>> + Send + 'a;
        fn get_snapshots<'a>(&'a self, identities: &'a [StreamIdentity])
            -> impl Future<Output = Result<Vec<SnapshotRecord>, RepositoryError>> + Send + 'a;
        fn save_snapshot<'a>(&'a self, identity: &'a StreamIdentity, record: SnapshotRecord)
            -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a;
        fn delete_snapshot<'a>(&'a self, identity: &'a StreamIdentity)
            -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a;
    }
}

impl<R: ReadModelWritePlanStore, L: LockManager> ReadModelWritePlanStore
    for QueuedRepository<R, L>
{
    delegate! {
        fn commit_write_plan(&self, plan: TableWritePlan)
            -> impl Future<Output = Result<(), TableStoreError>> + Send + '_;
    }
}

impl<R: RelationalReadModelQueryStore, L: LockManager> RelationalReadModelQueryStore
    for QueuedRepository<R, L>
{
    delegate! {
        fn scan_read_model(&self, request: ReadModelScanRequest)
            -> impl Future<Output = Result<ReadModelScanPage, TableStoreError>> + Send + '_;
        fn load_graph(&self, request: ReadModelLoadRequest)
            -> impl Future<Output = Result<ReadModelLoadGraph, TableStoreError>> + Send + '_;
    }
}

impl<R: InboxStore, L: LockManager> InboxStore for QueuedRepository<R, L> {
    delegate! {
        fn inbox_contains<'a>(&'a self, consumer: &'a str, message_id: &'a str)
            -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a;
        fn purge_inbox_older_than(&self, age: Duration)
            -> impl Future<Output = Result<u64, RepositoryError>> + Send;
    }
}

impl<R: BackendHost, L: LockManager> BackendHost for QueuedRepository<R, L> {
    delegate! {
        fn capabilities(&self) -> Capabilities;
        fn host_now(&self) -> impl Future<Output = Result<SystemTime, RepositoryError>> + Send + '_;
    }
}

#[cfg(feature = "runtime-tokio")]
#[async_trait::async_trait]
impl<R, L> crate::schedule::ScheduleStore for QueuedRepository<R, L>
where
    R: crate::schedule::ScheduleStore,
    L: LockManager + 'static,
{
    fn with_schedule_dispatch(self) -> Self {
        Self {
            inner: self.inner.with_schedule_dispatch(),
            lock_manager: self.lock_manager,
        }
    }

    async fn schedule_now(&self) -> Result<SystemTime, RepositoryError> {
        self.inner.schedule_now().await
    }

    async fn claim_due_schedules(
        &self,
        now_ms: i64,
        claim_until_ms: i64,
        limit: usize,
    ) -> Result<Vec<(StreamIdentity, crate::schedule::ArmedSchedule)>, RepositoryError> {
        self.inner
            .claim_due_schedules(now_ms, claim_until_ms, limit)
            .await
    }

    async fn settle_schedule(
        &self,
        owner: &StreamIdentity,
        name: &str,
        generation: u64,
        settlement: &crate::schedule::ScheduleSettlement,
    ) -> Result<(), RepositoryError> {
        self.inner
            .settle_schedule(owner, name, generation, settlement)
            .await
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
    use crate::lock::Lock;

    #[tokio::test]
    async fn causal_commit_does_not_release_a_lock_it_does_not_own() {
        let repository = QueuedRepository::new(InMemoryRepository::new());
        let aggregate_id = format!("queued-causal-lock-{}", Uuid::now_v7());
        let identity = StreamIdentity::new("queued-causal-test", &aggregate_id).unwrap();

        // Another writer holds the stream lock while the causal commit runs.
        let held_lock = repository
            .lock_manager()
            .get_lock(&identity.storage_key())
            .unwrap();
        let held = LockGuard::acquire(Arc::clone(&held_lock)).await.unwrap();

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
            "a causal commit must not release a lock another writer holds"
        );
        held.release().await.unwrap();
    }
}
