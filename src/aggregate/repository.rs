use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::command_ledger::CausalGetStream;
use crate::entity::Entity;
use crate::outbox::OutboxPublisherConfig;
use crate::queued_repo::{GetAllWithOpts, GetWithOpts, ReadOpts, UnlockableRepository};
use crate::repository::{
    CommitBatch, GetStream, RepositoryError, SnapshotWrite, StreamIdentity, StreamWrite,
    TransactionalCommit,
};
use crate::snapshot::SnapshotRecord;

use super::{hydrate, Aggregate};

fn stream_identity_for<A: Aggregate>(
    aggregate_id: &str,
) -> Result<StreamIdentity, RepositoryError> {
    StreamIdentity::new(A::aggregate_type(), aggregate_id)
}

/// Builder trait for creating typed async aggregate repositories.
pub trait AggregateBuilder: Sized {
    fn aggregate<A: Aggregate>(self) -> AggregateRepository<Self, A> {
        AggregateRepository::new(self)
    }
}

impl<T> AggregateBuilder for T {}

/// Snapshot behaviour for an [`AggregateRepository`], installed by
/// `with_snapshots`.
///
/// A snapshot is a rebuildable cache over the event stream, so enabling it must
/// not change the repository's API. The `Snapshottable` / `SnapshotStore`
/// requirements are captured here as monomorphized function pointers at
/// `with_snapshots` time, which keeps the repository's generic get/commit methods
/// unbounded — they just consult `Option<SnapshotPolicy>`.
pub(crate) struct SnapshotPolicy<R, A> {
    /// How many events between automatic snapshots.
    frequency: u64,
    /// Build a snapshot cache record for the aggregate when one is due.
    record: fn(&A, u64) -> Result<Option<SnapshotRecord>, RepositoryError>,
    /// Hydrate an already-loaded entity from the cache record (if any). Used on
    /// load paths that have the full stream in hand (locked reads).
    hydrate: HydrateFn<R, A>,
    /// Hydrate a batch of already-loaded entities, reading all cache records in
    /// one round trip. Used by batch loads (`get_all`).
    hydrate_all: HydrateAllFn<R, A>,
    /// Own the whole load: read the snapshot first, then fetch only the tail of
    /// the stream (skipping already-snapshotted I/O), falling back to a full
    /// load on a cache miss. Used by the single-aggregate `get` hot path.
    load: LoadFn<R, A>,
}

type HydrateFn<R, A> =
    for<'a> fn(
        &'a R,
        &'a StreamIdentity,
        Entity,
    ) -> Pin<Box<dyn Future<Output = Result<A, RepositoryError>> + Send + 'a>>;

type HydrateAllFn<R, A> =
    for<'a> fn(
        &'a R,
        Vec<(StreamIdentity, Entity)>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<A>, RepositoryError>> + Send + 'a>>;

type LoadFn<R, A> = for<'a> fn(
    &'a R,
    &'a StreamIdentity,
    StreamReads<R>,
) -> Pin<
    Box<dyn Future<Output = Result<Option<A>, RepositoryError>> + Send + 'a>,
>;

type StreamReadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a>>;

/// The same snapshot algorithm uses ordinary or explicitly non-locking reads.
/// Capturing the functions keeps with_snapshots independent of causal bounds.
pub(crate) struct StreamReads<R> {
    pub full: for<'a> fn(&'a R, &'a StreamIdentity) -> StreamReadFuture<'a>,
    pub tail: for<'a> fn(&'a R, &'a StreamIdentity, u64) -> StreamReadFuture<'a>,
}

impl<R: GetStream> StreamReads<R> {
    fn ordinary() -> Self {
        Self {
            full: |repo, identity| Box::pin(repo.get_stream(identity)),
            tail: |repo, identity, version| Box::pin(repo.get_stream_tail(identity, version)),
        }
    }
}

impl<R: CausalGetStream> StreamReads<R> {
    fn causal() -> Self {
        Self {
            full: |repo, identity| Box::pin(repo.get_causal_stream(identity)),
            tail: |repo, identity, version| {
                Box::pin(repo.get_causal_stream_tail(identity, version))
            },
        }
    }
}

impl<R, A> SnapshotPolicy<R, A> {
    /// Construct a policy from its captured hooks. Called by `with_snapshots`,
    /// which carries the `Snapshottable`/`SnapshotStore`/`GetStream` bounds.
    pub(crate) fn new(
        frequency: u64,
        record: fn(&A, u64) -> Result<Option<SnapshotRecord>, RepositoryError>,
        hydrate: HydrateFn<R, A>,
        hydrate_all: HydrateAllFn<R, A>,
        load: LoadFn<R, A>,
    ) -> Self {
        Self {
            frequency,
            record,
            hydrate,
            hydrate_all,
            load,
        }
    }
}

/// Repository wrapper for a specific aggregate type.
///
/// Snapshots are an optional, transparent optimization: `with_snapshots(n)`
/// configures snapshot caching on this same type, and every method behaves
/// identically with or without it — on commit a snapshot is staged in the same
/// transaction when due, and on load the aggregate is hydrated from a snapshot
/// when one exists.
pub struct AggregateRepository<R, A> {
    repo: R,
    snapshot: Option<SnapshotPolicy<R, A>>,
    outbox_publisher: Option<OutboxPublisherConfig>,
    _marker: PhantomData<A>,
}

impl<R, A> AggregateRepository<R, A> {
    pub fn new(repo: R) -> Self {
        Self {
            repo,
            snapshot: None,
            outbox_publisher: None,
            _marker: PhantomData,
        }
    }

    pub fn repo(&self) -> &R {
        &self.repo
    }

    pub fn repo_mut(&mut self) -> &mut R {
        &mut self.repo
    }

    /// Install the snapshot policy (used by `with_snapshots`).
    pub(crate) fn set_snapshot_policy(&mut self, policy: SnapshotPolicy<R, A>) {
        self.snapshot = Some(policy);
    }

    /// Install the outbox publisher so commits publish immediately (used by
    /// `Service::with_bus`).
    pub(crate) fn set_outbox_publisher(&mut self, config: OutboxPublisherConfig) {
        self.outbox_publisher = Some(config);
    }

    /// The configured outbox publisher, if any. Consulted by
    /// `OutboxCommit::commit`.
    pub(crate) fn outbox_publisher(&self) -> Option<&OutboxPublisherConfig> {
        self.outbox_publisher.as_ref()
    }
}

impl<R, A> AggregateRepository<R, A>
where
    A: Aggregate + Send,
{
    /// Hydrate one entity into an aggregate, using the snapshot cache when a
    /// policy is configured and a cache record is available, otherwise a full
    /// replay. Same result either way.
    async fn hydrate_entity(
        &self,
        identity: &StreamIdentity,
        entity: Entity,
    ) -> Result<A, RepositoryError> {
        match &self.snapshot {
            Some(policy) => (policy.hydrate)(&self.repo, identity, entity).await,
            None => hydrate::<A>(entity),
        }
    }

    /// Snapshot writes to stage alongside a commit of `aggregate`, plus the
    /// covered version to record on the entity afterwards. Empty when no policy
    /// is configured or a snapshot is not yet due.
    fn snapshot_writes(
        &self,
        aggregate: &A,
    ) -> Result<(Vec<SnapshotWrite>, Option<u64>), RepositoryError> {
        let Some(policy) = &self.snapshot else {
            return Ok((Vec::new(), None));
        };
        let Some(record) = (policy.record)(aggregate, policy.frequency)? else {
            return Ok((Vec::new(), None));
        };
        let version = record.version;
        let identity = stream_identity_for::<A>(aggregate.entity().id())?;
        Ok((
            vec![SnapshotWrite::Save { identity, record }],
            Some(version),
        ))
    }

    /// Snapshot writes for `aggregate`, exposed to the outbox/read-model commit
    /// builders so they stage snapshots in the same transaction.
    pub(crate) fn snapshot_writes_for(
        &self,
        aggregate: &A,
    ) -> Result<(Vec<SnapshotWrite>, Option<u64>), RepositoryError> {
        self.snapshot_writes(aggregate)
    }
}

impl<R, A> AggregateRepository<R, A>
where
    A: Aggregate + Send,
{
    /// Causal handlers use the same snapshot policy, with non-locking reads.
    pub(crate) async fn get_causal(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<A>, RepositoryError>
    where
        R: CausalGetStream,
    {
        match &self.snapshot {
            Some(policy) => (policy.load)(&self.repo, identity, StreamReads::causal()).await,
            None => self
                .repo
                .get_causal_stream(identity)
                .await?
                .map(hydrate::<A>)
                .transpose(),
        }
    }
}

impl<R, A> AggregateRepository<R, A>
where
    R: GetStream,
    A: Aggregate + Send,
{
    pub async fn get(&self, id: &str) -> Result<Option<A>, RepositoryError> {
        let identity = stream_identity_for::<A>(id)?;
        // With a snapshot policy, the policy owns the load so it can read the
        // snapshot first and fetch only the post-snapshot tail (skipping the I/O
        // and decode of already-snapshotted events). Without one, a plain full
        // stream load. Same hydrated aggregate either way.
        match &self.snapshot {
            Some(policy) => (policy.load)(&self.repo, &identity, StreamReads::ordinary()).await,
            None => {
                let Some(entity) = self.repo.get_stream(&identity).await? else {
                    return Ok(None);
                };
                Ok(Some(hydrate::<A>(entity)?))
            }
        }
    }

    /// Load existing aggregates for the provided ids.
    ///
    /// Each id is converted to a `StreamIdentity`, fetched through `get_streams`,
    /// and hydrated if present. Missing streams are skipped, and backend
    /// implementations may return aggregates in storage order rather than input
    /// order.
    pub async fn get_all(&self, ids: &[&str]) -> Result<Vec<A>, RepositoryError> {
        let identities = ids
            .iter()
            .map(|id| stream_identity_for::<A>(id))
            .collect::<Result<Vec<_>, _>>()?;
        let entities = self.repo.get_streams(&identities).await?;
        self.hydrate_entities(entities).await
    }
}

impl<R, A> AggregateRepository<R, A>
where
    A: Aggregate + Send,
{
    /// Hydrate a batch of entities, deriving each identity from the entity id.
    /// With a snapshot policy the cache records for the whole batch are read in
    /// one round trip; without one, a plain per-entity replay.
    async fn hydrate_entities(&self, entities: Vec<Entity>) -> Result<Vec<A>, RepositoryError> {
        match &self.snapshot {
            Some(policy) => {
                let mut pairs = Vec::with_capacity(entities.len());
                for entity in entities {
                    let identity = stream_identity_for::<A>(entity.id())?;
                    pairs.push((identity, entity));
                }
                (policy.hydrate_all)(&self.repo, pairs).await
            }
            None => entities.into_iter().map(hydrate::<A>).collect(),
        }
    }
}

impl<R, A> AggregateRepository<R, A>
where
    R: TransactionalCommit,
    A: Aggregate + Send,
{
    pub async fn commit(&self, aggregate: &mut A) -> Result<(), RepositoryError> {
        let (snapshots, snapshot_version) = self.snapshot_writes(aggregate)?;
        let identity = stream_identity_for::<A>(aggregate.entity().id())?;
        let stream = StreamWrite::new(identity, aggregate.entity_mut());
        let mut batch = CommitBatch::new(vec![stream]);
        batch.snapshots = snapshots;
        self.repo.commit_batch(batch).await?;
        if let Some(version) = snapshot_version {
            aggregate.entity_mut().set_snapshot_version(version);
        }
        Ok(())
    }

    pub async fn commit_all(&self, aggregates: &mut [&mut A]) -> Result<(), RepositoryError> {
        // Compute snapshot writes (immutable borrows) before taking the mutable
        // entity borrows for the streams.
        let mut snapshots = Vec::new();
        let mut snapshot_versions = Vec::with_capacity(aggregates.len());
        for aggregate in aggregates.iter() {
            let (mut writes, version) = self.snapshot_writes(aggregate)?;
            snapshots.append(&mut writes);
            snapshot_versions.push(version);
        }

        let mut streams = Vec::with_capacity(aggregates.len());
        for aggregate in aggregates.iter_mut() {
            let identity = stream_identity_for::<A>((*aggregate).entity().id())?;
            streams.push(StreamWrite::new(identity, (*aggregate).entity_mut()));
        }
        let mut batch = CommitBatch::new(streams);
        batch.snapshots = snapshots;
        self.repo.commit_batch(batch).await?;

        for (aggregate, version) in aggregates.iter_mut().zip(snapshot_versions) {
            if let Some(version) = version {
                (*aggregate).entity_mut().set_snapshot_version(version);
            }
        }
        Ok(())
    }

    pub async fn commit_entities(
        &self,
        streams: Vec<(StreamIdentity, &mut Entity)>,
    ) -> Result<(), RepositoryError> {
        let streams = streams
            .into_iter()
            .map(|(identity, entity)| StreamWrite::new(identity, entity))
            .collect();
        self.repo.commit_batch(CommitBatch::new(streams)).await
    }
}

impl<R, A> AggregateRepository<R, A>
where
    R: GetWithOpts,
    A: Aggregate + Send,
{
    /// Load an aggregate with options (e.g. `ReadOpts::no_lock()` to skip the
    /// queue lock when the repository is a `queued()` wrapper).
    pub async fn get_with(&self, id: &str, opts: ReadOpts) -> Result<Option<A>, RepositoryError> {
        let identity = stream_identity_for::<A>(id)?;
        let Some(entity) = self.repo.get_stream_with(&identity, opts).await? else {
            return Ok(None);
        };
        Ok(Some(self.hydrate_entity(&identity, entity).await?))
    }

    /// Non-locking read (alias for `get_with(ReadOpts::no_lock())`).
    pub async fn peek(&self, id: &str) -> Result<Option<A>, RepositoryError> {
        self.get_with(id, ReadOpts::no_lock()).await
    }
}

impl<R, A> AggregateRepository<R, A>
where
    R: GetAllWithOpts,
    A: Aggregate + Send,
{
    /// Load aggregates for the provided ids with options.
    pub async fn get_all_with(
        &self,
        ids: &[&str],
        opts: ReadOpts,
    ) -> Result<Vec<A>, RepositoryError> {
        let identities = ids
            .iter()
            .map(|id| stream_identity_for::<A>(id))
            .collect::<Result<Vec<_>, _>>()?;
        let entities = self.repo.get_streams_with(&identities, opts).await?;
        self.hydrate_entities(entities).await
    }

    /// Non-locking multi-read (alias for `get_all_with(ReadOpts::no_lock())`).
    pub async fn peek_all(&self, ids: &[&str]) -> Result<Vec<A>, RepositoryError> {
        self.get_all_with(ids, ReadOpts::no_lock()).await
    }
}

impl<R, A> AggregateRepository<R, A>
where
    R: UnlockableRepository,
    A: Aggregate,
{
    /// Release the lock held for an aggregate after an aborted load.
    pub async fn abort(&self, aggregate: &A) -> Result<(), RepositoryError> {
        let identity = stream_identity_for::<A>(aggregate.entity().id())?;
        // Forward to the repo's `abort` hook (not `unlock`) so an
        // `UnlockableRepository` that overrides `abort` for extra cleanup
        // is honored. The default `abort` delegates to `unlock`.
        self.repo.abort(&identity).await
    }

    /// Release the lock held for an aggregate id.
    pub async fn unlock(&self, id: &str) -> Result<(), RepositoryError> {
        let identity = stream_identity_for::<A>(id)?;
        self.repo.unlock(&identity).await
    }
}
