use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::command_ledger::CausalGetStream;
use crate::entity::Entity;
use crate::lock::LockManager;
use crate::outbox::{AggregateCommit, OutboxPublisherConfig};
use crate::queued_repo::{QueuedRepository, ReadOpts};
use crate::repository::{
    GetStream, RepositoryError, SnapshotWrite, StreamIdentity, TransactionalCommit,
};
use crate::snapshot::SnapshotRecord;

use super::{hydrate, Aggregate};

pub(crate) fn stream_identity_for<A: Aggregate>(
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
    /// Load aggregates: read their snapshots first, then only the stream tails
    /// after them, falling back to a full read for a snapshot that cannot be
    /// used.
    load: LoadFn<R, A>,
}

type LoadFn<R, A> =
    for<'a> fn(
        &'a R,
        &'a [StreamIdentity],
        StreamReads<R>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<A>, RepositoryError>> + Send + 'a>>;

type EntitiesFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Entity>, RepositoryError>> + Send + 'a>>;

/// How one load reads streams: whole streams, or tails after a version (0
/// reads a whole stream). Missing streams are skipped.
///
/// Every read of an [`AggregateRepository`] is one load through one of these:
/// ordinary reads (which lock on a queued repository), a queued repository's
/// non-locking reads, or causal reads.
pub(crate) struct StreamReads<R> {
    pub full: for<'a> fn(&'a R, &'a [StreamIdentity]) -> EntitiesFuture<'a>,
    pub tails: for<'a> fn(&'a R, &'a [(StreamIdentity, u64)]) -> EntitiesFuture<'a>,
}

impl<R> Clone for StreamReads<R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R> Copy for StreamReads<R> {}

impl<R: GetStream> StreamReads<R> {
    fn ordinary() -> Self {
        Self {
            full: |repo, identities| Box::pin(repo.get_streams(identities)),
            tails: |repo, requests| Box::pin(repo.get_stream_tails(requests)),
        }
    }
}

impl<R: GetStream, L: LockManager> StreamReads<QueuedRepository<R, L>> {
    /// Reads past a queued repository's locks.
    fn unlocked() -> Self {
        Self {
            full: |repo, identities| Box::pin(repo.inner().get_streams(identities)),
            tails: |repo, requests| Box::pin(repo.inner().get_stream_tails(requests)),
        }
    }
}

impl<R: CausalGetStream> StreamReads<R> {
    fn causal() -> Self {
        Self {
            full: |repo, identities| {
                Box::pin(async move {
                    let mut entities = Vec::with_capacity(identities.len());
                    for identity in identities {
                        entities.extend(repo.get_causal_stream(identity).await?);
                    }
                    Ok(entities)
                })
            },
            tails: |repo, requests| {
                Box::pin(async move {
                    let mut entities = Vec::with_capacity(requests.len());
                    for (identity, after_version) in requests {
                        entities.extend(
                            repo.get_causal_stream_tail(identity, *after_version)
                                .await?,
                        );
                    }
                    Ok(entities)
                })
            },
        }
    }
}

impl<R, A> SnapshotPolicy<R, A> {
    /// Construct a policy from its captured hooks. Called by `with_snapshots`,
    /// which carries the `Snapshottable`/`SnapshotStore` bounds.
    pub(crate) fn new(
        frequency: u64,
        record: fn(&A, u64) -> Result<Option<SnapshotRecord>, RepositoryError>,
        load: LoadFn<R, A>,
    ) -> Self {
        Self {
            frequency,
            record,
            load,
        }
    }
}

/// Repository wrapper for a specific aggregate type.
///
/// Snapshots are an optional, transparent optimization: `with_snapshots(n)`
/// configures snapshot caching on this same type, and every method behaves
/// identically with or without it — on commit a snapshot is staged in the same
/// transaction when due, and every read hydrates from a snapshot plus the
/// stream tail after it when one exists.
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
    /// [`AggregateCommit::commit`].
    pub(crate) fn outbox_publisher(&self) -> Option<&OutboxPublisherConfig> {
        self.outbox_publisher.as_ref()
    }
}

impl<R, A> AggregateRepository<R, A>
where
    A: Aggregate + Send,
{
    /// Snapshot writes to stage alongside a commit of `aggregate`, plus the
    /// covered version to record on the entity afterwards. Empty when no policy
    /// is configured or a snapshot is not yet due.
    pub(crate) fn snapshot_writes(
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

    /// The one load: snapshot then tail under a snapshot policy, whole streams
    /// without one. Same hydrated aggregates either way.
    async fn load(
        &self,
        identities: &[StreamIdentity],
        reads: StreamReads<R>,
    ) -> Result<Vec<A>, RepositoryError> {
        match &self.snapshot {
            Some(policy) => (policy.load)(&self.repo, identities, reads).await,
            None => (reads.full)(&self.repo, identities)
                .await?
                .into_iter()
                .map(hydrate::<A>)
                .collect(),
        }
    }

    async fn load_one(
        &self,
        id: &str,
        reads: StreamReads<R>,
    ) -> Result<Option<A>, RepositoryError> {
        let identity = stream_identity_for::<A>(id)?;
        Ok(self
            .load(std::slice::from_ref(&identity), reads)
            .await?
            .pop())
    }

    async fn load_many(
        &self,
        ids: &[&str],
        reads: StreamReads<R>,
    ) -> Result<Vec<A>, RepositoryError> {
        let identities = ids
            .iter()
            .map(|id| stream_identity_for::<A>(id))
            .collect::<Result<Vec<_>, _>>()?;
        self.load(&identities, reads).await
    }

    /// Causal handlers use the same snapshot policy, with non-locking reads.
    pub(crate) async fn get_causal(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Option<A>, RepositoryError>
    where
        R: CausalGetStream,
    {
        Ok(self
            .load(std::slice::from_ref(identity), StreamReads::causal())
            .await?
            .pop())
    }
}

impl<R, A> AggregateRepository<R, A>
where
    R: GetStream,
    A: Aggregate + Send,
{
    pub async fn get(&self, id: &str) -> Result<Option<A>, RepositoryError> {
        self.load_one(id, StreamReads::ordinary()).await
    }

    /// Load existing aggregates for the provided ids.
    ///
    /// Missing streams are skipped, and backend implementations may return
    /// aggregates in storage order rather than input order.
    pub async fn get_all(&self, ids: &[&str]) -> Result<Vec<A>, RepositoryError> {
        self.load_many(ids, StreamReads::ordinary()).await
    }
}

impl<R, A> AggregateRepository<R, A>
where
    R: TransactionalCommit,
    A: Aggregate + Send,
{
    /// Commit the aggregate's new events (and a snapshot, when due): the
    /// zero-option form of the commit builder ([`AggregateCommit`]).
    ///
    /// Domain events the aggregate captured must be published; a commit that
    /// would drop them fails (use [`publish_events`](Self::publish_events)).
    pub async fn commit(&self, aggregate: &mut A) -> Result<(), RepositoryError> {
        AggregateCommit::new(self).commit(aggregate).await?;
        Ok(())
    }

    /// Commit several aggregates of this type in one transaction: the
    /// zero-option form of [`AggregateCommit::commit_all`].
    pub async fn commit_all(&self, aggregates: &mut [&mut A]) -> Result<(), RepositoryError> {
        AggregateCommit::new(self).commit_all(aggregates).await?;
        Ok(())
    }
}

impl<R, L, A> AggregateRepository<QueuedRepository<R, L>, A>
where
    R: GetStream,
    L: LockManager,
    A: Aggregate + Send,
{
    /// Load an aggregate with options (`ReadOpts::no_lock()` skips the queue
    /// lock).
    pub async fn get_with(&self, id: &str, opts: ReadOpts) -> Result<Option<A>, RepositoryError> {
        self.load_one(id, Self::reads(opts)).await
    }

    /// Non-locking read (alias for `get_with(ReadOpts::no_lock())`).
    pub async fn peek(&self, id: &str) -> Result<Option<A>, RepositoryError> {
        self.get_with(id, ReadOpts::no_lock()).await
    }

    /// Load aggregates for the provided ids with options.
    pub async fn get_all_with(
        &self,
        ids: &[&str],
        opts: ReadOpts,
    ) -> Result<Vec<A>, RepositoryError> {
        self.load_many(ids, Self::reads(opts)).await
    }

    /// Non-locking multi-read (alias for `get_all_with(ReadOpts::no_lock())`).
    pub async fn peek_all(&self, ids: &[&str]) -> Result<Vec<A>, RepositoryError> {
        self.get_all_with(ids, ReadOpts::no_lock()).await
    }

    fn reads(opts: ReadOpts) -> StreamReads<QueuedRepository<R, L>> {
        if opts.lock {
            StreamReads::ordinary()
        } else {
            StreamReads::unlocked()
        }
    }
}
