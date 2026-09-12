//! Typed dependency wrappers for microsvc handlers.

use crate::aggregate::{Aggregate, AggregateRepository};
use crate::command_ledger::{
    CausalGetStream, CausalRepositoryIdentity, CausalTransactionalCommit, CommandLedgerStore,
};
use crate::outbox::OutboxPublisherConfig;
use crate::projection_protocol::ProjectionProtocolStore;
use crate::repository::{
    ReadModelWritePlanStore, RelationalReadModelQueryStore, Repository, TransactionalCommit,
};

/// Dependency capability for services that expose an aggregate repository.
pub trait HasRepo {
    type Repo;

    fn repo(&self) -> &Self::Repo;
}

/// Sealed repository capability required by typed causal routes.
///
/// The framework implements this for its causal-capable repository adapters;
/// applications never need to name or implement it.
#[doc(hidden)]
#[allow(private_bounds)]
pub trait CausalRepositoryBackend:
    CausalGetStream
    + CommandLedgerStore
    + CausalTransactionalCommit
    + CausalRepositoryIdentity
    + CausalHostProjections
    + TransactionalCommit
    + Send
    + Sync
    + 'static
{
}

impl<T> CausalRepositoryBackend for T where
    T: CausalGetStream
        + CommandLedgerStore
        + CausalTransactionalCommit
        + CausalRepositoryIdentity
        + CausalHostProjections
        + TransactionalCommit
        + Send
        + Sync
        + 'static
{
}

pub(crate) trait CausalHostProjections: Send + Sync {
    #[cfg(feature = "graphql")]
    fn command_obligation_evidence<'a>(
        &'a self,
        _request: &'a crate::projection_protocol::ProjectionObligationEvidenceBatchRequest,
    ) -> impl std::future::Future<
        Output = Result<
            crate::projection_protocol::ProjectionObligationEvidenceBatch,
            crate::projection_protocol::ProjectionProtocolError,
        >,
    > + Send
           + 'a {
        async {
            Err(
                crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                    "command-only cells do not query projection evidence".into(),
                ),
            )
        }
    }

    #[cfg(feature = "graphql")]
    fn command_causation_evidence<'a>(
        &'a self,
        _request: &'a crate::projection_protocol::ProjectionCausationEvidenceRequest,
    ) -> impl std::future::Future<
        Output = Result<
            crate::projection_protocol::ProjectionCausationEvidenceBatch,
            crate::projection_protocol::ProjectionProtocolError,
        >,
    > + Send
           + 'a {
        async {
            Err(
                crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                    "command-only cells do not query projection evidence".into(),
                ),
            )
        }
    }
    /// Bootstrap direct projections only on hosts that own a projection store.
    /// Command-only hosts reject this operation without inventing query storage.
    fn __register_direct_projection_models<'a>(
        &'a self,
        topology: &'a crate::projection_protocol::ProjectorTopologyId,
        ownership: &'a [crate::projection_protocol::ProjectionModelOwnership],
    ) -> impl std::future::Future<
        Output = Result<(), crate::projection_protocol::ProjectionProtocolError>,
    > + Send
           + 'a;
}

// Concrete query-owning adapters forward only the causal host operations.
// A queued command-only adapter forwards this smaller capability independently.
macro_rules! direct_causal_projection_methods {
    () => {
        #[cfg(feature = "graphql")]
        async fn command_obligation_evidence(
            &self,
            request: &crate::projection_protocol::ProjectionObligationEvidenceBatchRequest,
        ) -> Result<
            crate::projection_protocol::ProjectionObligationEvidenceBatch,
            crate::projection_protocol::ProjectionProtocolError,
        > {
            crate::projection_protocol::ProjectionProtocolStore::projection_obligation_evidence_batch(
                self, request,
            ).await
        }

        #[cfg(feature = "graphql")]
        async fn command_causation_evidence(
            &self,
            request: &crate::projection_protocol::ProjectionCausationEvidenceRequest,
        ) -> Result<
            crate::projection_protocol::ProjectionCausationEvidenceBatch,
            crate::projection_protocol::ProjectionProtocolError,
        > {
            crate::projection_protocol::ProjectionProtocolStore::projection_causation_evidence(
                self, request,
            ).await
        }

        async fn __register_direct_projection_models(
            &self,
            topology: &crate::projection_protocol::ProjectorTopologyId,
            ownership: &[crate::projection_protocol::ProjectionModelOwnership],
        ) -> Result<(), crate::projection_protocol::ProjectionProtocolError> {
            crate::projection_protocol::ProjectionProtocolStore::register_projection_models(
                self, topology, ownership,
            ).await
        }
    };
}
#[cfg(all(test, feature = "graphql"))]
pub(crate) use direct_causal_projection_methods;

impl CausalHostProjections for crate::InMemoryRepository {
    direct_causal_projection_methods!();
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
impl<DB: sqlx::Database> CausalHostProjections for crate::sqlx_repo::repo::SqlxRepository<DB>
where
    Self: ProjectionProtocolStore,
{
    direct_causal_projection_methods!();
}

/// Compile-time extraction of the one aggregate repository owned by a typed
/// causal route bundle.
///
/// This is public only because it appears in the bounds of the public route
/// builder; the framework supplies the implementations. Application handlers
/// never receive the dependency value or the underlying backend through the
/// causal command context.
#[doc(hidden)]
pub trait CausalRouteDependencies {
    type Backend: CausalRepositoryBackend;
    type Aggregate: Aggregate;

    #[doc(hidden)]
    fn __causal_aggregate_repository(&self)
        -> &AggregateRepository<Self::Backend, Self::Aggregate>;
}

/// Dependency capability for services that expose a read-model store.
pub trait HasReadModelStore {
    type ReadModelStore;

    fn read_model_store(&self) -> &Self::ReadModelStore;
}

/// Sealed storage capability required by framework-owned causal projector
/// routes. Applications select one of the framework repository adapters; they
/// never receive this commit capability in a projector handler.
#[doc(hidden)]
#[allow(private_bounds)]
pub trait CausalProjectionStore: ProjectionProtocolStore + Clone + Send + Sync + 'static {}

impl<T> CausalProjectionStore for T where T: ProjectionProtocolStore + Clone + Send + Sync + 'static {}

/// Compile-time extraction of the read-model/projection store owned by a route
/// bundle. Public only because it appears in the causal-projector builder's
/// inferred bounds.
#[doc(hidden)]
pub trait CausalProjectionRouteDependencies {
    type Store: CausalProjectionStore;

    #[doc(hidden)]
    fn __causal_projection_store(&self) -> &Self::Store;
}

impl<D> CausalProjectionRouteDependencies for D
where
    D: HasReadModelStore,
    D::ReadModelStore: CausalProjectionStore,
{
    type Store = D::ReadModelStore;

    fn __causal_projection_store(&self) -> &Self::Store {
        self.read_model_store()
    }
}

/// Dependency capability for repositories whose outbox commits can publish
/// immediately.
///
/// `Service::with_bus` installs an [`OutboxPublisherConfig`] through this so that
/// `repo.outbox(msg).commit(agg)` enqueues the pending row for the bounded
/// worker. Without it, commits leave the row `pending` for a polling worker.
pub trait ConfigurableOutboxPublisher {
    /// Install the outbox publisher.
    fn configure_outbox_publisher(&mut self, config: OutboxPublisherConfig);
}

impl<R, A> ConfigurableOutboxPublisher for AggregateRepository<R, A> {
    fn configure_outbox_publisher(&mut self, config: OutboxPublisherConfig) {
        self.set_outbox_publisher(config);
    }
}

impl<R: ConfigurableOutboxPublisher, S> ConfigurableOutboxPublisher
    for RepoReadModelDependencies<R, S>
{
    fn configure_outbox_publisher(&mut self, config: OutboxPublisherConfig) {
        self.repo.configure_outbox_publisher(config);
    }
}

impl<R: HasOutboxStore, S> HasOutboxStore for RepoReadModelDependencies<R, S> {
    type OutboxStore = R::OutboxStore;
    fn outbox_store(&self) -> Self::OutboxStore {
        self.repo().outbox_store()
    }
}

/// Dependency capability for repositories that expose a durable outbox store.
///
/// A runtime uses this to build an `OutboxDispatcher` that drains committed
/// outbox rows to a transport, without naming the concrete repository type. The
/// capability resolves through the repository wrappers
/// (`AggregateRepository` -> `QueuedRepository` -> the leaf SQL/in-memory repo).
pub trait HasOutboxStore {
    /// The concrete outbox store this repository produces.
    type OutboxStore: crate::outbox_worker::OutboxStore;

    /// Produce a handle to the durable outbox store.
    fn outbox_store(&self) -> Self::OutboxStore;
}

// Leaf repositories own the store. Each calls its inherent `outbox_store()` —
// inherent methods take precedence over the trait method of the same name, so
// `self.outbox_store()` here does not recurse.
impl HasOutboxStore for crate::InMemoryRepository {
    type OutboxStore = crate::InMemoryOutboxStore;
    fn outbox_store(&self) -> Self::OutboxStore {
        crate::InMemoryRepository::outbox_store(self)
    }
}

#[cfg(feature = "sqlite")]
impl HasOutboxStore for crate::SqliteRepository {
    type OutboxStore = crate::SqliteOutboxStore;
    fn outbox_store(&self) -> Self::OutboxStore {
        crate::SqliteRepository::outbox_store(self)
    }
}

#[cfg(feature = "postgres")]
impl HasOutboxStore for crate::PostgresRepository {
    type OutboxStore = crate::PostgresOutboxStore;
    fn outbox_store(&self) -> Self::OutboxStore {
        crate::PostgresRepository::outbox_store(self)
    }
}

// Wrappers delegate inward.
impl<R, A> HasOutboxStore for AggregateRepository<R, A>
where
    R: HasOutboxStore,
{
    type OutboxStore = R::OutboxStore;
    fn outbox_store(&self) -> Self::OutboxStore {
        self.repo().outbox_store()
    }
}

impl<R, L> HasOutboxStore for crate::QueuedRepository<R, L>
where
    R: HasOutboxStore,
{
    type OutboxStore = R::OutboxStore;
    fn outbox_store(&self) -> Self::OutboxStore {
        self.inner().outbox_store()
    }
}

impl<R> HasRepo for R
where
    R: Repository,
{
    type Repo = R;

    fn repo(&self) -> &Self::Repo {
        self
    }
}

impl<R, A> HasRepo for AggregateRepository<R, A> {
    type Repo = Self;

    fn repo(&self) -> &Self::Repo {
        self
    }
}

impl<R, A> CausalRouteDependencies for AggregateRepository<R, A>
where
    R: CausalRepositoryBackend,
    A: Aggregate,
{
    type Backend = R;
    type Aggregate = A;

    fn __causal_aggregate_repository(&self) -> &AggregateRepository<R, A> {
        self
    }
}

impl<S> HasReadModelStore for S
where
    S: ReadModelWritePlanStore + RelationalReadModelQueryStore,
{
    type ReadModelStore = S;

    fn read_model_store(&self) -> &Self::ReadModelStore {
        self
    }
}

/// Dependencies for a service that only needs an aggregate repository.
#[derive(Clone)]
pub struct RepoDependencies<R> {
    repo: R,
}

impl<R> RepoDependencies<R> {
    pub fn new(repo: R) -> Self {
        Self { repo }
    }
}

impl<R> HasRepo for RepoDependencies<R> {
    type Repo = R;

    fn repo(&self) -> &Self::Repo {
        &self.repo
    }
}

impl<R, A> CausalRouteDependencies for RepoDependencies<AggregateRepository<R, A>>
where
    R: CausalRepositoryBackend,
    A: Aggregate,
{
    type Backend = R;
    type Aggregate = A;

    fn __causal_aggregate_repository(&self) -> &AggregateRepository<R, A> {
        &self.repo
    }
}

/// Dependencies for a service that only needs a read-model store.
#[derive(Clone)]
pub struct ReadModelStoreDependencies<S> {
    read_model_store: S,
}

impl<S> ReadModelStoreDependencies<S> {
    pub fn new(read_model_store: S) -> Self {
        Self { read_model_store }
    }
}

impl<S> HasReadModelStore for ReadModelStoreDependencies<S> {
    type ReadModelStore = S;

    fn read_model_store(&self) -> &Self::ReadModelStore {
        &self.read_model_store
    }
}

/// Dependencies for a service that needs both an aggregate repository and a
/// read-model store.
#[derive(Clone)]
pub struct RepoReadModelDependencies<R, S> {
    repo: R,
    read_model_store: S,
}

impl<R, S> RepoReadModelDependencies<R, S> {
    pub fn new(repo: R, read_model_store: S) -> Self {
        Self {
            repo,
            read_model_store,
        }
    }
}

impl<R, S> HasRepo for RepoReadModelDependencies<R, S> {
    type Repo = R;

    fn repo(&self) -> &Self::Repo {
        &self.repo
    }
}

impl<R, A, S> CausalRouteDependencies for RepoReadModelDependencies<AggregateRepository<R, A>, S>
where
    R: CausalRepositoryBackend,
    A: Aggregate,
{
    type Backend = R;
    type Aggregate = A;

    fn __causal_aggregate_repository(&self) -> &AggregateRepository<R, A> {
        &self.repo
    }
}

impl<R, S> HasReadModelStore for RepoReadModelDependencies<R, S> {
    type ReadModelStore = S;

    fn read_model_store(&self) -> &Self::ReadModelStore {
        &self.read_model_store
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_has_outbox_store<T: HasOutboxStore>() {}

    fn assert_causal_backend<T: CausalRepositoryBackend>() {}

    // Keep this generic: a concrete SQL/memory instantiation would also satisfy
    // ProjectionProtocolStore and conceal an accidental query-store bound.
    fn assert_queued_causal_backend<R: CausalRepositoryBackend, L: crate::LockManager + 'static>() {
        assert_causal_backend::<crate::QueuedRepository<R, L>>();
    }

    #[test]
    fn causal_capability_resolves_without_a_projection_store() {
        assert_queued_causal_backend::<crate::cell_host::CellStreamStore, crate::InMemoryLockManager>(
        );
        assert_queued_causal_backend::<crate::InMemoryRepository, crate::InMemoryLockManager>();
    }

    #[test]
    fn has_outbox_store_resolves_through_repo_wrappers() {
        // The capability must resolve for the leaf repo and through the
        // AggregateRepository -> QueuedRepository wrapper chain the canonical
        // `repo.queued().aggregate::<A>()` builder produces. `A` is unbounded,
        // so the unit type stands in for any aggregate.
        assert_has_outbox_store::<crate::InMemoryRepository>();
        assert_has_outbox_store::<AggregateRepository<crate::InMemoryRepository, ()>>();
        assert_has_outbox_store::<
            AggregateRepository<crate::QueuedRepository<crate::InMemoryRepository>, ()>,
        >();
    }
}
