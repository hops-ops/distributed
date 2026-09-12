use super::routes::{configure_outbox_for, Routes};
use crate::microsvc::dependencies::{
    ConfigurableOutboxPublisher, HasOutboxStore, HasReadModelStore, HasRepo,
    RepoReadModelDependencies,
};

impl Default for Routes<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl Routes<()> {
    /// Start building a typed route bundle.
    pub fn new() -> Self {
        Self::from_dependencies(())
    }

    /// Framework helper: wire an aggregate repository + read-model store without
    /// application code naming `QueuedRepository` / `RepoReadModelDependencies`.
    ///
    /// Product modules should start here, then register `typed_command` mounts.
    pub fn for_aggregate<R, L, A, S>(
        repo: R,
        locks: L,
        read_models: S,
    ) -> Routes<
        RepoReadModelDependencies<crate::AggregateRepository<crate::QueuedRepository<R, L>, A>, S>,
    >
    where
        R: crate::GetStream + crate::TransactionalCommit + Clone + Send + Sync + 'static,
        L: crate::LockManager + Clone + 'static,
        A: crate::Aggregate + Send + Sync + 'static,
        S: HasReadModelStore + Send + Sync + 'static,
        crate::QueuedRepository<R, L>: Clone
            + crate::AggregateBuilder
            + HasOutboxStore
            + crate::TransactionalCommit
            + Send
            + Sync
            + 'static,
        crate::AggregateRepository<crate::QueuedRepository<R, L>, A>:
            HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
    {
        use crate::{AggregateBuilder, Queueable};
        Routes::new()
            .with_repo(repo.queued_with(locks).aggregate::<A>())
            .with_read_model_store(read_models)
    }

    /// Use any custom dependency value for this route bundle.
    pub fn with_dependencies<D>(self, dependencies: D) -> Routes<D>
    where
        D: Send + Sync + 'static,
    {
        self.assert_no_registrations("with_dependencies");
        Routes::from_dependencies(dependencies)
    }

    /// Use an aggregate repository as the route bundle's dependency.
    pub fn with_repo<R>(self, repo: R) -> Routes<R>
    where
        R: HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
    {
        self.assert_no_registrations("with_repo");
        Routes::from_dependencies(repo).with_outbox_configurator(configure_outbox_for::<R>)
    }

    /// Use a read-model store as the route bundle's dependency.
    pub fn with_read_model_store<S>(self, read_model_store: S) -> Routes<S>
    where
        S: HasReadModelStore + Send + Sync + 'static,
    {
        self.assert_no_registrations("with_read_model_store");
        Routes::from_dependencies(read_model_store)
    }
}

impl<R> Routes<R>
where
    R: HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
{
    /// Add a read-model store alongside the aggregate repository, so handlers can
    /// reach both via `ctx.repo()` and `ctx.read_model_store()`. Call after
    /// `with_repo`.
    pub fn with_read_model_store<S>(
        self,
        read_model_store: S,
    ) -> Routes<RepoReadModelDependencies<R, S>>
    where
        S: HasReadModelStore + Send + Sync + 'static,
    {
        self.assert_no_registrations("with_read_model_store");
        Routes::from_dependencies(RepoReadModelDependencies::new(
            self.dependencies,
            read_model_store,
        ))
        .with_outbox_configurator(configure_outbox_for::<RepoReadModelDependencies<R, S>>)
    }
}
