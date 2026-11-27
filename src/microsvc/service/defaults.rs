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

    /// Start one aggregate's routes over a service's stores: commands load
    /// and commit `A` through the locked event store and write read models
    /// to the read-model store.
    ///
    /// Product modules start here, then `mount` their commands:
    ///
    /// ```ignore
    /// pub fn routes<St: ServiceStores>(stores: &St) -> Routes<AggregateDependencies<St, Todo>> {
    ///     Routes::for_aggregate(stores).mount(todo_domain::commands::create())
    /// }
    /// ```
    pub fn for_aggregate<A, St>(
        stores: &St,
    ) -> Routes<crate::microsvc::AggregateDependencies<St, A>>
    where
        A: crate::Aggregate + Send + Sync + 'static,
        St: crate::microsvc::ServiceStores,
    {
        use crate::AggregateBuilder;
        Routes::new()
            .with_repo(stores.queued().aggregate::<A>())
            .with_read_model_store(stores.read_models().clone())
    }

    /// Wire a repository and a read-model store with no aggregate, for
    /// commands that own no stream (a provider webhook, a reconciliation).
    ///
    /// Typed commands registered here run the same ledgered pipeline as an
    /// aggregate's. Their handlers receive `CausalCommandContext<'_, ()>` and
    /// seal outbox messages and read-model plans with
    /// `ctx.outbox(message).commit()`, or return `PreparedCommand::prepare`
    /// when they stage nothing.
    pub fn for_stores<R, S>(
        repo: R,
        read_models: S,
    ) -> Routes<RepoReadModelDependencies<crate::AggregateRepository<R, ()>, S>>
    where
        S: HasReadModelStore + Send + Sync + 'static,
        crate::AggregateRepository<R, ()>:
            HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
    {
        Routes::new()
            .with_repo(crate::AggregateRepository::new(repo))
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
