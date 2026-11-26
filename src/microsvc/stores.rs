//! The stores one service runs on, named once.
//!
//! A module's routes need an event store, a lock manager and a read-model
//! store, and the framework needs a long list of capabilities from each.
//! [`ServiceStores`] states that list once: a module is generic over one
//! `St: ServiceStores` and starts its routes with
//! [`Routes::for_aggregate`](crate::microsvc::Routes::for_aggregate).
//! [`Stores`] is the value a host builds from its backend.

use crate::lock::LockManager;
use crate::microsvc::dependencies::{
    CausalProjectionStore, CausalRepositoryBackend, HasOutboxStore, RepoReadModelDependencies,
};
use crate::{
    AggregateRepository, GetStream, QueuedRepository, ReadModelWritePlanStore,
    RelationalReadModelQueryStore,
};

/// An event store, its locks and a read-model store: what one service runs
/// on. A host builds it once from its backend and hands it to every module.
#[derive(Clone, Debug)]
pub struct Stores<R, L, S> {
    /// Event store: streams, the command ledger and the outbox.
    pub events: R,
    /// Locks that serialize aggregate commands per stream.
    pub locks: L,
    /// Read-model store the projections write and GraphQL reads.
    pub read_models: S,
}

impl<R, L, S> Stores<R, L, S> {
    /// Name the event store, locks and read-model store of one service.
    pub fn new(events: R, locks: L, read_models: S) -> Self {
        Self {
            events,
            locks,
            read_models,
        }
    }
}

/// Every capability a module's routes need from its stores, in one bound.
///
/// Implemented for [`Stores`] whenever its parts are framework stores (in
/// memory, SQLite, Postgres, …); applications never implement it.
pub trait ServiceStores: Clone + Send + Sync + 'static {
    /// The event store, for commands that own no stream
    /// ([`Routes::for_stores`](crate::microsvc::Routes::for_stores)).
    type Events: CausalRepositoryBackend + GetStream + HasOutboxStore;
    /// The event store behind the locks, for aggregate commands.
    type Queued: CausalRepositoryBackend + GetStream + HasOutboxStore;
    /// The read-model store.
    type ReadModels: CausalProjectionStore + ReadModelWritePlanStore + RelationalReadModelQueryStore;

    /// The event store.
    fn events(&self) -> &Self::Events;
    /// The event store serialized by the locks.
    fn queued(&self) -> Self::Queued;
    /// The read-model store.
    fn read_models(&self) -> &Self::ReadModels;
}

impl<R, L, S> ServiceStores for Stores<R, L, S>
where
    R: CausalRepositoryBackend + GetStream + HasOutboxStore,
    L: LockManager + Clone + 'static,
    S: CausalProjectionStore + ReadModelWritePlanStore + RelationalReadModelQueryStore,
    QueuedRepository<R, L>: CausalRepositoryBackend + GetStream + HasOutboxStore,
{
    type Events = R;
    type Queued = QueuedRepository<R, L>;
    type ReadModels = S;

    fn events(&self) -> &R {
        &self.events
    }

    fn queued(&self) -> QueuedRepository<R, L> {
        QueuedRepository::with_lock_manager(self.events.clone(), self.locks.clone())
    }

    fn read_models(&self) -> &S {
        &self.read_models
    }
}

/// Route dependencies of one aggregate's commands over `St`
/// ([`Routes::for_aggregate`](crate::microsvc::Routes::for_aggregate)).
pub type AggregateDependencies<St, A> = RepoReadModelDependencies<
    AggregateRepository<<St as ServiceStores>::Queued, A>,
    <St as ServiceStores>::ReadModels,
>;

/// Route dependencies of commands that own no stream over `St`
/// (`Routes::for_stores(stores.events().clone(), stores.read_models().clone())`).
pub type StoreDependencies<St> = RepoReadModelDependencies<
    AggregateRepository<<St as ServiceStores>::Events, ()>,
    <St as ServiceStores>::ReadModels,
>;

#[cfg(feature = "postgres")]
pub use sql::PostgresStores;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use sql::SqlStores;
#[cfg(feature = "sqlite")]
pub use sql::SqliteStores;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod sql {
    use super::{ServiceStores, Stores};
    use crate::bus::{Bus, BusConsumer};
    use crate::table::ReadModelCatalog;
    use std::error::Error;
    use std::future::Future;

    type BoxError = Box<dyn Error + Send + Sync>;

    /// [`Stores`] kept in one SQL database: one connection pool is the
    /// event store, the read-model store, the locks and the bus.
    ///
    /// A host written once against `SqlStores` runs on SQLite
    /// ([`SqliteStores`]) and Postgres ([`PostgresStores`]) alike.
    pub trait SqlStores: ServiceStores + Sized {
        /// The bus kept in the same database.
        type Bus: Bus + BusConsumer + 'static;

        /// Connect, apply the framework migrations and create the bus tables.
        fn connect(database_url: &str) -> impl Future<Output = Result<Self, BoxError>> + Send;

        /// Create the tables of `catalog` (development and tests; production
        /// applies generated migrations instead).
        fn bootstrap_read_models(
            &self,
            catalog: &ReadModelCatalog,
        ) -> impl Future<Output = Result<(), BoxError>> + Send;

        /// The bus, consuming as `group`.
        fn bus(&self, group: &str) -> Self::Bus;

        /// Committed read-model changes, for GraphQL live queries.
        fn read_model_changes(
            &self,
        ) -> tokio::sync::broadcast::Receiver<crate::read_model::ReadModelChange>;

        /// The database GraphQL queries read through, carrying the
        /// repository identity that proves `Projected` commands update the
        /// database GraphQL reads.
        #[cfg(feature = "graphql")]
        fn graphql_pool(&self) -> crate::graphql::GraphqlPoolSource;
    }

    macro_rules! sql_stores {
        ($(#[$doc:meta])* $alias:ident, $repo:ty, $locks:ty, $bus:ty) => {
            $(#[$doc])*
            pub type $alias = Stores<$repo, $locks, $repo>;

            impl SqlStores for $alias {
                type Bus = $bus;

                async fn connect(database_url: &str) -> Result<Self, BoxError> {
                    let repo = <$repo>::connect_and_migrate(database_url).await?;
                    <$bus>::new(repo.pool().clone()).ensure_tables().await?;
                    let locks = <$locks>::new(repo.pool().clone());
                    Ok(Stores::new(repo.clone(), locks, repo))
                }

                async fn bootstrap_read_models(
                    &self,
                    catalog: &ReadModelCatalog,
                ) -> Result<(), BoxError> {
                    self.read_models
                        .bootstrap_table_schema_for_dev(&catalog.table_registry()?)
                        .await?;
                    Ok(())
                }

                fn bus(&self, group: &str) -> $bus {
                    <$bus>::new(self.events.pool().clone()).group(group)
                }

                fn read_model_changes(
                    &self,
                ) -> tokio::sync::broadcast::Receiver<crate::read_model::ReadModelChange> {
                    self.read_models.read_model_changes()
                }

                #[cfg(feature = "graphql")]
                fn graphql_pool(&self) -> crate::graphql::GraphqlPoolSource {
                    (&self.read_models).into()
                }
            }
        };
    }

    #[cfg(feature = "sqlite")]
    sql_stores!(
        /// Every store of a service in one SQLite database.
        SqliteStores,
        crate::SqliteRepository,
        crate::lock::SqliteLockManager,
        crate::bus::SqliteBus
    );

    #[cfg(feature = "postgres")]
    sql_stores!(
        /// Every store of a service in one Postgres database.
        PostgresStores,
        crate::PostgresRepository,
        crate::lock::PostgresLockManager,
        crate::bus::PostgresBus
    );
}
