//! Aggregate cell class: one Durable Object analogue per aggregate type,
//! one instance per shard (`{aggregate_type}:{shard}`).

use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value;

use super::causal::{CellCommandIdentity, CellDispatchError, CellDispatchResult};
use super::store::CellStreamStore;
#[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
use super::store::{
    DurableAggregateCellState, DurableCellCommand, DurableCellEvents, DurableCellSnapshot,
};
use crate::aggregate::{Aggregate, AggregateRepository};
use crate::entity::EventRecord;
use crate::microsvc::error::HandlerError;
use crate::microsvc::service::{PortableCommand, Routes};
use crate::microsvc::session::Session;
use crate::microsvc::HasOutboxStore;
use crate::repository::{GetStream, RepositoryError, SnapshotStore, StreamIdentity};
use crate::snapshot::{SnapshotRecord, Snapshottable};
use crate::OutboxDispatcher;

/// Cell class for aggregate `A`. Equivalent to
/// `#[distributed::cell(aggregate = A)]`: mount the same domain
/// [`PortableCommand`] values used by SOA `Routes::mount`.
///
/// Projectors, GraphQL, and ingest are not methods on this type
/// (`PCH-REQ-005`).
///
/// ```compile_fail
/// fn projectors_are_not_cell_methods<A>(cell: distributed::cell_host::AggregateCell<A>)
/// where
///     A: distributed::Aggregate + Send + Sync + 'static,
/// {
///     let _ = cell.causal_projector;
/// }
/// ```
///
/// ```compile_fail
/// fn graphql_is_not_a_cell_method<A>(cell: distributed::cell_host::AggregateCell<A>)
/// where
///     A: distributed::Aggregate + Send + Sync + 'static,
/// {
///     let _ = cell.bind_graphql;
/// }
/// ```
pub struct AggregateCell<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    shard: StreamIdentity,
    routes: Routes<AggregateRepository<CellStreamStore, A>>,
    #[cfg(feature = "workers-rs")]
    celld_outbox: Option<super::celld_outbox::CelldOutbox>,
    #[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
    activity: super::celld_outbox::CommandActivity,
    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    storage: worker::send::SendWrapper<worker::Storage>,
}

impl<A> AggregateCell<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    /// Open a cell instance addressed as `{aggregate_type}:{shard_id}`.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn new(shard_id: impl Into<String>) -> Result<Self, RepositoryError> {
        let shard = StreamIdentity::new(A::aggregate_type(), shard_id.into())?;
        let store = CellStreamStore::for_identity(shard.clone());
        Ok(Self {
            shard,
            routes: Routes::from_dependencies(AggregateRepository::new(store)),
            #[cfg(feature = "workers-rs")]
            celld_outbox: None,
            #[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
            activity: Default::default(),
        })
    }

    /// Open a durable cell backed by its own SQLite file (`cell-sqlite`).
    ///
    /// The file is `location.resolve("{aggregate_type}:{shard_id}")`. Each
    /// command commits events, snapshots, its ledger receipt and outbox rows
    /// in one SQLite transaction, exactly as `from_state` does on Workers.
    #[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
    pub fn open_sqlite(
        location: &super::CellSqlitePath,
        shard_id: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let shard = StreamIdentity::new(A::aggregate_type(), shard_id.into())?;
        Self::from_store(CellStreamStore::open_sqlite(location, shard)?)
    }

    /// Host a prepared exclusive store (for example a SQLite cell configured
    /// with [`CellStreamStore::with_sealed_row_reducer`]).
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn from_store(store: CellStreamStore) -> Result<Self, RepositoryError> {
        let shard = Self::store_shard(&store)?;
        Ok(Self::from_repository(
            shard,
            AggregateRepository::new(store),
        ))
    }

    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    fn store_shard(store: &CellStreamStore) -> Result<StreamIdentity, RepositoryError> {
        match store.identity() {
            Some(identity) if identity.aggregate_type() == A::aggregate_type() => {
                Ok(identity.clone())
            }
            Some(identity) => Err(RepositoryError::Model(format!(
                "cell store `{identity}` does not hold aggregate type `{}`",
                A::aggregate_type()
            ))),
            None => Err(RepositoryError::Model(
                "aggregate cells require an exclusive cell store".into(),
            )),
        }
    }

    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    fn from_repository(
        shard: StreamIdentity,
        repository: AggregateRepository<CellStreamStore, A>,
    ) -> Self {
        Self {
            shard,
            routes: Routes::from_dependencies(repository),
            #[cfg(feature = "workers-rs")]
            celld_outbox: None,
            #[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
            activity: Default::default(),
        }
    }

    /// Native equivalent of the celld alarm drain: one claim -> publish ->
    /// delete pass over this cell's committed outbox (lease-fenced rows).
    ///
    /// Call it at startup (rows survive a restart), after dispatch for low
    /// latency, and periodically; or hand [`Self::outbox_dispatcher`] to an
    /// `OutboxDrainRunner` on a tokio runtime. Publication failures are
    /// released (or failed after `max_attempts`) for the next pass. A crash
    /// between publish and delete re-delivers the same stable message id, so
    /// delivery is at-least-once and consumers must deduplicate.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub async fn drain_outbox_with<P>(
        &self,
        publisher: P,
        lease: Duration,
        max_attempts: u32,
        batch_size: usize,
    ) -> Result<crate::outbox_worker::OutboxDispatchOutcome, crate::bus::TransportError>
    where
        P: crate::bus::MessagePublisher,
    {
        self.outbox_dispatcher(
            publisher,
            format!("cell-outbox:{}", self.instance_name()),
            lease,
            max_attempts,
        )
        .dispatch_batch(batch_size)
        .await
    }

    /// Open the named runtime-owned SQLite cell. Mount commands and configure
    /// its Queue binding before dispatch; no in-memory persistence path exists.
    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    pub fn from_state(state: worker::State) -> Result<Self, RepositoryError> {
        let name = state.id().name().ok_or_else(|| {
            RepositoryError::Model("aggregate cells require a named Durable Object".into())
        })?;
        let shard = StreamIdentity::new(A::aggregate_type(), name)?;
        let storage = worker::send::SendWrapper::new(state.storage());
        let store = CellStreamStore::from_state(state, shard.clone())?;
        Ok(Self {
            shard,
            routes: Routes::from_dependencies(AggregateRepository::new(store)),
            celld_outbox: None,
            activity: Default::default(),
            storage,
        })
    }

    /// Durable Object name: `format!("{}:{}", type, shard)`.
    pub fn instance_name(&self) -> String {
        self.shard.to_string()
    }

    /// Shard id used for cell addressing and SOA `load_by`.
    pub fn shard_id(&self) -> &str {
        self.shard.aggregate_id()
    }

    /// Install a domain command declaration. Same value SOA mounts.
    pub fn mount(
        mut self,
        command: impl PortableCommand<AggregateRepository<CellStreamStore, A>>,
    ) -> Self {
        self.routes = self.routes.mount(command);
        self
    }

    /// Command ids mounted on this cell class instance.
    pub fn command_names(&self) -> Vec<String> {
        self.routes
            .command_specs()
            .unwrap_or_default()
            .into_iter()
            .map(|spec| spec.id)
            .collect()
    }

    /// True when this cell has only command mounts (no projectors/GraphQL services).
    pub fn is_command_only(&self) -> bool {
        self.routes.is_command_only()
    }

    /// Dispatch a mounted command through the cell-local workspace adapter.
    pub async fn dispatch(
        &self,
        command: &str,
        input: Value,
        session: Session,
    ) -> Result<Value, HandlerError> {
        #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
        let _command = self
            .begin_command()
            .await
            .map_err(|error| HandlerError::Other(Box::new(error)))?;
        self.routes
            .dispatch_cell_command(command, input, session, &self.shard)
            .await
    }

    /// Dispatch a wait-path command through this cell's fenced command ledger.
    ///
    /// Same principal/command ID plus the same canonical typed input replays
    /// the original payload without invoking the handler. Reusing the ID for a
    /// different command or input fails before domain effects can commit.
    pub async fn dispatch_idempotent(
        &self,
        command: &str,
        identity: &CellCommandIdentity,
        input: Value,
        session: Session,
    ) -> Result<CellDispatchResult, CellDispatchError> {
        #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
        let _command = self
            .begin_command()
            .await
            .map_err(|error| CellDispatchError::Internal(error.to_string()))?;
        self.routes
            .dispatch_cell_causal(command, identity, input, session, &self.shard)
            .await
    }

    /// Load this cell's aggregate from the private stream store.
    ///
    /// HTTP GET on the cell host is a stream load, not a GraphQL/projector
    /// method (`PCH-REQ-005`).
    pub async fn load(&self) -> Result<Option<A>, RepositoryError> {
        self.routes.repo().get(self.shard.aggregate_id()).await
    }

    /// Durable event history of one stream this cell owns, oldest first
    /// (empty when the stream has no events). Reads the backing store, so it
    /// works on the in-process reference host **and** on native SQLite
    /// cells (`cell-sqlite`), unlike the whole-state
    /// [`durable_events`](Self::durable_events) export. A stream the cell
    /// does not own is rejected.
    pub async fn stream_events(
        &self,
        identity: &StreamIdentity,
    ) -> Result<Vec<EventRecord>, RepositoryError> {
        Ok(GetStream::get_stream(self.routes.repo().repo(), identity)
            .await?
            .map(|entity| entity.events().to_vec())
            .unwrap_or_default())
    }

    /// Durable event history of this cell's own aggregate stream
    /// (`{aggregate_type}:{shard}`), oldest first. See
    /// [`stream_events`](Self::stream_events).
    pub async fn history(&self) -> Result<Vec<EventRecord>, RepositoryError> {
        self.stream_events(&self.shard).await
    }

    /// Event log export of the in-process reference host (restart tests).
    /// SQLite cells persist rows directly and return an error here; read
    /// history with [`history`](Self::history) /
    /// [`stream_events`](Self::stream_events) instead.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_events(&self) -> Result<Vec<DurableCellEvents>, RepositoryError> {
        self.routes.repo().repo().durable_events()
    }

    /// Restore the working event log from Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_events(
        &self,
        events: Vec<DurableCellEvents>,
    ) -> Result<(), RepositoryError> {
        self.routes.repo().repo().restore_durable_events(events)
    }

    /// Outbox rows committed with the aggregate (same cell SQLite).
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_outbox(&self) -> Result<Vec<crate::OutboxMessage>, RepositoryError> {
        self.routes.repo().repo().durable_outbox()
    }

    /// Build the standard claim → publish → settle drain over this cell's
    /// committed outbox. A celld Worker normally supplies a
    /// `CelldQueuePublisher`, persists the settled outbox state, and lets
    /// celld's output gate order Queue egress after the cell write.
    pub fn outbox_dispatcher<P>(
        &self,
        publisher: P,
        worker_id: impl Into<String>,
        lease: Duration,
        max_attempts: u32,
    ) -> OutboxDispatcher<<CellStreamStore as HasOutboxStore>::OutboxStore, P>
    where
        P: crate::bus::MessagePublisher,
    {
        OutboxDispatcher::new(
            self.routes.repo().repo().outbox_store(),
            publisher,
            worker_id,
            lease,
            max_attempts,
        )
    }

    /// Attach the celld Queue binding and drain policy used by this cell host.
    #[cfg(feature = "workers-rs")]
    pub fn with_celld_outbox(mut self, outbox: super::celld_outbox::CelldOutbox) -> Self {
        self.celld_outbox = Some(outbox);
        self
    }

    /// Drain committed rows to the configured Queue. Call from the alarm handler
    /// and optionally after dispatch for low latency. No persistence callback is
    /// needed. A post-commit drain error must not reject an accepted command.
    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    pub async fn drain_outbox(
        &self,
        env: &worker::Env,
    ) -> Result<super::celld_outbox::CelldOutboxDrainOutcome, crate::bus::TransportError> {
        self.outbox_config()?.drain(self, env, &self.storage).await
    }

    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    fn outbox_config(
        &self,
    ) -> Result<&super::celld_outbox::CelldOutbox, crate::bus::TransportError> {
        self.celld_outbox.as_ref().ok_or_else(|| {
            crate::bus::TransportError::permanent(
                "aggregate cell has no celld outbox binding configured",
            )
        })
    }

    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    async fn begin_command(
        &self,
    ) -> Result<super::celld_outbox::CommandGuard, crate::bus::TransportError> {
        self.outbox_config()?
            .begin_command(
                &self.activity,
                &super::celld_outbox::WorkerAlarm(&self.storage),
            )
            .await
    }

    #[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
    pub(super) fn command_activity(&self) -> &super::celld_outbox::CommandActivity {
        &self.activity
    }

    #[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
    pub(super) fn outbox_store(&self) -> <CellStreamStore as HasOutboxStore>::OutboxStore {
        self.routes.repo().repo().outbox_store()
    }

    /// Restore outbox rows from Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_outbox(
        &self,
        messages: Vec<crate::OutboxMessage>,
    ) -> Result<(), RepositoryError> {
        self.routes.repo().repo().restore_durable_outbox(messages)
    }

    /// Snapshot cache for Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_snapshots(&self) -> Result<Vec<DurableCellSnapshot>, RepositoryError> {
        self.routes.repo().repo().durable_snapshots()
    }

    /// Restore the working snapshot cache from Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_snapshots(
        &self,
        snapshots: Vec<DurableCellSnapshot>,
    ) -> Result<(), RepositoryError> {
        self.routes
            .repo()
            .repo()
            .restore_durable_snapshots(snapshots)
    }

    /// Command-ledger rows for Durable Object SQLite.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_commands(&self) -> Result<Vec<DurableCellCommand>, RepositoryError> {
        self.routes.repo().repo().durable_commands()
    }

    /// Restore command-ledger rows before accepting another wait-path request.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_commands(
        &self,
        commands: Vec<DurableCellCommand>,
    ) -> Result<(), RepositoryError> {
        self.routes.repo().repo().restore_durable_commands(commands)
    }

    /// Export events, snapshots, command ledger, outbox, and sealed row as one
    /// versioned value suitable for a single durable storage write.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn durable_state(&self) -> Result<DurableAggregateCellState, RepositoryError> {
        self.routes.repo().repo().durable_state()
    }

    /// Restore the complete working copy from one durable storage value.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn restore_durable_state(
        &self,
        state: DurableAggregateCellState,
    ) -> Result<(), RepositoryError> {
        self.routes.repo().repo().restore_durable_state(state)
    }

    /// Read the repository snapshot cache for this cell's shard.
    pub async fn cached_snapshot(&self) -> Result<Option<SnapshotRecord>, RepositoryError> {
        SnapshotStore::get_snapshot(self.routes.repo().repo(), &self.shard).await
    }

    /// Sealed read-model JSON for GET on this instance.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn sealed_row(&self) -> Result<Option<Value>, RepositoryError> {
        self.routes.repo().repo().sealed_row()
    }

    /// Persist the sealed read-model row next to events/snapshots.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn replace_sealed_row(&self, row: Value) -> Result<(), RepositoryError> {
        self.routes.repo().repo().replace_sealed_row(row)
    }
}

impl<A> AggregateCell<A>
where
    A: Aggregate + Snapshottable + Send + Sync + 'static,
{
    /// Open a SQL cell with the ordinary repository snapshot policy.
    #[cfg(all(feature = "workers-rs", target_arch = "wasm32"))]
    pub fn from_state_with_snapshots(
        state: worker::State,
        frequency: u64,
    ) -> Result<Self, RepositoryError> {
        let name = state.id().name().ok_or_else(|| {
            RepositoryError::Model("aggregate cells require a named Durable Object".into())
        })?;
        let shard = StreamIdentity::new(A::aggregate_type(), name)?;
        let storage = worker::send::SendWrapper::new(state.storage());
        let store = CellStreamStore::from_state(state, shard.clone())?;
        Ok(Self {
            shard,
            routes: Routes::from_dependencies(
                AggregateRepository::new(store).with_snapshots(frequency),
            ),
            celld_outbox: None,
            activity: Default::default(),
            storage,
        })
    }

    /// Open a durable SQLite cell with repository snapshot caching.
    #[cfg(all(feature = "cell-sqlite", not(target_arch = "wasm32")))]
    pub fn open_sqlite_with_snapshots(
        location: &super::CellSqlitePath,
        shard_id: impl Into<String>,
        frequency: u64,
    ) -> Result<Self, RepositoryError> {
        let shard = StreamIdentity::new(A::aggregate_type(), shard_id.into())?;
        Self::from_store_with_snapshots(CellStreamStore::open_sqlite(location, shard)?, frequency)
    }

    /// Host a prepared exclusive store with repository snapshot caching.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn from_store_with_snapshots(
        store: CellStreamStore,
        frequency: u64,
    ) -> Result<Self, RepositoryError> {
        let shard = Self::store_shard(&store)?;
        Ok(Self::from_repository(
            shard,
            AggregateRepository::new(store).with_snapshots(frequency),
        ))
    }

    /// Open a cell with repository snapshot caching (`with_snapshots`).
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn new_with_snapshots(
        shard_id: impl Into<String>,
        frequency: u64,
    ) -> Result<Self, RepositoryError> {
        let shard = StreamIdentity::new(A::aggregate_type(), shard_id.into())?;
        let store = CellStreamStore::for_identity(shard.clone());
        Ok(Self {
            shard,
            routes: Routes::from_dependencies(
                AggregateRepository::new(store).with_snapshots(frequency),
            ),
            #[cfg(feature = "workers-rs")]
            celld_outbox: None,
            #[cfg(all(feature = "workers-rs", any(target_arch = "wasm32", test)))]
            activity: Default::default(),
        })
    }
}

/// Worker-side namespace: `getByName(format!("{}:{}", type, shard))`.
pub struct CellNamespace<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    cells: HashMap<String, AggregateCell<A>>,
}

impl<A> Default for CellNamespace<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> CellNamespace<A>
where
    A: Aggregate + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            cells: HashMap::new(),
        }
    }

    /// Address a cell by Durable Object name.
    pub fn get_by_name(&self, name: &str) -> Option<&AggregateCell<A>> {
        self.cells.get(name)
    }

    /// Mutable address by Durable Object name.
    pub fn get_by_name_mut(&mut self, name: &str) -> Option<&mut AggregateCell<A>> {
        self.cells.get_mut(name)
    }

    /// Insert a fully mounted cell instance.
    pub fn insert(&mut self, cell: AggregateCell<A>) {
        self.cells.insert(cell.instance_name(), cell);
    }

    /// Create or return the cell for `shard_id`.
    #[cfg(not(all(feature = "workers-rs", target_arch = "wasm32")))]
    pub fn get_or_create(
        &mut self,
        shard_id: &str,
        mount: impl FnOnce(AggregateCell<A>) -> AggregateCell<A>,
    ) -> Result<&mut AggregateCell<A>, RepositoryError> {
        let name = instance_name::<A>(shard_id);
        if !self.cells.contains_key(&name) {
            let cell = mount(AggregateCell::new(shard_id)?);
            self.cells.insert(name.clone(), cell);
        }
        Ok(self.cells.get_mut(&name).expect("just inserted"))
    }
}

/// Cell instance name: `{aggregate_type}:{shard_id}`.
pub fn instance_name<A: Aggregate>(shard_id: &str) -> String {
    format!("{}:{shard_id}", A::aggregate_type())
}

/// Parent-shard cell name (`game:{game_id}` for bomberman tick).
///
/// Child streams (player, bomb, explosion, map, saga) live inside this cell.
/// There is no two-cell transaction API (`PCH-REQ-006`).
pub fn parent_cell_name(parent_type: &str, parent_id: &str) -> String {
    format!("{parent_type}:{parent_id}")
}
