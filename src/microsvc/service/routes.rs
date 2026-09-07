#[cfg(feature = "graphql")]
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "graphql")]
use std::time::SystemTime;

#[cfg(feature = "graphql")]
use super::causal::{
    abandon_causal_attempt, causal_handler_error_code, commit_causal_rejection,
    ensure_causal_grant, evaluate_causal_command_status, internal_ledger_error,
    load_committed_dispatch_result, recover_causal_commit_error, replay_result,
    CausalCommandPublicStatus, CausalDispatchError, CausalDispatchResult,
};
use super::handlers::{
    boxed_causal_guard, boxed_handler, boxed_prepared_handler, CausalCommandContext, CausalGuardFn,
    GuardFn, Handler, HandlerFn, HandlerFuture, PreparedCommandHandler, PreparedHandlerFn,
    ProjectorBootstrapFuture,
};
use crate::aggregate::Aggregate;
use crate::application::{CommandMount, CommandMountRegistrar, CommandSpec};
use crate::bus::{Bus, Message, MessageKind, MessagePublisher, OrderedDelivery, TransportError};
use crate::command::input::canonicalize_command_input;
use crate::command::{
    command_transition, CommandConsistency, CommandEventSet, CommandInputType, CommandOutcome,
    CompiledInputDefaults, TypedCommand, TypedCommandContract,
};
use crate::command_ledger::{
    CanonicalInputHash, CausalCommitBatch, CausalTransactionalCommit, CommandContractFingerprint,
    CommandLedgerStore, CommandReservation, ReservationOutcome, TerminalCommandState,
};
#[cfg(feature = "graphql")]
use crate::command_ledger::{
    CausalRepositoryIdentity, CommandId, CommandLedgerKey, CommandLookup, CommandLookupScope,
    PrincipalPartitionId,
};
#[cfg(feature = "graphql")]
use crate::graphql::identity::VerifiedPrincipal;
use crate::graphql::SurfaceProjector;
use crate::microsvc::causal::CausalWorkspace;
use crate::microsvc::cell_host::{CellCommandIdentity, CellDispatchError, CellDispatchResult};
use crate::microsvc::context::Context;
use crate::microsvc::dependencies::{
    CausalProjectionRouteDependencies, CausalRouteDependencies, ConfigurableOutboxPublisher,
    HasOutboxStore, HasReadModelStore, HasRepo,
};
use crate::microsvc::error::HandlerError;
use crate::microsvc::projector::{
    CausalProjectorRouteBuilder, ErasedProjectorHandler, ProjectionRepairHandle,
    ProjectorRegistration, ProjectorRepairFuture, ProjectorRepairLookupFuture,
};
#[cfg(feature = "graphql")]
use crate::microsvc::projector::{ModeledProjectorHandlerFn, ModeledProjectorRouteBuilder};
use crate::microsvc::session::Session;
use crate::outbox::OutboxPublisherConfig;
use crate::outbox_worker::BusOutboxPublishHook;
#[cfg(any(
    feature = "http",
    feature = "grpc",
    feature = "postgres",
    feature = "sqlite",
    feature = "nats",
    feature = "rabbitmq",
    feature = "kafka",
    test,
))]
use crate::outbox_worker::{
    OutboxDispatcher, OutboxDrainRunner, OutboxPublishMailbox, DEFAULT_DRAIN_LEASE,
    DEFAULT_OUTBOX_HINT_CAPACITY,
};
#[cfg(feature = "graphql")]
use crate::projection_protocol::ProjectionProtocolStore;
#[cfg(feature = "graphql")]
use crate::projection_protocol::{CompiledProjectionTopology, ProjectorTopologyId};
use crate::repository::{StreamIdentity, TransactionalCommit};
use serde_json::Value;

/// How a handler expects the transport to deliver matching messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryKind {
    /// Point-to-point delivery, normally used for command queues.
    PointToPoint,
    /// Fan-out delivery, normally used for event subscriptions.
    FanOut,
}

/// Static message names attached to a handler spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerNames {
    /// A single command or event name.
    One(&'static str),
    /// Multiple event names handled by one projection-style handler.
    Many(&'static [&'static str]),
    /// Compiler-owned event names retained by a causal projector declaration.
    Owned(Vec<String>),
}

impl HandlerNames {
    fn to_vec(&self) -> Vec<&str> {
        match self {
            Self::One(name) => vec![*name],
            Self::Many(names) => names.to_vec(),
            Self::Owned(names) => names.iter().map(String::as_str).collect(),
        }
    }
}

/// Transport-visible metadata for a registered handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerSpec {
    names: HandlerNames,
    pub kind: MessageKind,
    pub delivery: DeliveryKind,
}

impl HandlerSpec {
    /// A command handler that consumes JSON payloads.
    pub const fn command(name: &'static str) -> Self {
        Self {
            names: HandlerNames::One(name),
            kind: MessageKind::Command,
            delivery: DeliveryKind::PointToPoint,
        }
    }

    /// An event handler that consumes JSON payloads.
    pub const fn event(name: &'static str) -> Self {
        Self {
            names: HandlerNames::One(name),
            kind: MessageKind::Event,
            delivery: DeliveryKind::FanOut,
        }
    }

    /// An event handler that consumes several event names.
    pub const fn events(names: &'static [&'static str]) -> Self {
        Self {
            names: HandlerNames::Many(names),
            kind: MessageKind::Event,
            delivery: DeliveryKind::FanOut,
        }
    }

    pub(crate) fn projector(names: Vec<String>) -> Self {
        Self {
            names: HandlerNames::Owned(names),
            kind: MessageKind::Event,
            delivery: DeliveryKind::FanOut,
        }
    }

    /// Message names consumed by this handler.
    pub fn names(&self) -> Vec<&str> {
        self.names.to_vec()
    }
}

enum RegisteredHandler<D> {
    Legacy {
        guard: Option<Arc<GuardFn<D>>>,
        handle: Arc<HandlerFn<D>>,
    },
    Causal(Box<dyn ErasedCausalHandler<D>>),
    Projector(Vec<Arc<dyn ErasedProjectorHandler<D>>>),
}

#[derive(Clone, Copy)]
#[cfg_attr(not(feature = "graphql"), allow(dead_code))]
pub(super) struct CausalCommandPolicy {
    pub(super) attempt_lease: Duration,
    pub(super) replay_retention: Duration,
}

impl Default for CausalCommandPolicy {
    fn default() -> Self {
        Self {
            attempt_lease: Duration::from_secs(30),
            replay_retention: Duration::from_secs(30 * 24 * 60 * 60),
        }
    }
}
#[cfg(feature = "graphql")]
pub(super) type CausalHandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CausalDispatchResult, CausalDispatchError>> + Send + 'a>>;
#[cfg(feature = "graphql")]
pub(super) type CausalStatusFuture<'a> = Pin<
    Box<dyn Future<Output = Result<CausalCommandPublicStatus, CausalDispatchError>> + Send + 'a>,
>;
pub(super) type CellCausalHandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<CellDispatchResult, CellDispatchError>> + Send + 'a>>;

pub(super) trait ErasedCausalHandler<D>: Send + Sync {
    fn contract(&self) -> &TypedCommandContract;

    #[cfg(feature = "graphql")]
    fn contract_mut(&mut self) -> &mut TypedCommandContract;

    #[cfg(feature = "graphql")]
    fn storage_identity(&self, dependencies: &D) -> crate::command_ledger::CausalStorageIdentity;

    #[cfg(feature = "graphql")]
    #[allow(clippy::too_many_arguments)]
    fn dispatch<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalHandlerFuture<'a>;

    fn dispatch_cell_causal<'a>(
        &'a self,
        dependencies: &'a D,
        identity: &'a CellCommandIdentity,
        input: Value,
        session: Session,
        shard: &'a StreamIdentity,
    ) -> CellCausalHandlerFuture<'a>;

    #[cfg(feature = "graphql")]
    #[allow(dead_code)]
    fn lookup<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a str,
        session: &'a Session,
        principal: VerifiedPrincipal,
    ) -> Pin<Box<dyn Future<Output = Result<CommandLookup, CausalDispatchError>> + Send + 'a>>;

    #[cfg(feature = "graphql")]
    fn status<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a CommandId,
        principal_partition: &'a PrincipalPartitionId,
        session: &'a Session,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalStatusFuture<'a>;

    /// Run the same typed `handle` inside one cell, without GraphQL receipts.
    fn invoke_cell<'a>(
        &'a self,
        dependencies: &'a D,
        input: Value,
        session: Session,
        shard: &'a StreamIdentity,
    ) -> Pin<Box<dyn Future<Output = Result<Value, HandlerError>> + Send + 'a>>;
}

struct RegisteredCausalHandler<A, I, K>
where
    A: Aggregate + Send + Sync + 'static,
    K: CommandOutcome,
{
    contract: TypedCommandContract,
    guard: Option<Arc<CausalGuardFn<A>>>,
    handle: Arc<PreparedHandlerFn<A, I, K>>,
    /// Retryable, fail-closed bootstrap for the bound projector's complete
    /// model/table ownership inventory. `get_or_try_init` leaves the cell empty
    /// after a transient registration failure.
    #[cfg(feature = "graphql")]
    direct_projection_bootstrap: tokio::sync::OnceCell<()>,
    _types: std::marker::PhantomData<fn(A, I) -> K>,
}

impl<A, I, K> RegisteredCausalHandler<A, I, K>
where
    A: Aggregate + Send + Sync + 'static,
    K: CommandOutcome,
{
    fn new(
        contract: TypedCommandContract,
        guard: Option<Arc<CausalGuardFn<A>>>,
        handle: Arc<PreparedHandlerFn<A, I, K>>,
    ) -> Self {
        Self {
            contract,
            guard,
            handle,
            #[cfg(feature = "graphql")]
            direct_projection_bootstrap: tokio::sync::OnceCell::new(),
            _types: std::marker::PhantomData,
        }
    }
}

pub(super) type OutboxConfigurator<D> =
    fn(&mut D, DynBusPublisher, String, Duration, u32, Option<String>);

pub(super) trait ErasedRoutes: Send + Sync {
    fn handler_specs(&self) -> &[HandlerSpec];

    fn typed_command_contracts(&self) -> Vec<&TypedCommandContract>;

    fn projector_registrations(&self) -> Vec<ProjectorRegistration>;

    fn modeled_local_services(&self) -> Vec<&str>;

    fn bootstrap_projectors(&self) -> ProjectorBootstrapFuture<'_>;

    fn is_causal_projector(&self, message: &Message) -> bool;

    fn repair_projection<'a>(
        &'a self,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairFuture<'a>;

    fn locates_projection_failure<'a>(
        &'a self,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairLookupFuture<'a>;

    #[cfg(feature = "graphql")]
    fn bind_typed_command_contracts(
        &mut self,
        contracts: &BTreeMap<String, TypedCommandContract>,
    ) -> Result<(), String>;

    fn dispatch<'a>(
        &'a self,
        message: &'a Message,
        input: Value,
        session: Session,
        ordered: Option<&'a OrderedDelivery>,
    ) -> HandlerFuture<'a>;

    #[cfg(feature = "graphql")]
    #[allow(clippy::too_many_arguments)]
    fn dispatch_causal<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalHandlerFuture<'a>;

    #[cfg(feature = "graphql")]
    #[allow(dead_code)]
    fn lookup_causal<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        session: &'a Session,
        principal: VerifiedPrincipal,
    ) -> Pin<Box<dyn Future<Output = Result<CommandLookup, CausalDispatchError>> + Send + 'a>>;

    #[cfg(feature = "graphql")]
    fn causal_command_status<'a>(
        &'a self,
        service_id: &'a str,
        command_id: &'a CommandId,
        principal_partition: &'a PrincipalPartitionId,
        session: &'a Session,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalStatusFuture<'a>;

    #[cfg(feature = "graphql")]
    fn projected_storage_identities(&self) -> Vec<crate::command_ledger::CausalStorageIdentity>;

    fn configure_outbox_publisher(
        &mut self,
        publisher: DynBusPublisher,
        worker_id: String,
        lease: Duration,
        max_attempts: u32,
        service_name: Option<String>,
    );
}

trait DynPublish: Send + Sync {
    fn publish<'a>(
        &'a self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>>;
}

struct BusDynPublisher<B> {
    bus: Arc<B>,
}

impl<B: Bus> DynPublish for BusDynPublisher<B> {
    fn publish<'a>(
        &'a self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>> {
        Box::pin(async move {
            match message.kind {
                MessageKind::Command => self.bus.send_message(message).await,
                MessageKind::Event => self.bus.publish_message(message).await,
            }
        })
    }
}

#[derive(Clone)]
pub(crate) struct DynBusPublisher {
    inner: Arc<dyn DynPublish>,
}

impl DynBusPublisher {
    pub(crate) fn new<B>(bus: Arc<B>) -> Self
    where
        B: Bus + 'static,
    {
        Self {
            inner: Arc::new(BusDynPublisher { bus }),
        }
    }
}

impl MessagePublisher for DynBusPublisher {
    fn publish(
        &self,
        message: Message,
    ) -> impl Future<Output = Result<(), TransportError>> + Send + '_ {
        self.inner.publish(message)
    }
}

pub(super) fn configure_outbox_for<D>(
    dependencies: &mut D,
    publisher: DynBusPublisher,
    worker_id: String,
    lease: Duration,
    max_attempts: u32,
    service_name: Option<String>,
) where
    D: HasOutboxStore + ConfigurableOutboxPublisher,
    D::OutboxStore: 'static,
{
    let hook =
        BusOutboxPublishHook::new(dependencies.outbox_store(), publisher.clone(), max_attempts)
            .with_service(service_name.clone());

    #[cfg(any(
        feature = "http",
        feature = "grpc",
        feature = "postgres",
        feature = "sqlite",
        feature = "nats",
        feature = "rabbitmq",
        feature = "kafka",
        test,
    ))]
    let config = {
        let (mailbox, rx, wake) = OutboxPublishMailbox::channel(DEFAULT_OUTBOX_HINT_CAPACITY);
        let mut dispatcher = OutboxDispatcher::new(
            dependencies.outbox_store(),
            publisher,
            worker_id.clone(),
            DEFAULT_DRAIN_LEASE,
            max_attempts,
        );
        if let Some(name) = service_name {
            dispatcher = dispatcher.with_service(name);
        }
        OutboxDrainRunner::new(dispatcher)
            .with_hints(rx, wake)
            .spawn();
        OutboxPublisherConfig::new(Arc::new(hook), worker_id, lease)
            .with_schedule(Arc::new(move |ids| mailbox.try_submit(ids)))
    };

    #[cfg(not(any(
        feature = "http",
        feature = "grpc",
        feature = "postgres",
        feature = "sqlite",
        feature = "nats",
        feature = "rabbitmq",
        feature = "kafka",
        test,
    )))]
    let config = {
        let _ = publisher;
        OutboxPublisherConfig::new(Arc::new(hook), worker_id, lease)
    };

    dependencies.configure_outbox_publisher(config);
}

/// Domain-owned command that can install itself onto a host [`Routes`] bundle.
///
/// Command declarations live next to the aggregate. SOA and later cell hosts
/// call [`Routes::mount`] with the same value; the declaration must not name
/// sqlx, celld, or `QueuedRepository`.
///
/// Prefer [`crate::portable_command!`] (`PCH-DEC-001`) for shard + invoke +
/// Eventual. The fluent builder on [`Routes`] remains the expansion target.
pub trait PortableCommand<D> {
    /// Register this command on `routes` and return the bundle.
    fn install(self, routes: Routes<D>) -> Routes<D>;
}

impl<D, F> PortableCommand<D> for F
where
    F: FnOnce(Routes<D>) -> Routes<D>,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        self(routes)
    }
}

/// Builder returned by [`Routes::command`], [`Routes::event`],
/// [`Routes::events`], and [`Routes::handler`].
pub struct RouteBuilder<D> {
    routes: Routes<D>,
    spec: HandlerSpec,
}

/// Builder returned by [`Routes::typed_command`].
///
/// Unlike a legacy JSON route, the declaration and executable handler share
/// one route object, the same input and committed-outcome types, and the route
/// bundle's single aggregate repository.
pub struct TypedRouteBuilder<D, I, K: CommandOutcome> {
    routes: Routes<D>,
    route_name: &'static str,
    contract: TypedCommandContract,
    mount: Option<CommandMount>,
    _types: std::marker::PhantomData<fn(I) -> K>,
}

impl<D: Send + Sync + 'static> RouteBuilder<D> {
    /// Register an async handler without a guard.
    pub fn handle<F>(self, handler: F) -> Routes<D>
    where
        F: for<'a> Handler<'a, D> + 'static,
    {
        self.routes
            .register_handler(self.spec, None, boxed_handler(handler))
    }

    /// Register an async handler with a (synchronous) guard.
    pub fn guarded<G, F>(self, guard: G, handler: F) -> Routes<D>
    where
        G: Fn(&Context<D>) -> bool + Send + Sync + 'static,
        F: for<'a> Handler<'a, D> + 'static,
    {
        self.routes
            .register_handler(self.spec, Some(Arc::new(guard)), boxed_handler(handler))
    }
}

impl<D, I, K> TypedRouteBuilder<D, I, K>
where
    K: CommandOutcome,
{
    /// Override the GraphQL mutation field name (defaults from the command id).
    #[must_use]
    pub fn field_name(mut self, field_name: impl Into<String>) -> Self {
        self.contract.field_name = field_name.into();
        self
    }

    /// Restrict the command to these surface roles.
    #[must_use]
    pub fn roles(mut self, roles: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.contract.roles = roles.into_iter().map(Into::into).collect();
        self.contract.roles.sort();
        self.contract.roles.dedup();
        self
    }

    /// Declare values generated once into the canonical command input before
    /// dispatch.
    #[must_use]
    pub fn input_defaults(mut self, defaults: CompiledInputDefaults<I>) -> Self {
        self.contract.input_defaults = defaults.0;
        self.contract
            .input_defaults
            .sort_by(|left, right| left.path.cmp(&right.path));
        self
    }

    /// Declare that one emitted state field is the authenticated `x-user-id`.
    ///
    /// This is preview provenance only; command admission and handler-side
    /// principal binding remain mandatory.
    #[must_use]
    pub fn authenticated_user_field<E, S>(mut self, rust_field: &'static str) -> Self
    where
        E: crate::domain_event::DomainEventBodyContract<S>,
        S: crate::DomainState + crate::projection::lower::ProjectionBodyMetadata,
    {
        let values = crate::command::__command_projection_state_known_values::<E, S>(vec![(
            rust_field,
            crate::command::CommandProjectionPreviewSource::trusted("x-user-id", "string"),
        )]);
        self.contract
            .projections
            .add_authenticated_user_field(rust_field, values);
        self
    }

    /// Pure reducer over a known cache row for client auto-optimism.
    #[must_use]
    pub fn preview_reduce_known_record(
        mut self,
        reduce: crate::command::CommandProjectionPureReduce,
    ) -> Self {
        self.contract.projections.add_pure_reduce(reduce);
        self
    }

    /// Attach the mount produced by the same command declaration. The route
    /// adapter checks its identity before registration.
    pub fn mount(mut self, mount: CommandMount) -> Self {
        self.mount = Some(mount);
        self
    }
}

impl<D, I, K> TypedRouteBuilder<D, I, K>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    /// Register a typed causal command handler without a guard.
    pub fn handle<F>(self, handler: F) -> Routes<D>
    where
        F: for<'a> PreparedCommandHandler<'a, D::Aggregate, I, K> + 'static,
    {
        self.routes.register_typed_handler(
            self.route_name,
            self.contract,
            self.mount,
            None,
            boxed_prepared_handler(handler),
        )
    }

    /// Register a typed causal command handler with a synchronous guard.
    pub fn guarded<G, F>(self, guard: G, handler: F) -> Routes<D>
    where
        G: for<'a> Fn(&CausalCommandContext<'a, D::Aggregate>) -> bool + Send + Sync + 'static,
        F: for<'a> PreparedCommandHandler<'a, D::Aggregate, I, K> + 'static,
    {
        let guard = boxed_causal_guard(guard);
        self.routes.register_typed_handler(
            self.route_name,
            self.contract,
            self.mount,
            Some(guard),
            boxed_prepared_handler(handler),
        )
    }

    /// Load the aggregate by an id taken from the command input.
    ///
    /// Chain [`.invoke`](ThinCommandBuilder::invoke) then
    /// [`.eventual`](ThinCommandBuilder::eventual). Application code does not
    /// write a [`CausalCommandContext`] body.
    pub fn load_by<F>(self, id: F) -> ThinCommandBuilder<D, I, K, ThinCommandLoaded>
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        ThinCommandBuilder {
            inner: self,
            load_id: Some(std::sync::Arc::new(id)),
            create: false,
            transition: None,
            _phase: std::marker::PhantomData,
        }
    }

    /// Create a new aggregate, then invoke a domain method and commit.
    pub fn create(self) -> ThinCommandBuilder<D, I, K, ThinCommandLoaded> {
        ThinCommandBuilder {
            inner: self,
            load_id: None,
            create: true,
            transition: None,
            _phase: std::marker::PhantomData,
        }
    }
}

/// Marker: id source chosen (`load_by` or `create`).
pub struct ThinCommandLoaded;

/// Marker: domain transition registered.
pub struct ThinCommandInvoked;

/// Load/create + domain invoke + commit. The framework owns the handler body.
pub struct ThinCommandBuilder<D, I, K: CommandOutcome, Phase>
where
    D: CausalRouteDependencies,
{
    inner: TypedRouteBuilder<D, I, K>,
    load_id: Option<std::sync::Arc<dyn Fn(&I) -> String + Send + Sync>>,
    create: bool,
    transition: Option<
        std::sync::Arc<
            dyn Fn(
                    &mut crate::microsvc::AggregateCheckout<D::Aggregate>,
                    &I,
                    &str,
                ) -> Result<(), String>
                + Send
                + Sync,
        >,
    >,
    _phase: std::marker::PhantomData<Phase>,
}

impl<D, I, K> ThinCommandBuilder<D, I, K, ThinCommandLoaded>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    K: CommandOutcome,
{
    /// Apply a domain method. The third argument is the session principal
    /// when `.roles` require one.
    pub fn invoke<T, E>(mut self, transition: T) -> ThinCommandBuilder<D, I, K, ThinCommandInvoked>
    where
        T: Fn(&mut crate::microsvc::AggregateCheckout<D::Aggregate>, &I, &str) -> Result<(), E>
            + Send
            + Sync
            + 'static,
        E: std::fmt::Display,
    {
        self.transition = Some(std::sync::Arc::new(move |aggregate, input, owner| {
            transition(aggregate, input, owner).map_err(|error| error.to_string())
        }));
        ThinCommandBuilder {
            inner: self.inner,
            load_id: self.load_id,
            create: self.create,
            transition: self.transition,
            _phase: std::marker::PhantomData,
        }
    }
}

impl<D, I, T> ThinCommandBuilder<D, I, crate::command::Eventual<T>, ThinCommandInvoked>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    T: crate::command::CommandOutputType + serde::Serialize + Send + Sync + 'static,
{
    /// Publish captured events, commit, and return an Eventual payload.
    ///
    /// This registers the command. Application code supplies only the payload
    /// mapping — not a handler-context body.
    pub fn eventual<F>(self, payload: F) -> Routes<D>
    where
        F: Fn(&crate::microsvc::AggregateCheckout<D::Aggregate>) -> T + Send + Sync + 'static,
    {
        let load_id = self.load_id;
        let create = self.create;
        let transition = self
            .transition
            .expect("invoke must be chained before eventual");
        let payload = std::sync::Arc::new(payload);
        let roles = self.inner.contract.roles.clone();
        self.inner.handle(ThinEventualHandler {
            load_id,
            create,
            transition,
            payload,
            roles,
        })
    }
}

impl<D, I, T> ThinCommandBuilder<D, I, crate::command::Succeeded<T>, ThinCommandInvoked>
where
    D: CausalRouteDependencies + Send + Sync + 'static,
    D::Aggregate: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    T: crate::command::CommandOutputType + serde::Serialize + Send + Sync + 'static,
{
    /// Commit and return a Succeeded payload. Used when the command is not Eventual.
    pub fn succeeded<F>(self, payload: F) -> Routes<D>
    where
        F: Fn(&crate::microsvc::AggregateCheckout<D::Aggregate>) -> T + Send + Sync + 'static,
    {
        let load_id = self.load_id;
        let create = self.create;
        let transition = self
            .transition
            .expect("invoke must be chained before succeeded");
        let payload = std::sync::Arc::new(payload);
        let roles = self.inner.contract.roles.clone();
        self.inner.handle(ThinSucceededHandler {
            load_id,
            create,
            transition,
            payload,
            roles,
        })
    }
}

struct ThinEventualHandler<A, I, T>
where
    A: Aggregate,
{
    load_id: Option<std::sync::Arc<dyn Fn(&I) -> String + Send + Sync>>,
    create: bool,
    transition: std::sync::Arc<
        dyn Fn(&mut crate::microsvc::AggregateCheckout<A>, &I, &str) -> Result<(), String>
            + Send
            + Sync,
    >,
    payload: std::sync::Arc<dyn Fn(&crate::microsvc::AggregateCheckout<A>) -> T + Send + Sync>,
    roles: Vec<String>,
}

impl<'a, A, I, T> PreparedCommandHandler<'a, A, I, crate::command::Eventual<T>>
    for ThinEventualHandler<A, I, T>
where
    A: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    T: crate::command::CommandOutputType + serde::Serialize + Send + Sync + 'static,
{
    type Future = Pin<
        Box<
            dyn Future<
                    Output = Result<
                        crate::command::PreparedCommand<crate::command::Eventual<T>>,
                        HandlerError,
                    >,
                > + Send
                + 'a,
        >,
    >;

    fn call(&self, ctx: &'a CausalCommandContext<'a, A>, input: I) -> Self::Future {
        let load_id = self.load_id.clone();
        let create = self.create;
        let transition = self.transition.clone();
        let payload = self.payload.clone();
        let roles = self.roles.clone();
        Box::pin(async move {
            let owner = if crate::application::command_roles_require_principal(&roles) {
                ctx.user_id()?.to_string()
            } else {
                String::new()
            };
            let mut checkout = if create {
                ctx.create()
            } else {
                let id = load_id.as_ref().expect("load_by")(&input);
                super::require_loaded(ctx.get(&id).await?, id)?
            };
            super::invoke_transition(&mut checkout, |checkout| {
                transition(checkout, &input, &owner)
            })?;
            let body = payload(&checkout);
            ctx.publish_events().commit(checkout)?.eventual(body)
        })
    }
}

struct ThinSucceededHandler<A, I, T>
where
    A: Aggregate,
{
    load_id: Option<std::sync::Arc<dyn Fn(&I) -> String + Send + Sync>>,
    create: bool,
    transition: std::sync::Arc<
        dyn Fn(&mut crate::microsvc::AggregateCheckout<A>, &I, &str) -> Result<(), String>
            + Send
            + Sync,
    >,
    payload: std::sync::Arc<dyn Fn(&crate::microsvc::AggregateCheckout<A>) -> T + Send + Sync>,
    roles: Vec<String>,
}

impl<'a, A, I, T> PreparedCommandHandler<'a, A, I, crate::command::Succeeded<T>>
    for ThinSucceededHandler<A, I, T>
where
    A: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    T: crate::command::CommandOutputType + serde::Serialize + Send + Sync + 'static,
{
    type Future = Pin<
        Box<
            dyn Future<
                    Output = Result<
                        crate::command::PreparedCommand<crate::command::Succeeded<T>>,
                        HandlerError,
                    >,
                > + Send
                + 'a,
        >,
    >;

    fn call(&self, ctx: &'a CausalCommandContext<'a, A>, input: I) -> Self::Future {
        let load_id = self.load_id.clone();
        let create = self.create;
        let transition = self.transition.clone();
        let payload = self.payload.clone();
        let roles = self.roles.clone();
        Box::pin(async move {
            let owner = if crate::application::command_roles_require_principal(&roles) {
                ctx.user_id()?.to_string()
            } else {
                String::new()
            };
            let mut checkout = if create {
                ctx.create()
            } else {
                let id = load_id.as_ref().expect("load_by")(&input);
                super::require_loaded(ctx.get(&id).await?, id)?
            };
            super::invoke_transition(&mut checkout, |checkout| {
                transition(checkout, &input, &owner)
            })?;
            let body = payload(&checkout);
            ctx.commit(checkout)?.succeeded(body)
        })
    }
}

/// A typed bundle of command/event handlers and the dependency value they use.
///
/// Handlers are keyed by kind, then name, so dispatch looks up by `&str`
/// without allocating a key.
pub struct Routes<D> {
    pub(super) dependencies: D,
    handlers: HashMap<MessageKind, HashMap<String, RegisteredHandler<D>>>,
    handler_specs: Vec<HandlerSpec>,
    command_mounts: Vec<CommandMount>,
    projectors: Vec<Arc<dyn ErasedProjectorHandler<D>>>,
    modeled_local_services: BTreeSet<String>,
    outbox_configurator: Option<OutboxConfigurator<D>>,
}

impl<D: Send + Sync + 'static> Routes<D> {
    /// Build routes around an already-assembled dependency value.
    pub(crate) fn from_dependencies(dependencies: D) -> Self {
        Self {
            dependencies,
            handlers: HashMap::new(),
            handler_specs: Vec::new(),
            command_mounts: Vec::new(),
            projectors: Vec::new(),
            modeled_local_services: BTreeSet::new(),
            outbox_configurator: None,
        }
    }

    pub(super) fn with_outbox_configurator(mut self, configurator: OutboxConfigurator<D>) -> Self {
        self.outbox_configurator = Some(configurator);
        self
    }

    /// Fail fast if handlers are already registered. Dependency builders
    /// reconstruct the route bundle around a new dependency type, which would
    /// otherwise silently drop previously registered handlers.
    pub(super) fn assert_no_registrations(&self, builder: &str) {
        assert!(
            self.handlers.is_empty()
                && self.handler_specs.is_empty()
                && self.command_mounts.is_empty()
                && self.projectors.is_empty()
                && self.modeled_local_services.is_empty(),
            "Routes::{builder} must be called before registering handlers"
        );
    }

    /// Get a reference to the route dependencies.
    pub fn dependencies(&self) -> &D {
        &self.dependencies
    }

    /// Get the aggregate repository for routes whose dependencies expose one.
    pub fn repo(&self) -> &D::Repo
    where
        D: HasRepo,
    {
        self.dependencies.repo()
    }

    /// Get the read-model store for routes whose dependencies expose one.
    pub fn read_model_store(&self) -> &D::ReadModelStore
    where
        D: HasReadModelStore,
    {
        self.dependencies.read_model_store()
    }

    /// Start registering a command handler that consumes JSON payload input.
    pub fn command(self, name: &'static str) -> RouteBuilder<D> {
        self.handler(HandlerSpec::command(name))
    }

    /// Install a domain-owned command declaration onto this host bundle.
    pub fn mount(self, command: impl PortableCommand<D>) -> Self {
        command.install(self)
    }

    /// Dispatch a typed causal command inside one cell (no GraphQL envelope).
    pub(in crate::microsvc) async fn dispatch_cell_command(
        &self,
        command: &str,
        input: Value,
        session: Session,
        shard: &StreamIdentity,
    ) -> Result<Value, HandlerError> {
        let handler = self
            .handlers
            .get(&MessageKind::Command)
            .and_then(|handlers| handlers.get(command));
        match handler {
            Some(RegisteredHandler::Causal(handler)) => {
                handler
                    .invoke_cell(&self.dependencies, input, session, shard)
                    .await
            }
            Some(_) | None => Err(HandlerError::UnknownCommand(command.to_string())),
        }
    }

    /// Dispatch a typed cell command through the same fenced ledger contract
    /// as the in-process causal wait path.
    pub(in crate::microsvc) async fn dispatch_cell_causal(
        &self,
        command: &str,
        identity: &CellCommandIdentity,
        input: Value,
        session: Session,
        shard: &StreamIdentity,
    ) -> Result<CellDispatchResult, CellDispatchError> {
        let handler = self
            .handlers
            .get(&MessageKind::Command)
            .and_then(|handlers| handlers.get(command));
        match handler {
            Some(RegisteredHandler::Causal(handler)) => {
                handler
                    .dispatch_cell_causal(&self.dependencies, identity, input, session, shard)
                    .await
            }
            Some(_) | None => Err(CellDispatchError::BadRequest(format!(
                "`{command}` is not a typed causal command"
            ))),
        }
    }

    pub(in crate::microsvc) fn is_command_only(&self) -> bool {
        self.projectors.is_empty()
            && self.modeled_local_services.is_empty()
            && self
                .handler_specs
                .iter()
                .all(|spec| spec.kind == MessageKind::Command)
    }

    /// Register a typed command declaration and its executable handler as one
    /// inventory entry.
    pub fn typed_command<I, K>(self, command: TypedCommand<I, K>) -> TypedRouteBuilder<D, I, K>
    where
        I: serde::de::DeserializeOwned + Send + 'static,
        K: CommandOutcome,
    {
        let (route_name, contract) = command.into_parts();
        TypedRouteBuilder {
            routes: self,
            route_name,
            contract,
            mount: None,
            _types: std::marker::PhantomData,
        }
    }

    /// Register a typed command whose outward emit set comes from a domain
    /// transition witness (`domain_commands::*` or a domain-event marker).
    ///
    /// Equivalent to
    /// [`typed_command`](Self::typed_command)`(command_transition::<S, I, K>(name))`
    /// so callers do not also declare `.emits` / `.emits_events`.
    ///
    /// Chain `.field_name`, `.roles`, optional provenance/default declarations,
    /// then `.handle`.
    pub fn command_transition<S, I, K>(self, name: &'static str) -> TypedRouteBuilder<D, I, K>
    where
        S: CommandEventSet,
        I: CommandInputType + serde::de::DeserializeOwned + Send + 'static,
        K: CommandOutcome,
    {
        self.typed_command(command_transition::<S, I, K>(name))
    }

    /// Compile the registered typed declarations into portable command specs.
    pub fn command_specs(&self) -> crate::application::ApplicationResult<Vec<CommandSpec>> {
        let mut specs = self
            .typed_contracts()
            .into_iter()
            .map(CommandSpec::from_contract)
            .collect::<crate::application::ApplicationResult<Vec<_>>>()?;
        specs.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(specs)
    }

    /// Executable mounts created when typed handlers are registered. These
    /// mounts are never part of a serialized application manifest.
    pub fn command_mounts(&self) -> &[CommandMount] {
        &self.command_mounts
    }

    /// Register one typed, ordered causal projector using the exact
    /// [`SurfaceProjector`] declaration also supplied to the GraphQL engine.
    pub fn causal_projector<I>(
        self,
        projector: SurfaceProjector,
    ) -> CausalProjectorRouteBuilder<D, I>
    where
        D: CausalProjectionRouteDependencies,
        I: serde::de::DeserializeOwned + Send + 'static,
    {
        CausalProjectorRouteBuilder::new(self, projector)
    }

    /// Register a compiler-modeled projection through an explicit event
    /// handler.
    ///
    /// The handler receives the resolved projection token and must apply it
    /// through its causal projector context. The framework still owns ordered
    /// delivery, checkpoints, failures, and the atomic read-model commit.
    #[cfg(feature = "graphql")]
    pub fn modeled_projector(self, projector: SurfaceProjector) -> ModeledProjectorRouteBuilder<D>
    where
        D: CausalProjectionRouteDependencies,
    {
        ModeledProjectorRouteBuilder::new(self, projector)
    }

    /// Mount every catalog-pinned local executor in one modeled projection
    /// declaration.
    ///
    /// Remote bindings remain producer-visible registry entries but install no
    /// local event route. Active and draining local bindings both execute so a
    /// draining epoch can finish already-published work; only Active+Causal is
    /// eligible for new client obligations.
    #[cfg(feature = "graphql")]
    pub fn consume_projection(self, projector: SurfaceProjector) -> Self
    where
        D: CausalProjectionRouteDependencies,
    {
        self.register_modeled_projection(projector, None)
    }

    #[cfg(feature = "graphql")]
    pub(in crate::microsvc) fn register_modeled_projection(
        mut self,
        projector: SurfaceProjector,
        handler: Option<Arc<ModeledProjectorHandlerFn>>,
    ) -> Self
    where
        D: CausalProjectionRouteDependencies,
    {
        let modeled = projector.modeled.clone();
        for projection in modeled {
            let crate::projection::placement::ProjectionExecutorRoute::Local { service } =
                projection.route()
            else {
                continue;
            };
            let executor = projection.server_executor().cloned().unwrap_or_else(|| {
                panic!(
                    "local modeled projection `{}` / `{}` must be registered from a generated descriptor",
                    projection.program_id(),
                    projection.binding_id()
                )
            });
            let (_, binding) = projection.raw().unwrap_or_else(|| {
                panic!("local modeled projection lost its exact catalog binding")
            });
            let physical = binding.physical_topology().unwrap_or_else(|| {
                panic!(
                    "local modeled projection binding `{}` has no physical topology",
                    binding.id()
                )
            });
            let topology =
                ProjectorTopologyId::new(physical.version(), physical.name(), physical.digest())
                    .unwrap_or_else(|error| {
                        panic!(
                    "local modeled projection binding `{}` has invalid physical topology: {error}",
                    binding.id()
                )
                    });
            let compiled = CompiledProjectionTopology::from_modeled_binding(
                topology,
                binding.outputs().iter().map(|output| {
                    (output.model(), output.storage(), output.schema())
                }),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "local modeled projection binding `{}` cannot compile its physical runtime: {error}",
                    binding.id()
                )
            });
            let change_epoch =
                crate::projection_protocol::ProjectionEpoch::new(projection.epoch().as_str())
                    .unwrap_or_else(|error| {
                        panic!(
                            "local modeled projection binding `{}` has invalid epoch: {error}",
                            binding.id()
                        )
                    });
            let facts = projection.event_names();
            assert!(
                !facts.is_empty(),
                "local modeled projection binding `{}` consumes no domain events",
                binding.id()
            );
            self.modeled_local_services.insert(service.clone());
            self = self.register_projector(
                HandlerSpec::projector(facts),
                Box::new(crate::microsvc::projector::RegisteredModeledProjector {
                    compiled,
                    change_epoch,
                    executor,
                    handle: handler.clone(),
                }),
            );
        }
        self
    }

    /// Start registering an event handler that consumes JSON payload input.
    pub fn event(self, name: &'static str) -> RouteBuilder<D> {
        self.handler(HandlerSpec::event(name))
    }

    /// Start registering an event handler for several event names that consume JSON
    /// payload input.
    pub fn events(self, names: &'static [&'static str]) -> RouteBuilder<D> {
        self.handler(HandlerSpec::events(names))
    }

    /// Start registering a handler from a transport-visible spec.
    pub fn handler(self, spec: HandlerSpec) -> RouteBuilder<D> {
        RouteBuilder { routes: self, spec }
    }

    fn register_handler(
        mut self,
        spec: HandlerSpec,
        guard: Option<Arc<GuardFn<D>>>,
        handle: Arc<HandlerFn<D>>,
    ) -> Self {
        let by_name = self.handlers.entry(spec.kind).or_default();
        let names = spec.names();
        for (position, name) in names.iter().enumerate() {
            assert!(
                !by_name.contains_key(*name) && !names[..position].contains(name),
                "duplicate route registration for {:?} `{}`",
                spec.kind,
                name
            );
        }

        for name in names {
            by_name.insert(
                name.to_string(),
                RegisteredHandler::Legacy {
                    guard: guard.clone(),
                    handle: handle.clone(),
                },
            );
        }
        self.handler_specs.push(spec);
        self
    }

    fn register_typed_handler<I, K>(
        mut self,
        route_name: &'static str,
        contract: TypedCommandContract,
        declared_mount: Option<CommandMount>,
        guard: Option<Arc<CausalGuardFn<D::Aggregate>>>,
        handle: Arc<PreparedHandlerFn<D::Aggregate, I, K>>,
    ) -> Self
    where
        D: CausalRouteDependencies,
        D::Aggregate: Aggregate + Send + Sync + 'static,
        I: serde::de::DeserializeOwned + Send + 'static,
        K: CommandOutcome,
    {
        assert_eq!(
            route_name, contract.name,
            "typed command route and contract ids must match"
        );
        let by_name = self.handlers.entry(MessageKind::Command).or_default();
        assert!(
            !by_name.contains_key(route_name),
            "duplicate route registration for {:?} `{}`",
            MessageKind::Command,
            route_name,
        );
        let spec = CommandSpec::from_contract(&contract)
            .unwrap_or_else(|error| panic!("typed command contract cannot compile: {error}"));
        let mount = declared_mount
            .unwrap_or_else(|| CommandMount::from_typed_route(spec.clone(), route_name));
        assert_eq!(
            mount.spec().id,
            spec.id,
            "typed command mount and route declaration ids must match"
        );
        assert_eq!(
            mount.spec().fingerprint,
            spec.fingerprint,
            "typed command mount and route declaration fingerprints must match"
        );
        by_name.insert(
            route_name.to_string(),
            RegisteredHandler::Causal(Box::new(
                RegisteredCausalHandler::<D::Aggregate, I, K>::new(contract, guard, handle),
            )),
        );
        mount
            .register_with(&mut self)
            .unwrap_or_else(|error| panic!("typed command mount registration failed: {error}"));
        self.handler_specs.push(HandlerSpec::command(route_name));
        self
    }

    pub(in crate::microsvc) fn register_projector(
        mut self,
        spec: HandlerSpec,
        handler: Box<dyn ErasedProjectorHandler<D>>,
    ) -> Self {
        let by_name = self.handlers.entry(MessageKind::Event).or_default();
        let names = spec.names();
        for (position, name) in names.iter().enumerate() {
            assert!(
                !names[..position].contains(name),
                "causal projector repeats {:?} route `{}`",
                MessageKind::Event,
                name
            );
        }
        let handler: Arc<dyn ErasedProjectorHandler<D>> = Arc::from(handler);
        for name in names {
            match by_name.get_mut(name) {
                Some(RegisteredHandler::Projector(projectors)) => {
                    projectors.push(Arc::clone(&handler));
                    projectors.sort_by(|left, right| {
                        let left = left.registration().topology;
                        let right = right.registration().topology;
                        left.name()
                            .cmp(right.name())
                            .then_with(|| left.digest().cmp(&right.digest()))
                    });
                }
                Some(RegisteredHandler::Legacy { .. } | RegisteredHandler::Causal(_)) => {
                    panic!(
                        "causal projector route {:?} `{name}` collides with a non-projector handler",
                        MessageKind::Event
                    );
                }
                None => {
                    by_name.insert(
                        name.to_string(),
                        RegisteredHandler::Projector(vec![Arc::clone(&handler)]),
                    );
                }
            }
        }
        self.projectors.push(handler);
        self.handler_specs.push(spec);
        self
    }

    pub(super) fn typed_contracts(&self) -> Vec<&TypedCommandContract> {
        self.handlers
            .values()
            .flat_map(HashMap::values)
            .filter_map(|handler| match handler {
                RegisteredHandler::Causal(handler) => Some(handler.contract()),
                RegisteredHandler::Legacy { .. } | RegisteredHandler::Projector(_) => None,
            })
            .collect()
    }

    pub(super) fn registered_keys(&self) -> Vec<(MessageKind, String)> {
        self.handlers
            .iter()
            .flat_map(|(kind, by_name)| by_name.keys().map(move |name| (*kind, name.clone())))
            .collect()
    }

    async fn invoke(
        &self,
        message: &Message,
        input: Value,
        session: Session,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<Value, HandlerError> {
        // Clone the handler/guard Arcs so the handler map is not borrowed across
        // the (awaited) handler future.
        let (guard, handle) = {
            let handler = self
                .handlers
                .get(&message.kind)
                .and_then(|by_name| by_name.get(message.name.as_str()))
                .ok_or_else(|| HandlerError::UnknownCommand(message.name.clone()))?;
            match handler {
                RegisteredHandler::Legacy { guard, handle } => (guard.clone(), handle.clone()),
                RegisteredHandler::Causal(_) => {
                    return Err(HandlerError::Unauthorized(
                        "typed causal commands require a verified GraphQL bearer envelope".into(),
                    ));
                }
                RegisteredHandler::Projector(projectors) => {
                    for projector in projectors {
                        if let Err(error) = projector
                            .dispatch(&self.dependencies, message, ordered)
                            .await
                        {
                            if error.is_projection_retryable()
                                || matches!(
                                    error,
                                    HandlerError::ProjectionTerminalRecorded { .. }
                                        | HandlerError::ProjectionDeliveryHalted { .. }
                                )
                            {
                                return Err(error);
                            }
                            // Causal-projector delivery is stricter than the
                            // service's ordinary permanent-failure policy. If a
                            // permanent error was not converted to a durable
                            // terminal record, dead-lettering or acknowledging
                            // it would let later input cross an unproven gap.
                            return Err(HandlerError::ProjectionDeliveryHalted {
                                source: Box::new(error),
                            });
                        }
                    }
                    return Ok(Value::Null);
                }
            }
        };
        let ctx = Context::new(message, input, session, &self.dependencies);

        // Run guard (synchronous) if present.
        if let Some(guard) = &guard {
            if !guard(&ctx) {
                return Err(HandlerError::GuardRejected(message.name.clone()));
            }
        }

        handle(&ctx).await
    }
}

impl<D: Send + Sync + 'static> CommandMountRegistrar for Routes<D> {
    fn register_command_mount(
        &mut self,
        mount: CommandMount,
    ) -> Result<(), crate::microsvc::HandlerError> {
        let Some(handlers) = self.handlers.get(&MessageKind::Command) else {
            return Err(crate::microsvc::HandlerError::UnknownCommand(
                mount.spec().id.clone(),
            ));
        };
        if !handlers.contains_key(&mount.spec().id) {
            return Err(crate::microsvc::HandlerError::UnknownCommand(
                mount.spec().id.clone(),
            ));
        }
        if mount.typed_route_name() != Some(mount.spec().id.as_str()) {
            return Err(crate::microsvc::HandlerError::Rejected(format!(
                "command mount `{}` is not the generated typed-route registration for its declaration",
                mount.spec().id
            )));
        }
        let expected = self
            .typed_contracts()
            .into_iter()
            .find(|contract| contract.name == mount.spec().id)
            .and_then(|contract| CommandSpec::from_contract(contract).ok())
            .ok_or_else(|| {
                crate::microsvc::HandlerError::Rejected(format!(
                    "command mount `{}` is not backed by a typed causal declaration",
                    mount.spec().id
                ))
            })?;
        if expected.fingerprint != mount.spec().fingerprint {
            return Err(crate::microsvc::HandlerError::Rejected(format!(
                "command mount `{}` has a stale declaration fingerprint",
                mount.spec().id
            )));
        }
        if self
            .command_mounts
            .iter()
            .any(|registered| registered.spec().id == mount.spec().id)
        {
            return Err(crate::microsvc::HandlerError::Rejected(format!(
                "command mount `{}` is registered more than once",
                mount.spec().id
            )));
        }
        self.command_mounts.push(mount);
        Ok(())
    }
}

impl<D, A, I, K> ErasedCausalHandler<D> for RegisteredCausalHandler<A, I, K>
where
    D: CausalRouteDependencies<Aggregate = A> + Send + Sync + 'static,
    D::Backend: TransactionalCommit,
    A: Aggregate + Send + Sync + 'static,
    I: serde::de::DeserializeOwned + Send + 'static,
    K: CommandOutcome,
{
    fn contract(&self) -> &TypedCommandContract {
        &self.contract
    }

    fn dispatch_cell_causal<'a>(
        &'a self,
        dependencies: &'a D,
        identity: &'a CellCommandIdentity,
        input: Value,
        session: Session,
        shard: &'a StreamIdentity,
    ) -> CellCausalHandlerFuture<'a> {
        Box::pin(async move {
            match crate::application::admit_command_session(
                &self.contract.roles,
                session.user_id(),
                &session.roles(),
            ) {
                Ok(()) => {}
                Err("unauthenticated") => return Err(CellDispatchError::Unauthorized),
                Err(_) => return Err(CellDispatchError::Forbidden),
            }
            if self.contract.consistency == CommandConsistency::Atomic {
                return Err(CellDispatchError::BadRequest(
                    "atomic typed commands require a same-transaction relational projection host"
                        .into(),
                ));
            }

            let canonical = canonicalize_command_input(&self.contract.input, input)
                .map_err(|error| CellDispatchError::BadRequest(error.to_string()))?;
            let typed = canonical
                .decode::<I>()
                .map_err(|error| CellDispatchError::BadRequest(error.to_string()))?;
            let (input, wire, input_digest) = typed.into_parts();
            let policy = CausalCommandPolicy::default();
            let reservation = CommandReservation::new(
                identity.key().clone(),
                self.contract.name.clone(),
                CommandContractFingerprint::new(self.contract.fingerprint_bytes()),
                CanonicalInputHash::new(input_digest),
                policy.attempt_lease,
                policy.replay_retention,
            )
            .map_err(crate::microsvc::cell_host::causal::internal_ledger_error)?;

            let aggregate_repository = dependencies.__causal_aggregate_repository();
            let repository = aggregate_repository.repo();
            let attempt = match repository
                .reserve_command(reservation)
                .await
                .map_err(crate::microsvc::cell_host::causal::internal_ledger_error)?
            {
                ReservationOutcome::Acquired(attempt) => attempt,
                ReservationOutcome::InProgress { .. } => {
                    return Err(CellDispatchError::InProgress);
                }
                ReservationOutcome::Replay(replay) => {
                    return crate::microsvc::cell_host::causal::replay_result(replay, true);
                }
                ReservationOutcome::Conflict => return Err(CellDispatchError::CommandIdReuse),
                ReservationOutcome::Expired => return Err(CellDispatchError::Expired),
            };

            let payload = serde_json::to_vec(&wire).map_err(|error| {
                CellDispatchError::Internal(format!(
                    "canonical cell command input could not be encoded: {error}"
                ))
            })?;
            let mut metadata = session
                .variables()
                .iter()
                .filter(|(name, _)| !name.eq_ignore_ascii_case(crate::trace_context::CAUSATION_ID))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<Vec<_>>();
            metadata.push((
                crate::trace_context::CAUSATION_ID.to_string(),
                attempt.causation_id().as_str().to_string(),
            ));
            let message = Message {
                id: Some(identity.command_id().to_string()),
                name: self.contract.name.clone(),
                kind: MessageKind::Command,
                payload,
                content_type: "application/json".into(),
                metadata,
            };

            let workspace = CausalWorkspace::new(aggregate_repository);
            let context = CausalCommandContext::new(&message, &session, &workspace);
            if self.guard.as_ref().is_some_and(|guard| !guard(&context)) {
                return crate::microsvc::cell_host::causal::commit_rejection(
                    repository,
                    attempt,
                    policy.replay_retention,
                    "REJECTED",
                    422,
                    format!("guard rejected command: {}", self.contract.name),
                )
                .await;
            }

            let mut prepared = match (self.handle)(&context, input).await {
                Ok(prepared) => prepared,
                Err(error) if error.status_code() < 500 => {
                    return crate::microsvc::cell_host::causal::commit_rejection(
                        repository,
                        attempt,
                        policy.replay_retention,
                        crate::microsvc::cell_host::causal::handler_error_code(&error),
                        error.status_code(),
                        error.client_facing_message(),
                    )
                    .await;
                }
                Err(error) => {
                    return crate::microsvc::cell_host::causal::abandon_attempt(
                        repository,
                        attempt,
                        error.to_string(),
                    )
                    .await;
                }
            };

            let mut parts = match workspace.into_parts() {
                Ok(parts) => parts,
                Err(error) => {
                    return crate::microsvc::cell_host::causal::abandon_attempt(
                        repository,
                        attempt,
                        error.to_string(),
                    )
                    .await;
                }
            };
            if let Err(error) = parts.prepare_domain_publications(attempt.causation_id().as_str()) {
                return crate::microsvc::cell_host::causal::abandon_attempt(
                    repository,
                    attempt,
                    error.to_string(),
                )
                .await;
            }
            if let Err(error) = parts.validate_prepared(&self.contract, &mut prepared) {
                return crate::microsvc::cell_host::causal::abandon_attempt(
                    repository,
                    attempt,
                    error.to_string(),
                )
                .await;
            }

            let replay_payload = prepared.serialized_payload().clone();
            let batch = match parts.prepare_commit_batch() {
                Ok(batch) => batch,
                Err(error) => {
                    return crate::microsvc::cell_host::causal::abandon_attempt(
                        repository,
                        attempt,
                        format!("cell causal commit batch preparation failed: {error}"),
                    )
                    .await;
                }
            };
            if let Err(error) =
                crate::microsvc::cell_host::validate_cell_projection_events(&batch.outbox_messages)
            {
                drop(batch);
                return crate::microsvc::cell_host::causal::abandon_attempt(
                    repository,
                    attempt,
                    format!("cell projection events violate response bounds: {error}"),
                )
                .await;
            }
            let foreign_stream = batch
                .streams
                .iter()
                .find(|stream| stream.identity != *shard)
                .map(|stream| stream.identity.to_string());
            if let Some(foreign_stream) = foreign_stream {
                drop(batch);
                return crate::microsvc::cell_host::causal::abandon_attempt(
                    repository,
                    attempt,
                    format!("cell `{shard}` cannot commit stream `{foreign_stream}`"),
                )
                .await;
            }

            let fence = attempt.fence();
            let completion = attempt
                .complete(
                    TerminalCommandState::Succeeded,
                    replay_payload.clone(),
                    policy.replay_retention,
                )
                .map_err(crate::microsvc::cell_host::causal::internal_ledger_error)?;
            match repository
                .commit_causal_batch(CausalCommitBatch::new(batch, completion))
                .await
            {
                Ok(()) => {
                    parts.mark_committed_state().map_err(|error| {
                        CellDispatchError::Internal(format!(
                            "committed cell workspace cleanup failed: {error}"
                        ))
                    })?;
                    let (_committed, serialized) = prepared.finalize_after_commit();
                    let result = crate::microsvc::cell_host::causal::load_committed_result(
                        repository, &fence, false,
                    )
                    .await?;
                    if result.payload() != &serialized {
                        return Err(CellDispatchError::Internal(
                            "durable cell replay differs from the committed handler payload".into(),
                        ));
                    }
                    Ok(result)
                }
                Err(error) => {
                    crate::microsvc::cell_host::causal::recover_commit_error(
                        repository,
                        fence,
                        error.to_string(),
                    )
                    .await
                }
            }
        })
    }

    #[cfg(feature = "graphql")]
    fn contract_mut(&mut self) -> &mut TypedCommandContract {
        &mut self.contract
    }

    #[cfg(feature = "graphql")]
    fn storage_identity(&self, dependencies: &D) -> crate::command_ledger::CausalStorageIdentity {
        dependencies
            .__causal_aggregate_repository()
            .repo()
            .causal_storage_identity()
    }

    #[cfg(feature = "graphql")]
    fn dispatch<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalHandlerFuture<'a> {
        Box::pin(async move {
            ensure_causal_grant(&self.contract, &session)?;

            let canonical = canonicalize_command_input(&self.contract.input, input)
                .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
            let typed = canonical
                .decode::<I>()
                .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
            let (input, wire, input_digest) = typed.into_parts();
            let projection_obligations = self
                .contract
                .resolve_projection_obligations_from_session(&wire, Some(&session))
                .map_err(|error| CausalDispatchError::Internal(error.to_string()))?;
            let direct_projection_target = self
                .contract
                .resolve_direct_projection_target_from_session(&wire, Some(&session))
                .map_err(|error| CausalDispatchError::Internal(error.to_string()))?;

            let command_id = CommandId::parse(command_id)
                .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
            let partition = PrincipalPartitionId::new(principal.partition_for_service(service_id))
                .map_err(internal_ledger_error)?;
            let key = CommandLedgerKey::new(service_id, partition, command_id)
                .map_err(internal_ledger_error)?;
            let reservation = CommandReservation::new(
                key,
                self.contract.name.clone(),
                CommandContractFingerprint::new(self.contract.fingerprint_bytes()),
                CanonicalInputHash::new(input_digest),
                policy.attempt_lease,
                policy.replay_retention,
            )
            .map_err(internal_ledger_error)?;

            let aggregate_repository = dependencies.__causal_aggregate_repository();
            let repository = aggregate_repository.repo();
            if let Some(target) = direct_projection_target.as_ref() {
                let (topology, ownership) = target.registration();
                self.direct_projection_bootstrap
                    .get_or_try_init(|| async {
                        repository
                            .register_projection_models(topology, ownership)
                            .await
                            .map_err(|error| {
                                CausalDispatchError::Internal(format!(
                                    "direct projection ownership bootstrap failed: {error}"
                                ))
                            })
                    })
                    .await?;
            }
            let attempt = match repository
                .reserve_command(reservation)
                .await
                .map_err(internal_ledger_error)?
            {
                ReservationOutcome::Acquired(attempt) => attempt,
                ReservationOutcome::InProgress { .. } => {
                    return Err(CausalDispatchError::InProgress);
                }
                ReservationOutcome::Replay(replay) => {
                    return replay_result(self.contract.consistency, replay);
                }
                ReservationOutcome::Conflict => return Err(CausalDispatchError::CommandIdReuse),
                ReservationOutcome::Expired => return Err(CausalDispatchError::Expired),
            };

            let payload = serde_json::to_vec(&wire).map_err(|error| {
                CausalDispatchError::Internal(format!(
                    "canonical command input could not be encoded: {error}"
                ))
            })?;
            let mut metadata = session
                .variables()
                .iter()
                .filter(|(name, _)| !name.eq_ignore_ascii_case(crate::trace_context::CAUSATION_ID))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect::<Vec<_>>();
            metadata.push((
                crate::trace_context::CAUSATION_ID.to_string(),
                attempt.causation_id().as_str().to_string(),
            ));
            let message = Message {
                id: Some(attempt.key().command_id().to_string()),
                name: self.contract.name.clone(),
                kind: MessageKind::Command,
                payload,
                content_type: "application/json".into(),
                metadata,
            };

            let workspace = CausalWorkspace::new(aggregate_repository);
            let context = CausalCommandContext::new(&message, &session, &workspace);
            if self.guard.as_ref().is_some_and(|guard| !guard(&context)) {
                return commit_causal_rejection(
                    repository,
                    attempt,
                    self.contract.consistency,
                    policy.replay_retention,
                    "REJECTED",
                    422,
                    format!("guard rejected command: {}", self.contract.name),
                )
                .await;
            }

            let mut prepared = match (self.handle)(&context, input).await {
                Ok(prepared) => prepared,
                Err(error) if error.status_code() < 500 => {
                    let code = causal_handler_error_code(&error);
                    let status = error.status_code();
                    let message = error.client_facing_message();
                    return commit_causal_rejection(
                        repository,
                        attempt,
                        self.contract.consistency,
                        policy.replay_retention,
                        code,
                        status,
                        message,
                    )
                    .await;
                }
                Err(error) => {
                    return abandon_causal_attempt(
                        repository,
                        attempt,
                        self.contract.consistency,
                        error.to_string(),
                    )
                    .await;
                }
            };

            let mut parts = match workspace.into_parts() {
                Ok(parts) => parts,
                Err(error) => {
                    return abandon_causal_attempt(
                        repository,
                        attempt,
                        self.contract.consistency,
                        error.to_string(),
                    )
                    .await;
                }
            };
            if let Err(error) = parts.prepare_domain_publications(attempt.causation_id().as_str()) {
                return abandon_causal_attempt(
                    repository,
                    attempt,
                    self.contract.consistency,
                    error.to_string(),
                )
                .await;
            }
            if let Err(error) = parts.validate_prepared(&self.contract, &mut prepared) {
                return abandon_causal_attempt(
                    repository,
                    attempt,
                    self.contract.consistency,
                    error.to_string(),
                )
                .await;
            }
            let projection_metadata = match protocol.as_ref() {
                Some(protocol)
                    if self.contract.consistency != CommandConsistency::Atomic
                        && !self.contract.projections.selectors.is_empty() =>
                {
                    if !projection_obligations.is_empty() {
                        return abandon_causal_attempt(
                        repository,
                        attempt,
                        self.contract.consistency,
                        "typed command cannot mix modeled projection metadata with legacy confirmations"
                            .to_owned(),
                    )
                    .await;
                    }
                    let occurrences = match parts.prepared_domain_occurrences() {
                        Ok(occurrences) => occurrences,
                        Err(error) => {
                            return abandon_causal_attempt(
                                repository,
                                attempt,
                                self.contract.consistency,
                                error.to_string(),
                            )
                            .await;
                        }
                    };
                    match protocol.projection_metadata_for_actual(
                        attempt.causation_id().clone(),
                        policy.replay_retention,
                        occurrences,
                        &self.contract.projections.selectors,
                    ) {
                        Ok(metadata) => Some(metadata),
                        Err(error) => {
                            return abandon_causal_attempt(
                                repository,
                                attempt,
                                self.contract.consistency,
                                error.to_string(),
                            )
                            .await;
                        }
                    }
                }
                _ => None,
            };
            let projection_retention_expires_at = match projection_metadata
                .as_ref()
                .map(crate::graphql::protocol::CommandProjectionMetadataV1::expires_at)
                .transpose()
            {
                Ok(deadline) => deadline,
                Err(error) => {
                    return abandon_causal_attempt(
                        repository,
                        attempt,
                        self.contract.consistency,
                        format!("modeled projection metadata lifetime failed: {error}"),
                    )
                    .await;
                }
            };
            let projection_metadata_bytes = match projection_metadata
                .as_ref()
                .map(crate::graphql::protocol::CommandProjectionMetadataV1::canonical_bytes)
                .transpose()
            {
                Ok(bytes) => bytes,
                Err(error) => {
                    return abandon_causal_attempt(
                        repository,
                        attempt,
                        self.contract.consistency,
                        format!("modeled projection metadata encoding failed: {error}"),
                    )
                    .await;
                }
            };
            let direct_projection = match parts.seal_direct_projection(
                &prepared,
                direct_projection_target,
                attempt.causation_id().as_str(),
            ) {
                Ok(direct_projection) => direct_projection,
                Err(error) => {
                    return abandon_causal_attempt(
                        repository,
                        attempt,
                        self.contract.consistency,
                        error.to_string(),
                    )
                    .await;
                }
            };

            let terminal_state = match (&projection_metadata, self.contract.consistency) {
                (Some(metadata), CommandConsistency::Succeeded | CommandConsistency::Eventual)
                    if metadata.obligations.is_empty() =>
                {
                    TerminalCommandState::Succeeded
                }
                (Some(_), CommandConsistency::Succeeded | CommandConsistency::Eventual) => {
                    TerminalCommandState::SucceededPendingProjection
                }
                (Some(_), CommandConsistency::Atomic) => unreachable!(
                    "same-transaction commands do not persist eventual modeled metadata"
                ),
                (None, CommandConsistency::Succeeded) if self.contract.confirmations.is_empty() => {
                    TerminalCommandState::Succeeded
                }
                (None, CommandConsistency::Succeeded | CommandConsistency::Eventual) => {
                    TerminalCommandState::SucceededPendingProjection
                }
                (None, CommandConsistency::Atomic) => TerminalCommandState::Atomic,
            };
            let replay_payload = prepared.serialized_payload().clone();
            let publisher = aggregate_repository.outbox_publisher();
            let mut batch = match parts.prepare_commit_batch() {
                Ok(batch) => batch,
                Err(error) => {
                    return abandon_causal_attempt(
                        repository,
                        attempt,
                        self.contract.consistency,
                        format!("causal commit batch preparation failed: {error}"),
                    )
                    .await;
                }
            };

            // Stamp causation, leave rows pending for the bounded worker, and
            // only claim in-transaction when there is no mailbox (hook fallback).
            let mut fallback_rows = Vec::new();
            let mut outbox_ids = Vec::new();
            if let Some(config) = publisher {
                let claim_now = config.schedule.is_none().then(crate::time::now);
                let mut claim_error = None;
                for message in &mut batch.outbox_messages {
                    message.overwrite_causation_id(attempt.causation_id().as_str());
                    outbox_ids.push(message.id().to_string());
                    if let Some(now) = claim_now {
                        if let Err(error) = message.claim_at(&config.worker_id, config.lease, now) {
                            claim_error = Some(error.to_string());
                            break;
                        }
                    }
                }
                if let Some(error) = claim_error {
                    drop(batch);
                    return abandon_causal_attempt(
                        repository,
                        attempt,
                        self.contract.consistency,
                        format!("causal outbox claim failed before commit: {error}"),
                    )
                    .await;
                }
                if config.schedule.is_none() {
                    fallback_rows = batch.outbox_messages.clone();
                }
            }

            let fence = attempt.fence();
            let completion = match (projection_metadata_bytes, projection_retention_expires_at) {
                (Some(metadata), Some(retention_expires_at)) => attempt
                    .complete_with_projection_metadata_until(
                        terminal_state,
                        replay_payload.clone(),
                        metadata,
                        policy.replay_retention,
                        retention_expires_at,
                    ),
                (None, None) => attempt.complete_with_obligations(
                    terminal_state,
                    replay_payload.clone(),
                    projection_obligations,
                    policy.replay_retention,
                ),
                _ => unreachable!("modeled projection bytes and lifetime are derived together"),
            }
            .map_err(internal_ledger_error)?;
            let causal_batch = match direct_projection {
                Some(direct_projection) => {
                    CausalCommitBatch::with_direct_projection(batch, completion, direct_projection)
                }
                None => CausalCommitBatch::new(batch, completion),
            };
            match repository.commit_causal_batch(causal_batch).await {
                Ok(()) => {
                    parts.mark_committed_state().map_err(|error| {
                        CausalDispatchError::Internal(format!(
                            "committed causal workspace cleanup failed: {error}"
                        ))
                    })?;
                    if let Some(config) = publisher {
                        crate::outbox::start_immediate_publish(config, outbox_ids, fallback_rows)
                            .await;
                    }
                    let (_committed, serialized) = prepared.finalize_after_commit();
                    let result = load_committed_dispatch_result(
                        repository,
                        &fence,
                        self.contract.consistency,
                    )
                    .await?;
                    if result.payload != serialized {
                        return Err(CausalDispatchError::Internal(
                            "durable command replay outcome differs from the committed handler payload"
                                .into(),
                        ));
                    }
                    Ok(result)
                }
                Err(error) => {
                    recover_causal_commit_error(
                        repository,
                        fence,
                        self.contract.consistency,
                        error.to_string(),
                    )
                    .await
                }
            }
        })
    }

    #[cfg(feature = "graphql")]
    fn lookup<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a str,
        session: &'a Session,
        principal: VerifiedPrincipal,
    ) -> Pin<Box<dyn Future<Output = Result<CommandLookup, CausalDispatchError>> + Send + 'a>> {
        Box::pin(async move {
            ensure_causal_grant(&self.contract, session)?;
            let command_id = CommandId::parse(command_id)
                .map_err(|error| CausalDispatchError::BadRequest(error.to_string()))?;
            let partition = PrincipalPartitionId::new(principal.partition_for_service(service_id))
                .map_err(internal_ledger_error)?;
            let key = CommandLedgerKey::new(service_id, partition, command_id)
                .map_err(internal_ledger_error)?;
            dependencies
                .__causal_aggregate_repository()
                .repo()
                .lookup_command(&key, CommandLookupScope::CommandName(&self.contract.name))
                .await
                .map_err(internal_ledger_error)
        })
    }

    #[cfg(feature = "graphql")]
    fn status<'a>(
        &'a self,
        dependencies: &'a D,
        service_id: &'a str,
        command_id: &'a CommandId,
        principal_partition: &'a PrincipalPartitionId,
        session: &'a Session,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalStatusFuture<'a> {
        Box::pin(async move {
            // Status is deliberately non-enumerating: handlers that are no
            // longer visible under the caller's current grant are skipped as
            // if no command had ever existed.
            if ensure_causal_grant(&self.contract, session).is_err() {
                return Ok(CausalCommandPublicStatus::unknown(command_id.as_str()));
            }

            let key =
                CommandLedgerKey::new(service_id, principal_partition.clone(), command_id.clone())
                    .map_err(internal_ledger_error)?;
            let contract_fingerprint = self.contract.fingerprint_bytes();
            let lookup = dependencies
                .__causal_aggregate_repository()
                .repo()
                .lookup_command(
                    &key,
                    CommandLookupScope::CommandContract {
                        command_name: &self.contract.name,
                        contract_fingerprint: &contract_fingerprint,
                    },
                )
                .await
                .map_err(internal_ledger_error)?;
            evaluate_causal_command_status(
                dependencies.__causal_aggregate_repository().repo(),
                command_id,
                self.contract.consistency,
                lookup,
                protocol.as_ref(),
            )
            .await
        })
    }

    fn invoke_cell<'a>(
        &'a self,
        dependencies: &'a D,
        input: Value,
        session: Session,
        shard: &'a StreamIdentity,
    ) -> Pin<Box<dyn Future<Output = Result<Value, HandlerError>> + Send + 'a>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&input)
                .map_err(|error| HandlerError::DecodeFailed(error.to_string()))?;
            let typed: I = serde_json::from_value(input)
                .map_err(|error| HandlerError::DecodeFailed(error.to_string()))?;
            let message = Message::new(self.contract.name.clone(), MessageKind::Command, payload);
            let aggregate_repository = dependencies.__causal_aggregate_repository();
            let workspace = CausalWorkspace::new(aggregate_repository);
            let context = CausalCommandContext::new(&message, &session, &workspace);
            if self.guard.as_ref().is_some_and(|guard| !guard(&context)) {
                return Err(HandlerError::GuardRejected(self.contract.name.clone()));
            }
            let mut prepared = (self.handle)(&context, typed).await?;
            let mut parts = workspace
                .into_parts()
                .map_err(super::handlers::workspace_handler_error)?;
            let causation = uuid::Uuid::now_v7().hyphenated().to_string();
            parts
                .prepare_domain_publications(&causation)
                .map_err(super::handlers::workspace_handler_error)?;
            parts
                .validate_prepared(&self.contract, &mut prepared)
                .map_err(|error| HandlerError::Rejected(error.to_string()))?;
            {
                let batch = parts
                    .prepare_commit_batch()
                    .map_err(super::handlers::workspace_handler_error)?;
                for stream in &batch.streams {
                    if stream.identity != *shard {
                        return Err(HandlerError::Rejected(format!(
                            "cell `{shard}` cannot commit stream `{}`",
                            stream.identity
                        )));
                    }
                }
                TransactionalCommit::commit_batch(aggregate_repository.repo(), batch)
                    .await
                    .map_err(HandlerError::from)?;
            }
            parts
                .mark_committed_state()
                .map_err(super::handlers::workspace_handler_error)?;
            Ok(prepared.serialized_payload().clone())
        })
    }
}

impl<D> ErasedRoutes for Routes<D>
where
    D: Send + Sync + 'static,
{
    fn handler_specs(&self) -> &[HandlerSpec] {
        &self.handler_specs
    }

    fn typed_command_contracts(&self) -> Vec<&TypedCommandContract> {
        self.typed_contracts()
    }

    fn projector_registrations(&self) -> Vec<ProjectorRegistration> {
        self.projectors
            .iter()
            .map(|projector| projector.registration())
            .collect()
    }

    fn modeled_local_services(&self) -> Vec<&str> {
        self.modeled_local_services
            .iter()
            .map(String::as_str)
            .collect()
    }

    fn bootstrap_projectors(&self) -> ProjectorBootstrapFuture<'_> {
        Box::pin(async move {
            for projector in &self.projectors {
                projector.bootstrap(&self.dependencies).await?;
            }
            Ok(())
        })
    }

    fn repair_projection<'a>(
        &'a self,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairFuture<'a> {
        Box::pin(async move {
            let mut owner = None;
            for (index, projector) in self.projectors.iter().enumerate() {
                if projector
                    .locates_failure(&self.dependencies, handle)
                    .await?
                    && owner.replace(index).is_some()
                {
                    return Err(HandlerError::Projection(
                        crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                            "projection failure ID resolved to multiple registered projectors"
                                .into(),
                        ),
                    ));
                }
            }
            let Some(owner) = owner else {
                return Ok(None);
            };
            self.projectors[owner]
                .repair(&self.dependencies, handle)
                .await
        })
    }

    fn locates_projection_failure<'a>(
        &'a self,
        handle: &'a ProjectionRepairHandle,
    ) -> ProjectorRepairLookupFuture<'a> {
        Box::pin(async move {
            let mut found = false;
            for projector in &self.projectors {
                if projector
                    .locates_failure(&self.dependencies, handle)
                    .await?
                {
                    if found {
                        return Err(HandlerError::Projection(
                            crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                                "projection failure ID resolved to multiple registered projectors"
                                    .into(),
                            ),
                        ));
                    }
                    found = true;
                }
            }
            Ok(found)
        })
    }

    fn is_causal_projector(&self, message: &Message) -> bool {
        matches!(
            self.handlers
                .get(&message.kind)
                .and_then(|handlers| handlers.get(message.name())),
            Some(RegisteredHandler::Projector(_))
        )
    }

    #[cfg(feature = "graphql")]
    fn bind_typed_command_contracts(
        &mut self,
        contracts: &BTreeMap<String, TypedCommandContract>,
    ) -> Result<(), String> {
        for handlers in self.handlers.values_mut() {
            for registered in handlers.values_mut() {
                let RegisteredHandler::Causal(handler) = registered else {
                    continue;
                };
                let current = handler.contract();
                let bound = contracts.get(&current.name).ok_or_else(|| {
                    format!(
                        "GraphQL engine is missing typed command `{}` from the executable service",
                        current.name
                    )
                })?;
                let mut current_without_owner = current.clone();
                current_without_owner.direct_projection = None;
                for confirmation in &mut current_without_owner.confirmations {
                    confirmation.clear_protocol_topology();
                }
                let mut bound_without_owner = bound.clone();
                bound_without_owner.direct_projection = None;
                for confirmation in &mut bound_without_owner.confirmations {
                    confirmation.clear_protocol_topology();
                }
                if current.input_type_id != bound.input_type_id
                    || current.output_type_id != bound.output_type_id
                {
                    return Err("typed command Rust input/output TypeId mismatch".into());
                }
                if current.consistency != bound.consistency
                    || current.projected_model != bound.projected_model
                    || current_without_owner.canonical_value()
                        != bound_without_owner.canonical_value()
                {
                    return Err(format!(
                        "typed command structural fingerprint mismatch for executable route `{}`",
                        current.name
                    ));
                }
                let contract = handler.contract_mut();
                contract.confirmations = bound.confirmations.clone();
                contract.direct_projection = bound.direct_projection.clone();
            }
        }
        Ok(())
    }

    fn dispatch<'a>(
        &'a self,
        message: &'a Message,
        input: Value,
        session: Session,
        ordered: Option<&'a OrderedDelivery>,
    ) -> HandlerFuture<'a> {
        Box::pin(self.invoke(message, input, session, ordered))
    }

    #[cfg(feature = "graphql")]
    fn dispatch_causal<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalHandlerFuture<'a> {
        let handler = self
            .handlers
            .get(&MessageKind::Command)
            .and_then(|handlers| handlers.get(command));
        match handler {
            Some(RegisteredHandler::Causal(handler)) => handler.dispatch(
                &self.dependencies,
                service_id,
                command_id,
                input,
                session,
                principal,
                policy,
                protocol,
            ),
            Some(RegisteredHandler::Legacy { .. })
            | Some(RegisteredHandler::Projector(_))
            | None => Box::pin(async move {
                Err(CausalDispatchError::BadRequest(format!(
                    "`{command}` is not a typed causal command"
                )))
            }),
        }
    }

    #[cfg(feature = "graphql")]
    fn lookup_causal<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        session: &'a Session,
        principal: VerifiedPrincipal,
    ) -> Pin<Box<dyn Future<Output = Result<CommandLookup, CausalDispatchError>> + Send + 'a>> {
        let handler = self
            .handlers
            .get(&MessageKind::Command)
            .and_then(|handlers| handlers.get(command));
        match handler {
            Some(RegisteredHandler::Causal(handler)) => handler.lookup(
                &self.dependencies,
                service_id,
                command_id,
                session,
                principal,
            ),
            Some(RegisteredHandler::Legacy { .. })
            | Some(RegisteredHandler::Projector(_))
            | None => Box::pin(async move {
                Err(CausalDispatchError::BadRequest(format!(
                    "`{command}` is not a typed causal command"
                )))
            }),
        }
    }

    #[cfg(feature = "graphql")]
    fn causal_command_status<'a>(
        &'a self,
        service_id: &'a str,
        command_id: &'a CommandId,
        principal_partition: &'a PrincipalPartitionId,
        session: &'a Session,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalStatusFuture<'a> {
        Box::pin(async move {
            let mut handlers = self
                .handlers
                .get(&MessageKind::Command)
                .into_iter()
                .flat_map(HashMap::values)
                .filter_map(|handler| match handler {
                    RegisteredHandler::Causal(handler) => Some(handler.as_ref()),
                    RegisteredHandler::Legacy { .. } | RegisteredHandler::Projector(_) => None,
                })
                .collect::<Vec<_>>();
            handlers.sort_by(|left, right| left.contract().name.cmp(&right.contract().name));

            for handler in handlers {
                let status = handler
                    .status(
                        &self.dependencies,
                        service_id,
                        command_id,
                        principal_partition,
                        session,
                        protocol.clone(),
                    )
                    .await?;
                if !status.is_unknown() {
                    return Ok(status);
                }
            }
            Ok(CausalCommandPublicStatus::unknown(command_id.as_str()))
        })
    }

    #[cfg(feature = "graphql")]
    fn projected_storage_identities(&self) -> Vec<crate::command_ledger::CausalStorageIdentity> {
        self.handlers
            .values()
            .flat_map(|handlers| handlers.values())
            .filter_map(|handler| match handler {
                RegisteredHandler::Causal(handler)
                    if handler.contract().consistency == CommandConsistency::Atomic =>
                {
                    Some(handler.storage_identity(&self.dependencies))
                }
                RegisteredHandler::Causal(_)
                | RegisteredHandler::Legacy { .. }
                | RegisteredHandler::Projector(_) => None,
            })
            .collect()
    }

    fn configure_outbox_publisher(
        &mut self,
        publisher: DynBusPublisher,
        worker_id: String,
        lease: Duration,
        max_attempts: u32,
        service_name: Option<String>,
    ) {
        if let Some(configurator) = self.outbox_configurator {
            configurator(
                &mut self.dependencies,
                publisher,
                worker_id,
                lease,
                max_attempts,
                service_name,
            );
        }
    }
}
