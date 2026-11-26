#[cfg(feature = "graphql")]
use super::super::causal::ensure_causal_grant;
#[cfg(feature = "graphql")]
use super::super::causal::CausalCommandPublicStatus;
#[cfg(feature = "http")]
use super::super::causal::ExternalCausalAttempt;
#[cfg(feature = "graphql")]
use super::super::causal::GraphqlServiceBindError;
use super::super::causal::{CausalDispatchError, CausalDispatchResult, SealModeledProjection};
use super::super::handlers::{
    CausalGuardFn, GuardFn, HandlerFn, HandlerFuture, PreparedHandlerFn, ProjectorBootstrapFuture,
};
use super::builder::{CommandShard, EventRouteBuilder, PortableCommand, TypedRouteBuilder};
#[cfg(feature = "http")]
use super::erased::ErasedCommandShard;
#[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
use super::erased::LedgerCompaction;
use super::erased::{CausalCommandPolicy, CausalHandlerFuture, ErasedCausalHandler, ErasedRoutes};
#[cfg(feature = "graphql")]
use super::erased::{CausalStatusFuture, CausalStatusRoute};
#[cfg(feature = "http")]
use super::erased::{ExternalAbandonFuture, ExternalCompletionFuture, ExternalReservationFuture};
use super::execute::RegisteredCausalHandler;
use super::outbox_wiring::{DynBusPublisher, OutboxConfigurator};
use super::spec::HandlerSpec;
use crate::application::{CommandMount, CommandMountRegistrar, CommandSpec};
use crate::bus::{Message, MessageKind, OrderedDelivery};
#[cfg(feature = "graphql")]
use crate::command::CommandConsistency;
use crate::command::{
    command_transition, CommandEventSet, CommandInputType, CommandOutcome, TypedCommand,
    TypedCommandContract,
};
use crate::command_dispatch::VerifiedPrincipal;
#[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
use crate::command_ledger::CausalStorageIdentity;
use crate::command_ledger::CommandLookup;
#[cfg(feature = "http")]
use crate::command_ledger::ExternalDispatchBinding;
#[cfg(feature = "graphql")]
use crate::command_ledger::{CommandId, CommandLedgerKey};
use crate::graphql::SurfaceProjector;
use crate::microsvc::cell_host::CellCommandIdentity;
use crate::microsvc::context::Context;
use crate::microsvc::dependencies::{
    CausalProjectionRouteDependencies, CausalRouteDependencies, HasReadModelStore, HasRepo,
};
use crate::microsvc::error::HandlerError;
use crate::microsvc::projector::{
    CausalProjectorRouteBuilder, ErasedProjectorHandler, ProjectionRepairHandle,
    ProjectorRegistration, ProjectorRepairFuture, ProjectorRepairLookupFuture,
};
#[cfg(feature = "graphql")]
use crate::microsvc::projector::{ModeledProjectorHandlerFn, ModeledProjectorRouteBuilder};
use crate::microsvc::session::Session;
#[cfg(feature = "graphql")]
use crate::projection_protocol::{CompiledProjectionTopology, ProjectorTopologyId};
use crate::repository::StreamIdentity;
use serde_json::Value;
#[cfg(feature = "graphql")]
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

enum RegisteredHandler<D> {
    Event {
        guard: Option<Arc<GuardFn<D>>>,
        handle: Arc<HandlerFn<D>>,
    },
    Causal(Box<dyn ErasedCausalHandler<D>>),
    Projector(Vec<Arc<dyn ErasedProjectorHandler<D>>>),
}

/// A typed bundle of command/event handlers and the dependency value they use.
///
/// Handlers are keyed by kind, then name, so dispatch looks up by `&str`
/// without allocating a key.
pub struct Routes<D> {
    pub(in crate::microsvc::service) dependencies: D,
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

    pub(in crate::microsvc::service) fn with_outbox_configurator(
        mut self,
        configurator: OutboxConfigurator<D>,
    ) -> Self {
        self.outbox_configurator = Some(configurator);
        self
    }

    /// Fail fast if handlers are already registered. Dependency builders
    /// reconstruct the route bundle around a new dependency type, which would
    /// otherwise silently drop previously registered handlers.
    pub(in crate::microsvc::service) fn assert_no_registrations(&self, builder: &str) {
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

    /// Install a domain-owned command declaration onto this host bundle.
    pub fn mount(self, command: impl PortableCommand<D>) -> Self {
        command.install(self)
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
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        self.causal_handler(command)?
            .dispatch_cell_causal(&self.dependencies, identity, input, session, shard)
            .await
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
            requires: Vec::new(),
            shard: None,
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
    pub fn event(self, name: &'static str) -> EventRouteBuilder<D> {
        EventRouteBuilder {
            routes: self,
            spec: HandlerSpec::event(name),
        }
    }

    /// Start registering an event handler for several event names that consume JSON
    /// payload input.
    pub fn events(self, names: &'static [&'static str]) -> EventRouteBuilder<D> {
        EventRouteBuilder {
            routes: self,
            spec: HandlerSpec::events(names),
        }
    }

    /// The typed causal handler registered for `command`.
    fn causal_handler(
        &self,
        command: &str,
    ) -> Result<&dyn ErasedCausalHandler<D>, CausalDispatchError> {
        match self
            .handlers
            .get(&MessageKind::Command)
            .and_then(|handlers| handlers.get(command))
        {
            Some(RegisteredHandler::Causal(handler)) => Ok(handler.as_ref()),
            _ => Err(CausalDispatchError::BadRequest(format!(
                "`{command}` is not a typed causal command"
            ))),
        }
    }

    pub(super) fn register_event_handler(
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
                RegisteredHandler::Event {
                    guard: guard.clone(),
                    handle: handle.clone(),
                },
            );
        }
        self.handler_specs.push(spec);
        self
    }

    pub(super) fn register_typed_handler<I, K>(
        mut self,
        route_name: &'static str,
        contract: TypedCommandContract,
        declared_mount: Option<CommandMount>,
        guard: Option<Arc<CausalGuardFn<D::Aggregate>>>,
        handle: Arc<PreparedHandlerFn<D::Aggregate, I, K>>,
        requires: Vec<crate::schedule::Capability>,
        shard: Option<CommandShard<I>>,
    ) -> Self
    where
        D: CausalRouteDependencies,
        I: serde::de::DeserializeOwned + Send + 'static,
        K: CommandOutcome,
    {
        assert_eq!(
            route_name, contract.name,
            "typed command route and contract ids must match"
        );
        // A `now()` input default is stamped from the host clock.
        let mut requires = requires;
        if contract
            .input_defaults
            .iter()
            .any(|default| default.generator == crate::command::InputDefaultGenerator::Now)
            && !requires.contains(&crate::schedule::Capability::Clock)
        {
            requires.push(crate::schedule::Capability::Clock);
        }
        let capabilities = crate::schedule::BackendHost::capabilities(
            self.dependencies.__causal_aggregate_repository().repo(),
        );
        let missing = capabilities.missing(&requires);
        assert!(
            missing.is_empty(),
            "command `{route_name}` requires {} that its backend does not provide \
             (backend capabilities: {capabilities:?})",
            missing
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" and "),
        );
        let requires_clock = requires.contains(&crate::schedule::Capability::Clock);
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
                RegisteredCausalHandler::<D::Aggregate, I, K>::new(
                    contract,
                    guard,
                    handle,
                    requires_clock,
                    requires.contains(&crate::schedule::Capability::Scheduling),
                    shard,
                )
                .unwrap_or_else(|error| panic!("typed command contract cannot compile: {error}")),
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
                Some(RegisteredHandler::Event { .. } | RegisteredHandler::Causal(_)) => {
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

    fn causal_handlers(&self) -> impl Iterator<Item = &dyn ErasedCausalHandler<D>> {
        self.handlers
            .values()
            .flat_map(HashMap::values)
            .filter_map(|handler| match handler {
                RegisteredHandler::Causal(handler) => Some(handler.as_ref()),
                RegisteredHandler::Event { .. } | RegisteredHandler::Projector(_) => None,
            })
    }

    pub(in crate::microsvc::service) fn typed_contracts(&self) -> Vec<&TypedCommandContract> {
        self.causal_handlers()
            .map(|handler| handler.contract())
            .collect()
    }

    pub(in crate::microsvc::service) fn registered_keys(&self) -> Vec<(MessageKind, String)> {
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
                RegisteredHandler::Event { guard, handle } => (guard.clone(), handle.clone()),
                RegisteredHandler::Causal(_) => {
                    // The service runs commands through the ledger and
                    // never routes one here.
                    return Err(HandlerError::Other(
                        format!(
                            "command `{}` runs through the command ledger, not as a message handler",
                            message.name
                        )
                        .into(),
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

    fn typed_command_contract(&self, command: &str) -> Option<&TypedCommandContract> {
        self.causal_handler(command)
            .ok()
            .map(|handler| handler.contract())
    }

    #[cfg(feature = "http")]
    fn aggregate_command_shards(
        &self,
        aggregate: std::any::TypeId,
    ) -> Vec<(&str, Option<ErasedCommandShard>)> {
        self.causal_handlers()
            .filter(|handler| handler.aggregate() == aggregate)
            .map(|handler| (handler.contract().name.as_str(), handler.shard()))
            .collect()
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
    ) -> Result<(), GraphqlServiceBindError> {
        for handlers in self.handlers.values_mut() {
            for registered in handlers.values_mut() {
                let RegisteredHandler::Causal(handler) = registered else {
                    continue;
                };
                let current = handler.contract();
                let bound = contracts.get(&current.name).ok_or_else(|| {
                    GraphqlServiceBindError::MissingTypedCommand {
                        command: current.name.clone(),
                    }
                })?;
                let mut current_without_owner = current.clone();
                current_without_owner.direct_projection = None;
                let mut bound_without_owner = bound.clone();
                bound_without_owner.direct_projection = None;
                if current.input_type_id != bound.input_type_id
                    || current.output_type_id != bound.output_type_id
                {
                    return Err(GraphqlServiceBindError::TypeIdMismatch);
                }
                let current_canonical = current_without_owner
                    .canonical_value()
                    .map_err(GraphqlServiceBindError::InvalidInventory)?;
                let bound_canonical = bound_without_owner
                    .canonical_value()
                    .map_err(GraphqlServiceBindError::InvalidInventory)?;
                if current.consistency != bound.consistency
                    || current.projected_model != bound.projected_model
                    || current_canonical != bound_canonical
                {
                    return Err(GraphqlServiceBindError::StructuralFingerprintMismatch(
                        format!("executable route `{}`", current.name),
                    ));
                }
                handler
                    .bind_direct_projection(bound.direct_projection.clone())
                    .map_err(GraphqlServiceBindError::InvalidInventory)?;
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

    fn dispatch_causal<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        seal_modeled_projection: Option<&'a SealModeledProjection<'a>>,
    ) -> CausalHandlerFuture<'a> {
        match self.causal_handler(command) {
            Ok(handler) => handler.dispatch(
                &self.dependencies,
                service_id,
                command_id,
                input,
                session,
                principal,
                policy,
                seal_modeled_projection,
            ),
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }

    fn lookup_causal<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        session: &'a Session,
        principal: VerifiedPrincipal,
    ) -> Pin<Box<dyn Future<Output = Result<CommandLookup, CausalDispatchError>> + Send + 'a>> {
        match self.causal_handler(command) {
            Ok(handler) => handler.lookup(
                &self.dependencies,
                service_id,
                command_id,
                session,
                principal,
            ),
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }

    #[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
    fn causal_storage_identity(&self) -> Option<CausalStorageIdentity> {
        self.causal_handlers()
            .next()
            .map(|handler| handler.storage_identity(&self.dependencies))
    }

    #[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
    fn ledger_compaction(&self) -> Option<LedgerCompaction> {
        self.causal_handlers()
            .next()
            .map(|handler| handler.ledger_compaction(&self.dependencies))
    }

    #[cfg(feature = "graphql")]
    fn causal_status_route(
        &self,
        command: &str,
        fingerprint: &[u8; 32],
        session: &Session,
    ) -> Option<CommandConsistency> {
        let Some(RegisteredHandler::Causal(handler)) = self
            .handlers
            .get(&MessageKind::Command)
            .and_then(|handlers| handlers.get(command))
        else {
            return None;
        };
        let contract = handler.contract();
        (handler.contract_fingerprint().as_bytes() == fingerprint
            && ensure_causal_grant(contract, session).is_ok())
        .then_some(contract.consistency)
    }

    #[cfg(feature = "graphql")]
    fn causal_command_status<'a>(
        &'a self,
        key: &'a CommandLedgerKey,
        command_id: &'a CommandId,
        route: &'a CausalStatusRoute<'a>,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> CausalStatusFuture<'a> {
        // Every typed command of a bundle commits to its one repository, so
        // any of its handlers reaches the ledger.
        match self.causal_handlers().next() {
            Some(handler) => handler.status(&self.dependencies, key, command_id, route, protocol),
            None => {
                Box::pin(async move { Ok(CausalCommandPublicStatus::unknown(command_id.as_str())) })
            }
        }
    }

    #[cfg(feature = "http")]
    fn reserve_external<'a>(
        &'a self,
        command: &'a str,
        service_id: &'a str,
        command_id: &'a str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        policy: CausalCommandPolicy,
        binding: ExternalDispatchBinding,
    ) -> ExternalReservationFuture<'a> {
        match self.causal_handler(command) {
            Ok(handler) => handler.reserve_external(
                &self.dependencies,
                service_id,
                command_id,
                input,
                session,
                principal,
                policy,
                binding,
            ),
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }

    #[cfg(feature = "http")]
    fn complete_external<'a>(
        &'a self,
        command: &'a str,
        attempt: ExternalCausalAttempt,
        result: &'a CausalDispatchResult,
    ) -> ExternalCompletionFuture<'a> {
        match self.causal_handler(command) {
            Ok(handler) => handler.complete_external(&self.dependencies, attempt, result),
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }

    #[cfg(feature = "http")]
    fn abandon_external<'a>(
        &'a self,
        command: &'a str,
        attempt: ExternalCausalAttempt,
    ) -> ExternalAbandonFuture<'a> {
        match self.causal_handler(command) {
            Ok(handler) => handler.abandon_external(&self.dependencies, attempt),
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }

    #[cfg(feature = "graphql")]
    fn projected_storage_identities(&self) -> Vec<CausalStorageIdentity> {
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
                | RegisteredHandler::Event { .. }
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
