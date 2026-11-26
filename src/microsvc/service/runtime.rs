use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "metrics")]
use std::time::Instant;

use serde_json::Value;

#[cfg(feature = "graphql")]
use super::causal::{
    internal_ledger_error, CausalCommandPublicStatus, GraphqlServiceBindError,
    ModeledProjectionMetadata,
};
use super::causal::{CausalDispatchError, CausalDispatchResult, SealModeledProjection};
#[cfg(feature = "http")]
use super::causal::{ExternalCausalAttempt, ExternalCausalReservation};
use super::helpers::{
    is_json_content_type, message_to_json_input, message_to_session, names_by_kind,
    transport_command_error, transport_command_id, transport_session,
};
#[cfg(feature = "otel")]
use super::helpers::{microsvc_dispatch_span, microsvc_handler_span};
use super::routes::{CausalCommandPolicy, DynBusPublisher, ErasedRoutes, HandlerSpec, Routes};
use crate::application::{
    Application, ApplicationError, ApplicationResult, CommandDefinition, CommandMount,
    CommandMountRegistrar, CommandSpec, Module, SurfaceSpec,
};
use crate::bus::{
    Message, MessageKind, OrderedDelivery, RunOptions, SubscriptionPlan, TransportError,
};
use crate::command::{TypedCommandContract, TypedServiceCommandBinding};
use crate::command_dispatch::{CommandHost, VerifiedPrincipal};
use crate::command_ledger::CommandLookup;
#[cfg(feature = "http")]
use crate::command_ledger::ExternalDispatchBinding;
#[cfg(feature = "graphql")]
use crate::command_ledger::{CommandId, CommandLedgerKey, PrincipalPartitionId};
use crate::microsvc::error::HandlerError;
use crate::microsvc::lifecycle::LifecycleGate;
use crate::microsvc::projector::{ProjectionRepairHandle, ProjectorRegistration};
use crate::microsvc::session::{Session, TRANSPORT_ROLE, TRANSPORT_USER_ID};

/// The bus run behavior captured by [`Service::with_bus`](crate::microsvc::Service::with_bus).
pub(crate) type ServiceRunner = Box<
    dyn Fn(
            Arc<Service>,
            RunOptions,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>,
        > + Send
        + Sync,
>;

/// Name of the delivery lane that plain [`Service::routes`] registers into.
pub const DEFAULT_DELIVERY_LANE: &str = "default";

/// A microservice deployment that routes messages to one or more route bundles.
pub struct Service {
    name: Option<String>,
    pub(super) routes: Vec<Box<dyn ErasedRoutes>>,
    index: HashMap<MessageKind, HashMap<String, Vec<usize>>>,
    /// Delivery lane of each route bundle (parallel to `routes`); 0 is default.
    route_lanes: Vec<usize>,
    /// Lane names by index; index 0 is the unnamed default lane.
    lane_names: Vec<&'static str>,
    handler_specs: Vec<HandlerSpec>,
    causal_command_policy: CausalCommandPolicy,
    runner: Option<ServiceRunner>,
    registered_command_mounts: Vec<CommandMount>,
    /// Admits or refuses every write this service is asked to run.
    lifecycle: LifecycleGate,
    /// When true, HTTP mounts `POST /{command}`. Off by default — GraphQL,
    /// health, and in-process `dispatch` stay available without this.
    http_command_routes: bool,
    /// Commands exposed one at a time at `POST /{command}`, each through
    /// its own HTTP edge.
    #[cfg(feature = "http")]
    http_commands: BTreeMap<String, crate::microsvc::http::HttpCommandEdge>,
    #[cfg(feature = "graphql")]
    graphql: Option<std::sync::Arc<crate::graphql::GraphqlEngine>>,
    /// Started by `with_bus`; stops when the service is dropped.
    #[cfg(feature = "runtime-tokio")]
    ledger_compaction: Option<crate::microsvc::workers::LedgerCompactionWorker>,
}

impl Service {
    /// Start building a deployment-level service.
    pub fn new() -> Self {
        Self {
            name: None,
            routes: Vec::new(),
            index: HashMap::new(),
            route_lanes: Vec::new(),
            lane_names: vec![DEFAULT_DELIVERY_LANE],
            handler_specs: Vec::new(),
            causal_command_policy: CausalCommandPolicy::default(),
            runner: None,
            registered_command_mounts: Vec::new(),
            lifecycle: LifecycleGate::always_open(),
            http_command_routes: false,
            #[cfg(feature = "http")]
            http_commands: BTreeMap::new(),
            #[cfg(feature = "graphql")]
            graphql: None,
            #[cfg(feature = "runtime-tokio")]
            ledger_compaction: None,
        }
    }

    /// Refuse writes while `lifecycle` is closed: commands, typed commands
    /// and bus deliveries fail with [`HandlerError::ApplicationReloading`].
    ///
    /// A service is always open unless given a gate. A process that
    /// `distributed dev` may run passes [`LifecycleGate::from_env`], resolved
    /// once and cloned into every service it builds. Set it before building a
    /// GraphQL engine from this service: the engine takes the service's gate.
    pub fn with_lifecycle(mut self, lifecycle: LifecycleGate) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    /// The gate set by [`Self::with_lifecycle`].
    #[cfg(feature = "graphql")]
    pub(crate) fn lifecycle(&self) -> &LifecycleGate {
        &self.lifecycle
    }

    fn ensure_mutations_open(&self) -> Result<(), HandlerError> {
        if self.lifecycle.mutations_open() {
            Ok(())
        } else {
            Err(HandlerError::ApplicationReloading)
        }
    }

    /// Mount `POST /{command}` HTTP routes.
    ///
    /// Off by default. Call this only when a process should accept raw command
    /// POSTs (ingress, tests). GraphQL mutations and bus consumers do not need it.
    pub fn with_http_command_routes(mut self) -> Self {
        self.http_command_routes = true;
        self
    }

    /// Whether the HTTP router mounts per-command `POST /{name}` routes.
    pub fn http_command_routes_enabled(&self) -> bool {
        self.http_command_routes
    }

    /// Expose one command at `POST /{command}` through `edge`.
    ///
    /// The edge authenticates the request from its headers and JSON body
    /// (`null` when the body is empty) and returns what to dispatch: the
    /// command ID, the input, and the session and principal the command runs
    /// as. Only that command runs, through the same ledgered path as every
    /// other dispatch, and no request header reaches it unless the edge puts
    /// it in the returned session. The answer is the wait-path response
    /// (`{ "payload", "receipt" }`, or `{ "code", "error" }` with its status).
    ///
    /// Use it for an ingress that cannot speak the GraphQL edge, such as a
    /// provider webhook signed with a shared secret. Every other command path
    /// stays closed; route all of them to the service with
    /// [`command_gateway_routes`](Self::command_gateway_routes) so none falls
    /// through to a UI.
    ///
    /// # Panics
    /// When `command` already has an edge.
    #[cfg(feature = "http")]
    pub fn with_http_command<F>(mut self, command: impl Into<String>, edge: F) -> Self
    where
        F: Fn(
                &axum::http::HeaderMap,
                Value,
            ) -> Result<crate::microsvc::HttpCommandDispatch, CausalDispatchError>
            + Send
            + Sync
            + 'static,
    {
        let command = command.into();
        assert!(
            !self.http_commands.contains_key(&command),
            "command `{command}` already has an HTTP edge"
        );
        self.http_commands.insert(command, Arc::new(edge));
        self
    }

    /// The commands [`with_http_command`](Self::with_http_command) exposes,
    /// with their edges.
    #[cfg(feature = "http")]
    pub(crate) fn http_commands(
        &self,
    ) -> impl Iterator<Item = (&str, &crate::microsvc::http::HttpCommandEdge)> {
        self.http_commands
            .iter()
            .map(|(command, edge)| (command.as_str(), edge))
    }

    /// Gateway routes that give `binding` (the binding serving
    /// [`router`](crate::microsvc::router)) every command path, `/{command}`
    /// exactly, so none falls through to a UI or another prefix owner. The
    /// service answers only the commands it exposes; every other command path
    /// is a 404 there.
    #[cfg(feature = "gateway")]
    pub fn command_gateway_routes(&self, binding: &str) -> Vec<crate::gateway::Route> {
        self.command_names()
            .into_iter()
            .map(|command| {
                crate::gateway::Route::new(
                    format!("command:{command}"),
                    crate::gateway::RoutePath::exact(format!("/{command}")),
                    binding,
                )
            })
            .collect()
    }

    /// Configure the durable command attempt lease and replay retention.
    ///
    /// The defaults are 30 seconds and 30 days. Retention must remain longer
    /// than the attempt lease; deployments must also keep it beyond the retry
    /// and resume window advertised to their generated clients.
    pub fn causal_command_timing(
        mut self,
        attempt_lease: Duration,
        replay_retention: Duration,
    ) -> Self {
        assert!(
            !attempt_lease.is_zero(),
            "causal command attempt lease must be positive"
        );
        assert!(
            replay_retention > attempt_lease,
            "causal command replay retention must exceed the attempt lease"
        );
        self.causal_command_policy = CausalCommandPolicy {
            attempt_lease,
            replay_retention,
        };
        self
    }

    /// Attach a GraphQL query engine served at `POST /graphql`.
    ///
    /// Panics when [`Self::try_with_graphql`] rejects the attachment. New code
    /// that registers typed commands should prefer the fallible form.
    #[cfg(feature = "graphql")]
    pub fn with_graphql(self, engine: crate::graphql::GraphqlEngine) -> Self {
        self.try_with_graphql(engine)
            .unwrap_or_else(|error| panic!("cannot enable GraphQL: {error}"))
    }

    /// Validate and attach a GraphQL engine.
    ///
    /// Typed commands are compared by service ID, a canonical structural
    /// fingerprint, and exact Rust input/output `TypeId`s. A validated engine
    /// may attach and serve reads and durable typed mutations only when its
    /// opaque causal protocol tokens are configured. `Atomic` commands
    /// additionally require the engine and command repository to carry the
    /// same opaque causal-storage identity. Services with no typed commands
    /// may attach a read-only engine.
    #[cfg(feature = "graphql")]
    pub fn try_with_graphql(
        mut self,
        engine: crate::graphql::GraphqlEngine,
    ) -> Result<Self, GraphqlServiceBindError> {
        if !self.typed_command_contracts().is_empty() {
            let contracts = engine
                .typed_command_contracts_for_service()
                .map_err(GraphqlServiceBindError::InvalidInventory)?
                .into_iter()
                .map(|contract| (contract.name.clone(), contract))
                .collect::<BTreeMap<_, _>>();
            for routes in &mut self.routes {
                routes.bind_typed_command_contracts(&contracts)?;
            }
        }
        self.validate_graphql_engine(&engine)?;
        self.graphql = Some(std::sync::Arc::new(engine));
        Ok(self)
    }

    #[cfg(feature = "graphql")]
    pub(crate) fn validate_graphql_engine(
        &self,
        engine: &crate::graphql::GraphqlEngine,
    ) -> Result<(), GraphqlServiceBindError> {
        let service_id = self.name().ok_or(GraphqlServiceBindError::UnnamedService)?;
        let engine_service_id = engine
            .service_id()
            .ok_or(GraphqlServiceBindError::EngineWithoutServiceId)?;
        if service_id != engine_service_id {
            return Err(GraphqlServiceBindError::ServiceIdMismatch {
                executable: service_id.to_owned(),
                engine: engine_service_id.to_owned(),
            });
        }
        if self.handles_message(crate::bus::MessageKind::Command, "graphql") {
            return Err(GraphqlServiceBindError::GraphqlCommandTaken);
        }

        let typed_commands = self.typed_command_contracts();
        match (typed_commands.is_empty(), engine.typed_command_binding()) {
            (true, None) => {}
            (_, Some(engine_binding)) => {
                if !engine.lifecycle().is(&self.lifecycle) {
                    return Err(GraphqlServiceBindError::LifecycleMismatch);
                }
                let service_binding = self
                    .typed_command_binding()
                    .map_err(GraphqlServiceBindError::InvalidInventory)?;
                if service_binding.service_id != engine_binding.service_id {
                    return Err(GraphqlServiceBindError::ServiceIdMismatch {
                        executable: service_binding.service_id,
                        engine: engine_binding.service_id.clone(),
                    });
                }
                if service_binding.structural_fingerprint != engine_binding.structural_fingerprint {
                    return Err(GraphqlServiceBindError::StructuralFingerprintMismatch(
                        format!(
                            "executable `{}` vs GraphQL `{}`",
                            service_binding.structural_fingerprint,
                            engine_binding.structural_fingerprint
                        ),
                    ));
                }
                if service_binding.types != engine_binding.types {
                    return Err(GraphqlServiceBindError::TypeIdMismatch);
                }
            }
            (false, None) => {
                return Err(GraphqlServiceBindError::EngineNotDerivedFromService);
            }
        }

        if !typed_commands.is_empty() && !engine.causal_protocol_configured() {
            return Err(GraphqlServiceBindError::MissingProtocolTokenKey);
        }

        let projected_identities = self
            .routes
            .iter()
            .flat_map(|routes| routes.projected_storage_identities())
            .collect::<Vec<_>>();
        if !projected_identities.is_empty() {
            let engine_identity = engine
                .causal_storage_identity()
                .ok_or(GraphqlServiceBindError::AtomicPoolNotFromRepository)?;
            if projected_identities
                .iter()
                .any(|identity| *identity != engine_identity)
            {
                return Err(GraphqlServiceBindError::AtomicStorageIdentityMismatch);
            }
        }

        Ok(())
    }

    /// The attached GraphQL engine, if any.
    #[cfg(feature = "graphql")]
    pub fn graphql_engine(&self) -> Option<std::sync::Arc<crate::graphql::GraphqlEngine>> {
        self.graphql.clone()
    }

    /// Build a service from a single route bundle.
    pub fn route<D>(routes: Routes<D>) -> Self
    where
        D: Send + Sync + 'static,
    {
        Self::new().routes(routes)
    }

    /// Assign a stable service/deployment identity.
    ///
    /// Broker-backed buses use this as the default durable consumer group when the
    /// bus itself was not configured with an explicit group. Use the same name for
    /// every replica of one service deployment; use different names for independent
    /// event consumers that each need their own event copy.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        assert!(!name.trim().is_empty(), "service name must not be empty");
        if let Some(existing) = self.name.as_deref() {
            assert_eq!(
                existing, name,
                "service identity was already configured and cannot be changed"
            );
        }
        #[cfg(feature = "graphql")]
        if let Some(engine) = &self.graphql {
            assert_eq!(
                engine.service_id(),
                Some(name.as_str()),
                "attached GraphQL engine identity does not match renamed service"
            );
        }
        for routes in &self.routes {
            for expected in routes.modeled_local_services() {
                assert_eq!(
                    expected, name,
                    "modeled projection local executor route does not match Service::named identity"
                );
            }
        }
        self.name = Some(name);
        self
    }

    /// The stable service/deployment identity, if one was configured.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Exact generated mounts registered with this service. The returned
    /// values contain portable identity only; typed execution still requires
    /// the authenticated causal dispatch path.
    pub fn registered_command_mounts(&self) -> &[CommandMount] {
        &self.registered_command_mounts
    }

    /// Register one explicit command mount against the already-installed
    /// typed route inventory. The route's canonical command spec is the only
    /// authority; a stale or lookalike mount is rejected before dispatch.
    pub fn register_command_mount(&mut self, mount: CommandMount) -> Result<(), HandlerError> {
        self.register_command_mount_inner(mount)
    }

    fn register_command_mount_inner(&mut self, mount: CommandMount) -> Result<(), HandlerError> {
        let Some(indices) = self
            .index
            .get(&MessageKind::Command)
            .and_then(|commands| commands.get(&mount.spec().id))
        else {
            return Err(HandlerError::UnknownCommand(mount.spec().id.clone()));
        };
        if indices.len() != 1 {
            return Err(HandlerError::Rejected(format!(
                "command mount `{}` has ambiguous service routes",
                mount.spec().id
            )));
        }
        if mount.typed_route_name() != Some(mount.spec().id.as_str()) {
            return Err(HandlerError::Rejected(format!(
                "command mount `{}` is not the generated typed-route registration for its declaration",
                mount.spec().id
            )));
        }
        let expected = self
            .routes
            .get(indices[0])
            .and_then(|routes| {
                routes
                    .typed_command_contracts()
                    .into_iter()
                    .find(|contract| contract.name == mount.spec().id)
                    .and_then(|contract| CommandSpec::from_contract(contract).ok())
            })
            .ok_or_else(|| {
                HandlerError::Rejected(format!(
                    "command mount `{}` is not backed by a typed causal route",
                    mount.spec().id
                ))
            })?;
        if expected.fingerprint != mount.spec().fingerprint {
            return Err(HandlerError::Rejected(format!(
                "command mount `{}` has a stale declaration fingerprint",
                mount.spec().id
            )));
        }
        if self
            .registered_command_mounts
            .iter()
            .any(|registered| registered.spec().id == mount.spec().id)
        {
            return Err(HandlerError::Rejected(format!(
                "command mount `{}` is registered more than once",
                mount.spec().id
            )));
        }
        self.registered_command_mounts.push(mount);
        Ok(())
    }

    /// Install the bus run behavior (used by `with_bus`).
    pub(crate) fn set_runner(&mut self, runner: ServiceRunner) {
        self.runner = Some(runner);
    }

    /// Take the installed bus run behavior (used by `run`).
    pub(crate) fn take_runner(&mut self) -> Option<ServiceRunner> {
        self.runner.take()
    }

    /// Add a typed route bundle to this service.
    pub fn routes<D>(mut self, routes: Routes<D>) -> Self
    where
        D: Send + Sync + 'static,
    {
        self.add_routes(routes);
        self
    }

    /// Add a route bundle to a named delivery lane.
    ///
    /// Lanes of one consumer run concurrently when the transport settles each
    /// delivery independently; each lane keeps delivery order and a delivery is
    /// settled only after all of its lanes finished. Use a lane only for routes
    /// that neither depend on nor are depended on by routes of other lanes
    /// within the same delivery (see `docs/consumer-delivery-lanes.md`).
    ///
    /// # Panics
    /// When `lane` is empty, names the default lane, or exceeds the lane limit.
    pub fn lane<D>(mut self, lane: &'static str, routes: Routes<D>) -> Self
    where
        D: Send + Sync + 'static,
    {
        assert!(
            !lane.trim().is_empty() && lane != DEFAULT_DELIVERY_LANE,
            "a named delivery lane must be non-empty and not `{DEFAULT_DELIVERY_LANE}`"
        );
        let index = match self.lane_names.iter().position(|name| *name == lane) {
            Some(index) => index,
            None => {
                assert!(
                    self.lane_names.len() < crate::bus::LaneSet::MAX_LANES,
                    "too many delivery lanes"
                );
                self.lane_names.push(lane);
                self.lane_names.len() - 1
            }
        };
        self.add_routes(routes);
        *self
            .route_lanes
            .last_mut()
            .expect("add_routes registers one bundle") = index;
        self
    }

    /// Delivery lane names by index; index 0 is the default lane.
    pub fn delivery_lane_names(&self) -> &[&'static str] {
        &self.lane_names
    }

    /// Lanes with a route for `(kind, name)`.
    pub fn lanes_for_message(&self, kind: MessageKind, name: &str) -> crate::bus::LaneSet {
        self.index
            .get(&kind)
            .and_then(|by_name| by_name.get(name))
            .into_iter()
            .flatten()
            .fold(crate::bus::LaneSet::EMPTY, |lanes, route| {
                lanes.with(self.route_lanes[*route])
            })
    }

    pub(super) fn add_routes<D>(&mut self, routes: Routes<D>)
    where
        D: Send + Sync + 'static,
    {
        if let Some(service_name) = self.name.as_deref() {
            for expected in routes.modeled_local_services() {
                assert_eq!(
                    expected, service_name,
                    "modeled projection local executor route does not match the service identity"
                );
            }
        }
        let keys = routes.registered_keys();
        let new_projectors = routes.projector_registrations();
        let existing_projectors = self
            .routes
            .iter()
            .flat_map(|routes| routes.projector_registrations())
            .collect::<Vec<_>>();
        validate_projector_registrations(existing_projectors.iter().chain(new_projectors.iter()));
        let typed_commands = routes
            .typed_contracts()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let command_mounts = routes.command_mounts().to_vec();
        #[cfg(feature = "graphql")]
        assert!(
            self.graphql.is_none() || typed_commands.is_empty(),
            "cannot add typed command routes after attaching a GraphQL engine"
        );
        for (kind, name) in &keys {
            if self
                .index
                .get(kind)
                .and_then(|by_name| by_name.get(name))
                .is_some()
            {
                assert!(
                    *kind == MessageKind::Event,
                    "duplicate route registration for {:?} `{}` is allowed only for domain-event fan-out",
                    kind, name
                );
            }
            #[cfg(feature = "graphql")]
            assert!(
                !(self.graphql.is_some()
                    && *kind == crate::bus::MessageKind::Command
                    && name == "graphql"),
                "cannot register command `graphql` while GraphQL is enabled on this service"
            );
        }
        let existing_commands = self.typed_command_contracts();
        for contract in &typed_commands {
            assert!(
                !existing_commands
                    .iter()
                    .any(|registered| registered.name == contract.name),
                "duplicate typed command declaration for `{}`",
                contract.name
            );
        }

        let route_index = self.routes.len();
        for (kind, name) in keys {
            self.index
                .entry(kind)
                .or_default()
                .entry(name)
                .or_default()
                .push(route_index);
        }
        self.handler_specs.extend_from_slice(routes.handler_specs());
        self.registered_command_mounts.extend(command_mounts);
        self.routes.push(Box::new(routes));
        self.route_lanes.push(0);
    }

    pub(crate) fn typed_command_contracts(&self) -> Vec<TypedCommandContract> {
        self.routes
            .iter()
            .flat_map(|routes| routes.typed_command_contracts())
            .cloned()
            .collect()
    }

    /// Compile every explicitly registered typed command into portable specs.
    /// The returned values contain no Service, repository, or handler pointer.
    pub fn command_specs(&self) -> crate::application::ApplicationResult<Vec<CommandSpec>> {
        let mut specs = self
            .typed_command_contracts()
            .iter()
            .map(crate::application::CommandSpec::from_contract)
            .collect::<crate::application::ApplicationResult<Vec<_>>>()?;
        specs.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(specs)
    }

    /// Compile this Service's typed command inventory and an authorized Surface
    /// into one logical application.
    ///
    /// Command namespaces (the segment before the first `.`) become modules.
    /// Every Service command must exist in the Surface so its authorization and
    /// projection contract can be bound, and application validation rejects any
    /// Surface command that is not owned by the Service.
    pub fn application(
        &self,
        name: impl Into<String>,
        surface: SurfaceSpec,
    ) -> ApplicationResult<Application> {
        let mut modules = BTreeMap::<String, Vec<CommandDefinition>>::new();
        for command in self.command_specs()? {
            let namespace = command
                .id
                .split_once('.')
                .map(|(namespace, _)| namespace)
                .filter(|namespace| !namespace.is_empty())
                .ok_or_else(|| {
                    ApplicationError::InvalidSpec(format!(
                        "typed command `{}` has no module namespace; expected `<module>.<action>`",
                        command.id
                    ))
                })?
                .to_string();
            let exposed = surface
                .contract
                .commands
                .iter()
                .find(|exposed| exposed.id == command.id)
                .ok_or_else(|| ApplicationError::Missing {
                    kind: "surface command",
                    identity: command.id.clone(),
                })?;
            let command = command.with_surface_binding(exposed)?;
            modules
                .entry(namespace)
                .or_default()
                .push(CommandDefinition::contract(command));
        }

        let modules = modules
            .into_iter()
            .map(|(namespace, commands)| {
                Module::new(namespace).command_definitions(commands).build()
            })
            .collect::<ApplicationResult<Vec<_>>>()?;

        Application::new(name)
            .modules(modules)
            .surface(surface)
            .build()
    }

    /// Every typed command that commits to `aggregate`, by name, with its
    /// declared shard (`None` when the command declares none).
    #[cfg(feature = "http")]
    pub(crate) fn aggregate_command_shards(
        &self,
        aggregate: std::any::TypeId,
    ) -> Vec<(String, Option<super::routes::ErasedCommandShard>)> {
        let mut shards = self
            .routes
            .iter()
            .flat_map(|routes| routes.aggregate_command_shards(aggregate))
            .map(|(command, shard)| (command.to_string(), shard))
            .collect::<Vec<_>>();
        shards.sort_by(|left, right| left.0.cmp(&right.0));
        shards
    }

    /// Attach Eventual projection metadata to a cell wait-path result using this
    /// process's command contract (no second aggregate write).
    #[cfg(feature = "graphql")]
    pub fn seal_wait_path_dispatch(
        &self,
        command: &str,
        protocol: &crate::graphql::protocol::ProtocolResponseAccumulator,
        result: CausalDispatchResult,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        let contract = self
            .typed_command_contracts()
            .into_iter()
            .find(|contract| contract.name == command)
            .ok_or_else(|| {
                CausalDispatchError::BadRequest(format!(
                    "unknown typed command `{command}` for wait-path protocol"
                ))
            })?;
        result.seal_wait_path_protocol(
            protocol,
            &contract,
            self.causal_command_policy.replay_retention,
        )
    }

    /// Reserve the gateway ledger row for a command that will be committed by
    /// an aggregate cell. The route performs canonical input and grant checks
    /// before any remote request is made.
    #[cfg(feature = "http")]
    pub(crate) async fn reserve_external_causal(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        binding: ExternalDispatchBinding,
    ) -> Result<ExternalCausalReservation, CausalDispatchError> {
        self.ensure_mutations_open()
            .map_err(CausalDispatchError::Handler)?;
        let service_id = self.name().ok_or_else(|| {
            CausalDispatchError::Internal(
                "external typed causal dispatch requires Service::named identity".into(),
            )
        })?;
        self.typed_command_routes(command)?
            .reserve_external(
                command,
                service_id,
                command_id,
                input,
                session,
                principal,
                self.causal_command_policy,
                binding,
            )
            .await
    }

    /// Persist a sealed terminal receipt for a command committed by a cell.
    /// This path only updates the gateway ledger and never invokes a local
    /// handler or appends a local event batch.
    #[cfg(feature = "http")]
    pub(crate) async fn complete_external_causal(
        &self,
        command: &str,
        attempt: ExternalCausalAttempt,
        result: &CausalDispatchResult,
    ) -> Result<(), CausalDispatchError> {
        self.typed_command_routes(command)?
            .complete_external(command, attempt, result)
            .await
    }

    /// The one route bundle that registers `command`.
    fn typed_command_routes(
        &self,
        command: &str,
    ) -> Result<&dyn ErasedRoutes, CausalDispatchError> {
        self.index
            .get(&MessageKind::Command)
            .and_then(|commands| commands.get(command))
            .and_then(|indices| (indices.len() == 1).then_some(indices[0]))
            .map(|route_index| self.routes[route_index].as_ref())
            .ok_or_else(|| CausalDispatchError::BadRequest("unknown typed command".into()))
    }

    /// Mark a remote attempt retryable after a transport/protocol failure.
    /// The durable fence remains the authority for later replay or reclaim.
    #[cfg(feature = "http")]
    pub(crate) async fn abandon_external_causal(
        &self,
        command: &str,
        attempt: ExternalCausalAttempt,
    ) -> Result<(), CausalDispatchError> {
        self.typed_command_routes(command)?
            .abandon_external(command, attempt)
            .await
    }

    pub(crate) fn typed_command_binding(&self) -> Result<TypedServiceCommandBinding, String> {
        let service_id = self
            .name()
            .ok_or_else(|| "typed command inventory requires Service::named".to_string())?;
        TypedServiceCommandBinding::from_contracts(service_id, &self.typed_command_contracts())
    }

    /// Execute one authenticated typed causal route through its durable ledger
    /// and framework-owned staged commit boundary.
    pub async fn dispatch_causal(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
    ) -> Result<Value, CausalDispatchError> {
        self.dispatch_causal_with_receipt(command, command_id, input, session, principal)
            .await
            .map(|result| result.payload)
    }

    /// Execute one authenticated typed causal route and retain the exact
    /// durable replay material needed to construct a causal receipt.
    pub async fn dispatch_causal_with_receipt(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        self.dispatch_causal_with_receipt_inner(
            command, command_id, input, session, principal, None,
        )
        .await
    }

    /// Execute one authenticated command while deriving its modeled
    /// projection delta from the exact GraphQL request authority.
    #[cfg(feature = "graphql")]
    pub(crate) async fn dispatch_causal_with_receipt_and_protocol(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        protocol: crate::graphql::protocol::ProtocolResponseAccumulator,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        let seal = move |causation_id: crate::command_ledger::CausationId,
                         retention: Duration,
                         occurrences: &[crate::DomainEventOccurrence],
                         selectors: &[crate::ProjectionEventSelector]|
              -> Result<ModeledProjectionMetadata, String> {
            let metadata = protocol
                .projection_metadata_for_actual(causation_id, retention, occurrences, selectors)
                .map_err(|error| error.to_string())?;
            let expires_at = metadata
                .expires_at()
                .map_err(|error| format!("modeled projection metadata lifetime failed: {error}"))?;
            let canonical_bytes = metadata
                .canonical_bytes()
                .map_err(|error| format!("modeled projection metadata encoding failed: {error}"))?;
            Ok(ModeledProjectionMetadata {
                canonical_bytes,
                expires_at,
                pending: !metadata.obligations.is_empty(),
            })
        };
        self.dispatch_causal_with_receipt_inner(
            command,
            command_id,
            input,
            session,
            principal,
            Some(&seal),
        )
        .await
    }

    /// Every typed dispatch that is not a transport delivery: it records the
    /// dispatch metric and runs inside the dispatch span. A transport
    /// delivery is measured by [`dispatch_lane_message`](Self::dispatch_lane_message).
    async fn dispatch_causal_with_receipt_inner(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        seal_modeled_projection: Option<&SealModeledProjection<'_>>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        #[cfg(feature = "metrics")]
        let started = Instant::now();
        #[cfg(feature = "otel")]
        let span_message = Message {
            id: Some(command_id.to_string()),
            name: command.to_string(),
            kind: MessageKind::Command,
            payload: Vec::new(),
            content_type: "application/json".to_string(),
            metadata: session
                .variables()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        };
        let work = self.execute_causal(
            command,
            command_id,
            input,
            session,
            principal,
            seal_modeled_projection,
        );
        #[cfg(feature = "otel")]
        let result = self.in_dispatch_span(&span_message, work).await;
        #[cfg(not(feature = "otel"))]
        let result = work.await;
        #[cfg(feature = "metrics")]
        {
            // An unregistered name is a caller's string: one fixed label.
            let known = self.typed_command_routes(command).is_ok();
            crate::metrics::record_microsvc_dispatch(
                self.name(),
                MessageKind::Command,
                if known {
                    command
                } else {
                    crate::telemetry::metric_values::UNKNOWN_MESSAGE
                },
                match &result {
                    Ok(_) => crate::telemetry::dispatch_status::SUCCESS,
                    Err(_) if !known => crate::telemetry::dispatch_status::UNKNOWN_COMMAND,
                    Err(error) => crate::telemetry::causal_error_status(error),
                },
                started.elapsed(),
            );
        }
        result
    }

    async fn execute_causal(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        seal_modeled_projection: Option<&SealModeledProjection<'_>>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        self.ensure_mutations_open()
            .map_err(CausalDispatchError::Handler)?;
        let service_id = self.name().ok_or_else(|| {
            CausalDispatchError::Internal(
                "typed causal dispatch requires Service::named identity".into(),
            )
        })?;
        self.typed_command_routes(command)?
            .dispatch_causal(
                command,
                service_id,
                command_id,
                input,
                session,
                principal,
                self.causal_command_policy,
                seal_modeled_projection,
            )
            .await
    }

    /// Resolve one client-created command ID without accepting a command name.
    ///
    /// The verified principal determines the private ledger partition. The
    /// row is looked up once per repository and answered only through the
    /// route that wrote it, under its current role grant and contract
    /// fingerprint. Malformed, absent, wrong-principal, revoked, drifted, and
    /// ambiguous IDs all collapse to `unknown`.
    #[cfg(feature = "graphql")]
    pub(crate) async fn causal_command_status(
        &self,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> Result<CausalCommandPublicStatus, CausalDispatchError> {
        let Ok(parsed_command_id) = CommandId::parse(command_id) else {
            return Ok(CausalCommandPublicStatus::unknown(command_id));
        };
        let service_id = self.name().ok_or_else(|| {
            CausalDispatchError::Internal(
                "typed causal status requires Service::named identity".into(),
            )
        })?;
        let principal_partition =
            PrincipalPartitionId::new(principal.partition_for_service(service_id))
                .map_err(internal_ledger_error)?;

        let key = CommandLedgerKey::new(service_id, principal_partition, parsed_command_id.clone())
            .map_err(internal_ledger_error)?;

        // One ledger lookup per repository. The row names the command that
        // wrote it; only a bundle committing to that repository can own it.
        let mut repositories = Vec::new();
        let mut found = None;
        for routes in &self.routes {
            let Some(repository) = routes.causal_storage_identity() else {
                continue;
            };
            if repositories.contains(&repository) {
                continue;
            }
            repositories.push(repository);
            let route = |command: &str, fingerprint: &[u8; 32]| {
                let mut owners = self
                    .routes
                    .iter()
                    .filter(|routes| routes.causal_storage_identity() == Some(repository))
                    .filter_map(|routes| routes.causal_status_route(command, fingerprint, session));
                let owner = owners.next()?;
                owners.next().is_none().then_some(owner)
            };
            let status = routes
                .causal_command_status(&key, &parsed_command_id, &route, protocol.clone())
                .await?;
            if status.is_unknown() {
                continue;
            }
            if found.replace(status).is_some() {
                // Separate route bundles may use separate repositories. A
                // duplicated bearer-scoped command ID is intentionally not
                // enumerated or resolved by registration order.
                return Ok(CausalCommandPublicStatus::unknown(
                    parsed_command_id.as_str(),
                ));
            }
        }
        Ok(found.unwrap_or_else(|| CausalCommandPublicStatus::unknown(parsed_command_id.as_str())))
    }

    /// Private lookup seam used by replay recovery and the authorized status
    /// envelope. The route rechecks the current role grant before deriving the
    /// bearer-scoped ledger key.
    #[allow(dead_code)]
    pub(crate) async fn lookup_causal_command(
        &self,
        command: &str,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
    ) -> Result<CommandLookup, CausalDispatchError> {
        let service_id = self.name().ok_or_else(|| {
            CausalDispatchError::Internal(
                "typed causal lookup requires Service::named identity".into(),
            )
        })?;
        self.typed_command_routes(command)?
            .lookup_causal(command, service_id, command_id, session, principal)
            .await
    }

    /// Dispatch a transport message.
    ///
    /// An event runs its event handlers. A command runs its typed command
    /// through the ledger as the transport identity: the command must
    /// declare [`TRANSPORT_ROLE`] and the message must carry an id.
    pub async fn dispatch_message(&self, message: &Message) -> Result<Value, HandlerError> {
        self.dispatch_ordered_message(message, None).await
    }

    pub(crate) async fn dispatch_ordered_message(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<Value, HandlerError> {
        self.dispatch_lane_message(message, ordered, None).await
    }

    /// Dispatch only the routes of one delivery lane (`None` runs every lane
    /// in registration order).
    pub(crate) async fn dispatch_lane_message(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
        lane: Option<usize>,
    ) -> Result<Value, HandlerError> {
        // A typed command is measured and traced by the typed dispatch it
        // runs through.
        if message.kind == MessageKind::Command {
            return self.dispatch_transport_command(message, lane).await;
        }
        #[cfg(feature = "metrics")]
        let started = Instant::now();
        let result = self.dispatch_message_inner(message, ordered, lane).await;
        #[cfg(feature = "metrics")]
        {
            let error = result.as_ref().err();
            crate::metrics::record_microsvc_dispatch(
                self.name(),
                message.kind,
                crate::telemetry::handler_message_label(message.name(), error),
                error
                    .map(crate::telemetry::handler_error_status)
                    .unwrap_or(crate::telemetry::dispatch_status::SUCCESS),
                started.elapsed(),
            );
        }
        result
    }

    async fn dispatch_message_inner(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
        lane: Option<usize>,
    ) -> Result<Value, HandlerError> {
        self.ensure_mutations_open()?;
        if !self.handles_message(message.kind, &message.name) {
            return Err(HandlerError::UnknownCommand(message.name.clone()));
        }

        let route_indices = self.lane_route_indices(message, lane)?;
        if route_indices.is_empty() {
            return Err(HandlerError::UnknownCommand(message.name.clone()));
        }
        let projector_only = route_indices
            .iter()
            .all(|index| self.routes[*index].is_causal_projector(message));
        let input = if projector_only {
            // The causal projector owns raw parsing so unit/constant partition
            // declarations can durably record typed decode failures at the
            // authenticated source cursor.
            Value::Null
        } else {
            match message_to_json_input(message) {
                Ok(input) => input,
                // Binary payloads (bitcode, octet-stream) legitimately fail JSON
                // parsing: handlers for those read `ctx.message().payload` directly,
                // so a `Null` input is the intended fallback. A payload that
                // *claims* to be JSON but does not parse is a decode error — surface
                // it instead of silently nulling the input.
                Err(_) if !is_json_content_type(&message.content_type) => Value::Null,
                Err(err) => return Err(err),
            }
        };
        let session = message_to_session(message);
        self.in_dispatch_span(
            message,
            self.run_event_routes(message, input, session, ordered, route_indices),
        )
        .await
    }

    /// Run a typed command a transport delivered (a bus consumer, the
    /// Knative ingress) through [`CommandHost::invoke`], as the trusted
    /// transport principal.
    ///
    /// The ledger command ID derives from the message ID, so a redelivery
    /// replays the committed receipt and the handler does not run again.
    async fn dispatch_transport_command(
        &self,
        message: &Message,
        lane: Option<usize>,
    ) -> Result<Value, HandlerError> {
        self.ensure_mutations_open()?;
        let route_indices = self.lane_route_indices(message, lane)?;
        let Some(route) = route_indices.first().map(|index| &self.routes[*index]) else {
            return Err(HandlerError::UnknownCommand(message.name.clone()));
        };
        // An undeclared role list admits any session, so the opt-in is
        // checked here rather than left to the role grant.
        let declared = route
            .typed_command_contract(&message.name)
            .is_some_and(|contract| contract.roles.iter().any(|role| role == TRANSPORT_ROLE));
        if !declared {
            return Err(transport_command_error(CausalDispatchError::Forbidden));
        }
        let message_id = message.id().filter(|id| !id.is_empty()).ok_or_else(|| {
            HandlerError::DecodeFailed(format!(
                "command message `{}` has no id, so it cannot be deduplicated",
                message.name
            ))
        })?;
        let service_id = self.name().ok_or_else(|| {
            transport_command_error(CausalDispatchError::Internal(
                "typed causal dispatch requires Service::named identity".into(),
            ))
        })?;
        let input = message_to_json_input(message)?;
        CommandHost::invoke(
            self,
            &message.name,
            &transport_command_id(service_id, &message.name, message_id),
            input,
            transport_session(message, message_id),
            VerifiedPrincipal::trusted_transport(TRANSPORT_USER_ID),
            None,
        )
        .await
        .map(|result| result.payload)
        .map_err(transport_command_error)
    }

    fn lane_route_indices(
        &self,
        message: &Message,
        lane: Option<usize>,
    ) -> Result<Vec<usize>, HandlerError> {
        let all = self
            .index
            .get(&message.kind)
            .and_then(|by_name| by_name.get(message.name()))
            .ok_or_else(|| HandlerError::UnknownCommand(message.name.clone()))?;
        Ok(all
            .iter()
            .copied()
            .filter(|route| lane.is_none_or(|lane| self.route_lanes[*route] == lane))
            .collect())
    }

    /// Run `work` inside the dispatch span of `message`, parented on the
    /// trace context its metadata carries.
    async fn in_dispatch_span<T>(
        &self,
        message: &Message,
        work: impl std::future::Future<Output = T>,
    ) -> T {
        #[cfg(feature = "otel")]
        {
            use tracing::Instrument as _;

            let span = microsvc_dispatch_span(message);
            crate::trace_context::set_span_parent_from_metadata_if_no_current_span(
                &span,
                &message.metadata,
            );
            return work.instrument(span).await;
        }

        #[cfg(not(feature = "otel"))]
        {
            let _ = message;
            work.await
        }
    }

    async fn run_event_routes(
        &self,
        message: &Message,
        input: Value,
        session: Session,
        ordered: Option<&OrderedDelivery>,
        route_indices: Vec<usize>,
    ) -> Result<Value, HandlerError> {
        #[cfg(feature = "otel")]
        let handler_span = microsvc_handler_span(message);
        let dispatch = async move {
            let mut result = Value::Null;
            for route_index in route_indices {
                result = self.routes[route_index]
                    .dispatch(message, input.clone(), session.clone(), ordered)
                    .await?;
            }
            Ok(result)
        };

        #[cfg(feature = "otel")]
        {
            use tracing::Instrument as _;

            return dispatch.instrument(handler_span).await;
        }

        #[cfg(not(feature = "otel"))]
        {
            dispatch.await
        }
    }

    /// List registered command names.
    pub fn command_names(&self) -> Vec<&str> {
        names_by_kind(&self.handler_specs, MessageKind::Command)
    }

    /// List registered event names.
    pub fn event_names(&self) -> Vec<&str> {
        names_by_kind(&self.handler_specs, MessageKind::Event)
    }

    /// Return transport metadata for registered handlers.
    pub fn handler_specs(&self) -> &[HandlerSpec] {
        &self.handler_specs
    }

    /// Return the command/event names a transport should subscribe to.
    pub fn subscription_plan(&self) -> SubscriptionPlan {
        let mut plan = SubscriptionPlan::default();

        for spec in &self.handler_specs {
            for name in spec.names() {
                let bucket = match spec.kind {
                    MessageKind::Command => &mut plan.commands,
                    MessageKind::Event => &mut plan.events,
                };
                if !bucket.iter().any(|existing| existing == name) {
                    bucket.push(name.to_string());
                }
            }
        }

        plan
    }

    /// Return whether this service has a handler for the message name.
    pub fn handles(&self, name: &str) -> bool {
        self.index
            .values()
            .any(|by_name| by_name.contains_key(name))
    }

    /// Return whether this service has a handler for this message kind and name.
    pub fn handles_message(&self, kind: MessageKind, name: &str) -> bool {
        self.index
            .get(&kind)
            .is_some_and(|by_name| by_name.contains_key(name))
    }

    /// Return whether this service has an event handler for the message name.
    pub fn handles_event(&self, name: &str) -> bool {
        self.handles_message(MessageKind::Event, name)
    }

    /// Configure every route bundle that supports immediate outbox publishing.
    pub(crate) fn configure_outbox_publishers(
        &mut self,
        publisher: DynBusPublisher,
        worker_id: String,
        lease: Duration,
        max_attempts: u32,
    ) {
        let service_name = self.name.clone();
        for route in &mut self.routes {
            route.configure_outbox_publisher(
                publisher.clone(),
                worker_id.clone(),
                lease,
                max_attempts,
                service_name.clone(),
            );
        }
    }

    /// Compaction of every command ledger this service's typed commands
    /// write, one per repository however many route bundles share it.
    #[cfg(any(feature = "graphql", feature = "runtime-tokio"))]
    pub(crate) fn command_ledgers(&self) -> Vec<super::routes::LedgerCompaction> {
        let mut identities = Vec::new();
        let mut ledgers = Vec::new();
        for routes in &self.routes {
            let Some(identity) = routes.causal_storage_identity() else {
                continue;
            };
            if identities.contains(&identity) {
                continue;
            }
            identities.push(identity);
            ledgers.extend(routes.ledger_compaction());
        }
        ledgers
    }

    /// Run command-ledger compaction beside the outbox drain for as long as
    /// this service lives.
    #[cfg(feature = "runtime-tokio")]
    pub(crate) fn start_ledger_compaction(&mut self) {
        let ledgers = self.command_ledgers();
        self.ledger_compaction = (!ledgers.is_empty())
            .then(|| crate::microsvc::workers::LedgerCompactionWorker::spawn(ledgers));
    }

    pub(crate) async fn bootstrap_projectors(&self) -> Result<(), HandlerError> {
        let modeled_local_services = self
            .routes
            .iter()
            .flat_map(|routes| routes.modeled_local_services())
            .collect::<Vec<_>>();
        if !modeled_local_services.is_empty() {
            let service_name = self.name.as_deref().ok_or_else(|| {
                HandlerError::Projection(
                    crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                        "modeled local projection executors require Service::named identity".into(),
                    ),
                )
            })?;
            if modeled_local_services
                .iter()
                .any(|expected| *expected != service_name)
            {
                return Err(HandlerError::Projection(
                    crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                        "modeled local projection executor route differs from the running service identity"
                            .into(),
                    ),
                ));
            }
        }
        for routes in &self.routes {
            routes.bootstrap_projectors().await?;
        }
        Ok(())
    }

    /// Begin a new repair generation for the durable terminal failure named by
    /// an opaque operator handle.
    ///
    /// The handle carries no partition bytes. Each configured store resolves
    /// the globally unique failure ID to its exact durable scope; repair is
    /// allowed only when that exact compiled topology belongs to this service.
    /// Rebuild the service with the same repository, call this method, then
    /// restart consumption so the retained failed delivery is retried first.
    pub async fn repair_projection(
        &self,
        handle: &ProjectionRepairHandle,
    ) -> Result<crate::projection_protocol::ProjectionGeneration, HandlerError> {
        // Resolve every candidate before mutating any store. A corrupt
        // deployment that presents the same globally unique failure ID through
        // multiple stores must fail without advancing even the first one.
        let mut owner = None;
        for (index, routes) in self.routes.iter().enumerate() {
            if !routes.locates_projection_failure(handle).await? {
                continue;
            }
            if owner.replace(index).is_some() {
                return Err(HandlerError::Projection(
                    crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                        "projection failure ID resolved through multiple service route stores"
                            .into(),
                    ),
                ));
            }
        }
        let owner = owner.ok_or_else(|| {
            HandlerError::Projection(
                crate::projection_protocol::ProjectionProtocolError::InvalidBatch(format!(
                    "projection repair handle `{handle}` does not name a failure owned by this service"
                )),
            )
        })?;
        self.routes[owner]
            .repair_projection(handle)
            .await?
            .ok_or_else(|| {
                HandlerError::Projection(
                    crate::projection_protocol::ProjectionProtocolError::InvalidBatch(
                        "projection failure disappeared after repair ownership resolution".into(),
                    ),
                )
            })
    }
}

impl CommandMountRegistrar for Service {
    fn register_command_mount(&mut self, mount: CommandMount) -> Result<(), HandlerError> {
        self.register_command_mount_inner(mount)
    }
}

pub(super) fn validate_projector_registrations<'a>(
    registrations: impl IntoIterator<Item = &'a ProjectorRegistration>,
) {
    let mut topologies = BTreeMap::new();
    let mut models = BTreeMap::new();
    let mut tables = BTreeMap::new();
    for registration in registrations {
        let name = registration.topology.name().to_string();
        if let Some(existing) = topologies.insert(name.clone(), registration.topology.clone()) {
            assert_eq!(
                existing, registration.topology,
                "causal projector `{name}` is registered with conflicting compiled topologies"
            );
            panic!("causal projector `{name}` is registered more than once");
        }
        for owner in &registration.ownership {
            if let Some((existing_projector, existing_table)) =
                models.insert(owner.model.clone(), (name.clone(), owner.table.clone()))
            {
                panic!(
                    "projection model `{}` has multiple owners: `{existing_projector}`/`{existing_table}` and `{name}`/`{}`",
                    owner.model, owner.table
                );
            }
            if let Some((existing_projector, existing_model)) =
                tables.insert(owner.table.clone(), (name.clone(), owner.model.clone()))
            {
                panic!(
                    "physical projection table `{}` has multiple owners: `{existing_projector}`/`{existing_model}` and `{name}`/`{}`",
                    owner.table, owner.model
                );
            }
        }
    }
}

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}
