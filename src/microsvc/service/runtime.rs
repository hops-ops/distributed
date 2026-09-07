use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "metrics")]
use std::time::Instant;

use serde_json::Value;

#[cfg(feature = "graphql")]
use super::causal::{
    internal_ledger_error, CausalCommandPublicStatus, CausalDispatchError, CausalDispatchResult,
    GraphqlServiceBindError,
};
use super::helpers::{
    is_json_content_type, message_to_json_input, message_to_session, names_by_kind,
};
#[cfg(feature = "otel")]
use super::helpers::{microsvc_dispatch_span, microsvc_handler_span};
use super::request::{CommandRequest, CommandResponse};
use super::routes::{CausalCommandPolicy, DynBusPublisher, ErasedRoutes, HandlerSpec, Routes};
use crate::application::{
    Application, ApplicationError, ApplicationResult, CommandDefinition, CommandMount,
    CommandMountRegistrar, CommandSpec, Module, SurfaceSpec,
};
use crate::bus::{
    Message, MessageKind, OrderedDelivery, RunOptions, SubscriptionPlan, TransportError,
};
use crate::command::{TypedCommandContract, TypedServiceCommandBinding};
#[cfg(feature = "graphql")]
use crate::command_ledger::{CommandId, CommandLookup, PrincipalPartitionId};
#[cfg(feature = "graphql")]
use crate::graphql::identity::VerifiedPrincipal;
use crate::microsvc::error::HandlerError;
use crate::microsvc::projector::{ProjectionRepairHandle, ProjectorRegistration};
use crate::microsvc::session::Session;

fn ensure_lifecycle_mutations_open() -> Result<(), HandlerError> {
    if crate::microsvc::lifecycle_mutations_open() {
        Ok(())
    } else {
        Err(HandlerError::Rejected(
            "application generation is reloading".into(),
        ))
    }
}

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

/// A microservice deployment that routes messages to one or more route bundles.
pub struct Service {
    name: Option<String>,
    pub(super) routes: Vec<Box<dyn ErasedRoutes>>,
    index: HashMap<MessageKind, HashMap<String, Vec<usize>>>,
    handler_specs: Vec<HandlerSpec>,
    causal_command_policy: CausalCommandPolicy,
    runner: Option<ServiceRunner>,
    registered_command_mounts: Vec<CommandMount>,
    /// When true, HTTP mounts `POST /{command}`. Off by default — GraphQL,
    /// health, and in-process `dispatch` stay available without this.
    http_command_routes: bool,
    #[cfg(feature = "graphql")]
    graphql: Option<std::sync::Arc<crate::graphql::GraphqlEngine>>,
}

impl Service {
    /// Start building a deployment-level service.
    pub fn new() -> Self {
        Self {
            name: None,
            routes: Vec::new(),
            index: HashMap::new(),
            handler_specs: Vec::new(),
            causal_command_policy: CausalCommandPolicy::default(),
            runner: None,
            registered_command_mounts: Vec::new(),
            http_command_routes: false,
            #[cfg(feature = "graphql")]
            graphql: None,
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
                .map_err(GraphqlServiceBindError)?
                .into_iter()
                .map(|contract| (contract.name.clone(), contract))
                .collect::<BTreeMap<_, _>>();
            for routes in &mut self.routes {
                routes
                    .bind_typed_command_contracts(&contracts)
                    .map_err(GraphqlServiceBindError)?;
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
        let service_id = self.name().ok_or_else(|| {
            GraphqlServiceBindError(
                "GraphQL attachment requires a stable Service::named identity".into(),
            )
        })?;
        let engine_service_id = engine.service_id().ok_or_else(|| {
            GraphqlServiceBindError(
                "GraphQL attachment requires an engine with a validated service ID".into(),
            )
        })?;
        if service_id != engine_service_id {
            return Err(GraphqlServiceBindError(format!(
                "service ID mismatch: executable service `{service_id}` vs GraphQL engine `{engine_service_id}`"
            )));
        }
        if self.handles_message(crate::bus::MessageKind::Command, "graphql") {
            return Err(GraphqlServiceBindError(
                "a command named `graphql` is already registered".into(),
            ));
        }

        let typed_commands = self.typed_command_contracts();
        match (typed_commands.is_empty(), engine.typed_command_binding()) {
            (true, None) => {}
            (_, Some(engine_binding)) => {
                let service_binding = self
                    .typed_command_binding()
                    .map_err(GraphqlServiceBindError)?;
                if service_binding.service_id != engine_binding.service_id {
                    return Err(GraphqlServiceBindError(format!(
                        "service ID mismatch: executable service `{}` vs GraphQL engine `{}`",
                        service_binding.service_id, engine_binding.service_id
                    )));
                }
                if service_binding.structural_fingerprint != engine_binding.structural_fingerprint {
                    return Err(GraphqlServiceBindError(format!(
                        "typed command structural fingerprint mismatch: executable `{}` vs GraphQL `{}`",
                        service_binding.structural_fingerprint,
                        engine_binding.structural_fingerprint
                    )));
                }
                if service_binding.types != engine_binding.types {
                    return Err(GraphqlServiceBindError(
                        "typed command Rust input/output TypeId mismatch".into(),
                    ));
                }
            }
            (false, None) => {
                return Err(GraphqlServiceBindError(
                    "GraphQL engine was not derived from this service's typed command inventory"
                        .into(),
                ));
            }
        }

        if !typed_commands.is_empty() && !engine.causal_protocol_configured() {
            return Err(GraphqlServiceBindError(
                "typed causal commands require a configured GraphQL protocol token key".into(),
            ));
        }

        let projected_identities = self
            .routes
            .iter()
            .flat_map(|routes| routes.projected_storage_identities())
            .collect::<Vec<_>>();
        if !projected_identities.is_empty() {
            let engine_identity = engine.causal_storage_identity().ok_or_else(|| {
                GraphqlServiceBindError(
                    "Atomic commands require a GraphQL pool derived from the same repository handle"
                        .into(),
                )
            })?;
            if projected_identities
                .iter()
                .any(|identity| *identity != engine_identity)
            {
                return Err(GraphqlServiceBindError(
                    "Atomic command repository and GraphQL query pool storage identities differ"
                        .into(),
                ));
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

    /// Invoke a registered mount through the service adapter. The request
    /// still enters the normal transport boundary, so typed causal routes
    /// retain their authentication/receipt/projection-proof requirements.
    pub async fn dispatch_mount(
        &self,
        mount: &CommandMount,
        request: &CommandRequest,
    ) -> CommandResponse {
        match self.dispatch_mount_result(mount, request).await {
            Ok(response) => response,
            Err(error) => CommandResponse {
                status: error.status_code(),
                body: serde_json::json!({ "error": error.to_string() }),
            },
        }
    }

    pub(crate) async fn dispatch_mount_result(
        &self,
        mount: &CommandMount,
        request: &CommandRequest,
    ) -> Result<CommandResponse, HandlerError> {
        ensure_lifecycle_mutations_open()?;
        if request.command != mount.spec().id {
            return Err(HandlerError::Rejected(format!(
                "command request `{}` does not match mount `{}`",
                request.command,
                mount.spec().id
            )));
        }
        if !self.registered_command_mounts.iter().any(|registered| {
            registered.spec().id == mount.spec().id
                && registered.spec().fingerprint == mount.spec().fingerprint
        }) {
            return Err(HandlerError::Rejected(
                "command mount was not registered against this service".into(),
            ));
        }
        if mount.typed_route_name().is_some() {
            return Err(HandlerError::Unauthorized(
                "typed command mounts require the authenticated causal dispatch adapter".into(),
            ));
        }
        mount.invoke(request).await
    }

    #[cfg(feature = "graphql")]
    #[allow(dead_code)]
    pub(crate) async fn dispatch_registered_mount_causally(
        &self,
        mount: &CommandMount,
        request: &CommandRequest,
        command_id: &str,
        session: Session,
        principal: VerifiedPrincipal,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        self.ensure_registered_mount(mount)
            .map_err(CausalDispatchError::Handler)?;
        if mount.typed_route_name() != Some(request.command.as_str()) {
            return Err(CausalDispatchError::BadRequest(
                "typed command mount route identity does not match the request".into(),
            ));
        }
        self.dispatch_causal_with_receipt(
            &request.command,
            command_id,
            request.input.clone(),
            session,
            principal,
        )
        .await
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

    #[cfg(feature = "graphql")]
    #[allow(dead_code)]
    fn ensure_registered_mount(&self, mount: &CommandMount) -> Result<(), HandlerError> {
        if self.registered_command_mounts.iter().any(|registered| {
            registered.spec().id == mount.spec().id
                && registered.spec().fingerprint == mount.spec().fingerprint
        }) {
            Ok(())
        } else {
            Err(HandlerError::Rejected(
                "command mount was not registered against this service".into(),
            ))
        }
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

    pub(crate) fn typed_command_binding(&self) -> Result<TypedServiceCommandBinding, String> {
        let service_id = self
            .name()
            .ok_or_else(|| "typed command inventory requires Service::named".to_string())?;
        TypedServiceCommandBinding::from_contracts(service_id, &self.typed_command_contracts())
    }

    /// Execute one authenticated typed causal route through its durable ledger
    /// and framework-owned staged commit boundary.
    #[cfg(feature = "graphql")]
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
    #[cfg(feature = "graphql")]
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
        self.dispatch_causal_with_receipt_inner(
            command,
            command_id,
            input,
            session,
            principal,
            Some(protocol),
        )
        .await
    }

    #[cfg(feature = "graphql")]
    async fn dispatch_causal_with_receipt_inner(
        &self,
        command: &str,
        command_id: &str,
        input: Value,
        session: Session,
        principal: VerifiedPrincipal,
        protocol: Option<crate::graphql::protocol::ProtocolResponseAccumulator>,
    ) -> Result<CausalDispatchResult, CausalDispatchError> {
        ensure_lifecycle_mutations_open().map_err(CausalDispatchError::Handler)?;
        let service_id = self.name().ok_or_else(|| {
            CausalDispatchError::Internal(
                "typed causal dispatch requires Service::named identity".into(),
            )
        })?;
        let route_index = self
            .index
            .get(&MessageKind::Command)
            .and_then(|commands| commands.get(command))
            .and_then(|indices| (indices.len() == 1).then_some(indices[0]))
            .ok_or_else(|| CausalDispatchError::BadRequest("unknown typed command".into()))?;
        self.routes[route_index]
            .dispatch_causal(
                command,
                service_id,
                command_id,
                input,
                session,
                principal,
                self.causal_command_policy,
                protocol,
            )
            .await
    }

    /// Resolve one client-created command ID without accepting a command name.
    ///
    /// The verified principal determines the private ledger partition and each
    /// finite causal handler rechecks its current role grant and contract
    /// fingerprint. Malformed, absent, wrong-principal, revoked, drifted, and
    /// ambiguous IDs all collapse to `unknown`.
    #[cfg(feature = "graphql")]
    pub(crate) async fn causal_command_status(
        &self,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
    ) -> Result<CausalCommandPublicStatus, CausalDispatchError> {
        self.causal_command_status_internal(command_id, session, principal, None)
            .await
    }

    #[cfg(feature = "graphql")]
    pub(crate) async fn causal_command_status_with_protocol(
        &self,
        command_id: &str,
        session: &Session,
        principal: VerifiedPrincipal,
        protocol: crate::graphql::protocol::ProtocolResponseAccumulator,
    ) -> Result<CausalCommandPublicStatus, CausalDispatchError> {
        self.causal_command_status_internal(command_id, session, principal, Some(protocol))
            .await
    }

    #[cfg(feature = "graphql")]
    async fn causal_command_status_internal(
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

        let mut found = None;
        for routes in &self.routes {
            let status = routes
                .causal_command_status(
                    service_id,
                    &parsed_command_id,
                    &principal_partition,
                    session,
                    protocol.clone(),
                )
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
    #[cfg(feature = "graphql")]
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
        let route_index = self
            .index
            .get(&MessageKind::Command)
            .and_then(|commands| commands.get(command))
            .and_then(|indices| (indices.len() == 1).then_some(indices[0]))
            .ok_or_else(|| CausalDispatchError::BadRequest("unknown typed command".into()))?;
        self.routes[route_index]
            .lookup_causal(command, service_id, command_id, session, principal)
            .await
    }

    /// Dispatch a command by name.
    ///
    /// Builds a `Context` from the input and session, looks up the handler,
    /// runs the guard (if any), then calls the handler.
    pub async fn dispatch(
        &self,
        command: &str,
        input: Value,
        session: Session,
    ) -> Result<Value, HandlerError> {
        #[cfg(feature = "metrics")]
        let started = Instant::now();
        let result = self.dispatch_command_inner(command, input, session).await;
        #[cfg(feature = "metrics")]
        {
            let error = result.as_ref().err();
            crate::metrics::record_microsvc_dispatch(
                self.name(),
                MessageKind::Command,
                crate::telemetry::handler_message_label(command, error),
                error
                    .map(crate::telemetry::handler_error_status)
                    .unwrap_or(crate::telemetry::dispatch_status::SUCCESS),
                started.elapsed(),
            );
        }
        result
    }

    async fn dispatch_command_inner(
        &self,
        command: &str,
        input: Value,
        session: Session,
    ) -> Result<Value, HandlerError> {
        ensure_lifecycle_mutations_open()?;
        if !self.handles_message(MessageKind::Command, command) {
            return Err(HandlerError::UnknownCommand(command.to_string()));
        }

        let payload = serde_json::to_vec(&input).map_err(|e| {
            HandlerError::DecodeFailed(format!("invalid JSON input for command '{command}': {e}"))
        })?;
        let metadata = session
            .variables()
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let message = Message {
            id: None,
            name: command.to_string(),
            kind: MessageKind::Command,
            payload,
            content_type: "application/json".to_string(),
            metadata,
        };

        self.invoke_with_dispatch_span(&message, input, session, None)
            .await
    }

    /// Dispatch a `CommandRequest`, returning a `CommandResponse`.
    pub async fn dispatch_request(&self, request: &CommandRequest) -> CommandResponse {
        let session = Session::from_map(request.session_variables.clone());
        match self
            .dispatch(&request.command, request.input.clone(), session)
            .await
        {
            Ok(value) => CommandResponse {
                status: 200,
                body: value,
            },
            Err(e) => CommandResponse {
                status: e.status_code(),
                body: serde_json::json!({ "error": e.to_string() }),
            },
        }
    }

    /// Dispatch a transport message.
    pub async fn dispatch_message(&self, message: &Message) -> Result<Value, HandlerError> {
        self.dispatch_ordered_message(message, None).await
    }

    pub(crate) async fn dispatch_ordered_message(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<Value, HandlerError> {
        #[cfg(feature = "metrics")]
        let started = Instant::now();
        let result = self.dispatch_message_inner(message, ordered).await;
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
    ) -> Result<Value, HandlerError> {
        if !crate::microsvc::lifecycle_mutations_open() {
            return Err(HandlerError::Rejected(
                "application generation is reloading".into(),
            ));
        }
        if !self.handles_message(message.kind, &message.name) {
            return Err(HandlerError::UnknownCommand(message.name.clone()));
        }

        let route_indices = self
            .index
            .get(&message.kind)
            .and_then(|by_name| by_name.get(message.name()))
            .ok_or_else(|| HandlerError::UnknownCommand(message.name.clone()))?;
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
        self.invoke_with_dispatch_span(message, input, session, ordered)
            .await
    }

    async fn invoke_with_dispatch_span(
        &self,
        message: &Message,
        input: Value,
        session: Session,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<Value, HandlerError> {
        #[cfg(feature = "otel")]
        {
            use tracing::Instrument as _;

            let span = microsvc_dispatch_span(message);
            crate::trace_context::set_span_parent_from_metadata_if_no_current_span(
                &span,
                &message.metadata,
            );
            return self
                .invoke(message, input, session, ordered)
                .instrument(span)
                .await;
        }

        #[cfg(not(feature = "otel"))]
        {
            self.invoke(message, input, session, ordered).await
        }
    }

    async fn invoke(
        &self,
        message: &Message,
        input: Value,
        session: Session,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<Value, HandlerError> {
        let route_indices = self
            .index
            .get(&message.kind)
            .and_then(|by_name| by_name.get(message.name.as_str()))
            .cloned()
            .ok_or_else(|| HandlerError::UnknownCommand(message.name.clone()))?;
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

impl crate::application::CommandMountExecution for Service {
    fn invoke_mount<'a>(
        &'a self,
        mount: &'a CommandMount,
        request: CommandRequest,
        invocation: crate::application::CommandMountInvocation,
    ) -> crate::application::CommandMountExecutionFuture<'a> {
        Box::pin(async move {
            match invocation {
                crate::application::CommandMountInvocation::Transport => self
                    .dispatch_mount_result(mount, &request)
                    .await
                    .map(crate::application::CommandMountExecutionResult::Transport)
                    .map_err(crate::application::CommandMountExecutionError::Handler),
                #[cfg(feature = "graphql")]
                crate::application::CommandMountInvocation::Authenticated {
                    command_id,
                    session,
                    principal,
                } => self
                    .dispatch_registered_mount_causally(
                        mount,
                        &request,
                        &command_id,
                        session,
                        principal,
                    )
                    .await
                    .map(crate::application::CommandMountExecutionResult::Causal)
                    .map_err(crate::application::CommandMountExecutionError::Causal),
            }
        })
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
