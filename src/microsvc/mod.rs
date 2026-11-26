//! microsvc — Convention-based microservice command handler framework.
//!
//! Build microservices by registering typed commands and event handlers on
//! `Routes<D>` bundles, then adding those bundles to a deployment-level
//! `Service`. A command handler receives a `CausalCommandContext` and its
//! decoded input; an event handler receives a `Context<D>` with access to the
//! payload, session variables, and its route dependencies.
//!
//! ## Quick Start
//!
//! Every command is typed, authorized by its declared roles, and recorded in
//! the command ledger under the caller's command ID. Dispatch is **async**.
//!
//! ```ignore
//! use std::sync::Arc;
//! use distributed::command::typed_command;
//! use distributed::command_dispatch::VerifiedPrincipal;
//! use distributed::{microsvc, InMemoryRepository};
//! use serde_json::json;
//!
//! let routes = microsvc::Routes::new()
//!     .with_repo(InMemoryRepository::new().queued().aggregate::<Order>())
//!     .typed_command(typed_command::<CreateOrder, Succeeded<OrderCreated>>("order.create"))
//!     .handle(order_create::handle);
//! let service = Arc::new(microsvc::Service::new().named("orders").routes(routes));
//!
//! // In-process dispatch (async): the caller supplies a UUIDv7 command ID
//! // and the principal it already verified.
//! let result = service
//!     .dispatch_causal(
//!         "order.create",
//!         "0190a000-0000-7000-8000-000000000001",
//!         json!({ "id": "o1" }),
//!         microsvc::Session::new(),
//!         VerifiedPrincipal::internal("orders-job")?,
//!     )
//!     .await?;
//!
//! // HTTP transport (requires "http" feature): POST /order.create with
//! // { "commandId": "...", "input": { "id": "o1" } }
//! // microsvc::serve(service, "0.0.0.0:3000").await?;
//! ```
//!
//! ## Handler Convention
//!
//! Each command handler file follows this convention. `handle` is **async**:
//!
//! ```ignore
//! // src/handlers/order_create.rs
//!
//! pub const COMMAND: &str = "order.create";
//!
//! pub fn command() -> TypedCommand<CreateOrder, Succeeded<OrderCreated>> {
//!     typed_command::<CreateOrder, Succeeded<OrderCreated>>(COMMAND).roles(["user"])
//! }
//!
//! pub async fn handle(
//!     ctx: &microsvc::CausalCommandContext<'_, Order>,
//!     input: CreateOrder,
//! ) -> Result<PreparedCommand<Succeeded<OrderCreated>>, microsvc::HandlerError> {
//!     let mut order = ctx.repo().create();
//!     order.create(input.id.clone())?;
//!     ctx.repo().commit(order)?.succeeded(OrderCreated { id: input.id })
//! }
//! ```

mod causal;
pub mod cell_host;
mod context;
mod dependencies;
pub(crate) use dependencies::CausalHostProjections;
mod descriptor;
mod error;
pub mod guards;
pub(crate) mod lifecycle;
mod message_router;
mod projector;
mod runtime;
pub mod schedule;
mod service;
mod session;
mod stores;
// Worker loops need a Tokio runtime (spawn/sleep); only compile when a feature
// that enables the optional `tokio` dep is active (default feature set does not).
#[cfg(feature = "runtime-tokio")]
mod workers;

pub use causal::AggregateCheckout;
pub use context::Context;
pub use dependencies::{
    CausalProjectionRouteDependencies, CausalProjectionStore, CausalRepositoryBackend,
    CausalRouteDependencies, ConfigurableOutboxPublisher, HasOutboxStore, HasReadModelStore,
    HasRepo, ReadModelStoreDependencies, RepoDependencies, RepoReadModelDependencies,
};
pub use descriptor::{
    MessageEndpointDescriptor, MetricsEndpointDescriptor, ServiceDescriptor,
    ServiceObservabilityDescriptor, TraceExportMode, TracePropagationMode, TracingDescriptor,
    TransportDescriptor,
};
pub use error::HandlerError;
pub use lifecycle::LifecycleGate;
pub use message_router::ProjectorConsumer;
pub use projector::{
    CausalProjectorContext, CausalProjectorRouteBuilder, LoadedProjection, ProjectionRepairHandle,
    ProjectionRepairHandleParseError,
};
#[cfg(feature = "graphql")]
pub use projector::{ModeledProjection, ModeledProjectorRouteBuilder};
pub use runtime::{DEFAULT_MAX_PUBLISH_ATTEMPTS, DEFAULT_PUBLISH_LEASE};
#[cfg(all(feature = "graphql", test))]
pub(crate) use service::CausalCommandProjectionEvidence;
#[cfg(feature = "graphql")]
pub use service::CausalCommandPublicStatus;
#[cfg(feature = "http")]
pub(crate) use service::ExternalCausalReservation;
#[cfg(feature = "graphql")]
pub use service::GraphqlServiceBindError;
pub use service::{
    invoke_transition, load_for_command, require_loaded, CausalCommandContext, CausalCommitBuilder,
    CausalRepository, CommandLoad, DeliveryKind, EventRouteBuilder, HandlerNames, HandlerSpec,
    PortableCommand, PreparedCausalCommit, PreparedCommandHandler, Routes, Service,
    ThinCommandBuilder, ThinCommandInvoked, ThinCommandLoaded, TypedRouteBuilder,
    DEFAULT_DELIVERY_LANE,
};
#[cfg(feature = "graphql")]
pub(crate) use service::{
    CausalCommandPublicState, CausalCommandReceiptSource, CausalProjectionEvidenceState,
};
pub use service::{CausalDispatchError, CausalDispatchResult};
#[cfg(feature = "runtime-tokio")]
pub use workers::{
    spawn_outbox_publish_loop, spawn_service_consumer_loop, spawn_service_consumer_loop_until,
};
#[cfg(any(feature = "http", feature = "grpc"))]
pub(crate) mod wait_path;
pub use session::{
    Session, ROLE_KEY, TRANSPORT_MESSAGE_ID_CLAIM, TRANSPORT_ROLE, TRANSPORT_USER_ID, USER_ID_KEY,
};
#[cfg(feature = "postgres")]
pub use stores::PostgresStores;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use stores::SqlStores;
#[cfg(feature = "sqlite")]
pub use stores::SqliteStores;
pub use stores::{AggregateDependencies, ServiceStores, StoreDependencies, Stores};

/// Maximum accepted HTTP request body size for the microsvc ingresses, in bytes
/// (1 MiB).
///
/// Pins axum's implicit 2 MiB default to an explicit, smaller ceiling shared by
/// the command [`router`] and the CloudEvents [`cloud_events_router`]: both
/// buffer the whole body into memory, so an unbounded body is a
/// memory-amplification vector. Raise it deliberately if a deployment needs
/// larger payloads.
#[cfg(feature = "http")]
pub const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;

// HTTP transport (requires "http" feature)
#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
// session_from_headers remains available for microsvc HTTP command path.
#[allow(unused_imports)]
pub(crate) use http::session_from_headers;
#[cfg(feature = "http")]
pub use http::{command_router, router, serve, HttpCommandDispatch};

// Knative / CloudEvents HTTP ingress (Service-coupled; the bus keeps only the
// produce/manifest helpers).
#[cfg(feature = "knative")]
mod knative_ingress;
#[cfg(feature = "knative")]
pub use knative_ingress::{cloud_events_router, decode_cloud_event};

// gRPC transport (requires "grpc" feature)
#[cfg(feature = "grpc")]
pub mod grpc;

/// Register event handler modules with a route bundle using the convention
/// pattern.
///
/// Each handler entry must be prefixed with `event` or `events`. Commands
/// are registered on the route bundle with [`Routes::typed_command`].
///
/// Event handler modules must export:
/// - `EVENT: &str` or `EVENTS: &[&str]` — event names
/// - `guard(ctx) -> bool` — input validation
/// - `handle(ctx) -> Result<Value, HandlerError>` — the handler
///
/// # Example
/// ```ignore
/// let routes = distributed::routes!(
///     microsvc::Routes::new()
///         .with_repo(repo)
///         .typed_command(handlers::counter_create::command())
///         .handle(handlers::counter_create::handle),
///     event handlers::counter_rebuilt,
///     events handlers::counter_projection,
/// );
/// let service = microsvc::Service::new().named("counters").routes(routes);
/// ```
#[macro_export]
macro_rules! routes {
    ($routes:expr $(,)?) => {
        $routes
    };
    ($routes:expr, $($rest:tt)+) => {
        $crate::__routes!($routes, $($rest)+)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __routes {
    ($routes:expr, event $($seg:ident)::+ $(, $($rest:tt)*)?) => {
        $crate::__routes_continue!(
            $routes.event($($seg)::+::EVENT).guarded(
                $($seg)::+::guard,
                $($seg)::+::handle,
            )
            $(, $($rest)*)?
        )
    };
    ($routes:expr, events $($seg:ident)::+ $(, $($rest:tt)*)?) => {
        $crate::__routes_continue!(
            $routes.events($($seg)::+::EVENTS).guarded(
                $($seg)::+::guard,
                $($seg)::+::handle,
            )
            $(, $($rest)*)?
        )
    };
    ($routes:expr, $($seg:ident)::+ $(, $($rest:tt)*)?) => {
        compile_error!(
            "routes! entries must be prefixed with `event` or `events`"
        )
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __routes_continue {
    ($routes:expr) => {
        $routes
    };
    ($routes:expr,) => {
        $routes
    };
    ($routes:expr, $($rest:tt)+) => {
        $crate::__routes!($routes, $($rest)+)
    };
}
