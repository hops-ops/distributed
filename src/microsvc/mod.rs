//! microsvc — Convention-based microservice command handler framework.
//!
//! Build microservices by registering command and event handlers on typed
//! `Routes<D>` bundles, then adding those bundles to a deployment-level
//! `Service`. Each handler receives a `Context<D>` with access to the input
//! payload, session variables, and its route dependencies.
//!
//! ## Quick Start
//!
//! Dispatch is **async** — `dispatch`, `handle`, and the commit path all return
//! futures and are awaited.
//!
//! ```ignore
//! use std::sync::Arc;
//! use distributed::{microsvc, InMemoryRepository};
//! use serde_json::json;
//!
//! let routes = microsvc::Routes::new()
//!     .with_repo(InMemoryRepository::new().queued().aggregate::<Order>())
//!     .command("order.create")
//!     .handle(|ctx| {
//!         let input = ctx.input::<CreateOrderInput>();
//!         async move { Ok(json!({ "id": input?.id })) }
//!     });
//! let service = Arc::new(microsvc::Service::new().routes(routes));
//!
//! // Direct dispatch (async)
//! let result = service
//!     .dispatch("order.create", json!({ "id": "o1" }), microsvc::Session::new())
//!     .await?;
//!
//! // HTTP transport (requires "http" feature)
//! // microsvc::serve(service, "0.0.0.0:3000").await?;
//! ```
//!
//! ## Handler Convention
//!
//! Each handler file follows this convention. `handle` is **async**:
//!
//! ```ignore
//! // src/handlers/order_create.rs
//!
//! pub const COMMAND: &str = "order.create";
//!
//! pub fn guard(ctx: &microsvc::Context<Repo>) -> bool {
//!     ctx.has_fields(&["id", "product_id"])
//! }
//!
//! pub async fn handle(ctx: &microsvc::Context<'_, Repo>) -> Result<Value, microsvc::HandlerError> {
//!     let input = ctx.input::<CreateOrderInput>()?;
//!     let mut order = Order::default();
//!     order.create(input.id)?;
//!     ctx.repo().commit(&mut order).await?;
//!     Ok(json!({ "id": order.entity().id() }))
//! }
//! ```

mod causal;
pub mod cell_host;
mod context;
mod descriptor;
mod dependencies;
mod error;
mod message_router;
mod projector;
mod runtime;
mod service;
mod session;
// Worker loops need a Tokio runtime (spawn/sleep); only compile when a feature
// that enables the optional `tokio` dep is active (default feature set does not).
#[cfg(any(
    feature = "http",
    feature = "grpc",
    feature = "postgres",
    feature = "sqlite",
    feature = "nats",
    feature = "rabbitmq",
    feature = "kafka",
))]
mod workers;

pub use crate::bus::{Message, MessageKind, PayloadDecodeError, SubscriptionPlan};
pub use causal::AggregateCheckout;
pub use context::Context;
pub use dependencies::{
    CausalProjectionRouteDependencies, CausalProjectionStore, CausalRepositoryBackend,
    CausalRouteDependencies, ConfigurableOutboxPublisher, HasOutboxStore, HasReadModelStore,
    HasRepo, ReadModelStoreDependencies, RepoDependencies, RepoReadModelDependencies,
};
pub use error::HandlerError;
pub use descriptor::{
    MessageEndpointDescriptor, MetricsEndpointDescriptor, ServiceDescriptor,
    ServiceObservabilityDescriptor, TraceExportMode, TracePropagationMode, TracingDescriptor,
    TransportDescriptor,
};
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
pub use service::GraphqlServiceBindError;
#[cfg(any(
    feature = "http",
    feature = "grpc",
    feature = "postgres",
    feature = "sqlite",
    feature = "nats",
    feature = "rabbitmq",
    feature = "kafka",
))]
pub use workers::{spawn_outbox_publish_loop, spawn_service_consumer_loop};
pub use service::{
    direct_read_model, invoke_transition, require_loaded, CausalCommandContext, CausalCommitBuilder,
    CausalRepository, CommandRequest, CommandResponse, DeliveryKind, DirectReadModelProjection,
    HandlerNames, HandlerSpec, PortableCommand, PreparedCausalCommit, PreparedCommandHandler,
    RouteBuilder, Routes, Service, ThinCommandBuilder, ThinCommandInvoked, ThinCommandLoaded,
    TypedRouteBuilder,
};
#[cfg(feature = "graphql")]
pub use service::{CausalCommandPublicStatus, CausalDispatchError, CausalDispatchResult};
#[cfg(feature = "graphql")]
pub(crate) use service::{
    CausalCommandProjectionObligation, CausalCommandPublicState, CausalCommandReceiptSource,
    CausalProjectionEvidenceState,
};
#[cfg(feature = "graphql")]
pub(crate) mod wait_path;
pub use session::{Session, ROLE_KEY, USER_ID_KEY};

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

/// Fail closed for writes while a `distributed dev` process is not the active
/// application generation. Outside the dev supervisor there is no lifecycle
/// directory, so ordinary production/runtime embedding remains unchanged.
pub(crate) fn lifecycle_mutations_open() -> bool {
    let Some(root) = std::env::var_os("DISTRIBUTED_LIFECYCLE_DIR") else {
        return true;
    };
    let Some(generation) = std::env::var_os("DISTRIBUTED_GENERATION_ID") else {
        return false;
    };
    let root = std::path::PathBuf::from(root);
    let Some(generation) = generation.to_str() else {
        return false;
    };
    if !root.is_absolute() {
        return false;
    }
    let state = root.join("dev.json");
    let Ok(metadata) = std::fs::symlink_metadata(&state) else {
        return false;
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > 1024 * 1024
    {
        return false;
    }
    std::fs::read(&state)
        .ok()
        .is_some_and(|source| lifecycle_state_allows_mutations(&source, generation))
}

fn lifecycle_state_allows_mutations(source: &[u8], generation: &str) -> bool {
    let Ok(state) = serde_json::from_slice::<serde_json::Value>(source) else {
        return false;
    };
    state.get("phase").and_then(serde_json::Value::as_str) == Some("active")
        && state
            .get("active")
            .and_then(|active| active.get("generationId"))
            .and_then(serde_json::Value::as_str)
            == Some(generation)
}

#[cfg(test)]
mod lifecycle_gate_tests {
    use super::lifecycle_state_allows_mutations;

    #[test]
    fn mutations_open_only_for_the_exact_active_generation() {
        let active = br#"{"phase":"active","active":{"generationId":"sha256:one"}}"#;
        let preparing =
            br#"{"phase":"preparing","active":{"generationId":"sha256:one"}}"#;
        assert!(lifecycle_state_allows_mutations(active, "sha256:one"));
        assert!(!lifecycle_state_allows_mutations(active, "sha256:two"));
        assert!(!lifecycle_state_allows_mutations(preparing, "sha256:one"));
        assert!(!lifecycle_state_allows_mutations(b"not-json", "sha256:one"));
    }
}

// HTTP transport (requires "http" feature)
#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
// session_from_headers remains available for microsvc HTTP command path.
#[allow(unused_imports)]
pub(crate) use http::session_from_headers;
#[cfg(feature = "http")]
pub use http::{router, serve};

// Knative / CloudEvents HTTP ingress (Service-coupled; the bus keeps only the
// produce/manifest helpers). Requires the "http" feature.
#[cfg(feature = "http")]
mod knative_ingress;
#[cfg(feature = "http")]
pub use knative_ingress::cloud_events_router;

// gRPC transport (requires "grpc" feature)
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "grpc")]
pub use grpc::{grpc_server, serve_grpc, GrpcServeError};

/// Register handler modules with a route bundle using the convention pattern.
///
/// Each handler entry must be prefixed with `command`, `event`, or `events`.
///
/// Command handler modules must export:
/// - `COMMAND: &str` — the command name
/// - `guard(ctx) -> bool` — input validation
/// - `handle(ctx) -> Result<Value, HandlerError>` — the handler
///
/// Event handler modules must export:
/// - `EVENT: &str` or `EVENTS: &[&str]` — event names
/// - `guard(ctx) -> bool` — input validation
/// - `handle(ctx) -> Result<Value, HandlerError>` — the handler
///
/// # Example
/// ```ignore
/// let routes = distributed::routes!(
///     microsvc::Routes::new().with_repo(repo),
///     command handlers::counter_create,
///     command handlers::counter_increment,
///     event handlers::counter_rebuilt,
///     events handlers::counter_projection,
/// );
/// let service = microsvc::Service::new().routes(routes);
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
    ($routes:expr, command $($seg:ident)::+ $(, $($rest:tt)*)?) => {
        $crate::__routes_continue!(
            $routes.command($($seg)::+::COMMAND).guarded(
                $($seg)::+::guard,
                $($seg)::+::handle,
            )
            $(, $($rest)*)?
        )
    };
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
            "routes! entries must be prefixed with `command`, `event`, or `events`"
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
