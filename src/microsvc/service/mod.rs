//! Routes and service dispatch for microsvc.
//!
//! `Routes<D>` holds one dependency value and its command/event handlers.
//! `Service` is the deployment-level router that collects one or more route
//! bundles. Each handler receives a `Context<D>` and returns
//! `Result<Value, HandlerError>`.
//!
//! ## Example
//!
//! The handler closure returns a future, and `dispatch` is awaited:
//!
//! ```ignore
//! use distributed::microsvc;
//! use serde_json::json;
//!
//! let routes = microsvc::Routes::new()
//!     .with_dependencies(())
//!     .command("order.create")
//!     .handle(|ctx| {
//!         let input = ctx.input::<CreateOrderInput>();
//!         async move { Ok(json!({ "id": input?.id })) }
//!     });
//! let service = microsvc::Service::new().routes(routes);
//!
//! let result = service
//!     .dispatch("order.create", json!({"id": "1"}), Session::new())
//!     .await?;
//! ```

mod causal;
mod defaults;
mod handlers;
mod helpers;
mod invoke;
mod request;
mod routes;
mod runtime;

#[cfg(all(feature = "graphql", test))]
pub(crate) use causal::CausalCommandProjectionEvidence;
#[cfg(feature = "graphql")]
pub use causal::GraphqlServiceBindError;
#[cfg(feature = "graphql")]
pub(crate) use causal::{
    CausalCommandProjectionObligation, CausalCommandPublicState, CausalCommandReceiptSource,
    CausalProjectionEvidenceState,
};
#[cfg(feature = "graphql")]
pub use causal::{CausalCommandPublicStatus, CausalDispatchError, CausalDispatchResult};
#[allow(unused_imports)] // public API surface for handler-owned projected commits
pub use handlers::StagedProjectedRow;
pub use handlers::{
    direct_read_model, CausalCommandContext, CausalCommitBuilder, CausalRepository,
    DirectReadModelProjection, PreparedCausalCommit, PreparedCommandHandler,
};
pub use invoke::{invoke_transition, require_loaded};
pub use request::{CommandRequest, CommandResponse};
pub(crate) use routes::DynBusPublisher;
pub use routes::{
    DeliveryKind, HandlerNames, HandlerSpec, PortableCommand, RouteBuilder, Routes,
    ThinCommandBuilder, ThinCommandInvoked, ThinCommandLoaded, TypedRouteBuilder,
};
pub use runtime::Service;

#[cfg(test)]
mod tests;
