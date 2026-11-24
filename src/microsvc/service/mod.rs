//! Routes and service dispatch for microsvc.
//!
//! `Routes<D>` holds one dependency value and its typed commands and event
//! handlers. `Service` is the deployment-level router that collects one or
//! more route bundles. A command handler receives a `CausalCommandContext`
//! and its decoded input and returns a `PreparedCommand`; an event handler
//! receives a `Context<D>` and returns `Result<Value, HandlerError>`.
//!
//! ## Example
//!
//! An event handler closure returns a future, and `dispatch_message` is
//! awaited:
//!
//! ```ignore
//! use distributed::microsvc::{self, Message, MessageKind};
//! use serde_json::json;
//!
//! let routes = microsvc::Routes::new()
//!     .with_dependencies(())
//!     .event("order.created")
//!     .handle(|ctx| {
//!         let input = ctx.input::<OrderCreated>();
//!         async move { Ok(json!({ "id": input?.id })) }
//!     });
//! let service = microsvc::Service::new().routes(routes);
//!
//! let message = Message::new("order.created", MessageKind::Event, br#"{"id":"1"}"#.to_vec());
//! let result = service.dispatch_message(&message).await?;
//! ```

mod causal;
mod defaults;
mod handlers;
mod helpers;
mod invoke;
mod routes;
mod runtime;

#[cfg(all(feature = "graphql", test))]
pub(crate) use causal::CausalCommandProjectionEvidence;
#[cfg(feature = "graphql")]
pub use causal::CausalCommandPublicStatus;
#[cfg(feature = "graphql")]
pub use causal::GraphqlServiceBindError;
#[cfg(feature = "graphql")]
pub(crate) use causal::{
    CausalCommandPublicState, CausalCommandReceiptSource, CausalProjectionEvidenceState,
    ExternalCausalReservation,
};
pub use causal::{CausalDispatchError, CausalDispatchResult};
#[allow(unused_imports)] // public API surface for handler-owned projected commits
pub use handlers::StagedProjectedRow;
pub use handlers::{
    CausalCommandContext, CausalCommitBuilder, CausalRepository, PreparedCausalCommit,
    PreparedCommandHandler,
};
pub use invoke::{invoke_transition, load_for_command, require_loaded, CommandLoad};
pub(crate) use routes::DynBusPublisher;
#[cfg(feature = "runtime-tokio")]
pub(crate) use routes::LedgerCompaction;
pub use routes::{
    DeliveryKind, EventRouteBuilder, HandlerNames, HandlerSpec, PortableCommand, Routes,
    ThinCommandBuilder, ThinCommandInvoked, ThinCommandLoaded, TypedRouteBuilder,
};
pub use runtime::{Service, DEFAULT_DELIVERY_LANE};

#[cfg(test)]
mod tests;
