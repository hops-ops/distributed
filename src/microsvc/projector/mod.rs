//! Typed, capability-restricted causal projector routes.
//!
//! Application projectors use modeled/mutation handlers
//! ([`ModeledProjection::apply`]). [`CausalProjectorContext`] also exposes
//! causal **protocol lifecycle** primitives (load/project/delete/recreate) for
//! recovery and protocol tests — not a competing multi-table ORM workspace.
//! Handlers never receive the dependency bundle, repository, trusted transport
//! cursor, or commit/failure methods.

mod context;
mod errors;
mod handle;
mod registration;
mod runtime;

pub use context::{CausalProjectorContext, LoadedProjection};
pub use handle::{ProjectionRepairHandle, ProjectionRepairHandleParseError, StoppedProjection};
pub use registration::CausalProjectorRouteBuilder;
#[cfg(feature = "graphql")]
pub(super) use registration::ModeledProjectorHandlerFn;
#[cfg(feature = "graphql")]
pub use registration::{ModeledProjection, ModeledProjectorRouteBuilder};

pub(super) use errors::projection_error_is_retryable;
#[cfg(feature = "graphql")]
pub(super) use runtime::RegisteredModeledProjector;
pub(super) use runtime::{
    ErasedProjectorHandler, ProjectorDispatchFuture, ProjectorRegistration, ProjectorRepairFuture,
    ProjectorRepairLookupFuture,
};

#[cfg(test)]
mod tests;
