//! Role-safe, deterministic client projection deltas.
//!
//! This module lowers logical projection provenance through an explicit
//! authorization boundary. It never derives client semantics from physical
//! table write plans.

#[cfg(any(feature = "graphql", test))]
pub(crate) mod authorization;
#[cfg(any(feature = "graphql", test))]
mod canonical;
#[cfg(any(feature = "graphql", test))]
pub(crate) mod lower;
#[cfg(feature = "graphql")]
pub(crate) mod runtime;
mod types;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod vector_tests;

pub use types::{
    DeltaField, DeltaKeyField, DeltaValue, ProjectionDelta, ProjectionDeltaError,
    ProjectionDeltaIdentity, ProjectionDeltaMutation, ProjectionDeltaOccurrence,
    ProjectionDeltaOperation, ProjectionDeltaPartition, ProjectionDeltaProjectionIdentity,
    ProjectionDeltaRecovery, ProjectionDeltaRecoveryCondition, ProjectionDeltaRecoveryTarget,
    ProjectionDeltaScope, ProjectionDeltaSurfaceIdentity, PROJECTION_DELTA_WIRE_VERSION,
};
