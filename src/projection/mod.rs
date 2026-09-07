//! Adapter-neutral, deterministic domain-event projection programs.
//!
//! This module models what a domain event means for read models. It does not
//! know how any ORM, database, projector lease, or client cache applies that
//! meaning. Downstream adapters lower a resolved plan without changing its
//! semantics.

#![deny(missing_docs)]

mod canonical;
mod error;
mod expression;
mod plan;
mod program;
mod provenance;

// Public module paths are reserved for downstream implementations. The empty
// seams intentionally expose no lowering, catalog, placement, or executor
// contract before their owning tasks define one.
pub mod catalog;
pub mod executor;
pub mod local_mounts;
pub mod lower;
pub mod placement;
#[cfg(feature = "graphql")]
pub mod rebuild;

pub use local_mounts::{
    LocalDirectMount, LocalEventualMount, LocalProjectionMounts, LocalProjectionMountsBuilder,
};

pub use error::ProjectionProgramError;
pub use expression::{
    ProjectionAssignment, ProjectionEnvelopeField, ProjectionExpression,
    ProjectionObjectValueField, ProjectionScalarTransform, ProjectionValue, ProjectionValueRef,
    ProjectionValueType, ResolvedProjectionValue, MAX_PROJECTION_EXPRESSION_DEPTH,
    MAX_PROJECTION_PATH_SEGMENTS,
};
pub(crate) use expression::{ProjectionAssignmentRef, ProjectionExpressionRef};
pub use plan::{
    ProjectionMutationProvenance, ProjectionOccurrenceProvenance, ResolvedProjectionField,
    ResolvedProjectionKey, ResolvedProjectionKeyField, ResolvedProjectionMutation,
    ResolvedProjectionMutationScope, ResolvedProjectionPartition, ResolvedProjectionPartitionRef,
    ResolvedProjectionPlan, ResolvedProjectionRelationshipEffect,
};
pub use program::{
    ProjectionArm, ProjectionEventSet, ProjectionField, ProjectionKeyField, ProjectionMutationKind,
    ProjectionOperation, ProjectionPartition, ProjectionPlanTemplate, ProjectionProgram,
    ProjectionProgramId, ProjectionProgramLimits, ProjectionRelationshipEffect,
    ProjectionRelationshipEffectKind, MAX_PROJECTION_OPERATIONS_PER_OCCURRENCE,
    PROJECTION_OPERATION_SEMANTICS_VERSION, PROJECTION_PROGRAM_IR_VERSION,
};
pub use provenance::{
    ProjectionEventSelector, ProjectionInvalidation, ProjectionRelationship, ProjectionTarget,
};

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "graphql"))]
mod source_snapshot_tests;
