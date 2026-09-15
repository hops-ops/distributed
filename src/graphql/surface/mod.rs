//! Shared GraphQL **surface IR** — single source of truth for the query/subscription
//! type system a catalog (and optionally a role) can see.
//!
//! SDL emission and (over time) runtime schema construction consume this IR so
//! dialect-honest comparison ops, roots, and column grants cannot diverge.
//!
//! Core types compile without the `graphql` feature so `distributed schema --format graphql`
//! can share the same IR path.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use sha2::{Digest, Sha256};

use super::filter::{validate_row_policy_operand_literal, FilterExpr, Operand};
use crate::command::{
    compiled_direct_projection_target, validate_projection_confirmation_count, CommandConsistency,
    CommandDirectProjectionTarget, CommandEffect, CommandEffects, CommandInputDefault,
    CommandProjectedModel, CommandProjectionConfirmation, CommandProjectionEvents,
    CommandProjectionPreviewSource, CompiledDirectProjectionTarget, EffectExpression,
    EffectFieldValue, EffectKey, EffectRelationship,
};
use crate::projection_protocol::ProjectionModelOwnership;
use crate::projection_protocol::ProjectionPartitionSpec;
use crate::table::{
    resolve_direct_join_keys, resolve_m2m_join_keys, ColumnType, RelationshipKind, TableColumn,
    TableSchema,
};

use super::naming::{
    by_pk_field, comparison_exp_name, comparison_op_fields, include_postgres_json_comparison_ops,
    is_valid_graphql_name, object_type_name, reserved_type_names, root_list_field,
    scalar_type_name, CUSTOM_SCALARS,
};

mod application;
mod build;
mod commands;
mod effects;
mod identity;
mod projections;
mod roots;
mod types;
mod validation;

#[cfg(test)]
mod tests;

pub use application::{
    role_grants_for_role, surface_for_application, surface_for_application_contract,
    surface_for_role, RoleGrant,
};
pub use build::build_surface;
pub use projections::SurfaceModeledProjection;
pub(crate) use projections::{
    compile_projection_owner_topology, modeled_owner_partition_contract,
    validate_direct_modeled_owner_compatibility, SurfaceProjectionArm, SurfaceProjectionOperation,
    SurfaceSelectedProjectionProgram,
};
pub use types::{
    ColumnField, RelField, RootField, RootKind, Surface, SurfaceArgument, SurfaceArgumentKind,
    SurfaceCommand, SurfaceCommandShape, SurfaceDialect, SurfaceDirectProjection, SurfaceModel,
    SurfaceOptions, SurfaceProjectionOwner, SurfaceProjector, SurfaceRelationshipAggregate,
    SurfaceRelationshipKeys, SurfaceRowPolicy, SurfaceTypeDef, SurfaceTypeField,
};

pub(crate) use commands::projected_output_reuses_surface_model;
pub(in crate::graphql::surface) use commands::{
    bind_surface_direct_projection_targets, validate_and_canonicalize_commands,
    validate_command_confirmation_topology,
};
pub(in crate::graphql::surface) use effects::*;
pub(in crate::graphql::surface) use identity::*;
pub(in crate::graphql::surface) use roots::*;
pub(crate) use types::{model_has_client_normalized_identity, SurfaceSelection};
pub(in crate::graphql::surface) use validation::*;
