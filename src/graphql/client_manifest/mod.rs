//! Versioned client contract derived from the shared, role-filtered GraphQL
//! [`Surface`](super::surface::Surface).
//!
//! This module is intentionally pool-free. Runtime schema construction, engine
//! export, and the `distributed` CLI all hand the same Surface to
//! [`DistributedClientSurfaceExport::manifest`]; no consumer re-walks a table,
//! command, permission, relationship, or projector registry.

mod build;
mod capabilities;
mod codec;
mod commands;
mod error;
mod export;
mod identity;
mod limits;
mod projections;
mod types;
mod validation;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::complexity_contract::{default_weights, DEFAULT_MAX_COMPLEXITY, DEFAULT_MAX_DEPTH};
use super::filter::{FilterExpr, Operand};
use super::naming::{aggregate_fields_type_name, aggregate_type_name};
use super::surface::{
    model_has_client_normalized_identity, RootKind, Surface, SurfaceArgument, SurfaceArgumentKind,
    SurfaceCommand, SurfaceCommandShape, SurfaceRelationshipKeys, SurfaceRowPolicy,
    SurfaceSelection, SurfaceTypeDef,
};
use crate::command::{CommandConsistency, EffectExpression};
use crate::table::RelationshipKind;

use build::client_manifest_from_surface_with_execution;
use capabilities::{
    model_has_visible_causal_owner, query_footprint_has_record_evidence,
    query_footprint_supports_live_resume,
};
use codec::*;
use commands::*;
use projections::{command_projection_extension, projection_manifest};
use validation::validate_surface_structure;

// Used by unit tests (and still a thin production-friendly wrapper).
#[cfg_attr(not(test), allow(unused_imports))]
pub(crate) use build::client_manifest_from_surface;
pub use error::ClientManifestError;
pub use export::{prune_client_manifest, DistributedClientSurfaceExport};
pub use identity::ClientSurfaceIdentity;
pub use limits::{ClientComplexityWeights, ClientExecutionLimits};
pub use types::{
    ClientAggregateSemantics, ClientArgument, ClientArgumentKind, ClientCapabilities,
    ClientCommand, ClientCommandExtensionSlots, ClientCommandPureArg, ClientCommandPureReduce,
    ClientCommandShape, ClientField, ClientFilterField, ClientFilterInput,
    ClientFilterInputRelationship, ClientFilterSemantics, ClientKeyField, ClientModel,
    ClientOrderSemantics, ClientPaginationSemantics, ClientProjectionArm,
    ClientProjectionAssignment, ClientProjectionBinding, ClientProjectionBindingState,
    ClientProjectionEnvelopeField, ClientProjectionEventRef, ClientProjectionExecutionClass,
    ClientProjectionExpression, ClientProjectionFallback, ClientProjectionField,
    ClientProjectionInvalidation, ClientProjectionKeyField, ClientProjectionMutationKind,
    ClientProjectionObjectField, ClientProjectionOperation, ClientProjectionPartition,
    ClientProjectionPlacement, ClientProjectionPreviewSource, ClientProjectionProgram,
    ClientProjectionRelationshipEffect, ClientProjectionRelationshipEffectKind,
    ClientProjectionScalarTransform, ClientProjectionTopologyIdentity, ClientProjectionValue,
    ClientProjectionValueField, ClientProjectionValueType, ClientProjector,
    ClientProtocolOperation, ClientProtocolOperations, ClientRelationship,
    ClientRelationshipAggregate, ClientRelationshipKind, ClientRelationshipMaintenance, ClientRoot,
    ClientRootKind, ClientRootOperation, ClientRowPolicy, ClientTrustedPresetDescriptor,
    ClientTypeDef, ClientTypeField, CommandConfirmationsExtension, CommandConsistencyExtension,
    CommandDirectProjectionExtension, CommandEffectsExtension, CommandInputDefaultsExtension,
    CommandProjectionArmRef, CommandProjectionExtension, CommandProjectionPreviewOccurrence,
    CommandProjectionPreviewValue, DistributedClientManifest, ModelNormalization,
    RelationshipKeyMapping, ScalarCodec,
};
pub(crate) use validation::trusted_preset_descriptors;

pub const DISTRIBUTED_CLIENT_MANIFEST_VERSION: u32 = 2;
pub const DISTRIBUTED_CLIENT_PROTOCOL_VERSION: u32 = 1;
// Protocol v1 is the first public wire family. The independent fingerprint below
// changes when its generated command/scope contract changes, including trusted-preset descriptor slots.
const DISTRIBUTED_CLIENT_PROTOCOL_MANIFEST_EPOCH: u32 = 2;
const COMMAND_EXTENSION_SLOTS_VERSION: u32 = 2;
const PROJECTOR_ENTRY_VERSION: u32 = 1;
const PROTOCOL_OPERATIONS_VERSION: u32 = 1;
const QUERY_CAPABILITIES_VERSION: u32 = 1;
const QUERY_COMPLEXITY_VERSION: u32 = 1;
const KEY_ENCODING: &str = "canonical_json_tuple_v1";
const DEFAULT_MAX_BOOL_WIDTH: u64 = 256;
const DEFAULT_MAX_IN_LIST: u64 = 1_000;
