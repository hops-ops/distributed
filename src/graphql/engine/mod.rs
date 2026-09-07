//! GraphqlEngine builder, validation, and execute entrypoint.
#![allow(clippy::items_after_test_module)]

use std::any::TypeId;
use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Request, Response, ServerError, Value};
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::microsvc::{Service, Session, ROLE_KEY, USER_ID_KEY};
use crate::read_model::{ReadModelChange, RelationalReadModelIncludes};
use crate::table::ReadModelCatalog;
use crate::table::{
    resolve_direct_join_keys, resolve_m2m_join_keys, ColumnType, RelationshipKind, TableKind,
    TableSchema,
};

use super::client_manifest::{
    trusted_preset_descriptors, ClientExecutionLimits, ClientManifestError, ClientSurfaceIdentity,
    ClientTrustedPresetDescriptor, DistributedClientManifest, DistributedClientSurfaceExport,
};
use super::commands::TypedCommandInventory;
use super::compile::{SqlDialect, SqlPlan};
use super::execute;
use super::filter::{validate_row_policy_operand_literal, FilterExpr, Operand};
use super::identity::{IdentityConfig, IdentityMode, OidcValidator, VerifiedPrincipal};
use super::naming::{
    by_pk_field, is_valid_graphql_name, object_type_name, reserved_type_names, root_list_field,
};
use super::permissions::{
    read, role_grants_from_model_role_perms, ModelPermissions, ReadPermission,
};
use super::protocol::{
    DistributedEnvelopeV1, DistributedLiveCursor, DistributedTrustedPreset, OpaqueProtocolToken,
    ProtocolResponseAccumulator, ProtocolTokenCodec, ProtocolTokenPurpose, RequestedLiveResume,
    MAX_LIVE_RESUME_CURSORS,
};
use super::query_protocol::QueryProtocolRuntime;
use super::schema as dyn_schema;
use super::sdl::{graphql_sdl_for_tables_with_options, graphql_sdl_from_surface, SdlOptions};
use super::surface::{
    build_surface, surface_for_application_contract, surface_for_role, Surface, SurfaceDialect,
    SurfaceOptions, SurfaceProjectionOwner, SurfaceProjector, SurfaceSelection,
};
use crate::command::TypedServiceCommandBinding;

const GRAPHIQL_INTROSPECTION_MAX_DEPTH_FLOOR: usize = 15;
const GRAPHIQL_INTROSPECTION_MAX_COMPLEXITY_FLOOR: usize = 10_000;
const REQUEST_ANALYSIS_MAX_DEPTH: usize = 128;
const REQUEST_ANALYSIS_MAX_SELECTIONS: usize = 4_096;

mod auth;
mod builder;
mod core;
mod introspection;
mod live_resume;
mod metrics;
mod protocol;
mod public_api;
mod request;
mod validation;

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
mod composite_relationship_tests;
#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
mod tests;

pub use core::{
    ExecutionAuthority, GraphqlBuildError, GraphqlEngine, GraphqlEngineBuilder, GraphqlPool,
    GraphqlPoolSource,
};
pub use introspection::{graphiql_enabled_from_env, graphiql_enabled_from_env_vars};

pub(crate) use auth::{
    identity_mode_label, insert_synthetic_privilege_permissions, role_authorization_info,
    role_authorization_info_for_roles,
};
pub(crate) use core::{
    CatalogEntry, ClientApplicationRegistration, EngineInner, ProtocolApplicationInfo,
    ProtocolRoleInfo, ProtocolRuntime, ProtocolSurfaceInfo, RoleModelPerm,
};
pub(crate) use introspection::is_pure_introspection_request;
pub(crate) use live_resume::parse_requested_live_resume;
pub(crate) use metrics::{
    attach_protocol_response, metrics_status_for_response, protocol_internal_error_response,
    record_metrics,
};
pub(crate) use protocol::{
    has_multiple_protocol_query_roots, operation_fingerprint, protocol_multi_root_error_response,
    protocol_trusted_presets, resolve_execution_authority, resolve_protocol_preset,
    select_protocol_surface,
};
pub(crate) use validation::{execute_plan, validate_filter, validate_generated_names};

/// Public helper for tests: compile + naming surface.
#[allow(dead_code)]
pub fn core_sdl_for_catalog(tables: &[TableSchema]) -> Result<String, String> {
    validation::core_sdl_for_catalog(tables)
}

#[cfg(feature = "gateway-delivery")]
pub(crate) mod read_routing;
#[cfg(feature = "gateway-delivery")]
pub use read_routing::ReadRouting;

#[cfg(feature = "gateway-delivery")]
mod delivery;
