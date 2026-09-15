//! GraphQL gateway for read-model queries, subscriptions, and core commands.
//!
//! `naming` and `sdl` always compile (zero deps beyond the rest of the crate)
//! so `distributed schema --format graphql` works without enabling the `graphql`
//! feature. Execution, the dynamic schema, and the axum router sit behind
//! `feature = "graphql"`.

pub mod client_manifest;
pub mod naming;
pub mod projection_delta;
pub mod sdl;
pub mod surface;

// These modules are structural GraphQL metadata and deliberately remain
// pool/server independent. `distributed` and the runtime engine must compile the same
// surface without pulling in async-graphql or a database adapter.
mod commands;
mod complexity_contract;
mod filter;
mod permissions;

pub use client_manifest::*;
pub use naming::{
    aggregate_field, by_pk_field, comparison_op_fields, include_postgres_json_comparison_ops,
    is_valid_graphql_name, mutation_delete_by_pk_field, mutation_insert_one_field,
    mutation_update_by_pk_field, mutation_upsert_field, object_type_name, root_list_field,
    scalar_type_name, PORTABLE_COMPARISON_OPS, POSTGRES_JSON_COMPARISON_OPS, STRING_COMPARISON_OPS,
};
pub use sdl::{
    graphql_sdl_for_role, graphql_sdl_for_tables, graphql_sdl_for_tables_with_options,
    graphql_sdl_from_surface, SdlOptions,
};
pub use surface::{
    build_surface, role_grants_for_role, surface_for_application, surface_for_application_contract,
    surface_for_role, RoleGrant, RootField, RootKind, Surface, SurfaceArgument,
    SurfaceArgumentKind, SurfaceCommand, SurfaceCommandShape, SurfaceDialect,
    SurfaceDirectProjection, SurfaceModel, SurfaceModeledProjection, SurfaceOptions,
    SurfaceProjectionOwner, SurfaceProjector, SurfaceRelationshipAggregate,
    SurfaceRelationshipKeys, SurfaceRowPolicy, SurfaceTypeDef, SurfaceTypeField,
};

pub use filter::{claim, col, lit, rel, ClaimRef, ColRef, FilterExpr, LitValue, Operand};
pub use permissions::{
    read, role_grant_from_read_permission, role_grants_from_model_role_perms, ModelPermissions,
    ReadPermission,
};

#[cfg(feature = "graphql")]
mod compile;
#[cfg(feature = "graphql")]
mod complexity;
#[cfg(feature = "graphql")]
mod engine;
#[cfg(feature = "graphql")]
mod execute;
#[cfg(feature = "graphql")]
pub mod http;
#[cfg(feature = "graphql")]
pub mod identity;
#[cfg(feature = "graphql")]
pub mod protocol;
#[cfg(feature = "graphql")]
pub(crate) mod query_protocol;
#[cfg(feature = "graphql")]
pub mod read_store;
#[cfg(feature = "graphql")]
mod schema;
#[cfg(feature = "graphql")]
pub mod subscribe;
#[cfg(feature = "graphql")]
pub use engine::{
    graphiql_enabled_from_env, graphiql_enabled_from_env_vars, GraphqlBuildError, GraphqlEngine,
    GraphqlEngineBuilder, GraphqlPool, GraphqlPoolSource,
};
#[cfg(feature = "graphql")]
pub use http::{
    graphiql_page, graphql_router, graphql_router_composed, graphql_router_with_dispatcher,
    graphql_router_with_host, graphql_router_with_service, GraphqlOperationFilter,
};
#[cfg(feature = "graphql")]
pub use identity::{
    extract_bearer, map_claims_to_session, public_oidc_identity_from_env,
    public_oidc_identity_from_env_vars, resolve_session, resolve_session_sync,
    strip_identity_headers, AuthError, ClaimMapConfig, IdentityConfig, IdentityMode,
    IdentityResolver, OidcConfig, OidcValidator, TrustedProxyConfig, ValidationError,
    VerifiedPrincipal, DEFAULT_IDENTITY_STRIP_HEADERS, UNSET_OIDC_AUDIENCE, UNSET_OIDC_ISSUER,
};
#[cfg(feature = "graphql")]
pub use read_store::{CellByKeyGetter, HttpCellByKey, MapCellByKey, ReadStore};
#[cfg(feature = "graphql")]
pub use subscribe::ChangeHub;

#[cfg(all(feature = "graphql", feature = "gateway-delivery"))]
pub use engine::ReadRouting;

#[cfg(all(feature = "graphql", feature = "gateway-delivery"))]
pub mod delivery;
