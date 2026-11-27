//! GraphQL gateway for read-model queries, subscriptions, and core commands.
//!
//! `naming` and `sdl` always compile (zero deps beyond the rest of the crate)
//! so `distributed schema --format graphql` works without enabling the `graphql`
//! feature. Execution, the dynamic schema, and the axum router sit behind
//! `feature = "graphql"`.

pub(crate) mod client_manifest;
pub mod naming;
pub mod projection_delta;
pub(crate) mod sdl;
pub(crate) mod surface;

// These modules are structural GraphQL metadata and deliberately remain
// pool/server independent. `distributed` and the runtime engine must compile the same
// surface without pulling in async-graphql or a database adapter.
mod commands;
mod cost;
mod filter;
mod permissions;

pub use client_manifest::*;
pub use sdl::{
    graphql_sdl_for_role, graphql_sdl_for_tables, graphql_sdl_for_tables_with_options,
    graphql_sdl_from_surface, SdlOptions,
};
pub use surface::{
    build_surface, surface_for_application, surface_for_role, ColumnField, ExternalField,
    ExternalReadError, ExternalReadErrorKind, ExternalReadExecutor, ExternalReadResult,
    ExternalReadSource, ExternalRoot, ExternalType, RelField, RootField, RootKind, Surface,
    SurfaceArgument, SurfaceArgumentKind, SurfaceCommand, SurfaceCommandShape, SurfaceDialect,
    SurfaceDirectProjection, SurfaceModel, SurfaceModeledProjection, SurfaceOptions,
    SurfaceProjectionOwner, SurfaceProjector, SurfaceRelationshipAggregate,
    SurfaceRelationshipKeys, SurfaceRowPolicy, SurfaceTypeDef, SurfaceTypeField,
};

pub use filter::{
    claim, col, lit, rel, ClaimRef, CmpOp, ColRef, FilterExpr, LitValue, Operand, Truth,
};
pub use permissions::{read, AuthorizedReadModel, ModelPermissions, ReadModels, ReadPermission};

#[cfg(feature = "graphql")]
mod compile;
#[cfg(feature = "graphql")]
mod engine;
#[cfg(feature = "graphql")]
mod execute;
#[cfg(feature = "graphql")]
mod external_roots;
#[cfg(feature = "graphql")]
pub mod http;
#[cfg(feature = "graphql")]
pub(crate) mod identity;
#[cfg(feature = "graphql")]
pub mod protocol;
#[cfg(feature = "graphql")]
pub(crate) mod query_protocol;
#[cfg(feature = "graphql")]
pub(crate) mod read_store;
#[cfg(feature = "graphql")]
mod schema;
#[cfg(feature = "graphql")]
pub mod subscribe;
#[cfg(feature = "graphql")]
pub use engine::{
    graphiql_enabled_from_env, graphiql_enabled_from_env_vars, GraphqlBuildError, GraphqlEngine,
    GraphqlEngineBuilder, GraphqlPool, GraphqlPoolSource, WebsocketLimits,
};
#[cfg(feature = "graphql")]
pub use identity::{
    extract_bearer, map_claims_to_session, public_oidc_identity_from_env,
    public_oidc_identity_from_env_vars, AuthError, ClaimMapConfig, GatewaySecret, IdentityConfig,
    IdentityConfigError, IdentityResolver, OidcConfig, OidcValidator, ValidationError,
    UNSET_OIDC_AUDIENCE, UNSET_OIDC_ISSUER,
};
#[cfg(feature = "graphql")]
pub use read_store::{CellByKeyGetter, HttpCellByKey, MapCellByKey, ReadStore};

#[cfg(all(feature = "graphql", feature = "gateway"))]
pub use engine::ReadRouting;
#[cfg(all(feature = "graphql", feature = "gateway"))]
pub use identity::OidcGatewayProvider;

#[cfg(all(feature = "graphql", feature = "gateway"))]
pub mod delivery;
