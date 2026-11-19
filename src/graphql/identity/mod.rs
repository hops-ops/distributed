//! GraphQL identity modes: OidcBearer, Gateway, DevHeaders.
//!
//! Normative behavior: package spec `specs/query-layer/identity` (D1–D14, F1–F10).
//! This module only populates [`Session`]; GraphQL RBAC is unchanged.

mod claims;
mod oidc;
mod resolve;

pub use claims::{map_claims_to_session, ClaimMapConfig};
pub use oidc::{InvalidPrincipal, VerifiedPrincipal};
pub use oidc::{OidcConfig, OidcValidator, ValidationError};
pub(crate) use resolve::ResolvedIdentity;
pub use resolve::{
    extract_bearer, public_oidc_identity_from_env, public_oidc_identity_from_env_vars, AuthError,
    GatewaySecret, IdentityConfig, IdentityConfigError, IdentityResolver, UNSET_OIDC_AUDIENCE,
    UNSET_OIDC_ISSUER,
};

#[cfg(feature = "gateway")]
mod gateway;
#[cfg(feature = "gateway")]
pub use gateway::OidcGatewayProvider;
