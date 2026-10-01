//! GraphQL identity modes: TrustedProxy, OidcBearer, Hybrid, DevHeaders.
//!
//! Normative behavior: package spec `specs/query-layer/identity` (D1–D14, F1–F10).
//! This module only populates [`Session`]; GraphQL RBAC is unchanged.

mod claims;
mod oidc;
mod resolve;

pub use claims::{map_claims_to_session, ClaimMapConfig};
pub use oidc::VerifiedPrincipal;
pub use oidc::{OidcConfig, OidcValidator, ValidationError};
pub use resolve::{
    extract_bearer, public_oidc_identity_from_env, public_oidc_identity_from_env_vars,
    resolve_session, resolve_session_sync, strip_identity_headers, AuthError, IdentityConfig,
    IdentityMode, IdentityResolver, TrustedProxyConfig, DEFAULT_IDENTITY_STRIP_HEADERS,
    UNSET_OIDC_AUDIENCE, UNSET_OIDC_ISSUER,
};
pub(crate) use resolve::{resolve_identity_with_validator, ResolvedIdentity};

use crate::microsvc::Session;
use axum::http::HeaderMap;

/// Build a Session from all request headers (DevHeaders / mesh trust).
pub fn session_from_all_headers(headers: &HeaderMap) -> Session {
    let mut vars = std::collections::HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            vars.insert(name.as_str().to_string(), v.to_string());
        }
    }
    Session::from_map(vars)
}

#[cfg(feature = "gateway")]
mod gateway;
#[cfg(feature = "gateway")]
pub use gateway::OidcGatewayProvider;
