use super::{Credentials, RequestContext};
use std::future::Future;

/// Authentication/admission failures carry no credentials or provider internals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthError {
    /// Missing, invalid, revoked or expired required credentials (401).
    Unauthorized,
    /// Authenticated but forbidden by the route policy (403).
    Forbidden,
    /// Provider cannot establish current identity (503); never serve stale data.
    Unavailable,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::Unavailable => "identity provider unavailable",
        })
    }
}
impl std::error::Error for AuthError {}

/// Replaceable credential validator/session provider. Implementations may use
/// local Worker handles; native implementations may return Send futures.
/// Never trust caller identity or forwarded-host headers. Session providers
/// delegate refresh/callback/logout to their existing auth lifecycle handlers.
pub trait AuthProvider {
    /// Validate current credentials, returning anonymous only when credentials
    /// are absent (or the provider explicitly recognizes an anonymous session).
    fn authenticate(
        &self,
        credentials: &Credentials,
    ) -> impl Future<Output = Result<RequestContext, AuthError>>;
}

/// Built-in route admission; applications can add policies in their adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Anonymous callers allowed; invalid credentials still fail authentication.
    Public,
    /// Any current authenticated subject.
    Authenticated,
    /// A provider-mapped role is required.
    Role(String),
}

impl Admission {
    /// Apply after provider authentication with a host-supplied Unix clock.
    /// Expiry is enforced on public routes too; it cannot downgrade to anonymous.
    pub fn check(&self, context: &RequestContext, now: u64) -> Result<(), AuthError> {
        let identity = context.identity();
        if identity.is_some_and(|identity| identity.expires_at() <= now) {
            return Err(AuthError::Unauthorized);
        }
        match self {
            Self::Public => Ok(()),
            Self::Authenticated => identity.map(|_| ()).ok_or(AuthError::Unauthorized),
            Self::Role(role) => {
                let identity = identity.ok_or(AuthError::Unauthorized)?;
                if identity.roles().contains(role) {
                    Ok(())
                } else {
                    Err(AuthError::Forbidden)
                }
            }
        }
    }
}

/// Headers a public ingress must remove before delegating to a backend or auth
/// handler. Reconstruct public-origin headers from configured origin only.
/// Deployments must additionally strip their own custom identity/secret names.
pub fn is_untrusted_identity_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        "forwarded"
            | "x-forwarded-host"
            | "x-forwarded-proto"
            | "x-forwarded-port"
            | "x-forwarded-for"
            | "x-real-ip"
            | "cf-connecting-ip"
            | "cf-access-jwt-assertion"
            | "x-user-id"
            | "x-role"
            | "x-roles"
    ) || name.starts_with("x-hasura-")
}
