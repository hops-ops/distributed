use super::{Credentials, GatewayError, RequestContext};
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

/// Header carrying the chain of gateways a request has crossed.
pub const HOPS_HEADER: &str = "x-distributed-gateway-hops";
/// Most gateways one request may cross, this one included.
pub const MAX_HOPS: usize = 8;
/// Largest hop chain accepted or produced, in bytes.
pub const MAX_HOPS_BYTES: usize = 1024;

/// The hop chain after this gateway, given the incoming one. Fails on a
/// loop (this gateway is already in the chain) or a chain that would exceed
/// [`MAX_HOPS`] entries or [`MAX_HOPS_BYTES`]. Every adapter checks the
/// incoming chain with this at ingress and writes its result on every
/// forwarded request.
pub fn append_hop(previous: Option<&str>, hop_id: &str) -> Result<String, GatewayError> {
    let previous = previous.filter(|chain| !chain.trim().is_empty());
    let Some(previous) = previous else {
        return Ok(hop_id.to_owned());
    };
    let mut count = 0;
    for hop in previous.split(',') {
        count += 1;
        if hop.trim() == hop_id || count >= MAX_HOPS {
            return Err(GatewayError("gateway loop"));
        }
    }
    let chain = format!("{previous},{hop_id}");
    if chain.len() > MAX_HOPS_BYTES {
        return Err(GatewayError("gateway loop"));
    }
    Ok(chain)
}

/// What a binding's backend receives from the client request. One rule for
/// every adapter and binding kind: adapters remove what [`Self::forwards`]
/// rejects, then add only what they vouch for (the provider's backend
/// credential, the public origin and their hop marker).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Forwarding {
    /// Handlers, assets and UI proxies: every header that is not a hop,
    /// credential or identity header, plus the provider's backend credential.
    Application,
    /// Zone hosts verify their own join tickets: no backend credential and no
    /// cookie.
    Zones,
    /// GraphQL executors read only the headers named in
    /// [`GRAPHQL_REQUEST_HEADERS`], plus the provider's backend credential.
    Graphql,
}

/// Client request headers a GraphQL executor receives. Its identity arrives
/// only as the provider's backend credential.
pub const GRAPHQL_REQUEST_HEADERS: [&str; 13] = [
    "accept",
    "accept-encoding",
    "accept-language",
    "content-length",
    "content-type",
    "origin",
    "sec-websocket-key",
    "sec-websocket-protocol",
    "sec-websocket-version",
    "traceparent",
    "tracestate",
    "user-agent",
    "x-distributed-gateway-hops",
];

impl Forwarding {
    /// Whether the client's header `name` reaches the backend. `strip` is the
    /// deployment's own list of identity and secret header names.
    pub fn forwards(self, name: &str, strip: &[String]) -> bool {
        let name = name.to_ascii_lowercase();
        let name = name.as_str();
        if matches!(
            name,
            "authorization"
                | "connection"
                | "host"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        ) || is_untrusted_identity_header(name)
            || strip.iter().any(|strip| strip.eq_ignore_ascii_case(name))
        {
            return false;
        }
        match self {
            Self::Application => true,
            Self::Zones => name != "cookie",
            Self::Graphql => GRAPHQL_REQUEST_HEADERS.contains(&name),
        }
    }

    /// Whether the backend receives the provider's backend credential.
    pub fn sends_backend_credential(self) -> bool {
        !matches!(self, Self::Zones)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_binding_receives_a_client_credential_or_identity_header() {
        for rule in [
            Forwarding::Application,
            Forwarding::Zones,
            Forwarding::Graphql,
        ] {
            for name in [
                "Authorization",
                "X-User-Id",
                "x-roles",
                "x-hasura-role",
                "X-Forwarded-Host",
                "Host",
                "Upgrade",
                "X-Tenant-Secret",
            ] {
                assert!(
                    !rule.forwards(name, &["x-tenant-secret".into()]),
                    "{rule:?} {name}"
                );
            }
            assert!(rule.forwards("Content-Type", &[]), "{rule:?}");
            assert!(rule.forwards("x-distributed-gateway-hops", &[]), "{rule:?}");
        }
    }

    #[test]
    fn the_hop_chain_has_one_limit_on_every_adapter() {
        assert_eq!(append_hop(None, "me").unwrap(), "me");
        assert_eq!(append_hop(Some(" "), "me").unwrap(), "me");
        assert_eq!(append_hop(Some("a, b"), "me").unwrap(), "a, b,me");
        assert!(append_hop(Some("a, me"), "me").is_err(), "loop");
        let seven = ["h"; MAX_HOPS - 1].join(",");
        assert_eq!(
            append_hop(Some(&seven), "me").unwrap().split(',').count(),
            MAX_HOPS
        );
        let eight = ["h"; MAX_HOPS].join(",");
        assert!(append_hop(Some(&eight), "me").is_err(), "a ninth hop");
        let long = "x".repeat(MAX_HOPS_BYTES - 2);
        assert!(
            append_hop(Some(&long), "me").is_err(),
            "over the byte bound"
        );
        assert_eq!(
            append_hop(Some(&long[1..]), "me").unwrap().len(),
            MAX_HOPS_BYTES
        );
    }

    #[test]
    fn zones_and_graphql_receive_no_cookie() {
        assert!(Forwarding::Application.forwards("cookie", &[]));
        assert!(!Forwarding::Zones.forwards("Cookie", &[]));
        assert!(!Forwarding::Graphql.forwards("cookie", &[]));
        assert!(!Forwarding::Graphql.forwards("x-anything-else", &[]));
        assert!(Forwarding::Application.sends_backend_credential());
        assert!(Forwarding::Graphql.sends_backend_credential());
        assert!(!Forwarding::Zones.sends_backend_credential());
    }
}
