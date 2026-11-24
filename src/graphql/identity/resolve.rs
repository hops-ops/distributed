//! How a GraphQL HTTP or WebSocket request becomes a [`Session`].
//!
//! There are three modes and no default: an engine is built with exactly one.

use axum::http::header::{AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION};
use axum::http::{HeaderMap, HeaderName};
use subtle::ConstantTimeEq;

use super::oidc::{OidcConfig, OidcValidator, ValidationError};
use crate::command_dispatch::VerifiedPrincipal;
use crate::microsvc::Session;

/// Who the GraphQL edge believes about the caller's identity.
// Built once per engine; boxing the OIDC settings would only complicate matching.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum IdentityConfig {
    /// Public edge. Identity comes only from a bearer JWT validated against
    /// the issuer; `x-user-id` / `x-roles` sent by the client are ignored.
    OidcBearer(OidcConfig),
    /// Behind a gateway that authenticates callers itself. A request is
    /// trusted only when it carries the shared secret; its remaining headers
    /// (`x-user-id`, `x-roles`, ...) then become the session.
    Gateway(GatewaySecret),
    /// Local development and tests only: every request header is believed.
    DevHeaders,
}

impl IdentityConfig {
    pub fn oidc_bearer(oidc: OidcConfig) -> Self {
        Self::OidcBearer(oidc)
    }

    /// Trust requests that carry `secret` in `header`. Both are required:
    /// there is no gateway mode without a secret.
    pub fn gateway(header: &str, secret: impl Into<String>) -> Result<Self, IdentityConfigError> {
        let header = HeaderName::try_from(header)
            .map_err(|_| IdentityConfigError("gateway secret header is not a valid header name"))?;
        let secret = secret.into();
        if secret.is_empty() {
            return Err(IdentityConfigError("gateway secret must not be empty"));
        }
        Ok(Self::Gateway(GatewaySecret { header, secret }))
    }

    pub fn dev_headers() -> Self {
        Self::DevHeaders
    }

    /// The OIDC settings, when this is [`IdentityConfig::OidcBearer`].
    pub fn oidc(&self) -> Option<&OidcConfig> {
        match self {
            Self::OidcBearer(oidc) => Some(oidc),
            Self::Gateway(_) | Self::DevHeaders => None,
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::OidcBearer(_) => "oidc_bearer",
            Self::Gateway(_) => "gateway",
            Self::DevHeaders => "dev_headers",
        }
    }
}

/// The header and value a trusted gateway presents on every request.
#[derive(Clone)]
pub struct GatewaySecret {
    header: HeaderName,
    secret: String,
}

impl GatewaySecret {
    fn verify(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        let presented = headers
            .get(&self.header)
            .map(|value| value.as_bytes())
            .unwrap_or_default();
        if bool::from(presented.ct_eq(self.secret.as_bytes())) {
            Ok(())
        } else {
            Err(AuthError::Unauthorized)
        }
    }
}

impl std::fmt::Debug for GatewaySecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewaySecret")
            .field("header", &self.header)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityConfigError(&'static str);

impl std::fmt::Display for IdentityConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for IdentityConfigError {}

/// Resolves requests under one [`IdentityConfig`], sharing one OIDC validator
/// and JWKS cache. Build one per service and reuse it for every request.
pub struct IdentityResolver {
    config: IdentityConfig,
    validator: Option<OidcValidator>,
}

impl IdentityResolver {
    pub fn new(config: IdentityConfig) -> Self {
        let validator = config.oidc().cloned().map(OidcValidator::new);
        Self { config, validator }
    }

    pub fn config(&self) -> &IdentityConfig {
        &self.config
    }

    /// Resolve the session for one request.
    pub async fn resolve_session(&self, headers: &HeaderMap) -> Result<Session, AuthError> {
        self.resolve_identity(headers)
            .await
            .map(|identity| identity.session)
    }

    pub(crate) async fn resolve_identity(
        &self,
        headers: &HeaderMap,
    ) -> Result<ResolvedIdentity, AuthError> {
        match (&self.config, &self.validator) {
            (IdentityConfig::DevHeaders, _) => Ok(ResolvedIdentity::unverified(
                session_from_headers(headers, None),
            )),
            (IdentityConfig::Gateway(gateway), _) => {
                gateway.verify(headers)?;
                Ok(ResolvedIdentity::unverified(session_from_headers(
                    headers,
                    Some(&gateway.header),
                )))
            }
            (IdentityConfig::OidcBearer(oidc), Some(validator)) => {
                match extract_bearer(headers)? {
                    // Client identity headers are never merged into either branch.
                    Some(token) => {
                        let (session, principal) =
                            validator.validate_and_map_principal_async(&token).await?;
                        Ok(ResolvedIdentity::verified(session, principal))
                    }
                    None if oidc.require_auth => Err(AuthError::Unauthorized),
                    None => Ok(ResolvedIdentity::unverified(Session::new())),
                }
            }
            (IdentityConfig::OidcBearer(_), None) => Err(AuthError::Unauthorized),
        }
    }
}

/// Request headers as session variables, without the credentials themselves:
/// a session is handed on to command dispatch and must not carry them.
fn session_from_headers(headers: &HeaderMap, secret: Option<&HeaderName>) -> Session {
    let vars = headers
        .iter()
        .filter(|(name, _)| {
            ![&AUTHORIZATION, &COOKIE, &PROXY_AUTHORIZATION].contains(name) && Some(*name) != secret
        })
        .filter_map(|(name, value)| {
            Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect();
    Session::from_map(vars)
}

/// Placeholder issuer/audience when OIDC env is unset — fail-closed (401) until configured.
pub const UNSET_OIDC_ISSUER: &str = "http://localhost/unset-oidc-issuer";
pub const UNSET_OIDC_AUDIENCE: &str = "unset-audience";

/// Public GraphQL scaffold identity (D6/D7): always **`OidcBearer`** + `require_auth=true`.
///
/// Pure inputs so tests do not mutate process env. See [`public_oidc_identity_from_env`].
///
/// - When `issuer` and `audience` (or `client_id` as audience fallback) are non-empty → use them.
/// - When unset → placeholder issuer/audience so requests still require Bearer and reject
///   ambient `x-user-id` / `x-roles` (never [`IdentityConfig::DevHeaders`]).
pub fn public_oidc_identity_from_env_vars(
    issuer: Option<&str>,
    audience: Option<&str>,
    client_id: Option<&str>,
    jwks_uri: Option<&str>,
) -> IdentityConfig {
    let iss = issuer
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(UNSET_OIDC_ISSUER);
    let aud = audience
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| client_id.map(str::trim).filter(|s| !s.is_empty()))
        .unwrap_or(UNSET_OIDC_AUDIENCE);

    let mut oidc = OidcConfig::new(iss, aud);
    oidc.require_auth = true;
    if let Some(jwks) = jwks_uri.map(str::trim).filter(|s| !s.is_empty()) {
        oidc.jwks_uri = Some(jwks.to_string());
    }
    IdentityConfig::oidc_bearer(oidc)
}

/// Read process env (`OIDC_ISSUER`, `OIDC_AUDIENCE` / `OIDC_CLIENT_ID`, `OIDC_JWKS_URI`)
/// and apply [`public_oidc_identity_from_env_vars`]. Always OidcBearer (D6).
pub fn public_oidc_identity_from_env() -> IdentityConfig {
    public_oidc_identity_from_env_vars(
        std::env::var("OIDC_ISSUER").ok().as_deref(),
        std::env::var("OIDC_AUDIENCE").ok().as_deref(),
        std::env::var("OIDC_CLIENT_ID").ok().as_deref(),
        std::env::var("OIDC_JWKS_URI").ok().as_deref(),
    )
}

#[cfg(test)]
mod public_default_tests {
    use super::*;

    #[test]
    fn d6_unset_env_is_oidc_bearer_not_dev_headers() {
        let cfg = public_oidc_identity_from_env_vars(None, None, None, None);
        let oidc = cfg.oidc().expect("oidc config");
        assert!(oidc.require_auth);
        assert_eq!(oidc.issuer, UNSET_OIDC_ISSUER);
        assert_eq!(oidc.audience, UNSET_OIDC_AUDIENCE);
    }

    #[test]
    fn d6_configured_env_uses_issuer_audience() {
        let cfg = public_oidc_identity_from_env_vars(
            Some("http://localhost:8080"),
            Some("graphql-api"),
            None,
            Some("http://localhost:8080/oauth/v2/keys"),
        );
        let oidc = cfg.oidc().unwrap();
        assert!(oidc.require_auth);
        assert_eq!(oidc.issuer, "http://localhost:8080");
        assert_eq!(oidc.audience, "graphql-api");
        assert_eq!(
            oidc.jwks_uri.as_deref(),
            Some("http://localhost:8080/oauth/v2/keys")
        );
    }

    #[test]
    fn d6_client_id_falls_back_as_audience() {
        let cfg =
            public_oidc_identity_from_env_vars(Some("http://iss"), None, Some("client-123"), None);
        assert_eq!(cfg.oidc().unwrap().audience, "client-123");
    }

    #[tokio::test]
    async fn d6_unset_rejects_ambient_headers_via_resolve() {
        use axum::http::{HeaderMap, HeaderValue};
        let cfg = public_oidc_identity_from_env_vars(None, None, None, None);
        let mut headers = HeaderMap::new();
        headers.insert("x-user-id", HeaderValue::from_static("attacker"));
        headers.insert("x-roles", HeaderValue::from_static("admin"));
        // No Bearer → require_auth → Unauthorized (not DevHeaders trust)
        assert_eq!(
            IdentityResolver::new(cfg)
                .resolve_session(&headers)
                .await
                .unwrap_err(),
            AuthError::Unauthorized
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    Unauthorized,
}

/// A normal authorization session plus the optional bearer-only proof required
/// for durable causal command dispatch.
pub(crate) struct ResolvedIdentity {
    session: Session,
    principal: Option<VerifiedPrincipal>,
}

impl ResolvedIdentity {
    pub(crate) fn unverified(session: Session) -> Self {
        Self {
            session,
            principal: None,
        }
    }

    fn verified(session: Session, principal: VerifiedPrincipal) -> Self {
        Self {
            session,
            principal: Some(principal),
        }
    }

    pub(crate) fn into_parts(self) -> (Session, Option<VerifiedPrincipal>) {
        (self.session, self.principal)
    }
}

impl From<ValidationError> for AuthError {
    fn from(_: ValidationError) -> Self {
        AuthError::Unauthorized
    }
}

/// Extract Bearer token. Returns:
/// - `Ok(None)` — no Authorization or non-Bearer scheme (missing)
/// - `Ok(Some(token))` — Bearer with non-empty token
/// - `Err(Unauthorized)` — Bearer scheme with empty token (present-but-invalid)
pub fn extract_bearer(headers: &HeaderMap) -> Result<Option<String>, AuthError> {
    let Some(value) = headers.get(axum::http::header::AUTHORIZATION) else {
        return Ok(None);
    };
    let Ok(s) = value.to_str() else {
        return Err(AuthError::Unauthorized);
    };
    let s = s.trim();
    let mut parts = s.splitn(2, char::is_whitespace);
    let scheme = parts.next().unwrap_or("");
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return Ok(None);
    }
    let token = parts.next().map(str::trim).unwrap_or("");
    if token.is_empty() {
        return Err(AuthError::Unauthorized);
    }
    Ok(Some(token.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::microsvc::USER_ID_KEY;
    use axum::http::HeaderValue;

    fn spoofed() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_ID_KEY, HeaderValue::from_static("spoofed-user"));
        headers.insert("x-roles", HeaderValue::from_static("admin"));
        headers.insert("authorization", HeaderValue::from_static("Basic abc"));
        headers.insert("cookie", HeaderValue::from_static("sid=1"));
        headers
    }

    #[test]
    fn gateway_mode_cannot_be_built_without_a_secret() {
        assert!(IdentityConfig::gateway("x-gateway-secret", "").is_err());
        assert!(IdentityConfig::gateway("not a header", "s3cret").is_err());
        let config = IdentityConfig::gateway("x-gateway-secret", "s3cret").unwrap();
        assert!(!format!("{config:?}").contains("s3cret"));
    }

    #[tokio::test]
    async fn gateway_mode_trusts_headers_only_with_the_secret() {
        let resolver =
            IdentityResolver::new(IdentityConfig::gateway("x-gateway-secret", "s3cret").unwrap());
        let mut headers = spoofed();
        assert_eq!(
            resolver.resolve_session(&headers).await.unwrap_err(),
            AuthError::Unauthorized
        );
        for wrong in ["", "s3cre", "s3cret ", "S3CRET"] {
            headers.insert("x-gateway-secret", HeaderValue::from_static(wrong));
            assert!(
                resolver.resolve_session(&headers).await.is_err(),
                "{wrong:?}"
            );
        }
        headers.insert("x-gateway-secret", HeaderValue::from_static("s3cret"));
        let (session, principal) = resolver
            .resolve_identity(&headers)
            .await
            .unwrap()
            .into_parts();
        assert_eq!(session.get(USER_ID_KEY), Some("spoofed-user"));
        assert!(principal.is_none(), "header trust never mints a proof");
        for credential in ["x-gateway-secret", "authorization", "cookie"] {
            assert!(session.get(credential).is_none(), "{credential} leaked");
        }
    }

    #[tokio::test]
    async fn dev_headers_believes_headers_but_keeps_credentials_out_of_the_session() {
        let resolver = IdentityResolver::new(IdentityConfig::dev_headers());
        let (session, principal) = resolver
            .resolve_identity(&spoofed())
            .await
            .unwrap()
            .into_parts();
        assert_eq!(session.get(USER_ID_KEY), Some("spoofed-user"));
        assert!(principal.is_none());
        assert!(session.get("authorization").is_none());
        assert!(session.get("cookie").is_none());
    }

    #[tokio::test]
    async fn oidc_mode_ignores_client_identity_headers() {
        let anonymous = IdentityResolver::new(IdentityConfig::oidc_bearer(
            OidcConfig::new("https://issuer", "api").require_auth(false),
        ));
        let mut headers = spoofed();
        headers.remove("authorization");
        let (session, principal) = anonymous
            .resolve_identity(&headers)
            .await
            .unwrap()
            .into_parts();
        assert!(session.get(USER_ID_KEY).is_none());
        assert!(session.roles().is_empty());
        assert!(principal.is_none());

        let required = IdentityResolver::new(IdentityConfig::oidc_bearer(OidcConfig::new(
            "https://issuer",
            "api",
        )));
        assert!(required.resolve_session(&headers).await.is_err());
    }
}
