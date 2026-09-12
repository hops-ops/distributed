use super::GatewayError;
use std::fmt;

/// Credentials received from a client. Possession is not authentication.
/// Providers must reject invalid credentials even on otherwise public routes.
#[derive(Clone, Default)]
pub struct Credentials {
    /// Raw Authorization value; never populated from a cookie by the gateway.
    pub authorization: Option<String>,
    /// Raw Cookie value, for a configured session provider only.
    pub cookie: Option<String>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("authorization_present", &self.authorization.is_some())
            .field("cookie_present", &self.cookie.is_some())
            .finish()
    }
}

/// A credential explicitly supplied by the configured provider for backend
/// validation. Gateway admission never grants backend authorization.
#[derive(Clone, Default)]
pub enum BackendCredential {
    /// No backend credential. The backend applies its anonymous policy.
    #[default]
    None,
    /// An access token validated or obtained by the provider. Backends still
    /// validate issuer, audience, expiry and their own authorization policy.
    Bearer(String),
}

impl fmt::Debug for BackendCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::None => "None",
            Self::Bearer(_) => "Bearer([redacted])",
        })
    }
}

/// Provider-authenticated identity shared across UI, API and custom routes.
/// This is an in-process assertion, never a deserializable client proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    authority: String,
    subject: String,
    roles: Vec<String>,
    expires_at: u64,
}

impl Identity {
    /// Called by a trusted provider after validating credentials. `expires_at`
    /// is Unix seconds, including the provider's session expiry ceiling.
    pub fn verified(
        authority: impl Into<String>,
        subject: impl Into<String>,
        roles: Vec<String>,
        expires_at: u64,
    ) -> Result<Self, GatewayError> {
        let authority = authority.into();
        let subject = subject.into();
        if [&authority, &subject]
            .iter()
            .any(|s| s.is_empty() || s.len() > 4096 || s.chars().any(char::is_control))
            || roles.len() > 128
            || roles
                .iter()
                .any(|s| s.is_empty() || s.len() > 256 || s.chars().any(char::is_control))
        {
            return Err(GatewayError("invalid provider identity"));
        }
        let mut roles = roles;
        roles.sort();
        roles.dedup();
        Ok(Self {
            authority,
            subject,
            roles,
            expires_at,
        })
    }
    /// Authority under which the subject is unique.
    pub fn authority(&self) -> &str {
        &self.authority
    }
    /// Authenticated subject, not a caller-supplied identity header.
    pub fn subject(&self) -> &str {
        &self.subject
    }
    /// Provider-mapped roles. The backend independently authorizes operations.
    pub fn roles(&self) -> &[String] {
        &self.roles
    }
    /// Unix second at which this assertion ceases to admit new work.
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }
}

/// Shared admission context. Re-authenticate every new consumer before any
/// cache/flight/live join; this value is not an authorization cache.
#[derive(Clone, Debug)]
pub struct RequestContext {
    identity: Option<Identity>,
    provider_revision: String,
    backend: BackendCredential,
}

impl RequestContext {
    /// Create an assertion from a trusted provider. Bump `provider_revision`
    /// when replacing provider configuration/policy to prevent reuse across it.
    pub fn from_provider(
        identity: Option<Identity>,
        provider_revision: impl Into<String>,
        backend: BackendCredential,
    ) -> Result<Self, GatewayError> {
        let provider_revision = provider_revision.into();
        if provider_revision.is_empty()
            || provider_revision.len() > 256
            || provider_revision.chars().any(char::is_control)
            || (identity.is_none() && !matches!(backend, BackendCredential::None))
        {
            return Err(GatewayError("invalid provider context"));
        }
        Ok(Self {
            identity,
            provider_revision,
            backend,
        })
    }
    /// Authenticated identity, or anonymous.
    pub fn identity(&self) -> Option<&Identity> {
        self.identity.as_ref()
    }
    /// Provider/policy configuration generation; not a credential.
    pub fn provider_revision(&self) -> &str {
        &self.provider_revision
    }
    /// Explicit downstream credential, never derived from asserted headers.
    pub fn backend_credential(&self) -> &BackendCredential {
        &self.backend
    }
}
