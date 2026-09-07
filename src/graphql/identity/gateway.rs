//! Gateway provider using the existing OIDC validator and role mapping.
use super::{OidcConfig, OidcValidator};
use crate::gateway::{
    AuthError, AuthProvider, BackendCredential, Credentials, Identity, RequestContext,
};

/// Strict bearer provider for public gateways. Ambient identity headers and
/// cookies cannot enter this adapter; use an explicit session provider for UI.
pub struct OidcGatewayProvider {
    validator: OidcValidator,
    revision: String,
}

impl OidcGatewayProvider {
    /// Reuse one validator/JWKS cache across all routes. The revision identifies
    /// this provider configuration and must change when its policy changes.
    pub fn new(config: OidcConfig, revision: impl Into<String>) -> Self {
        Self {
            validator: OidcValidator::new(config),
            revision: revision.into(),
        }
    }
}

impl AuthProvider for OidcGatewayProvider {
    async fn authenticate(&self, credentials: &Credentials) -> Result<RequestContext, AuthError> {
        let Some(authorization) = &credentials.authorization else {
            return RequestContext::from_provider(None, &self.revision, BackendCredential::None)
                .map_err(|_| AuthError::Unavailable);
        };
        let (scheme, token) = authorization
            .trim()
            .split_once(' ')
            .ok_or(AuthError::Unauthorized)?;
        let token = token.trim();
        if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
            return Err(AuthError::Unauthorized);
        }
        let session = self
            .validator
            .validate_and_map_async(token)
            .await
            .map_err(|_| AuthError::Unauthorized)?;
        // Use only verified claims for the lease; decoding an unverified JWT is
        // never sufficient to certify subject, scope or expiry. The second
        // validation reads the same cached keys, without another network fetch.
        let claims = self
            .validator
            .validate_token(token)
            .map_err(|_| AuthError::Unauthorized)?;
        let issuer = claims["iss"].as_str().ok_or(AuthError::Unauthorized)?;
        let subject = claims["sub"].as_str().ok_or(AuthError::Unauthorized)?;
        let expires = claims["exp"].as_u64().ok_or(AuthError::Unauthorized)?;
        let identity = Identity::verified(
            issuer,
            subject,
            session.roles().into_iter().map(str::to_owned).collect(),
            expires,
        )
        .map_err(|_| AuthError::Unauthorized)?;
        RequestContext::from_provider(
            Some(identity),
            &self.revision,
            BackendCredential::Bearer(token.into()),
        )
        .map_err(|_| AuthError::Unavailable)
    }
}
