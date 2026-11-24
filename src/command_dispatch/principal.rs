//! The authentication proof a typed command is dispatched under.
//!
//! The type lives here so durable dispatch does not depend on the `graphql`
//! feature. Verifying a bearer token into a principal is OIDC's job
//! (`graphql::identity`).

use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum VerifiedAudienceSource {
    Aud,
    #[cfg(feature = "graphql")]
    AuthorizedParty,
    #[cfg(feature = "graphql")]
    ClientId,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct VerifiedAudience {
    pub(crate) source: VerifiedAudienceSource,
    pub(crate) value: String,
}

/// Where a [`VerifiedPrincipal`] came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provenance {
    /// A bearer token validated against the issuer's keys.
    Oidc,
    /// Minted by the framework for work it starts itself, or rebuilt by a
    /// service from a subject its own authenticated transport delivered.
    Internal,
}

/// Issuer recorded on internal principals. It is not a URL an identity
/// provider can be configured with, so it cannot collide with a token issuer.
const INTERNAL_ISSUER: &str = "distributed:internal";
const INTERNAL_AUDIENCE: &str = "distributed:internal";

/// A subject that cannot name a principal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidPrincipal;

impl std::fmt::Display for InvalidPrincipal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("principal subject must not be empty")
    }
}

impl std::error::Error for InvalidPrincipal {}

/// Authentication proof admitted to durable causal dispatch.
///
/// There is no deserializer and no constructor from a [`Session`](crate::microsvc::Session) or header
/// map. It is obtained from a validated bearer token, or from
/// [`Self::internal`] by code that has already authenticated the subject.
#[derive(Clone, PartialEq, Eq)]
pub struct VerifiedPrincipal {
    provenance: Provenance,
    issuer: String,
    subject: String,
    audiences: Vec<VerifiedAudience>,
    tenant_partitions: Vec<(String, Value)>,
    expires_at: Option<u64>,
}

impl VerifiedPrincipal {
    /// A principal the framework vouches for itself: a schedule firing, or a
    /// subject carried by a service-to-service hop the caller has already
    /// authenticated. Never call this with a value a client can choose.
    pub fn internal(subject: &str) -> Result<Self, InvalidPrincipal> {
        if subject.trim().is_empty() {
            return Err(InvalidPrincipal);
        }
        Ok(Self {
            provenance: Provenance::Internal,
            issuer: INTERNAL_ISSUER.into(),
            subject: subject.to_string(),
            audiences: vec![VerifiedAudience {
                source: VerifiedAudienceSource::Aud,
                value: INTERNAL_AUDIENCE.into(),
            }],
            tenant_partitions: Vec::new(),
            expires_at: None,
        })
    }

    /// A principal proved by a bearer token the caller has validated against
    /// its issuer's keys.
    #[cfg(feature = "graphql")]
    pub(crate) fn oidc(
        issuer: String,
        subject: String,
        audiences: Vec<VerifiedAudience>,
        tenant_partitions: Vec<(String, Value)>,
        expires_at: Option<u64>,
    ) -> Self {
        Self {
            provenance: Provenance::Oidc,
            issuer,
            subject,
            audiences,
            tenant_partitions,
            expires_at,
        }
    }

    /// Test-only OIDC principal. Not a production identity constructor.
    pub fn test_oidc(issuer: &str, subject: &str, audiences: &[&str]) -> Self {
        assert!(
            !issuer.trim().is_empty(),
            "test OIDC issuer must not be empty"
        );
        assert!(
            !subject.trim().is_empty(),
            "test OIDC subject must not be empty"
        );
        assert!(
            !audiences.is_empty() && audiences.iter().all(|value| !value.trim().is_empty()),
            "test OIDC audiences must contain only non-empty values"
        );
        Self {
            provenance: Provenance::Oidc,
            issuer: normalize_issuer(issuer),
            subject: subject.to_string(),
            audiences: audiences
                .iter()
                .map(|value| VerifiedAudience {
                    source: VerifiedAudienceSource::Aud,
                    value: (*value).to_string(),
                })
                .collect(),
            tenant_partitions: Vec::new(),
            expires_at: None,
        }
    }

    #[cfg(all(test, feature = "graphql"))]
    pub(crate) fn issuer(&self) -> &str {
        &self.issuer
    }

    #[cfg(all(test, feature = "graphql"))]
    pub(crate) fn subject(&self) -> &str {
        &self.subject
    }

    /// Credential expiry used to bound admitted delivery work. Trusted local
    /// principals without an expiring credential return None.
    pub fn expires_at(&self) -> Option<u64> {
        self.expires_at
    }

    #[cfg(feature = "graphql")]
    pub(crate) fn subject_matches(&self, subject: &str) -> bool {
        self.subject == subject
    }

    /// Versioned, domain-separated partition for one exact service identity.
    pub(crate) fn partition_for_service(&self, service_id: &str) -> String {
        let mut audiences = self
            .audiences
            .iter()
            .map(|audience| audience.value.as_str())
            .collect::<Vec<_>>();
        audiences.sort_unstable();
        audiences.dedup();
        let canonical = serde_json::to_vec(&serde_json::json!({
            "domain": "distributed.principal-partition",
            "version": 1,
            "service_id": service_id,
            "issuer": self.issuer,
            "subject": self.subject,
            "audiences": audiences,
            "tenant_partitions": self.tenant_partitions,
        }))
        .expect("verified principal partition values are JSON serializable");
        format!("v1:sha256:{:x}", Sha256::digest(canonical))
    }
}

impl std::fmt::Debug for VerifiedPrincipal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedPrincipal")
            .field("provenance", &self.provenance)
            .field("issuer", &self.issuer)
            .field("subject", &"[redacted]")
            .field("audiences", &"[redacted]")
            .field("tenant_partitions", &"[redacted]")
            .finish()
    }
}

pub(crate) fn normalize_issuer(iss: &str) -> String {
    iss.trim_end_matches('/').to_string()
}
