use std::fmt;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::graphql::client_manifest::DISTRIBUTED_CLIENT_PROTOCOL_VERSION;

const TOKEN_FORMAT_VERSION: &str = "v1";
const TOKEN_MAC_BYTES: usize = 32;
const TOKEN_DOMAIN: &[u8] = b"distributed.graphql.protocol-token";
const MAX_TOKEN_MATERIAL_BYTES: usize = 1024 * 1024;
/// Maximum accepted opaque protocol token size.
///
/// Current HMAC tokens are deliberately much smaller. The wire bound remains
/// 4 KiB so future keyed codecs can evolve without allowing unbounded client
/// input or diverging from the projection partition contract.
pub(crate) const MAX_OPAQUE_TOKEN_BYTES: usize = 4 * 1024;

/// Maximum resumable projector partitions carried in one live operation.
/// Request parsing and response generation share this bound so the server
/// never emits a cursor set that a conforming client must reject.
pub(crate) const MAX_LIVE_RESUME_CURSORS: usize = 64;

/// One server-owned token. Its string contents have no public structure.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct OpaqueProtocolToken(pub(super) String);

impl OpaqueProtocolToken {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse one bounded framework token supplied back by a client.
    ///
    /// This validates only the canonical outer representation. Callers must
    /// still verify its purpose and server-owned material before using it.
    pub(crate) fn parse(value: &str) -> Result<Self, ProtocolTokenError> {
        if value.is_empty() || value.len() > MAX_OPAQUE_TOKEN_BYTES {
            return Err(ProtocolTokenError::Malformed);
        }
        let mut segments = value.split('.');
        let version = segments.next().ok_or(ProtocolTokenError::Malformed)?;
        let purpose = segments.next().ok_or(ProtocolTokenError::Malformed)?;
        let encoded_mac = segments.next().ok_or(ProtocolTokenError::Malformed)?;
        if segments.next().is_some()
            || version != TOKEN_FORMAT_VERSION
            || ProtocolTokenPurpose::from_label(purpose).is_none()
        {
            return Err(ProtocolTokenError::Malformed);
        }
        let supplied = URL_SAFE_NO_PAD
            .decode(encoded_mac)
            .map_err(|_| ProtocolTokenError::Malformed)?;
        if supplied.len() != TOKEN_MAC_BYTES || URL_SAFE_NO_PAD.encode(&supplied) != encoded_mac {
            return Err(ProtocolTokenError::Malformed);
        }
        Ok(Self(value.to_string()))
    }
}

impl fmt::Debug for OpaqueProtocolToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OpaqueProtocolToken([redacted])")
    }
}

/// Domain separation for tokens that are intentionally not interchangeable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtocolTokenPurpose {
    CacheScope,
    ProjectionLifecycle,
    ProjectionObligation,
    ProjectionObservation,
    PartitionId,
    RecordRevision,
    QuerySnapshot,
    QueryIndex,
    LiveResume,
}

impl ProtocolTokenPurpose {
    const fn label(self) -> &'static str {
        match self {
            Self::CacheScope => "cache-scope",
            Self::ProjectionLifecycle => "projection-lifecycle",
            Self::ProjectionObligation => "projection-obligation",
            Self::ProjectionObservation => "projection-observation",
            Self::PartitionId => "projection-partition",
            Self::RecordRevision => "record-revision",
            Self::QuerySnapshot => "query-snapshot",
            Self::QueryIndex => "query-index",
            Self::LiveResume => "live-resume",
        }
    }

    fn from_label(value: &str) -> Option<Self> {
        match value {
            "cache-scope" => Some(Self::CacheScope),
            "projection-lifecycle" => Some(Self::ProjectionLifecycle),
            "projection-obligation" => Some(Self::ProjectionObligation),
            "projection-observation" => Some(Self::ProjectionObservation),
            "projection-partition" => Some(Self::PartitionId),
            "record-revision" => Some(Self::RecordRevision),
            "query-snapshot" => Some(Self::QuerySnapshot),
            "query-index" => Some(Self::QueryIndex),
            "live-resume" => Some(Self::LiveResume),
            _ => None,
        }
    }
}

/// Stable deployment key for deterministic opaque protocol tokens.
///
/// The key is deliberately exact-sized and redacted from `Debug`. Deployments
/// must preserve it across replicas and restarts whenever they want existing
/// cache/resume tokens to remain comparable.
#[derive(Clone)]
pub(crate) struct ProtocolTokenCodec {
    key: [u8; 32],
}

impl ProtocolTokenCodec {
    pub(crate) fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Mint a deterministic token from a canonical serialization.
    ///
    /// Callers must use structs, tuples, ordered maps, or already-canonical
    /// protocol values. Unordered application maps are not accepted protocol
    /// material.
    pub(crate) fn issue<T: Serialize>(
        &self,
        purpose: ProtocolTokenPurpose,
        material: &T,
    ) -> Result<OpaqueProtocolToken, ProtocolTokenError> {
        let bytes =
            serde_json::to_vec(material).map_err(|_| ProtocolTokenError::InvalidMaterial)?;
        self.issue_bytes(purpose, &bytes)
    }

    pub(crate) fn issue_bytes(
        &self,
        purpose: ProtocolTokenPurpose,
        canonical_material: &[u8],
    ) -> Result<OpaqueProtocolToken, ProtocolTokenError> {
        if canonical_material.is_empty() || canonical_material.len() > MAX_TOKEN_MATERIAL_BYTES {
            return Err(ProtocolTokenError::InvalidMaterial);
        }
        let digest = self.mac(purpose, canonical_material);
        Ok(OpaqueProtocolToken(format!(
            "{TOKEN_FORMAT_VERSION}.{}.{}",
            purpose.label(),
            URL_SAFE_NO_PAD.encode(digest)
        )))
    }

    /// Verify a token against the expected purpose and canonical material.
    ///
    /// Tokens carry no plaintext payload, so successful verification proves
    /// only equality with server-owned expected material.
    pub(crate) fn verify<T: Serialize>(
        &self,
        token: &OpaqueProtocolToken,
        purpose: ProtocolTokenPurpose,
        material: &T,
    ) -> Result<(), ProtocolTokenError> {
        let bytes =
            serde_json::to_vec(material).map_err(|_| ProtocolTokenError::InvalidMaterial)?;
        self.verify_bytes(token, purpose, &bytes)
    }

    pub(crate) fn verify_bytes(
        &self,
        token: &OpaqueProtocolToken,
        purpose: ProtocolTokenPurpose,
        canonical_material: &[u8],
    ) -> Result<(), ProtocolTokenError> {
        if canonical_material.is_empty() || canonical_material.len() > MAX_TOKEN_MATERIAL_BYTES {
            return Err(ProtocolTokenError::InvalidMaterial);
        }
        let mut segments = token.as_str().split('.');
        let version = segments.next().ok_or(ProtocolTokenError::Malformed)?;
        let encoded_purpose = segments.next().ok_or(ProtocolTokenError::Malformed)?;
        let encoded_mac = segments.next().ok_or(ProtocolTokenError::Malformed)?;
        if segments.next().is_some()
            || version != TOKEN_FORMAT_VERSION
            || encoded_purpose != purpose.label()
        {
            return Err(ProtocolTokenError::Malformed);
        }
        let supplied = URL_SAFE_NO_PAD
            .decode(encoded_mac)
            .map_err(|_| ProtocolTokenError::Malformed)?;
        if supplied.len() != TOKEN_MAC_BYTES {
            return Err(ProtocolTokenError::Malformed);
        }
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC-SHA256 accepts a 32-byte key");
        update_mac(&mut mac, purpose, canonical_material);
        mac.verify_slice(&supplied)
            .map_err(|_| ProtocolTokenError::Mismatch)
    }

    fn mac(&self, purpose: ProtocolTokenPurpose, material: &[u8]) -> [u8; TOKEN_MAC_BYTES] {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC-SHA256 accepts a 32-byte key");
        update_mac(&mut mac, purpose, material);
        mac.finalize().into_bytes().into()
    }
}

impl fmt::Debug for ProtocolTokenCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtocolTokenCodec([redacted])")
    }
}

fn update_mac(mac: &mut Hmac<Sha256>, purpose: ProtocolTokenPurpose, material: &[u8]) {
    mac.update(TOKEN_DOMAIN);
    mac.update(&DISTRIBUTED_CLIENT_PROTOCOL_VERSION.to_be_bytes());
    update_segment(mac, purpose.label().as_bytes());
    update_segment(mac, material);
}

fn update_segment(mac: &mut Hmac<Sha256>, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtocolTokenError {
    InvalidMaterial,
    Malformed,
    Mismatch,
}

impl fmt::Display for ProtocolTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidMaterial => "invalid protocol token material",
            Self::Malformed => "malformed protocol token",
            Self::Mismatch => "protocol token does not match the expected scope",
        })
    }
}

impl std::error::Error for ProtocolTokenError {}
