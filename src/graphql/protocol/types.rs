use serde::{Deserialize, Serialize};

use crate::graphql::client_manifest::DISTRIBUTED_CLIENT_PROTOCOL_VERSION;

use super::OpaqueProtocolToken;

/// Stable command lifecycle vocabulary exposed to generated clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DistributedCommandState {
    InProgress,
    Succeeded,
    SucceededPendingProjection,
    /// Terminal state for an atomic command (same-tx read-model row sealed).
    Atomic,
    Rejected,
    ProjectionFailed,
    Expired,
    Unknown,
}

/// Typed Rust consistency guarantee, serialized independently of domain data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DistributedCommandConsistency {
    Succeeded,
    Eventual,
    Atomic,
}

/// Current-scope handling for durable projection work that was committed
/// against an older cache/schema scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DistributedProjectionDisposition {
    Revalidate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedProjectionExpectation {
    pub(crate) projection: String,
    pub(crate) model: String,
    pub(crate) scope_token: OpaqueProtocolToken,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedCommandMetadata {
    pub(crate) command_id: String,
    pub(crate) causation_id: String,
    pub(crate) state: DistributedCommandState,
    pub(crate) consistency: DistributedCommandConsistency,
    /// A lifecycle-safe status can acknowledge historical projector evidence
    /// without making its old-scope delta applicable to this client.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) projection_disposition: Option<DistributedProjectionDisposition>,
    pub(crate) expects: Vec<DistributedProjectionExpectation>,
    /// Exact role-safe actual delta and opaque modeled obligations. Legacy
    /// compiler confirmations remain in `expects` during the migration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) projection: Option<super::CommandProjectionMetadataV1>,
    /// Exact finite obligations already observed for this authorized command.
    /// Tokens are identical to their matching `expects` entry, so the client
    /// can retire optimism without reconstructing any hidden scope material.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) observations: Vec<DistributedProjectionObservation>,
    /// Exact same-transaction revision evidence retained in the durable replay
    /// envelope. Async commands leave this empty and confirm through expects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) records: Vec<DistributedRecordRevision>,
}

/// Comparable revision for one record in a GraphQL response.
///
/// `scopeToken` is stable across operations and revisions but contains no
/// model key, tenant, topology, or partition plaintext. Positions are decimal
/// strings so JavaScript never coerces a future 64-bit value through `number`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedRecordRevision {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) path: Option<Vec<String>>,
    pub(crate) model: String,
    pub(crate) scope_token: OpaqueProtocolToken,
    pub(crate) incarnation: String,
    pub(crate) revision: String,
    pub(crate) tombstone: bool,
}

/// One causation observation whose scope token is byte-for-byte comparable to
/// the corresponding command receipt obligation token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedProjectionObservation {
    pub(crate) causation_id: String,
    pub(crate) projection: String,
    pub(crate) model: String,
    pub(crate) scope_token: OpaqueProtocolToken,
}

/// Client-returned live cursor. `projection` selects one finite, role-visible
/// static projector scope; the token authenticates every hidden scope field,
/// operation instance, generation, and position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedLiveCursor {
    pub(crate) projection: String,
    pub(crate) position: String,
    pub(crate) token: OpaqueProtocolToken,
}

/// Bounded interpretation of the optional client resume extension.
///
/// Malformed or unverifiable input is intentionally represented as `Invalid`
/// rather than exposed as a request error: live execution must fall back to a
/// fresh snapshot and must never merge against an untrusted cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum RequestedLiveResume {
    #[default]
    Absent,
    Invalid,
    Cursors(Vec<DistributedLiveCursor>),
}

/// Comparable position for one projector/partition member of an exact query
/// index snapshot. Positions from different `scopeToken`s are incomparable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedIndexRevision {
    pub(crate) projection: String,
    pub(crate) scope_token: OpaqueProtocolToken,
    pub(crate) position: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) resume: Option<DistributedLiveCursor>,
}

/// Evidence attached to one exact operation instance.
///
/// These two completeness dimensions are deliberately independent:
///
/// - `recordsComplete=false` means some returned normalized record lacks
///   usable causal revision evidence.
/// - `indexesComparable=false` means the server cannot expose one safe,
///   complete projector-position vector for this authorized query.
///
/// A row-filtered query can therefore return complete authorized record
/// evidence while keeping its partition-wide index positions private. Clients
/// may treat that server-authorized payload as exact renderable membership, but
/// must not use it for causal index comparison, live handoff/resume, local
/// membership proofs, observations, or optimistic confirmation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedQuerySnapshot {
    pub(crate) scope_token: OpaqueProtocolToken,
    pub(crate) records_complete: bool,
    pub(crate) indexes_comparable: bool,
    pub(crate) records: Vec<DistributedRecordRevision>,
    pub(crate) indexes: Vec<DistributedIndexRevision>,
    pub(crate) observations: Vec<DistributedProjectionObservation>,
}

impl DistributedQuerySnapshot {
    pub(super) fn discard_incomparable_index_evidence(&mut self) {
        if self.indexes_comparable {
            return;
        }
        self.indexes.clear();
        self.observations.clear();
    }
}

/// Per-frame resumability decision for a live operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedLiveMetadata {
    pub(crate) supported: bool,
    pub(crate) reset: bool,
    pub(crate) cursors: Vec<DistributedLiveCursor>,
}

/// One server-derived value for a descriptor already present in the selected
/// static client surface. Values are valid only under this envelope's exact
/// cache scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedTrustedPreset {
    pub(crate) name: String,
    pub(crate) codec: String,
    pub(crate) value: serde_json::Value,
}

/// Canonical contents of GraphQL's top-level `extensions.distributed`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedEnvelopeV1 {
    pub(crate) protocol_version: u32,
    pub(crate) schema_hash: String,
    pub(crate) authorization_generation: String,
    pub(crate) cache_scope: OpaqueProtocolToken,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) generation: Option<DistributedGenerationEnvelope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) command: Option<DistributedCommandMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot: Option<DistributedQuerySnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) live: Option<DistributedLiveMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) trusted_presets: Vec<DistributedTrustedPreset>,
}

impl DistributedEnvelopeV1 {
    pub(crate) fn new(
        schema_hash: impl Into<String>,
        authorization_generation: impl Into<String>,
        cache_scope: OpaqueProtocolToken,
        operation: Option<String>,
    ) -> Self {
        Self {
            protocol_version: DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
            schema_hash: schema_hash.into(),
            authorization_generation: authorization_generation.into(),
            cache_scope,
            generation: DistributedGenerationEnvelope::from_environment(),
            operation,
            command: None,
            snapshot: None,
            live: None,
            trusted_presets: Vec::new(),
        }
    }

    pub(crate) fn with_trusted_presets(
        mut self,
        trusted_presets: Vec<DistributedTrustedPreset>,
    ) -> Self {
        self.trusted_presets = trusted_presets;
        self
    }
}

/// Process generation advertised independently from domain result objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DistributedGenerationEnvelope {
    version: u32,
    generation_id: String,
    release_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    topology_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compatibility_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    member_id: Option<String>,
}

impl DistributedGenerationEnvelope {
    fn from_environment() -> Option<Self> {
        if let Some(member) = crate::microsvc::lifecycle::membership_from_environment() {
            let active = member.generation;
            return Some(Self {
                version: 1,
                generation_id: active.generation_id,
                release_id: active.release_id,
                topology_id: Some(active.topology_id),
                compatibility_id: Some(active.compatibility_id),
                member_id: optional_environment_identity("DISTRIBUTED_MEMBER_ID"),
            });
        }
        let generation_id = std::env::var("DISTRIBUTED_GENERATION_ID").ok()?;
        let release_id = std::env::var("DISTRIBUTED_RELEASE_ID").ok()?;
        if !bounded_identity(&generation_id) || !bounded_identity(&release_id) {
            return None;
        }
        Some(Self {
            version: 1,
            generation_id,
            release_id,
            topology_id: optional_environment_identity("DISTRIBUTED_TOPOLOGY_ID"),
            compatibility_id: optional_environment_identity("DISTRIBUTED_COMPATIBILITY_ID"),
            member_id: optional_environment_identity("DISTRIBUTED_MEMBER_ID"),
        })
    }
}

fn optional_environment_identity(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| bounded_identity(value))
}

fn bounded_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value == value.trim()
        && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod generation_tests {
    use super::*;

    #[test]
    fn generation_envelope_is_camel_case_and_contains_no_process_environment() {
        let envelope = DistributedGenerationEnvelope {
            version: 1,
            generation_id: "sha256:generation".into(),
            release_id: "sha256:release".into(),
            topology_id: Some("sha256:topology".into()),
            compatibility_id: Some("sha256:compatibility".into()),
            member_id: Some("api".into()),
        };
        assert_eq!(
            serde_json::to_value(envelope).unwrap(),
            serde_json::json!({
                "version": 1,
                "generationId": "sha256:generation",
                "releaseId": "sha256:release",
                "topologyId": "sha256:topology",
                "compatibilityId": "sha256:compatibility",
                "memberId": "api"
            })
        );
    }

    #[test]
    fn generation_environment_identities_are_bounded() {
        assert!(bounded_identity("sha256:stable"));
        assert!(!bounded_identity(""));
        assert!(!bounded_identity(" leading"));
        assert!(!bounded_identity("line\nbreak"));
        assert!(!bounded_identity(&"x".repeat(513)));
    }
}
