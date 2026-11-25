use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::{Error as _, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::OpaqueProtocolToken;
use crate::graphql::projection_delta::{ProjectionDelta, ProjectionDeltaError};
use crate::MAX_DOMAIN_EVENT_BODY_BYTES;

/// Version of the command response/status projection metadata envelope.
pub(crate) const COMMAND_PROJECTION_METADATA_WIRE_VERSION: u16 = 1;
/// Maximum exact causal obligations carried by one command.
pub(crate) const MAX_COMMAND_PROJECTION_OBLIGATIONS: usize = 128;

/// MAC-bound immutable deployment identity for one delta projection.
///
/// The lifecycle state is deliberately excluded so an exact Active binding
/// can become Draining without making its already-committed work invisible.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CommandProjectionLifecycleProofV1 {
    pub(crate) projection_ref: u32,
    pub(crate) token: OpaqueProtocolToken,
}

/// One exact selected projector obligation.
///
/// `projection_ref` indexes the role-safe delta projection inventory. The
/// scope remains an authenticated opaque token; model keys, logical
/// partitions, physical plans, and raw events are never persisted here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CommandProjectionObligationV1 {
    pub(crate) projection_ref: u32,
    /// Exact role-safe output model observed by this physical scope.
    ///
    /// This is required for multi-table projections and edge effects, where a
    /// program-level label cannot identify which output fence was observed.
    pub(crate) model: String,
    pub(crate) scope_token: OpaqueProtocolToken,
}

/// Replay-stable command projection metadata persisted atomically with the
/// terminal command outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct CommandProjectionMetadataV1 {
    pub(crate) wire_version: u16,
    pub(crate) issued_at_unix_ms: u64,
    pub(crate) expires_at_unix_ms: u64,
    pub(crate) delta: ProjectionDelta,
    #[serde(deserialize_with = "deserialize_bounded_lifecycle_proofs")]
    pub(crate) lifecycle_proofs: Vec<CommandProjectionLifecycleProofV1>,
    #[serde(deserialize_with = "deserialize_bounded_obligations")]
    pub(crate) obligations: Vec<CommandProjectionObligationV1>,
    pub(crate) revalidate: bool,
}

impl CommandProjectionMetadataV1 {
    pub(crate) fn try_new(
        issued_at_unix_ms: u64,
        expires_at_unix_ms: u64,
        delta: ProjectionDelta,
        mut lifecycle_proofs: Vec<CommandProjectionLifecycleProofV1>,
        mut obligations: Vec<CommandProjectionObligationV1>,
        revalidate: bool,
    ) -> Result<Self, CommandProjectionMetadataError> {
        if obligations.len() > MAX_COMMAND_PROJECTION_OBLIGATIONS {
            return Err(CommandProjectionMetadataError::TooManyObligations {
                len: obligations.len(),
                max: MAX_COMMAND_PROJECTION_OBLIGATIONS,
            });
        }
        lifecycle_proofs.sort_by_key(|proof| proof.projection_ref);
        lifecycle_proofs.dedup();
        let mut scope_models = std::collections::BTreeMap::new();
        for obligation in &obligations {
            let key = (obligation.projection_ref, obligation.scope_token.as_str());
            if scope_models
                .insert(key, obligation.model.as_str())
                .is_some_and(|model| model != obligation.model)
            {
                return Err(CommandProjectionMetadataError::ConflictingObligationScope);
            }
        }
        obligations.sort_by(|left, right| {
            left.projection_ref
                .cmp(&right.projection_ref)
                .then_with(|| left.model.cmp(&right.model))
                .then_with(|| left.scope_token.as_str().cmp(right.scope_token.as_str()))
        });
        obligations.dedup();
        let metadata = Self {
            wire_version: COMMAND_PROJECTION_METADATA_WIRE_VERSION,
            issued_at_unix_ms,
            expires_at_unix_ms,
            delta,
            lifecycle_proofs,
            obligations,
            revalidate,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    pub(crate) fn from_json(bytes: &[u8]) -> Result<Self, CommandProjectionMetadataError> {
        if bytes.len() > MAX_DOMAIN_EVENT_BODY_BYTES {
            return Err(CommandProjectionMetadataError::BodyTooLarge {
                len: bytes.len(),
                max: MAX_DOMAIN_EVENT_BODY_BYTES,
            });
        }
        let metadata = serde_json::from_slice::<Self>(bytes)
            .map_err(|error| CommandProjectionMetadataError::InvalidWire(error.to_string()))?;
        metadata.validate()?;
        if metadata.canonical_bytes()? != bytes {
            return Err(CommandProjectionMetadataError::NonCanonical);
        }
        Ok(metadata)
    }

    pub(crate) fn canonical_bytes(&self) -> Result<Vec<u8>, CommandProjectionMetadataError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| CommandProjectionMetadataError::InvalidWire(error.to_string()))?;
        if bytes.len() > MAX_DOMAIN_EVENT_BODY_BYTES {
            return Err(CommandProjectionMetadataError::BodyTooLarge {
                len: bytes.len(),
                max: MAX_DOMAIN_EVENT_BODY_BYTES,
            });
        }
        Ok(bytes)
    }

    pub(crate) fn validate_not_expired(
        &self,
        now_unix_ms: u64,
    ) -> Result<(), CommandProjectionMetadataError> {
        self.validate()?;
        if now_unix_ms >= self.expires_at_unix_ms {
            return Err(CommandProjectionMetadataError::Expired);
        }
        Ok(())
    }

    pub(crate) fn expires_at(&self) -> Result<SystemTime, CommandProjectionMetadataError> {
        self.validate()?;
        UNIX_EPOCH
            .checked_add(Duration::from_millis(self.expires_at_unix_ms))
            .ok_or(CommandProjectionMetadataError::InvalidLifetime)
    }

    fn validate(&self) -> Result<(), CommandProjectionMetadataError> {
        if self.wire_version != COMMAND_PROJECTION_METADATA_WIRE_VERSION {
            return Err(CommandProjectionMetadataError::UnsupportedVersion(
                self.wire_version,
            ));
        }
        if self.issued_at_unix_ms >= self.expires_at_unix_ms {
            return Err(CommandProjectionMetadataError::InvalidLifetime);
        }
        self.delta
            .canonical_bytes()
            .map_err(CommandProjectionMetadataError::Delta)?;
        if self.lifecycle_proofs.len() != self.delta.projections.len()
            || self
                .lifecycle_proofs
                .iter()
                .enumerate()
                .any(|(index, proof)| proof.projection_ref as usize != index)
        {
            return Err(CommandProjectionMetadataError::InvalidLifecycleProofs);
        }
        for proof in &self.lifecycle_proofs {
            let parsed = OpaqueProtocolToken::parse(proof.token.as_str())
                .map_err(|_| CommandProjectionMetadataError::InvalidLifecycleProofs)?;
            if parsed.as_str().split('.').nth(1) != Some("projection-lifecycle") {
                return Err(CommandProjectionMetadataError::InvalidLifecycleProofs);
            }
        }
        if self.obligations.len() > MAX_COMMAND_PROJECTION_OBLIGATIONS {
            return Err(CommandProjectionMetadataError::TooManyObligations {
                len: self.obligations.len(),
                max: MAX_COMMAND_PROJECTION_OBLIGATIONS,
            });
        }
        if !self.revalidate && !self.delta.recoveries.is_empty() {
            return Err(CommandProjectionMetadataError::MissingRevalidation);
        }

        let mut previous: Option<(u32, &str, &str)> = None;
        let mut scope_models = std::collections::BTreeMap::new();
        for obligation in &self.obligations {
            if obligation.projection_ref as usize >= self.delta.projections.len()
                || obligation.model.trim().is_empty()
                || obligation.model.len() > 255
                || !self.delta.operations.iter().any(|operation| {
                    operation
                        .projection_refs
                        .binary_search(&obligation.projection_ref)
                        .is_ok()
                        && match &operation.mutation {
                            crate::graphql::projection_delta::ProjectionDeltaMutation::Upsert {
                                scope,
                                ..
                            }
                            | crate::graphql::projection_delta::ProjectionDeltaMutation::Patch {
                                scope,
                                ..
                            }
                            | crate::graphql::projection_delta::ProjectionDeltaMutation::Delete {
                                scope,
                            } => scope.model == obligation.model,
                            crate::graphql::projection_delta::ProjectionDeltaMutation::Link {
                                ..
                            }
                            | crate::graphql::projection_delta::ProjectionDeltaMutation::Unlink {
                                ..
                            } => true,
                            crate::graphql::projection_delta::ProjectionDeltaMutation::InvalidateModel {
                                ..
                            }
                            | crate::graphql::projection_delta::ProjectionDeltaMutation::InvalidateRelationship {
                                ..
                            } => false,
                        }
                })
            {
                return Err(CommandProjectionMetadataError::UnknownProjectionReference);
            }
            let parsed = OpaqueProtocolToken::parse(obligation.scope_token.as_str())
                .map_err(|_| CommandProjectionMetadataError::InvalidScopeToken)?;
            if parsed.as_str().split('.').nth(1) != Some("projection-obligation") {
                return Err(CommandProjectionMetadataError::InvalidScopeToken);
            }
            let scope_key = (obligation.projection_ref, obligation.scope_token.as_str());
            if scope_models
                .insert(scope_key, obligation.model.as_str())
                .is_some_and(|model| model != obligation.model)
            {
                return Err(CommandProjectionMetadataError::ConflictingObligationScope);
            }
            let current = (
                obligation.projection_ref,
                obligation.model.as_str(),
                obligation.scope_token.as_str(),
            );
            if previous.is_some_and(|previous| previous >= current) {
                return Err(CommandProjectionMetadataError::NonCanonical);
            }
            previous = Some(current);
        }
        Ok(())
    }
}

fn deserialize_bounded_lifecycle_proofs<'de, D>(
    deserializer: D,
) -> Result<Vec<CommandProjectionLifecycleProofV1>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoundedLifecycleProofsVisitor;

    impl<'de> Visitor<'de> for BoundedLifecycleProofsVisitor {
        type Value = Vec<CommandProjectionLifecycleProofV1>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {MAX_COMMAND_PROJECTION_OBLIGATIONS} projection lifecycle proofs"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut proofs = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or_default()
                    .min(MAX_COMMAND_PROJECTION_OBLIGATIONS),
            );
            while proofs.len() < MAX_COMMAND_PROJECTION_OBLIGATIONS {
                let Some(proof) = sequence.next_element()? else {
                    return Ok(proofs);
                };
                proofs.push(proof);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(format_args!(
                    "command projection metadata has more than \
                     {MAX_COMMAND_PROJECTION_OBLIGATIONS} lifecycle proofs"
                )));
            }
            Ok(proofs)
        }
    }

    deserializer.deserialize_seq(BoundedLifecycleProofsVisitor)
}

fn deserialize_bounded_obligations<'de, D>(
    deserializer: D,
) -> Result<Vec<CommandProjectionObligationV1>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoundedObligationsVisitor;

    impl<'de> Visitor<'de> for BoundedObligationsVisitor {
        type Value = Vec<CommandProjectionObligationV1>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {MAX_COMMAND_PROJECTION_OBLIGATIONS} command projection obligations"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence
                .size_hint()
                .is_some_and(|len| len > MAX_COMMAND_PROJECTION_OBLIGATIONS)
            {
                return Err(A::Error::custom(format_args!(
                    "command projection metadata has more than \
                     {MAX_COMMAND_PROJECTION_OBLIGATIONS} obligations"
                )));
            }
            let mut obligations = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or_default()
                    .min(MAX_COMMAND_PROJECTION_OBLIGATIONS),
            );
            while obligations.len() < MAX_COMMAND_PROJECTION_OBLIGATIONS {
                let Some(obligation) = sequence.next_element()? else {
                    return Ok(obligations);
                };
                obligations.push(obligation);
            }
            // Probe one additional element as ignored streaming input. This
            // detects an oversized array without materializing obligation 129
            // or any remaining hostile tail.
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(format_args!(
                    "command projection metadata has more than \
                     {MAX_COMMAND_PROJECTION_OBLIGATIONS} obligations"
                )));
            }
            Ok(obligations)
        }
    }

    deserializer.deserialize_seq(BoundedObligationsVisitor)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CommandProjectionMetadataError {
    InvalidWire(String),
    UnsupportedVersion(u16),
    InvalidLifetime,
    Expired,
    Delta(ProjectionDeltaError),
    TooManyObligations { len: usize, max: usize },
    UnknownProjectionReference,
    InvalidScopeToken,
    ConflictingObligationScope,
    MissingRevalidation,
    InvalidLifecycleProofs,
    NonCanonical,
    BodyTooLarge { len: usize, max: usize },
}

impl std::fmt::Display for CommandProjectionMetadataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWire(error) => {
                write!(formatter, "invalid command projection metadata: {error}")
            }
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported command projection metadata version `{version}`"
                )
            }
            Self::InvalidLifetime => {
                formatter.write_str("command projection metadata lifetime is invalid")
            }
            Self::Expired => formatter.write_str("command projection metadata has expired"),
            Self::Delta(error) => error.fmt(formatter),
            Self::TooManyObligations { len, max } => write!(
                formatter,
                "command projection metadata has {len} obligations, exceeding {max}"
            ),
            Self::UnknownProjectionReference => formatter
                .write_str("command projection obligation references an unknown projection"),
            Self::InvalidScopeToken => {
                formatter.write_str("command projection obligation scope token is invalid")
            }
            Self::ConflictingObligationScope => formatter.write_str(
                "command projection obligation scope has conflicting output model labels",
            ),
            Self::MissingRevalidation => {
                formatter.write_str("command projection recovery requires explicit revalidation")
            }
            Self::InvalidLifecycleProofs => {
                formatter.write_str("command projection lifecycle proofs are incomplete or invalid")
            }
            Self::NonCanonical => {
                formatter.write_str("command projection metadata is not canonical")
            }
            Self::BodyTooLarge { len, max } => write!(
                formatter,
                "command projection metadata is {len} bytes, exceeding {max}"
            ),
        }
    }
}

impl std::error::Error for CommandProjectionMetadataError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::client_manifest::{
        DISTRIBUTED_CLIENT_MANIFEST_VERSION, DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
    };
    use crate::graphql::projection_delta::{
        DeltaKeyField, DeltaValue, ProjectionDeltaIdentity, ProjectionDeltaMutation,
        ProjectionDeltaOccurrence, ProjectionDeltaOperation, ProjectionDeltaPartition,
        ProjectionDeltaProjectionIdentity, ProjectionDeltaScope, ProjectionDeltaSurfaceIdentity,
        PROJECTION_DELTA_WIRE_VERSION,
    };
    use crate::graphql::protocol::{ProtocolTokenCodec, ProtocolTokenPurpose};

    fn delta() -> ProjectionDelta {
        ProjectionDelta {
            wire_version: PROJECTION_DELTA_WIRE_VERSION,
            identity: ProjectionDeltaIdentity {
                manifest_version: DISTRIBUTED_CLIENT_MANIFEST_VERSION,
                client_protocol_version: DISTRIBUTED_CLIENT_PROTOCOL_VERSION,
                surface: ProjectionDeltaSurfaceIdentity::Role {
                    name: "member".into(),
                },
                schema_fingerprint: "sha256:schema".into(),
                protocol_fingerprint: "sha256:protocol".into(),
                authorization_generation: "auth-generation-1".into(),
                cache_scope_token: "v1.cache-scope.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    .into(),
                command_causation_id: "cause-1".into(),
            },
            projections: vec![ProjectionDeltaProjectionIdentity {
                program_id:
                    "pp1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .into(),
                binding_id:
                    "pb1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .into(),
                epoch: "todos-v1".into(),
                program_ir_version: crate::projection::PROJECTION_PROGRAM_IR_VERSION,
                operation_semantics_version:
                    crate::projection::PROJECTION_OPERATION_SEMANTICS_VERSION,
            }],
            occurrences: vec![ProjectionDeltaOccurrence {
                causation_id: "cause-1".into(),
                ordinal: 0,
                occurrence_id: "occurrence-1".into(),
            }],
            operations: vec![ProjectionDeltaOperation {
                occurrence_ordinal: 0,
                projection_refs: vec![0],
                mutation: ProjectionDeltaMutation::Delete {
                    scope: ProjectionDeltaScope {
                        partition: ProjectionDeltaPartition::Unit,
                        model: "Todos".into(),
                        key: vec![DeltaKeyField {
                            ordinal: 0,
                            field: "todo_id".into(),
                            value: DeltaValue::String("todo-1".into()),
                        }],
                    },
                },
            }],
            recoveries: Vec::new(),
        }
    }

    fn lifecycle_proofs(
        codec: &ProtocolTokenCodec,
        count: usize,
    ) -> Vec<CommandProjectionLifecycleProofV1> {
        (0..count)
            .map(|index| CommandProjectionLifecycleProofV1 {
                projection_ref: index.try_into().unwrap(),
                token: codec
                    .issue(
                        ProtocolTokenPurpose::ProjectionLifecycle,
                        &("projection-lifecycle", index),
                    )
                    .unwrap(),
            })
            .collect()
    }

    #[test]
    fn metadata_round_trips_canonical_bytes_and_deduplicates_scope_tokens() {
        let codec = ProtocolTokenCodec::new([0x31; 32]);
        let token = codec
            .issue(ProtocolTokenPurpose::ProjectionObligation, &("scope", 1))
            .unwrap();
        let metadata = CommandProjectionMetadataV1::try_new(
            100,
            200,
            delta(),
            lifecycle_proofs(&codec, 1),
            vec![
                CommandProjectionObligationV1 {
                    projection_ref: 0,
                    model: "Todos".into(),
                    scope_token: token.clone(),
                },
                CommandProjectionObligationV1 {
                    projection_ref: 0,
                    model: "Todos".into(),
                    scope_token: token,
                },
            ],
            false,
        )
        .unwrap();
        assert_eq!(metadata.obligations.len(), 1);
        let bytes = metadata.canonical_bytes().unwrap();
        assert_eq!(
            CommandProjectionMetadataV1::from_json(&bytes).unwrap(),
            metadata
        );
        assert_eq!(
            metadata.validate_not_expired(200),
            Err(CommandProjectionMetadataError::Expired)
        );
    }

    #[test]
    fn command_projection_metadata_v1_matches_frozen_wire_fixture() {
        let codec = ProtocolTokenCodec::new([0x35; 32]);
        let token = codec
            .issue(
                ProtocolTokenPurpose::ProjectionObligation,
                &("command-projection-metadata-v1", 1),
            )
            .unwrap();
        let lifecycle = codec
            .issue(
                ProtocolTokenPurpose::ProjectionLifecycle,
                &("command-projection-metadata-v1", 1),
            )
            .unwrap();
        let metadata = CommandProjectionMetadataV1::try_new(
            1_700_000_000_000,
            1_700_000_060_000,
            delta(),
            vec![CommandProjectionLifecycleProofV1 {
                projection_ref: 0,
                token: lifecycle,
            }],
            vec![CommandProjectionObligationV1 {
                projection_ref: 0,
                model: "Todos".into(),
                scope_token: token,
            }],
            false,
        )
        .unwrap();
        let expected = metadata.canonical_bytes().unwrap();
        let fixture = include_bytes!("../../../tests/fixtures/command-projection-metadata-v1.json");
        let fixture = fixture.strip_suffix(b"\n").unwrap_or(fixture);
        assert_eq!(fixture, expected);
        assert_eq!(
            CommandProjectionMetadataV1::from_json(fixture).unwrap(),
            metadata
        );
    }

    #[test]
    fn metadata_accepts_128_obligations_and_rejects_129_before_completion() {
        let codec = ProtocolTokenCodec::new([0x32; 32]);
        let obligations = (0..=MAX_COMMAND_PROJECTION_OBLIGATIONS)
            .map(|index| CommandProjectionObligationV1 {
                projection_ref: 0,
                model: "Todos".into(),
                scope_token: codec
                    .issue(
                        ProtocolTokenPurpose::ProjectionObligation,
                        &("scope", index),
                    )
                    .unwrap(),
            })
            .collect::<Vec<_>>();
        CommandProjectionMetadataV1::try_new(
            100,
            200,
            delta(),
            lifecycle_proofs(&codec, 1),
            obligations[..MAX_COMMAND_PROJECTION_OBLIGATIONS].to_vec(),
            false,
        )
        .unwrap();
        assert!(matches!(
            CommandProjectionMetadataV1::try_new(
                100,
                200,
                delta(),
                lifecycle_proofs(&codec, 1),
                obligations,
                false,
            ),
            Err(CommandProjectionMetadataError::TooManyObligations { len: 129, max: 128 })
        ));
    }

    #[test]
    fn wire_decode_streams_no_more_than_128_obligations() {
        let codec = ProtocolTokenCodec::new([0x34; 32]);
        let obligation = CommandProjectionObligationV1 {
            projection_ref: 0,
            model: "Todos".into(),
            scope_token: codec
                .issue(ProtocolTokenPurpose::ProjectionObligation, &("hostile", 1))
                .unwrap(),
        };
        let metadata = CommandProjectionMetadataV1::try_new(
            100,
            200,
            delta(),
            lifecycle_proofs(&codec, 1),
            vec![obligation],
            false,
        )
        .unwrap();
        let mut wire = serde_json::to_value(metadata).unwrap();
        let encoded = wire["obligations"][0].clone();
        wire["obligations"] = serde_json::Value::Array(
            std::iter::repeat_n(encoded.clone(), MAX_COMMAND_PROJECTION_OBLIGATIONS + 1).collect(),
        );
        let error = CommandProjectionMetadataV1::from_json(&serde_json::to_vec(&wire).unwrap())
            .unwrap_err();
        assert!(matches!(
            error,
            CommandProjectionMetadataError::InvalidWire(ref message)
                if message.contains("more than 128 obligations")
        ));

        wire["obligations"] =
            serde_json::Value::Array(std::iter::repeat_n(encoded, 4_096).collect());
        let bytes = serde_json::to_vec(&wire).unwrap();
        assert!(bytes.len() < MAX_DOMAIN_EVENT_BODY_BYTES);
        let error = CommandProjectionMetadataV1::from_json(&bytes).unwrap_err();
        assert!(matches!(
            error,
            CommandProjectionMetadataError::InvalidWire(ref message)
                if message.contains("more than 128 obligations")
        ));
    }

    #[test]
    fn lifecycle_proofs_are_dense_canonical_and_purpose_separated() {
        let codec = ProtocolTokenCodec::new([0x36; 32]);
        assert_eq!(
            CommandProjectionMetadataV1::try_new(100, 200, delta(), Vec::new(), Vec::new(), false),
            Err(CommandProjectionMetadataError::InvalidLifecycleProofs)
        );
        let wrong_purpose = CommandProjectionLifecycleProofV1 {
            projection_ref: 0,
            token: codec
                .issue(ProtocolTokenPurpose::ProjectionObligation, &("wrong", 1))
                .unwrap(),
        };
        assert_eq!(
            CommandProjectionMetadataV1::try_new(
                100,
                200,
                delta(),
                vec![wrong_purpose],
                Vec::new(),
                false,
            ),
            Err(CommandProjectionMetadataError::InvalidLifecycleProofs)
        );

        let mut two = delta();
        let mut second = two.projections[0].clone();
        second.program_id =
            "pp1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".into();
        second.binding_id =
            "pb1:sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".into();
        two.projections.push(second);
        let mut proofs = lifecycle_proofs(&codec, 2);
        proofs.reverse();
        let metadata =
            CommandProjectionMetadataV1::try_new(100, 200, two, proofs, Vec::new(), false).unwrap();
        assert_eq!(
            metadata
                .lifecycle_proofs
                .iter()
                .map(|proof| proof.projection_ref)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        let mut wire = serde_json::to_value(&metadata).unwrap();
        wire["lifecycleProofs"].as_array_mut().unwrap().reverse();
        assert_eq!(
            CommandProjectionMetadataV1::from_json(&serde_json::to_vec(&wire).unwrap()),
            Err(CommandProjectionMetadataError::InvalidLifecycleProofs)
        );

        let mut duplicate = metadata.lifecycle_proofs.clone();
        duplicate[1].projection_ref = 0;
        assert_eq!(
            CommandProjectionMetadataV1::try_new(
                100,
                200,
                metadata.delta,
                duplicate,
                Vec::new(),
                false,
            ),
            Err(CommandProjectionMetadataError::InvalidLifecycleProofs)
        );
    }

    #[test]
    fn wire_decode_streams_no_more_than_128_lifecycle_proofs() {
        let codec = ProtocolTokenCodec::new([0x37; 32]);
        let metadata = CommandProjectionMetadataV1::try_new(
            100,
            200,
            delta(),
            lifecycle_proofs(&codec, 1),
            Vec::new(),
            false,
        )
        .unwrap();
        let mut wire = serde_json::to_value(metadata).unwrap();
        let proof = wire["lifecycleProofs"][0].clone();
        wire["lifecycleProofs"] =
            serde_json::Value::Array(std::iter::repeat_n(proof, 129).collect());
        let error = CommandProjectionMetadataV1::from_json(&serde_json::to_vec(&wire).unwrap())
            .unwrap_err();
        assert!(matches!(
            error,
            CommandProjectionMetadataError::InvalidWire(ref message)
                if message.contains("more than 128 lifecycle proofs")
        ));
    }

    #[test]
    fn one_projection_preserves_distinct_obligations_for_each_output_model() {
        let codec = ProtocolTokenCodec::new([0x33; 32]);
        let mut delta = delta();
        delta.operations.insert(
            0,
            ProjectionDeltaOperation {
                occurrence_ordinal: 0,
                projection_refs: vec![0],
                mutation: ProjectionDeltaMutation::Delete {
                    scope: ProjectionDeltaScope {
                        partition: ProjectionDeltaPartition::Unit,
                        model: "TodoCounts".into(),
                        key: vec![DeltaKeyField {
                            ordinal: 0,
                            field: "owner_id".into(),
                            value: DeltaValue::String("owner-1".into()),
                        }],
                    },
                },
            },
        );
        let todos = codec
            .issue(ProtocolTokenPurpose::ProjectionObligation, &("Todos", 1))
            .unwrap();
        let counts = codec
            .issue(
                ProtocolTokenPurpose::ProjectionObligation,
                &("TodoCounts", 1),
            )
            .unwrap();
        let metadata = CommandProjectionMetadataV1::try_new(
            100,
            200,
            delta,
            lifecycle_proofs(&codec, 1),
            vec![
                CommandProjectionObligationV1 {
                    projection_ref: 0,
                    model: "TodoCounts".into(),
                    scope_token: counts.clone(),
                },
                CommandProjectionObligationV1 {
                    projection_ref: 0,
                    model: "Todos".into(),
                    scope_token: todos.clone(),
                },
            ],
            false,
        )
        .unwrap();

        assert_eq!(
            metadata
                .obligations
                .iter()
                .map(|obligation| (
                    obligation.projection_ref,
                    obligation.model.as_str(),
                    obligation.scope_token.as_str(),
                ))
                .collect::<Vec<_>>(),
            vec![
                (0, "TodoCounts", counts.as_str()),
                (0, "Todos", todos.as_str()),
            ]
        );
        assert_eq!(
            CommandProjectionMetadataV1::from_json(&metadata.canonical_bytes().unwrap()).unwrap(),
            metadata
        );
    }

    #[test]
    fn one_physical_scope_cannot_be_relabelled_as_two_output_models() {
        let codec = ProtocolTokenCodec::new([0x34; 32]);
        let token = codec
            .issue(ProtocolTokenPurpose::ProjectionObligation, &("scope", 1))
            .unwrap();
        let mut multi_model = delta();
        multi_model.operations.insert(
            0,
            ProjectionDeltaOperation {
                occurrence_ordinal: 0,
                projection_refs: vec![0],
                mutation: ProjectionDeltaMutation::Delete {
                    scope: ProjectionDeltaScope {
                        partition: ProjectionDeltaPartition::Unit,
                        model: "TodoCounts".into(),
                        key: vec![DeltaKeyField {
                            ordinal: 0,
                            field: "owner_id".into(),
                            value: DeltaValue::String("owner-1".into()),
                        }],
                    },
                },
            },
        );

        assert_eq!(
            CommandProjectionMetadataV1::try_new(
                100,
                200,
                multi_model,
                lifecycle_proofs(&codec, 1),
                vec![
                    CommandProjectionObligationV1 {
                        projection_ref: 0,
                        model: "Todos".into(),
                        scope_token: token.clone(),
                    },
                    CommandProjectionObligationV1 {
                        projection_ref: 0,
                        model: "TodoCounts".into(),
                        scope_token: token,
                    },
                ],
                false,
            ),
            Err(CommandProjectionMetadataError::ConflictingObligationScope)
        );
    }
}
