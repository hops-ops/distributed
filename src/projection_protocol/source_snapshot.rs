//! Authoritative source versions, separate from broker and row revisions.

use super::ProjectionProtocolError;
use crate::DomainEventOccurrence;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Durable fence for a full-state projection of one aggregate stream.
///
/// Constructed by the framework from a validated canonical occurrence, never
/// from transport headers or application-supplied timestamps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSnapshotVersion {
    aggregate_type: String,
    aggregate_id: String,
    sequence: u64,
    publication_ordinal: u32,
    occurrence_id: String,
    occurrence_fingerprint: [u8; 32],
}

impl SourceSnapshotVersion {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn validate_stored(&self) -> Result<(), ProjectionProtocolError> {
        if self.sequence == 0
            || crate::bus::validate_message_name(&self.aggregate_type).is_err()
            || crate::bus::validate_stable_message_id(Some(&self.aggregate_id)).is_err()
            || crate::bus::validate_stable_message_id(Some(&self.occurrence_id)).is_err()
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "invalid stored source snapshot identity".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn from_occurrence(
        event: &DomainEventOccurrence,
    ) -> Result<Self, ProjectionProtocolError> {
        if event.derivation().is_some() {
            return Err(ProjectionProtocolError::InvalidBatch(
                "derived facts cannot establish an aggregate source snapshot".into(),
            ));
        }
        let canonical = event
            .canonical_bytes()
            .map_err(|error| ProjectionProtocolError::InvalidBatch(error.to_string()))?;
        Ok(Self {
            aggregate_type: event.aggregate_type().into(),
            aggregate_id: event.aggregate_id().into(),
            sequence: event.aggregate_sequence(),
            publication_ordinal: event.publication_ordinal(),
            occurrence_id: event.id().into(),
            occurrence_fingerprint: Sha256::digest(canonical).into(),
        })
    }

    /// True only when this occurrence advances the same authoritative stream.
    pub(crate) fn advances(&self, current: &Self) -> Result<bool, ProjectionProtocolError> {
        if self.aggregate_type != current.aggregate_type
            || self.aggregate_id != current.aggregate_id
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "source snapshot row is owned by another aggregate stream".into(),
            ));
        }
        let incoming = (self.sequence, self.publication_ordinal);
        let stored = (current.sequence, current.publication_ordinal);
        if incoming == stored
            && (self.occurrence_id != current.occurrence_id
                || self.occurrence_fingerprint != current.occurrence_fingerprint)
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "source snapshot version has conflicting occurrence identities".into(),
            ));
        }
        Ok(incoming > stored)
    }
}

/// Defense at the atomic commit boundary, including non-modeled writers.
pub(crate) fn validate_snapshot_write(
    current: Option<&super::ProjectionRecordMetadata>,
    incoming: Option<&SourceSnapshotVersion>,
) -> Result<(), ProjectionProtocolError> {
    match (current, incoming) {
        (Some(record), Some(incoming)) => match &record.source_snapshot {
            Some(stored) if incoming.advances(stored)? => Ok(()),
            Some(_) => Err(ProjectionProtocolError::InvalidBatch(
                "source snapshot write does not advance the stored version".into(),
            )),
            None => Err(ProjectionProtocolError::InvalidBatch(
                "source snapshot requires rebuilding an unversioned row".into(),
            )),
        },
        (Some(record), None) if record.source_snapshot.is_some() => {
            Err(ProjectionProtocolError::InvalidBatch(
                "unversioned write cannot replace a source-fenced row".into(),
            ))
        }
        _ => Ok(()),
    }
}
