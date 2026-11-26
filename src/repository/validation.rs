//! Backend-agnostic commit-batch validation.
//!
//! These functions define what a *valid* [`CommitBatch`](super::CommitBatch) is:
//! they validate the repository-level types ([`StreamWrite`], [`PreparedEventAppend`],
//! [`OutboxMessage`], and snapshot identities). They have no storage-backend
//! dependency, so every backend (in-memory, sqlite, postgres) imports this single
//! copy. Keeping one definition here is what prevents the three backends from
//! drifting on what a valid commit batch is.

use std::collections::HashSet;

use crate::entity::{
    EventRecord, EventRecordError, BITCODE_PAYLOAD_CODEC, BITCODE_PAYLOAD_CODEC_VERSION,
};
use crate::outbox::OutboxMessage;
use crate::snapshot::SnapshotRecord;

use super::{
    CommitBatch, PreparedEventAppend, RepositoryError, SnapshotWrite, StreamIdentity, StreamWrite,
};

/// Validate a full [`CommitBatch`] and prepare its event appends.
///
/// This is the single validation preamble every backend runs before touching
/// storage: duplicate-stream and duplicate-outbox rejection, entity/identity
/// agreement, sequence-contiguity of the prepared appends, and snapshot
/// identity agreement. Backends must not add or skip batch-shape checks
/// locally — extending this function is what keeps them from drifting.
pub(crate) fn validate_commit_batch<'a>(
    batch: &'a CommitBatch<'_>,
) -> Result<Vec<PreparedEventAppend<'a>>, RepositoryError> {
    reject_duplicate_streams(&batch.streams)?;
    reject_duplicate_outbox_messages(&batch.outbox_messages)?;
    validate_entity_id_matches_identity(&batch.streams)?;

    let prepared = batch
        .streams
        .iter()
        .map(PreparedEventAppend::from_stream_write)
        .collect::<Vec<_>>();
    validate_prepared_appends(&prepared)?;

    for write in &batch.snapshots {
        match write {
            SnapshotWrite::Save { identity, record } => {
                validate_snapshot_identity(identity, record)?;
            }
        }
    }

    Ok(prepared)
}

/// A domain event an aggregate captured must leave in the same commit, as an
/// outbox row with the occurrence's id. Every backend runs this for a commit
/// that is not a causal command commit, so no builder can persist the replay
/// events and silently drop the publication. (A causal commit applies its own
/// guard: it may hand the occurrences to a same-transaction projection.)
pub(crate) fn require_captured_events_are_published(
    batch: &CommitBatch<'_>,
) -> Result<(), RepositoryError> {
    for stream in &batch.streams {
        let pending = stream
            .entity
            .pending_domain_events_for_commit()
            .map_err(|error| RepositoryError::Model(error.to_string()))?;
        for occurrence in pending {
            let id = occurrence.id().to_string();
            if !batch
                .outbox_messages
                .iter()
                .any(|message| message.id() == id)
            {
                return Err(RepositoryError::Model(format!(
                    "aggregate `{}` captured domain event `{}` but this commit does not publish \
                     it; commit with `publish_events()`",
                    stream.identity,
                    occurrence.descriptor().name
                )));
            }
        }
    }
    Ok(())
}

fn reject_duplicate_streams(streams: &[StreamWrite<'_>]) -> Result<(), RepositoryError> {
    let mut seen = HashSet::with_capacity(streams.len());
    for stream in streams {
        let key = stream.identity.storage_key();
        if !seen.insert(key) {
            return Err(RepositoryError::DuplicateStreamInBatch {
                id: stream.identity.to_string(),
            });
        }
    }
    Ok(())
}

fn reject_duplicate_outbox_messages(messages: &[OutboxMessage]) -> Result<(), RepositoryError> {
    let mut seen = HashSet::with_capacity(messages.len());
    for message in messages {
        let id = message.id();
        if id.trim().is_empty() {
            return Err(RepositoryError::Model(
                "outbox message id must not be empty".into(),
            ));
        }
        if message.event_type.trim().is_empty() {
            return Err(RepositoryError::Model(format!(
                "outbox message `{id}` event type must not be empty"
            )));
        }
        if message.payload_codec.trim().is_empty() || message.payload_codec_version == 0 {
            return Err(RepositoryError::Model(format!(
                "outbox message `{id}` needs a payload codec and a version above zero"
            )));
        }
        if !seen.insert(id.to_string()) {
            return Err(RepositoryError::DuplicateOutboxMessage { id: id.into() });
        }
    }
    Ok(())
}

fn validate_entity_id_matches_identity(streams: &[StreamWrite<'_>]) -> Result<(), RepositoryError> {
    for stream in streams {
        if stream.entity.id() != stream.identity.aggregate_id() {
            return Err(RepositoryError::Model(format!(
                "stream identity `{}` does not match entity id `{}`",
                stream.identity,
                stream.entity.id()
            )));
        }
    }
    Ok(())
}

fn validate_prepared_appends(appends: &[PreparedEventAppend<'_>]) -> Result<(), RepositoryError> {
    for append in appends {
        for (offset, event) in append.events.iter().enumerate() {
            validate_supported_event_codec(event)?;
            let expected_sequence = append.expected_version + offset as u64 + 1;
            if event.sequence != expected_sequence {
                return Err(RepositoryError::Model(format!(
                    "event `{}` for stream `{}` has sequence {}, expected {}",
                    event.event_name, append.identity, event.sequence, expected_sequence
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_supported_event_codec(event: &EventRecord) -> Result<(), RepositoryError> {
    if event.payload_codec != BITCODE_PAYLOAD_CODEC
        || event.payload_codec_version != BITCODE_PAYLOAD_CODEC_VERSION
    {
        return Err(EventRecordError::unsupported_codec(
            &event.payload_codec,
            event.payload_codec_version,
        )
        .into());
    }
    Ok(())
}

pub(crate) fn validate_snapshot_identity(
    identity: &StreamIdentity,
    record: &SnapshotRecord,
) -> Result<(), RepositoryError> {
    record.validate_for_identity(identity)
}
