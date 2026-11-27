//! Protocol decisions every store makes the same way.
//!
//! Each function is pure: a store fetches the durable facts it names, calls
//! it, and persists the answer. No store re-derives one of these rules.

use std::ops::ControlFlow;

use super::{
    checked_next, PartitionId, ProjectionCausationEvidenceBatch, ProjectionChange,
    ProjectionChangeCursor, ProjectionChangeRead, ProjectionCheckpoint, ProjectionEpoch,
    ProjectionFailure, ProjectionGeneration, ProjectionInputDisposition, ProjectionModelOwnership,
    ProjectionObservationTarget, ProjectionProtocolError, ProjectionRecordExpectation,
    ProjectionRecordMetadata, ProjectionRecordScope, ProjectorTopologyId, RecordRevision,
    RecordTransition, TrustedProjectionInput,
};
use crate::projection::ProjectionProgramId;
use crate::projection_protocol::RevisionComparison;

/// Stored protocol data that contradicts itself. Never caused by a caller.
pub(crate) fn corrupt_storage(message: impl Into<String>) -> ProjectionProtocolError {
    ProjectionProtocolError::StorageCorruption(message.into())
}

pub(crate) fn program_id_for_model(
    ownership: &[ProjectionModelOwnership],
    model: &str,
) -> Option<ProjectionProgramId> {
    ownership
        .iter()
        .find(|declaration| declaration.model == model)
        .and_then(|declaration| declaration.program_id)
}

/// Whether two inputs are the same logical input. The repair generation that
/// delivered an input is not part of its identity.
pub(crate) fn same_input(left: &TrustedProjectionInput, right: &TrustedProjectionInput) -> bool {
    left.cursor == right.cursor
        && left.fingerprint == right.fingerprint
        && left.message_id == right.message_id
        && left.causation_id == right.causation_id
        && left.gap_free == right.gap_free
}

/// Check an input against the generation-independent identities already
/// durable for it.
///
/// `source_gap_free` is the capability registered for the input's source,
/// `at_cursor` the input first recorded at this exact cursor, and
/// `for_message` the input first recorded under this message ID anywhere in
/// the topology. A changed source capability or cursor content is corruption
/// and wins over message-ID reuse.
pub(crate) fn validate_input_identity(
    input: &TrustedProjectionInput,
    source_gap_free: Option<bool>,
    at_cursor: Option<&TrustedProjectionInput>,
    for_message: Option<&TrustedProjectionInput>,
) -> Result<(), ProjectionProtocolError> {
    if source_gap_free.is_some_and(|registered| registered != input.gap_free)
        || at_cursor.is_some_and(|recorded| !same_input(recorded, input))
    {
        return Err(ProjectionProtocolError::InputCorruption);
    }
    if for_message.is_some_and(|recorded| {
        recorded.cursor.compare_position(&input.cursor) == RevisionComparison::Incomparable
            || recorded.fingerprint != input.fingerprint
            || recorded.causation_id != input.causation_id
            || recorded.gap_free != input.gap_free
    }) {
        return Err(ProjectionProtocolError::MessageIdReuse {
            message_id: input.message_id.clone(),
        });
    }
    Ok(())
}

/// Fence an input to its partition's active generation and stop state. A
/// partition with no durable state is at the initial generation, not stopped.
pub(crate) fn ensure_active_input(
    input: &TrustedProjectionInput,
    active_generation: ProjectionGeneration,
    stopped_failure_id: Option<&str>,
) -> Result<(), ProjectionProtocolError> {
    if input.generation != active_generation {
        return Err(ProjectionProtocolError::GenerationFenced {
            expected: active_generation.get(),
            actual: input.generation.get(),
        });
    }
    if let Some(failure_id) = stopped_failure_id {
        return Err(ProjectionProtocolError::PartitionStopped {
            failure_id: failure_id.to_string(),
        });
    }
    Ok(())
}

/// Classify an identity-checked input of the active generation.
///
/// `receipt` is the input applied at this exact cursor in this generation
/// with the change it produced, `repeated_effect` whether this generation
/// already applied this message ID in this partition, and `previous` the
/// newest input this generation holds for the source.
pub(crate) fn classify_input(
    input: &TrustedProjectionInput,
    receipt: Option<(&TrustedProjectionInput, &ProjectionChangeCursor)>,
    repeated_effect: bool,
    previous: Option<(&TrustedProjectionInput, &ProjectionChangeCursor)>,
) -> Result<ProjectionInputDisposition, ProjectionProtocolError> {
    let checkpoint = |(applied, change): (&TrustedProjectionInput, &ProjectionChangeCursor)| {
        ProjectionCheckpoint::new(applied.cursor.clone(), change.clone(), applied.gap_free)
            .map_err(ProjectionProtocolError::from)
    };
    if let Some(receipt) = receipt {
        if !same_input(receipt.0, input) {
            return Err(ProjectionProtocolError::InputCorruption);
        }
        return Ok(ProjectionInputDisposition::Duplicate(checkpoint(receipt)?));
    }
    let unapplied = if repeated_effect {
        ProjectionInputDisposition::Redelivery
    } else {
        ProjectionInputDisposition::Pending
    };
    let Some(previous) = previous else {
        return Ok(unapplied);
    };
    if previous.0.gap_free != input.gap_free {
        return Err(ProjectionProtocolError::InputCorruption);
    }
    match input.cursor.compare_position(&previous.0.cursor) {
        RevisionComparison::Incomparable => Err(ProjectionProtocolError::IncomparableInput),
        RevisionComparison::Older => Ok(ProjectionInputDisposition::Stale(checkpoint(previous)?)),
        RevisionComparison::Equal if !same_input(previous.0, input) => {
            Err(ProjectionProtocolError::InputCorruption)
        }
        RevisionComparison::Equal => {
            Ok(ProjectionInputDisposition::Duplicate(checkpoint(previous)?))
        }
        RevisionComparison::Newer
            if input.gap_free
                && input.cursor.position()
                    != checked_next(previous.0.cursor.position(), "gap-free projection input")? =>
        {
            Err(ProjectionProtocolError::IncomparableInput)
        }
        RevisionComparison::Newer => Ok(unapplied),
    }
}

/// After a repair, the new generation accepts only the exact input whose
/// failure it retries.
pub(crate) fn validate_pending_retry(
    input: &TrustedProjectionInput,
    active_generation: ProjectionGeneration,
    pending: &ProjectionFailure,
) -> Result<(), ProjectionProtocolError> {
    if pending.generation.checked_next()? != active_generation {
        return Err(corrupt_storage(format!(
            "pending retry failure generation {} does not precede active generation {}",
            pending.generation.get(),
            active_generation.get()
        )));
    }
    if pending.input != input.cursor {
        return Err(ProjectionProtocolError::IncomparableInput);
    }
    if pending.input_fingerprint != input.fingerprint
        || pending.message_id != input.message_id
        || pending.causation_id != input.causation_id
        || pending.gap_free != input.gap_free
    {
        return Err(ProjectionProtocolError::InputCorruption);
    }
    Ok(())
}

/// Decide the repair generation for the failure currently stopping a
/// partition. `failure` is the stored failure named `failure_id`, if any.
pub(crate) fn plan_repair(
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
    active_generation: ProjectionGeneration,
    stopped_failure_id: Option<&str>,
    failure_id: &str,
    failure: Option<&ProjectionFailure>,
) -> Result<ProjectionGeneration, ProjectionProtocolError> {
    match stopped_failure_id {
        Some(stopped) if stopped == failure_id => {}
        Some(stopped) => {
            return Err(ProjectionProtocolError::PartitionStopped {
                failure_id: stopped.to_string(),
            });
        }
        None => {
            return Err(ProjectionProtocolError::InvalidBatch(
                "projection partition is not stopped".into(),
            ));
        }
    }
    let failure = failure.ok_or_else(|| {
        corrupt_storage(format!(
            "stopped projection failure `{failure_id}` is missing"
        ))
    })?;
    if failure.input.topology() != topology || failure.input.projection_partition() != partition {
        return Err(ProjectionProtocolError::ScopeMismatch {
            field: "projection repair failure",
        });
    }
    if failure.generation != active_generation {
        return Err(corrupt_storage(format!(
            "stopped failure generation {} differs from active generation {}",
            failure.generation.get(),
            active_generation.get()
        )));
    }
    active_generation.checked_next()
}

/// A record's metadata and its physical row must agree before a mutation:
/// a live record has a row, a tombstone or unknown record has none. A row
/// without metadata in this partition belongs to someone else.
pub(crate) fn validate_physical_row(
    scope: &ProjectionRecordScope,
    current: Option<&ProjectionRecordMetadata>,
    row_exists: bool,
) -> Result<(), ProjectionProtocolError> {
    let live = current.is_some_and(|metadata| !metadata.tombstone);
    match (live, row_exists) {
        (false, true) => Err(ProjectionProtocolError::RecordAlreadyExists {
            model: scope.model().to_string(),
        }),
        (true, false) => Err(ProjectionProtocolError::RecordMissing {
            model: scope.model().to_string(),
        }),
        _ => Ok(()),
    }
}

/// The revision a mutation produces from the record's current metadata, and
/// whether it leaves a tombstone.
pub(crate) fn next_record(
    scope: &ProjectionRecordScope,
    expectation: &ProjectionRecordExpectation,
    kind: RecordTransition,
    current: Option<&ProjectionRecordMetadata>,
    source_snapshot: bool,
) -> Result<(RecordRevision, bool), ProjectionProtocolError> {
    let model = || scope.model().to_string();
    let advanced = |metadata: &ProjectionRecordMetadata| {
        Ok::<_, ProjectionProtocolError>(RecordRevision::new(
            scope.clone(),
            metadata.revision.incarnation(),
            checked_next(metadata.revision.revision(), "record revision")?,
        )?)
    };
    match (expectation, current, kind) {
        (ProjectionRecordExpectation::Missing, None, RecordTransition::Upsert) => {
            Ok((RecordRevision::new(scope.clone(), 1, 1)?, false))
        }
        (ProjectionRecordExpectation::Missing, None, RecordTransition::Delete) => {
            Ok((RecordRevision::new(scope.clone(), 1, 1)?, true))
        }
        (ProjectionRecordExpectation::Missing, Some(metadata), _) if metadata.tombstone => {
            Err(ProjectionProtocolError::RecordTombstoned { model: model() })
        }
        (ProjectionRecordExpectation::Missing, Some(_), _) => {
            Err(ProjectionProtocolError::RecordAlreadyExists { model: model() })
        }
        (ProjectionRecordExpectation::Exact(_), None, _) => {
            Err(ProjectionProtocolError::RecordMissing { model: model() })
        }
        (ProjectionRecordExpectation::Exact(expected), Some(metadata), _) => {
            if expected != &metadata.revision {
                return Err(revision_conflict(expected, &metadata.revision));
            }
            match kind {
                RecordTransition::Upsert if metadata.tombstone => {
                    Err(ProjectionProtocolError::RecordTombstoned { model: model() })
                }
                RecordTransition::Upsert => Ok((advanced(metadata)?, false)),
                RecordTransition::Delete if metadata.tombstone && !source_snapshot => {
                    Err(ProjectionProtocolError::RecordTombstoned { model: model() })
                }
                RecordTransition::Delete => Ok((advanced(metadata)?, true)),
                RecordTransition::Recreate if !metadata.tombstone => {
                    Err(ProjectionProtocolError::RecreateRequiresTombstone { model: model() })
                }
                RecordTransition::Recreate => Ok((
                    RecordRevision::new(
                        scope.clone(),
                        checked_next(metadata.revision.incarnation(), "record incarnation")?,
                        1,
                    )?,
                    false,
                )),
            }
        }
        (_, _, RecordTransition::Recreate) => Err(ProjectionProtocolError::InvalidBatch(
            "delete/recreate requires an exact record expectation".into(),
        )),
    }
}

fn revision_conflict(
    expected: &RecordRevision,
    actual: &RecordRevision,
) -> ProjectionProtocolError {
    ProjectionProtocolError::RecordRevisionConflict {
        model: expected.scope().model().to_string(),
        expected_incarnation: expected.incarnation(),
        expected_revision: expected.revision(),
        actual_incarnation: actual.incarnation(),
        actual_revision: actual.revision(),
    }
}

/// Resolve what an observation confirms: the record revision, and the change
/// that already publishes it when the same commit stages the record.
///
/// `record` is the target's metadata as this commit leaves it: the staged
/// metadata when the batch mutates the scope, the stored metadata otherwise.
/// An existing tombstone can be confirmed only when a source snapshot wrote
/// it, because only that tombstone is authoritative for a late input.
pub(crate) fn resolve_observation(
    target: &ProjectionObservationTarget,
    record: Option<&ProjectionRecordMetadata>,
) -> Result<(Option<RecordRevision>, Option<ProjectionChangeCursor>), ProjectionProtocolError> {
    match target {
        ProjectionObservationTarget::StagedRecord(scope) => {
            let staged = record.ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection observation references unstaged model `{}`",
                    scope.model()
                ))
            })?;
            Ok((Some(staged.revision.clone()), Some(staged.change.clone())))
        }
        ProjectionObservationTarget::ExistingRecord(expected) => {
            let model = || expected.scope().model().to_string();
            let existing =
                record.ok_or_else(|| ProjectionProtocolError::RecordMissing { model: model() })?;
            if &existing.revision != expected {
                return Err(revision_conflict(expected, &existing.revision));
            }
            if existing.tombstone && existing.source_snapshot.is_none() {
                return Err(ProjectionProtocolError::RecordTombstoned { model: model() });
            }
            Ok((Some(expected.clone()), None))
        }
        ProjectionObservationTarget::Dependency(_) => Ok((None, None)),
    }
}

/// Decide a change read from the partition's durable window, given as
/// `(change epoch, change head, compacted through)`.
///
/// `Break` is the complete answer. `Continue(start)` asks the store for up to
/// `limit` changes after `start`, to be returned through [`change_page`].
pub(crate) fn change_read_window(
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
    window: Option<(&ProjectionEpoch, u64, u64)>,
    after: Option<&ProjectionChangeCursor>,
    limit: usize,
) -> Result<ControlFlow<ProjectionChangeRead, u64>, ProjectionProtocolError> {
    let Some((change_epoch, change_head, compacted_through)) = window else {
        return Ok(ControlFlow::Break(match after {
            Some(_) => ProjectionChangeRead::ResetRequired {
                head: None,
                compacted_through: 0,
            },
            None => ProjectionChangeRead::Changes {
                head: None,
                compacted_through: 0,
                changes: Vec::new(),
            },
        }));
    };
    let head = change_log_head(topology, partition, change_epoch, change_head)?;
    let start = match after {
        None if compacted_through == 0 => Some(0),
        None => None,
        // `compacted_through` is the last removed change, so a cursor exactly
        // there resumes at the first retained change.
        Some(cursor)
            if cursor.topology() == topology
                && cursor.projection_partition() == partition
                && cursor.epoch() == change_epoch
                && (compacted_through..=change_head).contains(&cursor.position()) =>
        {
            Some(cursor.position())
        }
        Some(_) => None,
    };
    Ok(match start {
        None => ControlFlow::Break(ProjectionChangeRead::ResetRequired {
            head,
            compacted_through,
        }),
        Some(start) if limit == 0 || start == change_head => {
            ControlFlow::Break(ProjectionChangeRead::Changes {
                head,
                compacted_through,
                changes: Vec::new(),
            })
        }
        Some(start) => ControlFlow::Continue(start),
    })
}

/// Seal the page a store read after `start`. The retained log has no holes,
/// so the page is non-empty and contiguous from `start`.
pub(crate) fn change_page(
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
    (change_epoch, change_head, compacted_through): (&ProjectionEpoch, u64, u64),
    start: u64,
    changes: Vec<ProjectionChange>,
) -> Result<ProjectionChangeRead, ProjectionProtocolError> {
    let mut expected = start;
    for change in &changes {
        expected = checked_next(expected, "projection change read")?;
        if change.cursor.position() != expected {
            return Err(corrupt_storage(format!(
                "projection change log expected position {expected} but found {}",
                change.cursor.position()
            )));
        }
    }
    if changes.is_empty() || expected > change_head {
        return Err(corrupt_storage(format!(
            "projection change log does not hold positions {}..={change_head}",
            checked_next(start, "projection change read")?
        )));
    }
    Ok(ProjectionChangeRead::Changes {
        head: change_log_head(topology, partition, change_epoch, change_head)?,
        compacted_through,
        changes,
    })
}

fn change_log_head(
    topology: &ProjectorTopologyId,
    partition: &PartitionId,
    change_epoch: &ProjectionEpoch,
    change_head: u64,
) -> Result<Option<ProjectionChangeCursor>, ProjectionProtocolError> {
    if change_head == 0 {
        return Ok(None);
    }
    Ok(Some(ProjectionChangeCursor::new(
        topology.clone(),
        partition.clone(),
        change_epoch.clone(),
        change_head,
    )?))
}

/// The one order causation evidence is returned in: canonical identity bytes,
/// which are the same on every store and independent of database collation.
pub(crate) fn sort_causation_evidence(batch: &mut ProjectionCausationEvidenceBatch) {
    batch.observations.sort_by_cached_key(|observation| {
        let scope = &observation.scope;
        (
            scope.topology().canonical_bytes(),
            scope.projection_partition().canonical_bytes().to_vec(),
            scope.model().to_string(),
            observation.kind.as_storage_str(),
            scope.canonical_key_bytes().to_vec(),
        )
    });
    batch
        .terminal_failure_topologies
        .sort_by_key(ProjectorTopologyId::canonical_bytes);
}
