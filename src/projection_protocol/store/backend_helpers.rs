use super::{
    ProjectionChangeKind, ProjectionFailure, ProjectionFailureBatch, ProjectionProtocolError,
    RecordTransition,
};
use crate::projection_protocol::MAX_PROJECTION_POSITION;
use crate::table::TableMutation;

pub(crate) fn checked_next(
    value: u64,
    domain: &'static str,
) -> Result<u64, ProjectionProtocolError> {
    if value >= MAX_PROJECTION_POSITION {
        return Err(ProjectionProtocolError::PositionOverflow { domain });
    }
    Ok(value + 1)
}

pub(crate) fn table_model_name(mutation: &TableMutation) -> &str {
    match mutation {
        TableMutation::UpsertRow(mutation) => &mutation.schema.model_name,
        TableMutation::PatchRow(mutation) => &mutation.schema.model_name,
        TableMutation::DeleteRow(mutation) => &mutation.schema.model_name,
    }
}

pub(crate) fn change_kind_for_mutation(kind: RecordTransition) -> ProjectionChangeKind {
    match kind {
        RecordTransition::Upsert => ProjectionChangeKind::RecordUpsert,
        RecordTransition::Delete => ProjectionChangeKind::RecordDelete,
        RecordTransition::Recreate => ProjectionChangeKind::RecordRecreate,
    }
}

pub(crate) fn failure_matches_batch(
    failure: &ProjectionFailure,
    batch: &ProjectionFailureBatch,
) -> bool {
    failure.failure_id == batch.failure_id
        && failure.input == batch.input.cursor
        && failure.input_fingerprint == batch.input.fingerprint
        && failure.message_id == batch.input.message_id
        && failure.causation_id == batch.input.causation_id
        && failure.gap_free == batch.input.gap_free
        && failure.generation == batch.input.generation
        && failure.failure_code == batch.failure_code
        && failure.failure_bytes == batch.failure_bytes
        && failure.failure_digest == batch.failure_digest
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection_protocol::{
        PartitionId, ProjectionChangeCursor, ProjectionEpoch, ProjectionGeneration,
        ProjectionInputCursor, ProjectionInputFingerprint, ProjectionSource, ProjectorTopologyId,
        TrustedProjectionInput,
    };

    fn topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "todos", [7; 32]).unwrap()
    }

    fn partition() -> PartitionId {
        PartitionId::new(b"tenant:a".to_vec()).unwrap()
    }

    fn failure_batch() -> ProjectionFailureBatch {
        let input = TrustedProjectionInput::mint(
            ProjectionInputCursor::new(
                topology(),
                partition(),
                ProjectionSource::new("todo_stream", b"todo-1".to_vec()).unwrap(),
                ProjectionEpoch::new("source-v1").unwrap(),
                1,
            )
            .unwrap(),
            ProjectionInputFingerprint::from_canonical_bytes(b"input-1"),
            "message-1",
            "cause-1",
            ProjectionGeneration::initial(),
            true,
        )
        .unwrap();
        ProjectionFailureBatch::new(
            input,
            ProjectionEpoch::new("changes-v1").unwrap(),
            "failure-1",
            "decode_error",
            b"bad payload".to_vec(),
        )
        .unwrap()
    }

    #[test]
    fn failure_matches_batch_rejects_mismatched_gap_free_identity() {
        let batch = failure_batch();
        let mut failure = ProjectionFailure {
            failure_id: batch.failure_id.clone(),
            input: batch.input.cursor.clone(),
            input_fingerprint: batch.input.fingerprint,
            message_id: batch.input.message_id.clone(),
            causation_id: batch.input.causation_id.clone(),
            generation: batch.input.generation,
            gap_free: !batch.input.gap_free,
            failure_code: batch.failure_code.clone(),
            failure_bytes: batch.failure_bytes.clone(),
            failure_digest: batch.failure_digest,
            change: ProjectionChangeCursor::new(
                topology(),
                partition(),
                batch.change_epoch.clone(),
                1,
            )
            .unwrap(),
        };

        assert!(!failure_matches_batch(&failure, &batch));

        failure.gap_free = batch.input.gap_free;
        assert!(failure_matches_batch(&failure, &batch));
    }
}
