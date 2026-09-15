use super::{
    ProjectionChangeKind, ProjectionFailure, ProjectionFailureBatch,
    ProjectionGraphSnapshotRequest, ProjectionMutationKind, ProjectionProtocolError,
    MAX_PROJECTION_QUERY_BATCH_ROWS,
};
use crate::projection_protocol::MAX_PROJECTION_POSITION;
use crate::table::{
    resolve_direct_join_keys, RelationshipDef, RelationshipKind, RowValue, RowValues,
    TableMutation, TableSchema, TableStoreError,
};

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

pub(crate) fn change_kind_for_mutation(kind: ProjectionMutationKind) -> ProjectionChangeKind {
    match kind {
        ProjectionMutationKind::Upsert => ProjectionChangeKind::RecordUpsert,
        ProjectionMutationKind::Delete => ProjectionChangeKind::RecordDelete,
        ProjectionMutationKind::Recreate => ProjectionChangeKind::RecordRecreate,
    }
}

pub(crate) fn failure_matches_batch(
    failure: &ProjectionFailure,
    batch: &ProjectionFailureBatch,
) -> bool {
    failure.input == batch.input.cursor
        && failure.input_fingerprint == batch.input.fingerprint
        && failure.message_id == batch.input.message_id
        && failure.causation_id == batch.input.causation_id
        && failure.gap_free == batch.input.gap_free
        && failure.generation == batch.input.generation
        && failure.failure_code == batch.failure_code
        && failure.failure_bytes == batch.failure_bytes
        && failure.failure_digest == batch.failure_digest
        && failure.change.epoch() == &batch.change_epoch
}

pub(crate) fn validate_projection_graph_snapshot_request(
    request: &ProjectionGraphSnapshotRequest,
) -> Result<(), ProjectionProtocolError> {
    request.root.validate()?;
    if request.max_unique_record_scopes == 0
        || request.max_unique_record_scopes > MAX_PROJECTION_QUERY_BATCH_ROWS
    {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection graph snapshot record-scope budget is {}; expected 1..={MAX_PROJECTION_QUERY_BATCH_ROWS}",
            request.max_unique_record_scopes
        )));
    }
    let query_scopes = request.includes.len().checked_add(1).ok_or_else(|| {
        ProjectionProtocolError::InvalidBatch(
            "projection graph snapshot query-scope count overflowed".into(),
        )
    })?;
    if query_scopes > MAX_PROJECTION_QUERY_BATCH_ROWS {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection graph snapshot has {query_scopes} query scopes; maximum is {MAX_PROJECTION_QUERY_BATCH_ROWS}"
        )));
    }
    for (name, include) in &request.includes {
        include.target_schema.validate()?;
        let relationship = request
            .root
            .schema
            .relationships
            .iter()
            .find(|relationship| relationship.field_name == *name)
            .ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph snapshot model `{}` has no relationship `{name}`",
                    request.root.schema.model_name
                ))
            })?;
        if relationship != &include.relationship
            || relationship.target_model != include.target_schema.model_name
            || matches!(
                relationship.kind,
                crate::table::RelationshipKind::ManyToMany
            )
        {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection graph relationship `{name}` metadata is invalid or divergent"
            )));
        }
    }
    Ok(())
}

pub(crate) fn projection_relationship_values(
    root_schema: &TableSchema,
    root_row: &RowValues,
    relationship: &RelationshipDef,
    target_schema: &TableSchema,
) -> Result<Vec<(String, RowValue)>, ProjectionProtocolError> {
    let pairs =
        resolve_direct_join_keys(root_schema, relationship, target_schema).map_err(|error| {
            match error {
                TableStoreError::Metadata(message) => {
                    ProjectionProtocolError::InvalidBatch(message)
                }
                other => ProjectionProtocolError::Table(other),
            }
        })?;
    pairs
        .into_iter()
        .map(|pair| {
            let (source, target) = match relationship.kind {
                RelationshipKind::HasMany => (pair.primary_key_column, pair.foreign_key_column),
                RelationshipKind::BelongsTo => (pair.foreign_key_column, pair.primary_key_column),
                RelationshipKind::ManyToMany => {
                    unreachable!("direct resolver rejects many-to-many")
                }
            };
            let value = root_row.get(&source).cloned().ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph root `{}` is missing relationship key `{source}`",
                    root_schema.model_name
                ))
            })?;
            Ok((target, value))
        })
        .collect()
}

pub(crate) fn checked_projection_graph_materialization(
    current: usize,
) -> Result<usize, ProjectionProtocolError> {
    let next = current.checked_add(1).ok_or_else(|| {
        ProjectionProtocolError::InvalidBatch(
            "projection graph materialized-row count overflowed".into(),
        )
    })?;
    if next > MAX_PROJECTION_QUERY_BATCH_ROWS {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection graph materialized {next} record snapshots; maximum is {MAX_PROJECTION_QUERY_BATCH_ROWS}"
        )));
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection_protocol::{
        ProjectionChangeCursor, ProjectionEpoch, ProjectionGeneration, ProjectionInputCursor,
        ProjectionInputFingerprint, ProjectionPartition, ProjectionSource, ProjectorTopologyId,
        TrustedProjectionInput,
    };

    fn topology() -> ProjectorTopologyId {
        ProjectorTopologyId::new(1, "todos", [7; 32]).unwrap()
    }

    fn partition() -> ProjectionPartition {
        ProjectionPartition::new(b"tenant:a".to_vec()).unwrap()
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

    #[test]
    fn graph_materialization_budget_bounds_duplicate_include_expansion() {
        assert_eq!(
            checked_projection_graph_materialization(MAX_PROJECTION_QUERY_BATCH_ROWS - 1).unwrap(),
            MAX_PROJECTION_QUERY_BATCH_ROWS
        );
        let error =
            checked_projection_graph_materialization(MAX_PROJECTION_QUERY_BATCH_ROWS).unwrap_err();
        assert!(error
            .to_string()
            .contains("materialized 4097 record snapshots"));
    }
}
