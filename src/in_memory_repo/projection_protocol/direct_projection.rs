use super::*;

/// Stage one compiler-sealed same-transaction projected upsert against cloned
/// row/protocol state. The caller publishes both clones only after every
/// domain, ledger, and projection participant has passed validation.
pub(in crate::in_memory_repo) fn stage_same_transaction_projection(
    protocol: &mut InMemoryProjectionProtocolState,
    staged_rows: &mut HashMap<String, StoredRow>,
    batch: &SameTransactionProjectionBatch,
    retention: ProjectionChangeRetention,
) -> Result<SameTransactionProjectionEvidence, ProjectionProtocolError> {
    batch.validate()?;
    protocol.require_registered_topology(&batch.topology)?;

    let partition_key = PartitionKey::new(&batch.topology, &batch.partition);
    protocol.ensure_partition(&partition_key, &batch.change_epoch)?;
    if let Some(failure_id) = protocol
        .partitions
        .get(&partition_key)
        .and_then(|partition| partition.stopped_failure_id.clone())
    {
        return Err(ProjectionProtocolError::PartitionStopped { failure_id });
    }
    if protocol
        .partitions
        .get(&partition_key)
        .is_some_and(|partition| partition.pending_retry_failure_id.is_some())
    {
        return Err(ProjectionProtocolError::IncomparableInput);
    }
    protocol.register_same_transaction_ownership(&partition_key, batch)?;

    let mutation = &batch.mutations[0];
    let current = protocol.records.get(&mutation.scope);
    crate::projection_protocol::validate_snapshot_write(current, None)?;
    let lock_key = mutation.mutation.lock_key();
    validate_physical_row(
        &mutation.scope,
        current,
        staged_rows.contains_key(&lock_key),
    )?;
    let expectation = current.map_or(ProjectionRecordExpectation::Missing, |metadata| {
        ProjectionRecordExpectation::Exact(metadata.revision.clone())
    });
    let (revision, _) = next_record(
        &mutation.scope,
        &expectation,
        RecordTransition::Upsert,
        current,
        false,
    )?;

    let change = protocol.append_change(
        &partition_key,
        PendingChange {
            kind: ProjectionChangeKind::RecordUpsert,
            causation_id: batch.causation_id.clone(),
            observation_kind: None,
            scope: Some(mutation.scope.clone()),
            revision: Some(revision.clone()),
            failure_id: None,
            program_id: program_id_for_model(&batch.ownership, mutation.scope.model()),
        },
    )?;
    let metadata = ProjectionRecordMetadata {
        source_snapshot: None,
        revision: revision.clone(),
        tombstone: false,
        change: change.cursor.clone(),
    };
    protocol.ensure_live_record_identity_available(&metadata)?;
    protocol
        .records
        .insert(mutation.scope.clone(), metadata.clone());

    apply_read_model_write_plan(
        TableWritePlan::new(vec![mutation.mutation.clone()]),
        staged_rows,
    )?;
    if !staged_rows.contains_key(&lock_key) {
        return Err(corrupt_storage(format!(
            "direct projection upsert left model `{}` without a physical row",
            mutation.scope.model()
        )));
    }

    let observation_key = ObservationKey {
        causation_id: batch.causation_id.clone(),
        scope: mutation.scope.clone(),
        kind: ProjectionObservationKind::Record,
    };
    if protocol.observations.contains_key(&observation_key) {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "direct projection causation `{}` already observed this record",
            batch.causation_id
        )));
    }
    let observation = ProjectionObservation {
        causation_id: batch.causation_id.clone(),
        kind: ProjectionObservationKind::Record,
        revision: Some(revision),
        scope: mutation.scope.clone(),
        change: change.cursor.clone(),
        program_id: change.program_id,
    };
    protocol
        .observations
        .insert(observation_key, observation.clone());
    protocol.retain_change_suffix(&partition_key, retention)?;

    Ok(SameTransactionProjectionEvidence {
        records: vec![metadata],
        changes: vec![change],
        observations: vec![observation],
    })
}
