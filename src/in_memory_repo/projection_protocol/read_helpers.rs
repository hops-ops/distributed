use super::*;

pub(super) fn read_projection_execution_snapshot_batch_from_state(
    rows: &HashMap<String, StoredRow>,
    protocol: &InMemoryProjectionProtocolState,
    request: &ProjectionExecutionSnapshotBatchRequest,
) -> Result<ProjectionExecutionSnapshotBatch, ProjectionProtocolError> {
    request.validate()?;
    let snapshots = request
        .requests
        .iter()
        .map(|row_request| {
            let snapshot = read_projection_query_snapshot_from_state(rows, protocol, row_request)?;
            Ok(ProjectionScopedRowSnapshot {
                scope: row_request.scope.clone(),
                row: snapshot.row,
                record: snapshot.record,
            })
        })
        .collect::<Result<Vec<_>, ProjectionProtocolError>>()?;
    Ok(ProjectionExecutionSnapshotBatch { snapshots })
}

pub(super) fn read_projection_query_snapshot_from_state(
    rows: &HashMap<String, StoredRow>,
    protocol: &InMemoryProjectionProtocolState,
    request: &ProjectionQuerySnapshotRequest,
) -> Result<ProjectionQuerySnapshot, ProjectionProtocolError> {
    request.validate()?;
    let registered_key = RegisteredModelKey {
        topology: request.scope.topology().clone(),
        model: request.scope.model().to_string(),
    };
    if protocol
        .registered_models
        .get(&registered_key)
        .map(String::as_str)
        != Some(request.schema.table_name.as_str())
        || protocol
            .authoritative_table_owners
            .get(&request.schema.table_name)
            != Some(&registered_key)
    {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection query model `{}` is not the registered owner of table `{}`",
            request.scope.model(),
            request.schema.table_name
        )));
    }

    let storage_key = relational_storage_key(&request.schema.table_name, &request.key);
    let row = rows.get(&storage_key).map(|stored| stored.values.clone());
    let record = protocol.records.get(&request.scope).cloned();
    match (row.is_some(), record.as_ref()) {
        (true, None)
        | (
            true,
            Some(ProjectionRecordMetadata {
                tombstone: true, ..
            }),
        ) => {
            return Err(ProjectionProtocolError::RecordAlreadyExists {
                model: request.scope.model().to_string(),
            });
        }
        (
            false,
            Some(ProjectionRecordMetadata {
                tombstone: false, ..
            }),
        ) => {
            return Err(ProjectionProtocolError::RecordMissing {
                model: request.scope.model().to_string(),
            });
        }
        _ => {}
    }

    let partition_key = PartitionKey::new(
        request.scope.topology(),
        request.scope.projection_partition(),
    );
    let partition = protocol.partitions.get(&partition_key);
    if record.is_some() && partition.is_none() {
        return Err(corrupt_storage(
            "projection query record exists without partition state",
        ));
    }

    let (change_head, compacted_through) = match partition {
        Some(partition) => {
            if let Some(record) = &record {
                if record.change.epoch() != &partition.change_epoch
                    || record.change.position() > partition.change_head
                {
                    return Err(corrupt_storage(
                        "projection query record change lies outside its partition head",
                    ));
                }
            }
            let head = if partition.change_head == 0 {
                None
            } else {
                Some(ProjectionChangeCursor::new(
                    request.scope.topology().clone(),
                    request.scope.projection_partition().clone(),
                    partition.change_epoch.clone(),
                    partition.change_head,
                )?)
            };
            (head, partition.compacted_through)
        }
        None => (None, 0),
    };

    let mut checkpoints = Vec::with_capacity(request.checkpoint_probes.len());
    for probe in &request.checkpoint_probes {
        let stored = protocol.inputs.get(&InputKey {
            partition: partition_key.clone(),
            source: probe.source.clone(),
            generation: probe.generation,
        });
        let checkpoint = match stored {
            Some(stored) => {
                let Some(partition) = partition else {
                    return Err(corrupt_storage(
                        "projection checkpoint exists without partition state",
                    ));
                };
                let (input, change) = stored;
                if input.cursor.epoch() != &probe.epoch {
                    return Err(ProjectionProtocolError::IncomparableInput);
                }
                if change.epoch() != &partition.change_epoch
                    || change.position() > partition.change_head
                {
                    return Err(corrupt_storage(
                        "projection query checkpoint lies outside its partition head",
                    ));
                }
                Some(ProjectionCheckpoint::new(
                    input.cursor.clone(),
                    change.clone(),
                    input.gap_free,
                )?)
            }
            None => None,
        };
        checkpoints.push(crate::projection_protocol::ProjectionCheckpointSnapshot {
            probe: probe.clone(),
            checkpoint,
        });
    }

    Ok(ProjectionQuerySnapshot {
        row,
        record,
        checkpoints,
        change_head,
        compacted_through,
    })
}

pub(super) fn validate_observation_from_state(
    protocol: &InMemoryProjectionProtocolState,
    request: &crate::projection_protocol::ProjectionObligationEvidenceRequest,
    observation: &ProjectionObservation,
) -> Result<(), ProjectionProtocolError> {
    if observation.causation_id != request.causation_id
        || observation.kind != request.kind
        || observation.scope != request.scope
        || observation
            .revision
            .as_ref()
            .is_some_and(|revision| revision.scope() != &request.scope)
        || (request.kind == ProjectionObservationKind::Record) != observation.revision.is_some()
    {
        return Err(corrupt_storage(
            "stored projection observation does not match its exact evidence key",
        ));
    }
    let partition = protocol
        .partitions
        .get(&PartitionKey::new(
            request.scope.topology(),
            request.scope.projection_partition(),
        ))
        .ok_or_else(|| corrupt_storage("stored projection observation has no partition state"))?;
    if observation.change.topology() != request.scope.topology()
        || observation.change.projection_partition() != request.scope.projection_partition()
        || observation.change.epoch() != &partition.change_epoch
        || observation.change.position() > partition.change_head
    {
        return Err(corrupt_storage(
            "stored projection observation change lies outside its partition",
        ));
    }
    Ok(())
}
