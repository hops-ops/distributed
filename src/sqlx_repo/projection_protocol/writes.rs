use super::*;

pub(super) async fn ensure_partition_ownership_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    ownership: &[ProjectionModelOwnership],
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    for declaration in ownership {
        let mut registered = QueryBuilder::<DB>::new(
            "SELECT topology_bytes, table_name FROM projection_registered_models \
             WHERE topology_hash = ",
        );
        registered.push_bind(topology_hash.as_slice());
        registered.push(" AND model_name = ");
        registered.push_bind(declaration.model.as_str());
        let Some(row) = registered
            .build()
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| {
                protocol_storage_error::<DB>("verify causal projection registration", error)
            })?
        else {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection model `{}` was not registered before projector traffic",
                declaration.model
            )));
        };
        let registered_topology: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode causal projection registration", error)
        })?;
        verify_bytes(
            &registered_topology,
            &topology.canonical_bytes(),
            "registered projector topology",
        )?;
        let registered_table: String = row.try_get("table_name").map_err(|error| {
            protocol_storage_error::<DB>("decode causal projection registration", error)
        })?;
        if registered_table != declaration.table {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection model `{}` was registered for table `{registered_table}`, not `{}`",
                declaration.model, declaration.table
            )));
        }

        let mut by_model = QueryBuilder::<DB>::new(
            "SELECT table_name FROM projection_model_ownership WHERE topology_hash = ",
        );
        by_model.push_bind(topology_hash.as_slice());
        by_model.push(" AND partition_hash = ");
        by_model.push_bind(partition_hash.as_slice());
        by_model.push(" AND model_name = ");
        by_model.push_bind(declaration.model.as_str());
        if let Some(row) = by_model
            .build()
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| {
                protocol_storage_error::<DB>("load projection model ownership", error)
            })?
        {
            let table: String = row.try_get("table_name").map_err(|error| {
                protocol_storage_error::<DB>("decode projection model ownership", error)
            })?;
            if table != declaration.table {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection model `{}` is already bound to table `{table}`",
                    declaration.model
                )));
            }
            continue;
        }

        let mut by_table = QueryBuilder::<DB>::new(
            "SELECT model_name FROM projection_model_ownership WHERE topology_hash = ",
        );
        by_table.push_bind(topology_hash.as_slice());
        by_table.push(" AND partition_hash = ");
        by_table.push_bind(partition_hash.as_slice());
        by_table.push(" AND table_name = ");
        by_table.push_bind(declaration.table.as_str());
        if let Some(row) = by_table
            .build()
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| {
                protocol_storage_error::<DB>("load projection table ownership", error)
            })?
        {
            let model: String = row.try_get("model_name").map_err(|error| {
                protocol_storage_error::<DB>("decode projection table ownership", error)
            })?;
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection table `{}` is already bound to model `{model}`",
                declaration.table
            )));
        }

        let mut insert = QueryBuilder::<DB>::new(
            "INSERT INTO projection_model_ownership \
             (topology_hash, partition_hash, model_name, table_name) VALUES (",
        );
        insert.push_bind(topology_hash.as_slice());
        insert.push(", ");
        insert.push_bind(partition_hash.as_slice());
        insert.push(", ");
        insert.push_bind(declaration.model.as_str());
        insert.push(", ");
        insert.push_bind(declaration.table.as_str());
        insert.push(") ON CONFLICT (topology_hash, partition_hash, model_name) DO NOTHING");
        let result = insert.build().execute(&mut **tx).await.map_err(|error| {
            protocol_storage_error::<DB>("insert projection model ownership", error)
        })?;
        if DB::rows_affected(&result) != 1 {
            return Err(corrupt_storage(
                "projection ownership changed while its partition lock was held",
            ));
        }
    }
    Ok(())
}

pub(super) async fn verify_registered_topology_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = topology.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT topology_bytes FROM projection_registered_models WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" LIMIT 1");
    let Some(row) = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("verify registered projector topology", error)
        })?
    else {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projector topology `{}` has no registered model set",
            topology.name()
        )));
    };
    let topology_bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
        protocol_storage_error::<DB>("decode registered projector topology", error)
    })?;
    verify_bytes(
        &topology_bytes,
        &topology.canonical_bytes(),
        "registered projector topology",
    )
}

pub(super) async fn record_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    scope: &ProjectionRecordScope,
    expected_change_epoch: &ProjectionEpoch,
) -> Result<Option<StoredRecord>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = scope.topology().digest();
    let partition_hash = scope.projection_partition().digest();
    let key_hash = scope.key_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT canonical_key_bytes, canonical_key_hash, incarnation, revision, tombstone, \
         change_epoch, change_position, source_snapshot FROM projection_records WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND model_name = ");
    builder.push_bind(scope.model());
    builder.push(" AND canonical_key_hash = ");
    builder.push_bind(key_hash.as_slice());
    let Some(row) = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("load projection record", error))?
    else {
        return Ok(None);
    };
    let key_bytes: Vec<u8> = row.try_get("canonical_key_bytes").map_err(|error| {
        protocol_storage_error::<DB>("decode projection record key bytes", error)
    })?;
    let stored_key_hash: Vec<u8> = row.try_get("canonical_key_hash").map_err(|error| {
        protocol_storage_error::<DB>("decode projection record key hash", error)
    })?;
    verify_bytes(
        &key_bytes,
        scope.canonical_key_bytes(),
        "projection record key",
    )?;
    verify_digest(
        &stored_key_hash,
        scope.key_digest(),
        "projection record key",
    )?;
    let incarnation = from_i64::<DB>(
        row.try_get("incarnation")
            .map_err(|error| protocol_storage_error::<DB>("decode record incarnation", error))?,
        "record incarnation",
    )?;
    let revision = from_i64::<DB>(
        row.try_get("revision")
            .map_err(|error| protocol_storage_error::<DB>("decode record revision", error))?,
        "record revision",
    )?;
    let tombstone_value: i64 = row
        .try_get("tombstone")
        .map_err(|error| protocol_storage_error::<DB>("decode record tombstone", error))?;
    let tombstone = match tombstone_value {
        0 => false,
        1 => true,
        value => {
            return Err(corrupt_storage(format!(
                "record tombstone contains invalid value {value}"
            )))
        }
    };
    let change_epoch: String = row
        .try_get("change_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode record change epoch", error))?;
    let change_epoch = ProjectionEpoch::new(change_epoch)?;
    if &change_epoch != expected_change_epoch {
        return Err(corrupt_storage(
            "projection record change epoch differs from its partition",
        ));
    }
    let change_position = from_i64::<DB>(
        row.try_get("change_position").map_err(|error| {
            protocol_storage_error::<DB>("decode record change position", error)
        })?,
        "record change position",
    )?;
    Ok(Some(StoredRecord {
        metadata: ProjectionRecordMetadata {
            source_snapshot: decode_source_snapshot(
                row.try_get("source_snapshot").map_err(|error| {
                    protocol_storage_error::<DB>("decode source snapshot", error)
                })?,
            )?,
            revision: RecordRevision::new(scope.clone(), incarnation, revision)?,
            tombstone,
            change: ProjectionChangeCursor::new(
                scope.topology().clone(),
                scope.projection_partition().clone(),
                change_epoch,
                change_position,
            )?,
        },
    }))
}

pub(super) fn next_record(
    scope: &ProjectionRecordScope,
    expectation: &ProjectionRecordExpectation,
    kind: ProjectionMutationKind,
    current: Option<&StoredRecord>,
    source_snapshot: bool,
) -> Result<(RecordRevision, bool), ProjectionProtocolError> {
    let current = current.map(|record| &record.metadata);
    match (expectation, current, kind) {
        (ProjectionRecordExpectation::Missing, None, ProjectionMutationKind::Upsert) => {
            Ok((RecordRevision::new(scope.clone(), 1, 1)?, false))
        }
        (ProjectionRecordExpectation::Missing, None, ProjectionMutationKind::Delete) => {
            Ok((RecordRevision::new(scope.clone(), 1, 1)?, true))
        }
        (ProjectionRecordExpectation::Missing, Some(metadata), _) if metadata.tombstone => {
            Err(ProjectionProtocolError::RecordTombstoned {
                model: scope.model().to_string(),
            })
        }
        (ProjectionRecordExpectation::Missing, Some(_), _) => {
            Err(ProjectionProtocolError::RecordAlreadyExists {
                model: scope.model().to_string(),
            })
        }
        (ProjectionRecordExpectation::Exact(_), None, _) => {
            Err(ProjectionProtocolError::RecordMissing {
                model: scope.model().to_string(),
            })
        }
        (ProjectionRecordExpectation::Exact(expected), Some(metadata), _) => {
            if expected != &metadata.revision {
                return Err(ProjectionProtocolError::RecordRevisionConflict {
                    model: scope.model().to_string(),
                    expected_incarnation: expected.incarnation(),
                    expected_revision: expected.revision(),
                    actual_incarnation: metadata.revision.incarnation(),
                    actual_revision: metadata.revision.revision(),
                });
            }
            match kind {
                ProjectionMutationKind::Upsert if metadata.tombstone => {
                    Err(ProjectionProtocolError::RecordTombstoned {
                        model: scope.model().to_string(),
                    })
                }
                ProjectionMutationKind::Upsert => Ok((
                    RecordRevision::new(
                        scope.clone(),
                        metadata.revision.incarnation(),
                        checked_next(metadata.revision.revision(), "record revision")?,
                    )?,
                    false,
                )),
                ProjectionMutationKind::Delete if metadata.tombstone && !source_snapshot => {
                    Err(ProjectionProtocolError::RecordTombstoned {
                        model: scope.model().to_string(),
                    })
                }
                ProjectionMutationKind::Delete => Ok((
                    RecordRevision::new(
                        scope.clone(),
                        metadata.revision.incarnation(),
                        checked_next(metadata.revision.revision(), "record revision")?,
                    )?,
                    true,
                )),
                ProjectionMutationKind::Recreate if !metadata.tombstone => {
                    Err(ProjectionProtocolError::RecreateRequiresTombstone {
                        model: scope.model().to_string(),
                    })
                }
                ProjectionMutationKind::Recreate => Ok((
                    RecordRevision::new(
                        scope.clone(),
                        checked_next(metadata.revision.incarnation(), "record incarnation")?,
                        1,
                    )?,
                    false,
                )),
            }
        }
        (_, _, ProjectionMutationKind::Recreate) => Err(ProjectionProtocolError::InvalidBatch(
            "delete/recreate requires an exact record expectation".into(),
        )),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the persisted change record keeps each protocol identity and optional evidence field explicit"
)]
pub(super) fn allocate_change(
    state: &mut PartitionState,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    kind: ProjectionChangeKind,
    causation_id: String,
    observation_kind: Option<ProjectionObservationKind>,
    scope: Option<ProjectionRecordScope>,
    revision: Option<RecordRevision>,
    failure_id: Option<String>,
) -> Result<ProjectionChange, ProjectionProtocolError> {
    state.change_head = checked_next(state.change_head, "projection change")?;
    Ok(ProjectionChange {
        cursor: ProjectionChangeCursor::new(
            topology.clone(),
            partition.clone(),
            state.change_epoch.clone(),
            state.change_head,
        )?,
        kind,
        causation_id,
        observation_kind,
        scope,
        revision,
        failure_id,
    })
}

pub(super) async fn insert_change_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    change: &ProjectionChange,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let cursor = &change.cursor;
    let position = to_i64::<DB>(cursor.position(), "projection change position")?;
    let topology_hash = cursor.topology().digest();
    let partition_hash = cursor.projection_partition().digest();
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO projection_changes \
         (topology_hash, partition_hash, change_epoch, change_position, change_kind, \
         causation_id, model_name, scope_kind, canonical_key_bytes, canonical_key_hash, \
         incarnation, revision, failure_id) VALUES (",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(", ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(cursor.epoch().as_str());
    builder.push(", ");
    builder.push_bind(position);
    builder.push(", ");
    builder.push_bind(change.kind.as_storage_str());
    builder.push(", ");
    builder.push_bind(change.causation_id.as_str());
    builder.push(", ");
    if let Some(scope) = &change.scope {
        builder.push_bind(scope.model());
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    if let Some(kind) = change.observation_kind {
        builder.push_bind(kind.as_storage_str());
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    if let Some(scope) = &change.scope {
        builder.push_bind(scope.canonical_key_bytes());
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    if let Some(scope) = &change.scope {
        let key_hash = scope.key_digest();
        builder.push_bind(key_hash.as_slice());
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    if let Some(revision) = &change.revision {
        builder.push_bind(to_i64::<DB>(
            revision.incarnation(),
            "projection record incarnation",
        )?);
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    if let Some(revision) = &change.revision {
        builder.push_bind(to_i64::<DB>(
            revision.revision(),
            "projection record revision",
        )?);
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    if let Some(failure_id) = &change.failure_id {
        builder.push_bind(failure_id.as_str());
    } else {
        builder.push("NULL");
    }
    builder.push(")");
    builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("append projection change", error))?;
    Ok(())
}

pub(super) async fn upsert_record_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    metadata: &ProjectionRecordMetadata,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let scope = metadata.revision.scope();
    let topology_hash = scope.topology().digest();
    let partition_hash = scope.projection_partition().digest();
    let key_hash = scope.key_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO projection_records \
         (topology_hash, partition_hash, model_name, canonical_key_bytes, canonical_key_hash, \
         incarnation, revision, tombstone, change_epoch, change_position, source_snapshot) VALUES (",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(", ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(scope.model());
    builder.push(", ");
    builder.push_bind(scope.canonical_key_bytes());
    builder.push(", ");
    builder.push_bind(key_hash.as_slice());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        metadata.revision.incarnation(),
        "projection record incarnation",
    )?);
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        metadata.revision.revision(),
        "projection record revision",
    )?);
    builder.push(", ");
    builder.push_bind(i64::from(metadata.tombstone));
    builder.push(", ");
    builder.push_bind(metadata.change.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        metadata.change.position(),
        "projection record change position",
    )?);
    builder.push(", ");
    let source_json = metadata
        .source_snapshot
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| corrupt_storage(format!("encode source snapshot: {error}")))?;
    if let Some(value) = &source_json {
        builder.push_bind(value.as_str());
    } else {
        builder.push("NULL");
    }
    builder.push(
        ") ON CONFLICT (topology_hash, partition_hash, model_name, canonical_key_hash) \
         DO UPDATE SET canonical_key_bytes = excluded.canonical_key_bytes, \
         incarnation = excluded.incarnation, revision = excluded.revision, \
         tombstone = excluded.tombstone, change_epoch = excluded.change_epoch, \
         change_position = excluded.change_position, source_snapshot = excluded.source_snapshot",
    );
    builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("store projection record", error))?;
    Ok(())
}

pub(super) fn decode_source_snapshot(
    value: Option<String>,
) -> Result<Option<crate::projection_protocol::SourceSnapshotVersion>, ProjectionProtocolError> {
    value
        .map(|value| {
            if value.len() > 16384 {
                return Err(corrupt_storage(
                    "source snapshot exceeds bounded metadata size",
                ));
            }
            let snapshot: crate::projection_protocol::SourceSnapshotVersion =
                serde_json::from_str(&value)
                    .map_err(|error| corrupt_storage(format!("decode source snapshot: {error}")))?;
            snapshot
                .validate_stored()
                .map_err(|error| corrupt_storage(error.to_string()))?;
            Ok(snapshot)
        })
        .transpose()
}

pub(super) fn decode_observation_row<DB>(
    row: &DB::Row,
    causation_id: &str,
    scope: &ProjectionRecordScope,
    kind: ProjectionObservationKind,
    expected_change_epoch: &ProjectionEpoch,
) -> Result<ProjectionObservation, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let key_bytes: Vec<u8> = row
        .try_get("canonical_key_bytes")
        .map_err(|error| protocol_storage_error::<DB>("decode observation key bytes", error))?;
    let stored_key_hash: Vec<u8> = row
        .try_get("canonical_key_hash")
        .map_err(|error| protocol_storage_error::<DB>("decode observation key hash", error))?;
    verify_bytes(
        &key_bytes,
        scope.canonical_key_bytes(),
        "projection observation key",
    )?;
    verify_digest(
        &stored_key_hash,
        scope.key_digest(),
        "projection observation key",
    )?;
    let incarnation: Option<i64> = row
        .try_get("incarnation")
        .map_err(|error| protocol_storage_error::<DB>("decode observation incarnation", error))?;
    let revision: Option<i64> = row
        .try_get("revision")
        .map_err(|error| protocol_storage_error::<DB>("decode observation revision", error))?;
    let revision = match (kind, incarnation, revision) {
        (ProjectionObservationKind::Record, Some(incarnation), Some(revision)) => {
            Some(RecordRevision::new(
                scope.clone(),
                from_i64::<DB>(incarnation, "observation record incarnation")?,
                from_i64::<DB>(revision, "observation record revision")?,
            )?)
        }
        (ProjectionObservationKind::Dependency, None, None) => None,
        _ => {
            return Err(corrupt_storage(
                "projection observation kind/revision shape is inconsistent",
            ))
        }
    };
    let change_epoch: String = row
        .try_get("change_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode observation change epoch", error))?;
    let change_epoch = ProjectionEpoch::new(change_epoch)?;
    if &change_epoch != expected_change_epoch {
        return Err(corrupt_storage(
            "projection observation change epoch differs from its partition",
        ));
    }
    let change_position = from_i64::<DB>(
        row.try_get("change_position").map_err(|error| {
            protocol_storage_error::<DB>("decode observation change position", error)
        })?,
        "observation change position",
    )?;
    Ok(ProjectionObservation {
        causation_id: causation_id.to_string(),
        kind,
        revision,
        scope: scope.clone(),
        change: ProjectionChangeCursor::new(
            scope.topology().clone(),
            scope.projection_partition().clone(),
            change_epoch,
            change_position,
        )?,
    })
}

pub(super) async fn observation_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    causation_id: &str,
    scope: &ProjectionRecordScope,
    kind: ProjectionObservationKind,
    expected_change_epoch: &ProjectionEpoch,
) -> Result<Option<ProjectionObservation>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = scope.topology().digest();
    let partition_hash = scope.projection_partition().digest();
    let key_hash = scope.key_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT canonical_key_bytes, canonical_key_hash, incarnation, revision, \
         change_epoch, change_position FROM projection_observations WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND causation_id = ");
    builder.push_bind(causation_id);
    builder.push(" AND model_name = ");
    builder.push_bind(scope.model());
    builder.push(" AND scope_kind = ");
    builder.push_bind(kind.as_storage_str());
    builder.push(" AND canonical_key_hash = ");
    builder.push_bind(key_hash.as_slice());
    let Some(row) = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("load projection observation", error))?
    else {
        return Ok(None);
    };
    decode_observation_row::<DB>(&row, causation_id, scope, kind, expected_change_epoch).map(Some)
}

pub(super) async fn insert_observation_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    observation: &ProjectionObservation,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let scope = &observation.scope;
    let topology_hash = scope.topology().digest();
    let partition_hash = scope.projection_partition().digest();
    let key_hash = scope.key_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO projection_observations \
         (topology_hash, partition_hash, causation_id, model_name, scope_kind, \
         canonical_key_bytes, canonical_key_hash, incarnation, revision, \
         change_epoch, change_position) VALUES (",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(", ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(observation.causation_id.as_str());
    builder.push(", ");
    builder.push_bind(scope.model());
    builder.push(", ");
    builder.push_bind(observation.kind.as_storage_str());
    builder.push(", ");
    builder.push_bind(scope.canonical_key_bytes());
    builder.push(", ");
    builder.push_bind(key_hash.as_slice());
    builder.push(", ");
    if let Some(revision) = &observation.revision {
        builder.push_bind(to_i64::<DB>(
            revision.incarnation(),
            "observation record incarnation",
        )?);
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    if let Some(revision) = &observation.revision {
        builder.push_bind(to_i64::<DB>(
            revision.revision(),
            "observation record revision",
        )?);
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    builder.push_bind(observation.change.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        observation.change.position(),
        "observation change position",
    )?);
    builder.push(")");
    builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("insert projection observation", error))?;
    Ok(())
}

pub(super) async fn store_input_cursor_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
    change: &ProjectionChangeCursor,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let cursor = &input.cursor;
    let topology_hash = cursor.topology().digest();
    let partition_hash = cursor.projection_partition().digest();
    let source = cursor.source();
    let source_bytes = source.canonical_name_bytes();
    let source_hash = source.digest();
    let source_partition_hash = source.partition_digest();
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO projection_input_cursors \
         (topology_hash, partition_hash, source_bytes, source_hash, source_partition_bytes, \
         source_partition_hash, source_epoch, source_position, input_hash, message_id, \
         causation_id, gap_free, generation, change_epoch, change_position) VALUES (",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(", ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(source_bytes.as_slice());
    builder.push(", ");
    builder.push_bind(source_hash.as_slice());
    builder.push(", ");
    builder.push_bind(source.canonical_partition_bytes());
    builder.push(", ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(cursor.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        cursor.position(),
        "projection input position",
    )?);
    builder.push(", ");
    let input_hash = input.fingerprint.digest();
    builder.push_bind(input_hash.as_slice());
    builder.push(", ");
    builder.push_bind(input.message_id.as_str());
    builder.push(", ");
    builder.push_bind(input.causation_id.as_str());
    builder.push(", ");
    builder.push_bind(i64::from(input.gap_free));
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        input.generation.get(),
        "projection generation",
    )?);
    builder.push(", ");
    builder.push_bind(change.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        change.position(),
        "projection change position",
    )?);
    builder.push(
        ") ON CONFLICT \
         (topology_hash, partition_hash, source_hash, source_partition_hash, generation) \
         DO UPDATE SET source_bytes = excluded.source_bytes, \
         source_partition_bytes = excluded.source_partition_bytes, source_epoch = excluded.source_epoch, \
         source_position = excluded.source_position, input_hash = excluded.input_hash, \
         message_id = excluded.message_id, causation_id = excluded.causation_id, \
         gap_free = excluded.gap_free, change_epoch = excluded.change_epoch, \
         change_position = excluded.change_position",
    );
    builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("store projection input cursor", error))?;
    Ok(())
}

pub(super) async fn insert_input_identity_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let cursor = &input.cursor;
    let topology_hash = cursor.topology().digest();
    let partition_hash = cursor.projection_partition().digest();
    let source = cursor.source();
    let source_bytes = source.canonical_name_bytes();
    let source_hash = source.digest();
    let source_partition_hash = source.partition_digest();
    let input_hash = input.fingerprint.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO projection_input_identities \
         (topology_hash, partition_hash, source_bytes, source_hash, source_partition_bytes, \
         source_partition_hash, source_epoch, source_position, input_hash, message_id, \
         causation_id, gap_free) VALUES (",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(", ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(source_bytes.as_slice());
    builder.push(", ");
    builder.push_bind(source_hash.as_slice());
    builder.push(", ");
    builder.push_bind(source.canonical_partition_bytes());
    builder.push(", ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(cursor.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        cursor.position(),
        "projection input identity position",
    )?);
    builder.push(", ");
    builder.push_bind(input_hash.as_slice());
    builder.push(", ");
    builder.push_bind(input.message_id.as_str());
    builder.push(", ");
    builder.push_bind(input.causation_id.as_str());
    builder.push(", ");
    builder.push_bind(i64::from(input.gap_free));
    builder.push(") ON CONFLICT DO NOTHING");
    let result =
        builder.build().execute(&mut **tx).await.map_err(|error| {
            protocol_storage_error::<DB>("insert projection input identity", error)
        })?;
    if DB::rows_affected(&result) == 1 {
        return Ok(());
    }

    if let Some(existing) = input_identity_by_cursor_in_tx(tx, input).await? {
        if input_identity_matches(&existing, input) {
            return Ok(());
        }
        return Err(ProjectionProtocolError::InputCorruption);
    }
    if input_identity_by_message_in_tx(tx, input).await?.is_some() {
        return Err(ProjectionProtocolError::MessageIdReuse {
            message_id: input.message_id.clone(),
        });
    }
    Err(corrupt_storage(
        "projection input identity collided without a readable conflicting row",
    ))
}

pub(super) async fn insert_input_receipt_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
    outcome_kind: &'static str,
    failure_id: Option<&str>,
    change: &ProjectionChangeCursor,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let cursor = &input.cursor;
    let topology_hash = cursor.topology().digest();
    let partition_hash = cursor.projection_partition().digest();
    let source = cursor.source();
    let source_bytes = source.canonical_name_bytes();
    let source_hash = source.digest();
    let source_partition_hash = source.partition_digest();
    let input_hash = input.fingerprint.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO projection_input_receipts \
         (topology_hash, partition_hash, generation, message_id, source_bytes, source_hash, \
         source_partition_bytes, source_partition_hash, source_epoch, source_position, input_hash, \
         causation_id, gap_free, outcome_kind, failure_id, change_epoch, change_position) VALUES (",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(", ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        input.generation.get(),
        "projection generation",
    )?);
    builder.push(", ");
    builder.push_bind(input.message_id.as_str());
    builder.push(", ");
    builder.push_bind(source_bytes.as_slice());
    builder.push(", ");
    builder.push_bind(source_hash.as_slice());
    builder.push(", ");
    builder.push_bind(source.canonical_partition_bytes());
    builder.push(", ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(cursor.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        cursor.position(),
        "projection input position",
    )?);
    builder.push(", ");
    builder.push_bind(input_hash.as_slice());
    builder.push(", ");
    builder.push_bind(input.causation_id.as_str());
    builder.push(", ");
    builder.push_bind(i64::from(input.gap_free));
    builder.push(", ");
    builder.push_bind(outcome_kind);
    builder.push(", ");
    if let Some(failure_id) = failure_id {
        builder.push_bind(failure_id);
    } else {
        builder.push("NULL");
    }
    builder.push(", ");
    builder.push_bind(change.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        change.position(),
        "projection change position",
    )?);
    builder.push(") ON CONFLICT DO NOTHING");
    let result =
        builder.build().execute(&mut **tx).await.map_err(|error| {
            protocol_storage_error::<DB>("insert projection input receipt", error)
        })?;
    if DB::rows_affected(&result) != 1 {
        return Err(corrupt_storage(
            "projection input receipt collided while its partition lock was held",
        ));
    }
    Ok(())
}

pub(super) async fn ensure_inbox_available_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    let receipt = input.inbox_receipt();
    receipt.validate()?;
    let mut builder = QueryBuilder::<DB>::new("SELECT 1 FROM consumer_inbox WHERE consumer = ");
    builder.push_bind(receipt.consumer.as_str());
    builder.push(" AND message_id = ");
    builder.push_bind(receipt.message_id.as_str());
    builder.push(" LIMIT 1");
    if builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("check projection consumer inbox", error))?
        .is_some()
    {
        return Err(ProjectionProtocolError::MessageIdReuse {
            message_id: input.message_id.clone(),
        });
    }
    Ok(())
}

pub(super) async fn insert_inbox_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    input: &TrustedProjectionInput,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
{
    let receipt = input.inbox_receipt();
    let mut builder =
        QueryBuilder::<DB>::new("INSERT INTO consumer_inbox (consumer, message_id) VALUES (");
    builder.push_bind(receipt.consumer.as_str());
    builder.push(", ");
    builder.push_bind(receipt.message_id.as_str());
    builder.push(") ON CONFLICT DO NOTHING");
    let result =
        builder.build().execute(&mut **tx).await.map_err(|error| {
            protocol_storage_error::<DB>("insert projection consumer inbox", error)
        })?;
    if DB::rows_affected(&result) != 1 {
        return Err(corrupt_storage(
            "projection consumer inbox collided while its partition lock was held",
        ));
    }
    Ok(())
}

pub(super) async fn update_partition_head_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    change_head: u64,
    clear_pending_retry: Option<&str>,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let mut builder = QueryBuilder::<DB>::new("UPDATE projection_partitions SET change_head = ");
    builder.push_bind(to_i64::<DB>(change_head, "projection change head")?);
    if clear_pending_retry.is_some() {
        builder.push(", pending_retry_failure_id = NULL");
    }
    builder.push(" WHERE topology_hash = ");
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    if let Some(failure_id) = clear_pending_retry {
        builder.push(" AND pending_retry_failure_id = ");
        builder.push_bind(failure_id);
    }
    let result =
        builder.build().execute(&mut **tx).await.map_err(|error| {
            protocol_storage_error::<DB>("advance projection change head", error)
        })?;
    if DB::rows_affected(&result) != 1 {
        return Err(corrupt_storage(
            "projection partition or pending retry fence changed while its lock was held",
        ));
    }
    Ok(())
}

pub(super) async fn retain_projection_change_suffix_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    state: &PartitionState,
    retention: ProjectionChangeRetention,
) -> Result<u64, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let target = state.compacted_through.max(
        state
            .change_head
            .saturating_sub(retention.max_retained_changes()),
    );
    if target <= state.compacted_through {
        return Ok(state.compacted_through);
    }

    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let old_watermark = to_i64::<DB>(state.compacted_through, "projection compaction watermark")?;
    let target_watermark = to_i64::<DB>(target, "projection compaction watermark")?;
    let mut delete =
        QueryBuilder::<DB>::new("DELETE FROM projection_changes WHERE topology_hash = ");
    delete.push_bind(topology_hash.as_slice());
    delete.push(" AND partition_hash = ");
    delete.push_bind(partition_hash.as_slice());
    delete.push(" AND change_epoch = ");
    delete.push_bind(state.change_epoch.as_str());
    delete.push(" AND change_position > ");
    delete.push_bind(old_watermark);
    delete.push(" AND change_position <= ");
    delete.push_bind(target_watermark);
    let result =
        delete.build().execute(&mut **tx).await.map_err(|error| {
            protocol_storage_error::<DB>("retain projection change suffix", error)
        })?;
    let expected_removed = target - state.compacted_through;
    if DB::rows_affected(&result) != expected_removed {
        return Err(corrupt_storage(format!(
            "projection retention expected to remove {expected_removed} changes but removed {}",
            DB::rows_affected(&result)
        )));
    }

    let mut update =
        QueryBuilder::<DB>::new("UPDATE projection_partitions SET compacted_through = ");
    update.push_bind(target_watermark);
    update.push(" WHERE topology_hash = ");
    update.push_bind(topology_hash.as_slice());
    update.push(" AND partition_hash = ");
    update.push_bind(partition_hash.as_slice());
    update.push(" AND change_epoch = ");
    update.push_bind(state.change_epoch.as_str());
    update.push(" AND compacted_through = ");
    update.push_bind(old_watermark);
    update.push(" AND change_head = ");
    update.push_bind(to_i64::<DB>(state.change_head, "projection change head")?);
    let result = update.build().execute(&mut **tx).await.map_err(|error| {
        protocol_storage_error::<DB>("advance projection retention watermark", error)
    })?;
    if DB::rows_affected(&result) != 1 {
        return Err(corrupt_storage(
            "projection partition changed while retaining its change suffix",
        ));
    }
    Ok(target)
}

pub(super) fn decode_failure_row<DB>(
    row: &DB::Row,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    expected_change_epoch: &ProjectionEpoch,
) -> Result<StoredFailure, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let source_bytes: Vec<u8> = row
        .try_get("source_bytes")
        .map_err(|error| protocol_storage_error::<DB>("decode failure source bytes", error))?;
    let source_hash: Vec<u8> = row
        .try_get("source_hash")
        .map_err(|error| protocol_storage_error::<DB>("decode failure source hash", error))?;
    let source_partition_bytes: Vec<u8> =
        row.try_get("source_partition_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode failure source partition bytes", error)
        })?;
    let source_partition_hash: Vec<u8> = row.try_get("source_partition_hash").map_err(|error| {
        protocol_storage_error::<DB>("decode failure source partition hash", error)
    })?;
    let source =
        ProjectionSource::from_canonical_name_bytes(&source_bytes, source_partition_bytes.clone())?;
    verify_digest(&source_hash, source.digest(), "projection failure source")?;
    verify_digest(
        &source_partition_hash,
        source.partition_digest(),
        "projection failure source partition",
    )?;
    let source_epoch: String = row
        .try_get("source_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode failure source epoch", error))?;
    let source_position = from_i64::<DB>(
        row.try_get("source_position").map_err(|error| {
            protocol_storage_error::<DB>("decode failure source position", error)
        })?,
        "failure source position",
    )?;
    let input_hash = decode_digest(
        row.try_get("input_hash")
            .map_err(|error| protocol_storage_error::<DB>("decode failure input hash", error))?,
        "projection input",
    )?;
    let gap_free = match row
        .try_get::<i64, _>("gap_free")
        .map_err(|error| protocol_storage_error::<DB>("decode failure gap-free flag", error))?
    {
        0 => false,
        1 => true,
        value => {
            return Err(corrupt_storage(format!(
                "failure gap-free flag contains invalid value {value}"
            )))
        }
    };
    let failure_bytes: Vec<u8> = row
        .try_get("failure_bytes")
        .map_err(|error| protocol_storage_error::<DB>("decode projection failure bytes", error))?;
    let failure_digest = decode_digest(
        row.try_get("failure_hash").map_err(|error| {
            protocol_storage_error::<DB>("decode projection failure hash", error)
        })?,
        "projection failure",
    )?;
    if ProjectionFailureBatch::fingerprint_bytes(&failure_bytes) != failure_digest {
        return Err(corrupt_storage(
            "projection failure bytes do not match their stored digest",
        ));
    }
    let change_epoch: String = row
        .try_get("change_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode failure change epoch", error))?;
    let change_epoch = ProjectionEpoch::new(change_epoch)?;
    if &change_epoch != expected_change_epoch {
        return Err(corrupt_storage(
            "projection failure change epoch differs from its partition",
        ));
    }
    let change_position = from_i64::<DB>(
        row.try_get("change_position").map_err(|error| {
            protocol_storage_error::<DB>("decode failure change position", error)
        })?,
        "failure change position",
    )?;
    let generation = ProjectionGeneration::new(from_i64::<DB>(
        row.try_get("generation")
            .map_err(|error| protocol_storage_error::<DB>("decode failure generation", error))?,
        "projection failure generation",
    )?)?;
    Ok(StoredFailure {
        failure: ProjectionFailure {
            failure_id: row.try_get("failure_id").map_err(|error| {
                protocol_storage_error::<DB>("decode projection failure ID", error)
            })?,
            input: ProjectionInputCursor::new(
                topology.clone(),
                partition.clone(),
                source,
                ProjectionEpoch::new(source_epoch)?,
                source_position,
            )?,
            input_fingerprint: ProjectionInputFingerprint::from_digest(input_hash),
            message_id: row.try_get("message_id").map_err(|error| {
                protocol_storage_error::<DB>("decode failure message ID", error)
            })?,
            causation_id: row.try_get("causation_id").map_err(|error| {
                protocol_storage_error::<DB>("decode failure causation ID", error)
            })?,
            generation,
            gap_free,
            failure_code: row.try_get("failure_code").map_err(|error| {
                protocol_storage_error::<DB>("decode projection failure code", error)
            })?,
            failure_bytes,
            failure_digest,
            change: ProjectionChangeCursor::new(
                topology.clone(),
                partition.clone(),
                change_epoch,
                change_position,
            )?,
        },
    })
}

pub(super) async fn failure_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    failure_id: &str,
    expected_change_epoch: &ProjectionEpoch,
) -> Result<Option<StoredFailure>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT failure_id, source_bytes, source_hash, source_partition_bytes, \
         source_partition_hash, source_epoch, source_position, input_hash, message_id, \
         causation_id, gap_free, generation, failure_code, failure_bytes, failure_hash, \
         change_epoch, change_position FROM projection_failures WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND failure_id = ");
    builder.push_bind(failure_id);
    let row = builder
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("load projection failure", error))?;
    row.map(|row| decode_failure_row::<DB>(&row, topology, partition, expected_change_epoch))
        .transpose()
}

pub(super) fn failure_matches_batch(
    failure: &StoredFailure,
    batch: &ProjectionFailureBatch,
) -> bool {
    crate::projection_protocol::failure_matches_batch(&failure.failure, batch)
}

pub(super) async fn ensure_pending_retry_input_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    state: &PartitionState,
    input: &TrustedProjectionInput,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let Some(failure_id) = &state.pending_retry_failure_id else {
        return Ok(());
    };
    let failure = failure_in_tx(
        tx,
        input.cursor.topology(),
        input.cursor.projection_partition(),
        failure_id,
        &state.change_epoch,
    )
    .await?
    .ok_or_else(|| {
        corrupt_storage(format!(
            "pending projection retry failure `{failure_id}` is missing"
        ))
    })?;
    if failure.failure.generation.checked_next()? != state.active_generation {
        return Err(corrupt_storage(format!(
            "pending retry failure generation {} does not precede active generation {}",
            failure.failure.generation.get(),
            state.active_generation.get()
        )));
    }
    if failure.failure.input != input.cursor {
        return Err(ProjectionProtocolError::IncomparableInput);
    }
    if failure.failure.input_fingerprint != input.fingerprint
        || failure.failure.message_id != input.message_id
        || failure.failure.causation_id != input.causation_id
        || failure.failure.gap_free != input.gap_free
    {
        return Err(ProjectionProtocolError::InputCorruption);
    }
    Ok(())
}

pub(super) async fn insert_failure_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    batch: &ProjectionFailureBatch,
    change: &ProjectionChangeCursor,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let cursor = &batch.input.cursor;
    let topology_hash = cursor.topology().digest();
    let partition_hash = cursor.projection_partition().digest();
    let source = cursor.source();
    let source_bytes = source.canonical_name_bytes();
    let source_hash = source.digest();
    let source_partition_hash = source.partition_digest();
    let input_hash = batch.input.fingerprint.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "INSERT INTO projection_failures \
         (topology_hash, partition_hash, failure_id, source_bytes, source_hash, \
         source_partition_bytes, source_partition_hash, source_epoch, source_position, \
         input_hash, message_id, causation_id, gap_free, generation, failure_code, \
         failure_bytes, failure_hash, change_epoch, change_position) VALUES (",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(", ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(batch.failure_id.as_str());
    builder.push(", ");
    builder.push_bind(source_bytes.as_slice());
    builder.push(", ");
    builder.push_bind(source_hash.as_slice());
    builder.push(", ");
    builder.push_bind(source.canonical_partition_bytes());
    builder.push(", ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(", ");
    builder.push_bind(cursor.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        cursor.position(),
        "projection failure source position",
    )?);
    builder.push(", ");
    builder.push_bind(input_hash.as_slice());
    builder.push(", ");
    builder.push_bind(batch.input.message_id.as_str());
    builder.push(", ");
    builder.push_bind(batch.input.causation_id.as_str());
    builder.push(", ");
    builder.push_bind(i64::from(batch.input.gap_free));
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        batch.input.generation.get(),
        "projection failure generation",
    )?);
    builder.push(", ");
    builder.push_bind(batch.failure_code.as_str());
    builder.push(", ");
    builder.push_bind(batch.failure_bytes.as_slice());
    builder.push(", ");
    builder.push_bind(batch.failure_digest.as_slice());
    builder.push(", ");
    builder.push_bind(change.epoch().as_str());
    builder.push(", ");
    builder.push_bind(to_i64::<DB>(
        change.position(),
        "projection failure change position",
    )?);
    builder.push(") ON CONFLICT DO NOTHING");
    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("insert projection failure", error))?;
    if DB::rows_affected(&result) != 1 {
        return Err(corrupt_storage(
            "projection failure collided while its partition lock was held",
        ));
    }
    Ok(())
}

pub(super) async fn stop_partition_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    batch: &ProjectionFailureBatch,
    change_head: u64,
) -> Result<(), ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
{
    let cursor = &batch.input.cursor;
    let topology_hash = cursor.topology().digest();
    let partition_hash = cursor.projection_partition().digest();
    let source = cursor.source();
    let source_bytes = source.canonical_name_bytes();
    let source_hash = source.digest();
    let source_partition_hash = source.partition_digest();
    let input_hash = batch.input.fingerprint.digest();
    let mut builder = QueryBuilder::<DB>::new("UPDATE projection_partitions SET change_head = ");
    builder.push_bind(to_i64::<DB>(change_head, "projection change head")?);
    builder.push(", pending_retry_failure_id = NULL, stopped_failure_id = ");
    builder.push_bind(batch.failure_id.as_str());
    builder.push(", stopped_source_bytes = ");
    builder.push_bind(source_bytes.as_slice());
    builder.push(", stopped_source_hash = ");
    builder.push_bind(source_hash.as_slice());
    builder.push(", stopped_source_partition_bytes = ");
    builder.push_bind(source.canonical_partition_bytes());
    builder.push(", stopped_source_partition_hash = ");
    builder.push_bind(source_partition_hash.as_slice());
    builder.push(", stopped_source_epoch = ");
    builder.push_bind(cursor.epoch().as_str());
    builder.push(", stopped_source_position = ");
    builder.push_bind(to_i64::<DB>(
        cursor.position(),
        "stopped projection source position",
    )?);
    builder.push(", stopped_generation = ");
    builder.push_bind(to_i64::<DB>(
        batch.input.generation.get(),
        "stopped projection generation",
    )?);
    builder.push(", stopped_input_hash = ");
    builder.push_bind(input_hash.as_slice());
    builder.push(", stopped_message_id = ");
    builder.push_bind(batch.input.message_id.as_str());
    builder.push(", stopped_causation_id = ");
    builder.push_bind(batch.input.causation_id.as_str());
    builder.push(", stopped_gap_free = ");
    builder.push_bind(i64::from(batch.input.gap_free));
    builder.push(" WHERE topology_hash = ");
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|error| protocol_storage_error::<DB>("stop projection partition", error))?;
    if DB::rows_affected(&result) != 1 {
        return Err(corrupt_storage(
            "projection partition disappeared while recording its failure",
        ));
    }
    Ok(())
}
