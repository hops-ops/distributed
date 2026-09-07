use super::*;

pub(super) fn decode_change_row<DB>(
    row: &DB::Row,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    expected_epoch: &ProjectionEpoch,
) -> Result<ProjectionChange, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    for<'q> i64: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let change_epoch: String = row
        .try_get("change_epoch")
        .map_err(|error| protocol_storage_error::<DB>("decode projection change epoch", error))?;
    let change_epoch = ProjectionEpoch::new(change_epoch)?;
    if &change_epoch != expected_epoch {
        return Err(corrupt_storage(
            "projection change epoch differs from its partition",
        ));
    }
    let change_position = from_i64::<DB>(
        row.try_get("change_position").map_err(|error| {
            protocol_storage_error::<DB>("decode projection change position", error)
        })?,
        "projection change position",
    )?;
    let kind_value: String = row
        .try_get("change_kind")
        .map_err(|error| protocol_storage_error::<DB>("decode projection change kind", error))?;
    let kind = decode_change_kind(&kind_value)?;
    let causation_id: String = row
        .try_get("causation_id")
        .map_err(|error| protocol_storage_error::<DB>("decode change causation ID", error))?;
    let model_name: Option<String> = row
        .try_get("model_name")
        .map_err(|error| protocol_storage_error::<DB>("decode change model", error))?;
    let scope_kind: Option<String> = row
        .try_get("scope_kind")
        .map_err(|error| protocol_storage_error::<DB>("decode change scope kind", error))?;
    let key_bytes: Option<Vec<u8>> = row
        .try_get("canonical_key_bytes")
        .map_err(|error| protocol_storage_error::<DB>("decode change key bytes", error))?;
    let key_hash: Option<Vec<u8>> = row
        .try_get("canonical_key_hash")
        .map_err(|error| protocol_storage_error::<DB>("decode change key hash", error))?;
    let incarnation: Option<i64> = row
        .try_get("incarnation")
        .map_err(|error| protocol_storage_error::<DB>("decode change record incarnation", error))?;
    let revision_value: Option<i64> = row
        .try_get("revision")
        .map_err(|error| protocol_storage_error::<DB>("decode change record revision", error))?;
    let failure_id: Option<String> = row
        .try_get("failure_id")
        .map_err(|error| protocol_storage_error::<DB>("decode change failure ID", error))?;

    let scope = match (model_name, key_bytes, key_hash) {
        (Some(model), Some(bytes), Some(hash)) => {
            let scope =
                ProjectionRecordScope::new(topology.clone(), partition.clone(), model, bytes)?;
            verify_digest(&hash, scope.key_digest(), "projection change key")?;
            Some(scope)
        }
        (None, None, None) => None,
        _ => {
            return Err(corrupt_storage(
                "projection change record scope has an inconsistent shape",
            ));
        }
    };
    let revision = match (scope.as_ref(), incarnation, revision_value) {
        (Some(scope), Some(incarnation), Some(revision)) => Some(RecordRevision::new(
            scope.clone(),
            from_i64::<DB>(incarnation, "change record incarnation")?,
            from_i64::<DB>(revision, "change record revision")?,
        )?),
        (_, None, None) => None,
        _ => {
            return Err(corrupt_storage(
                "projection change record revision has an inconsistent shape",
            ));
        }
    };
    let observation_kind = scope_kind
        .as_deref()
        .map(decode_observation_kind)
        .transpose()?;
    match kind {
        ProjectionChangeKind::RecordUpsert
        | ProjectionChangeKind::RecordDelete
        | ProjectionChangeKind::RecordRecreate
            if scope.is_some()
                && revision.is_some()
                && observation_kind.is_none()
                && failure_id.is_none() => {}
        ProjectionChangeKind::Observation
            if scope.is_some()
                && observation_kind.is_some()
                && failure_id.is_none()
                && ((observation_kind == Some(ProjectionObservationKind::Record)
                    && revision.is_some())
                    || (observation_kind == Some(ProjectionObservationKind::Dependency)
                        && revision.is_none())) => {}
        ProjectionChangeKind::Checkpoint
            if scope.is_none()
                && revision.is_none()
                && observation_kind.is_none()
                && failure_id.is_none() => {}
        ProjectionChangeKind::Failure
            if scope.is_none()
                && revision.is_none()
                && observation_kind.is_none()
                && failure_id.is_some() => {}
        _ => {
            return Err(corrupt_storage(format!(
                "projection change `{kind_value}` payload has an inconsistent shape"
            )));
        }
    }
    Ok(ProjectionChange {
        cursor: ProjectionChangeCursor::new(
            topology.clone(),
            partition.clone(),
            change_epoch,
            change_position,
        )?,
        kind,
        causation_id,
        observation_kind,
        scope,
        revision,
        failure_id,
    })
}

/// Apply one sealed same-transaction command projection inside its caller's
/// already-open domain/ledger transaction.
///
/// The adapter, rather than the command handler, allocates record revisions
/// and change cursors while holding the authoritative projection partition
/// lock. The returned evidence is attached to the command completion before
/// the caller executes the final ledger fence.
pub(crate) async fn apply_same_transaction_projection_in_tx<DB>(
    tx: &mut Transaction<'_, DB>,
    batch: &SameTransactionProjectionBatch,
    retention: ProjectionChangeRetention,
) -> Result<SameTransactionProjectionEvidence, ProjectionProtocolError>
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
    batch.validate()?;
    let write_plan = TableWritePlan::new(
        batch
            .mutations
            .iter()
            .map(|mutation| mutation.mutation.clone())
            .collect(),
    );
    validate_sql_write_plan(&write_plan)?;
    verify_registered_topology_in_tx(tx, &batch.topology).await?;
    let mut state =
        lock_partition_in_tx(tx, &batch.topology, &batch.partition, &batch.change_epoch).await?;
    if let Some(failure_id) = &state.stopped_failure_id {
        return Err(ProjectionProtocolError::PartitionStopped {
            failure_id: failure_id.clone(),
        });
    }
    if state.pending_retry_failure_id.is_some() {
        return Err(ProjectionProtocolError::IncomparableInput);
    }
    ensure_partition_ownership_in_tx(tx, &batch.topology, &batch.partition, &batch.ownership)
        .await?;

    let staged = batch
        .mutations
        .first()
        .expect("same-transaction projection validation requires one mutation");
    let owner = batch
        .ownership
        .first()
        .expect("same-transaction projection validation requires one owner");
    if staged.mutation.table_name() != owner.table
        || table_model_name(&staged.mutation) != staged.scope.model()
    {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "direct projection mutation for model `{}` does not target its registered table",
            staged.scope.model()
        )));
    }

    let current = record_in_tx(tx, &staged.scope, &state.change_epoch).await?;
    crate::projection_protocol::validate_snapshot_write(
        current.as_ref().map(|record| &record.metadata),
        None,
    )?;
    let physical_exists = physical_row_exists_in_tx(tx, &staged.mutation).await?;
    match current.as_ref().map(|record| &record.metadata) {
        None if physical_exists => {
            return Err(ProjectionProtocolError::RecordAlreadyExists {
                model: staged.scope.model().to_string(),
            });
        }
        Some(metadata) if metadata.tombstone => {
            return Err(ProjectionProtocolError::RecordTombstoned {
                model: staged.scope.model().to_string(),
            });
        }
        Some(_) if !physical_exists => {
            return Err(ProjectionProtocolError::RecordMissing {
                model: staged.scope.model().to_string(),
            });
        }
        _ => {}
    }
    let expectation = current
        .as_ref()
        .map(|record| ProjectionRecordExpectation::Exact(record.metadata.revision.clone()))
        .unwrap_or(ProjectionRecordExpectation::Missing);
    let (revision, tombstone) = next_record(
        &staged.scope,
        &expectation,
        ProjectionMutationKind::Upsert,
        current.as_ref(),
        false,
    )?;
    debug_assert!(!tombstone);
    let change = allocate_change(
        &mut state,
        &batch.topology,
        &batch.partition,
        ProjectionChangeKind::RecordUpsert,
        batch.causation_id.clone(),
        None,
        Some(staged.scope.clone()),
        Some(revision.clone()),
        None,
    )?;
    let metadata = ProjectionRecordMetadata {
        source_snapshot: None,
        revision: revision.clone(),
        tombstone,
        change: change.cursor.clone(),
    };

    let observation_request = batch
        .observations
        .first()
        .expect("same-transaction projection validation requires one observation");
    if observation_in_tx(
        tx,
        &batch.causation_id,
        &staged.scope,
        observation_request.kind,
        &state.change_epoch,
    )
    .await?
    .is_some()
    {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "direct projection causation `{}` already has record evidence",
            batch.causation_id
        )));
    }
    let observation = ProjectionObservation {
        causation_id: batch.causation_id.clone(),
        kind: observation_request.kind,
        scope: staged.scope.clone(),
        revision: Some(revision),
        change: change.cursor.clone(),
    };

    apply_read_model_write_plan_in_tx(tx, write_plan).await?;
    insert_change_in_tx(tx, &change).await?;
    upsert_record_in_tx(tx, &metadata).await?;
    insert_observation_in_tx(tx, &observation).await?;
    update_partition_head_in_tx(
        tx,
        &batch.topology,
        &batch.partition,
        state.change_head,
        None,
    )
    .await?;
    retain_projection_change_suffix_in_tx(tx, &batch.topology, &batch.partition, &state, retention)
        .await?;

    Ok(SameTransactionProjectionEvidence {
        records: vec![metadata],
        changes: vec![change],
        observations: vec![observation],
    })
}

/// Execute the entire query-row/evidence read as one SQL statement.
///
/// PostgreSQL's default transaction isolation takes a fresh snapshot for every
/// statement, so merely wrapping independent row and metadata selects in a
/// transaction would still permit mixed evidence. This joined statement gives
/// SQLite and PostgreSQL the same statement-level snapshot and repeats the
/// physical/protocol columns only when multiple explicit checkpoint probes
/// match.
pub(crate) async fn read_projection_query_snapshot_in_executor<'e, DB, E>(
    executor: E,
    request: &ProjectionQuerySnapshotRequest,
) -> Result<ProjectionQuerySnapshot, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    E: Executor<'e, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    request.validate()?;
    let schema = request.schema.as_ref();
    let physical_version_column = version_column(schema)?;
    let topology_hash = request.scope.topology().digest();
    let partition_hash = request.scope.projection_partition().digest();
    let key_hash = request.scope.key_digest();

    let mut builder = QueryBuilder::<DB>::new("SELECT ");
    for (index, column) in schema.columns.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        builder.push("snapshot_row.");
        builder.push(quote_identifier(&column.column_name));
        builder.push(" AS ");
        builder.push(quote_identifier(&format!("ps_row_{index}")));
    }
    if !schema.columns.is_empty() {
        builder.push(", ");
    }
    builder.push("snapshot_row.");
    builder.push(quote_identifier(physical_version_column));
    builder.push(
        " AS ps_row_version, \
         registered.topology_bytes AS ps_registered_topology_bytes, \
         registered.table_name AS ps_registered_table, \
         partition.topology_bytes AS ps_partition_topology_bytes, \
         partition.partition_bytes AS ps_partition_bytes, \
         partition.active_generation AS ps_active_generation, \
         partition.change_epoch AS ps_change_epoch, \
         partition.change_head AS ps_change_head, \
         partition.compacted_through AS ps_compacted, \
         record.canonical_key_bytes AS ps_record_key_bytes, \
         record.canonical_key_hash AS ps_record_key_hash, \
         record.incarnation AS ps_record_incarnation, \
         record.revision AS ps_record_revision, \
         record.tombstone AS ps_record_tombstone, \
         record.change_epoch AS ps_record_change_epoch, \
         record.change_position AS ps_record_change_position, \
         record.source_snapshot AS ps_source_snapshot, \
         checkpoint.source_bytes AS ps_cursor_source_bytes, \
         checkpoint.source_hash AS ps_cursor_source_hash, \
         checkpoint.source_partition_bytes AS ps_cursor_partition_bytes, \
         checkpoint.source_partition_hash AS ps_cursor_partition_hash, \
         checkpoint.source_epoch AS ps_cursor_source_epoch, \
         checkpoint.source_position AS ps_cursor_source_position, \
         checkpoint.gap_free AS ps_cursor_gap_free, \
         checkpoint.generation AS ps_cursor_generation, \
         checkpoint.change_epoch AS ps_cursor_change_epoch, \
         checkpoint.change_position AS ps_cursor_change_position \
         FROM (SELECT 1 AS snapshot_anchor) AS snapshot_anchor \
         LEFT JOIN (SELECT ",
    );
    for (index, column) in schema.columns.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        DB::push_select_column(&mut builder, column);
    }
    if !schema.columns.is_empty() {
        builder.push(", ");
    }
    builder.push(quote_identifier(physical_version_column));
    builder.push(" FROM ");
    builder.push(quote_identifier(&schema.table_name));
    push_key_predicates(&mut builder, schema, &request.key)?;
    builder.push(") AS snapshot_row ON 1 = 1 ");

    builder.push(
        "LEFT JOIN projection_registered_models AS registered \
         ON registered.topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND registered.model_name = ");
    builder.push_bind(request.scope.model());

    builder.push(
        " LEFT JOIN projection_partitions AS partition \
         ON partition.topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition.partition_hash = ");
    builder.push_bind(partition_hash.as_slice());

    builder.push(
        " LEFT JOIN projection_records AS record \
         ON record.topology_hash = partition.topology_hash \
         AND record.partition_hash = partition.partition_hash \
         AND record.model_name = ",
    );
    builder.push_bind(request.scope.model());
    builder.push(" AND record.canonical_key_hash = ");
    builder.push_bind(key_hash.as_slice());

    builder.push(
        " LEFT JOIN projection_input_cursors AS checkpoint \
         ON checkpoint.topology_hash = partition.topology_hash \
         AND checkpoint.partition_hash = partition.partition_hash AND ",
    );
    if request.checkpoint_probes.is_empty() {
        builder.push("1 = 0");
    } else {
        builder.push("(");
        for (index, probe) in request.checkpoint_probes.iter().enumerate() {
            if index > 0 {
                builder.push(" OR ");
            }
            builder.push("(checkpoint.source_hash = ");
            let source_hash = probe.source.digest();
            builder.push_bind(source_hash.as_slice());
            builder.push(" AND checkpoint.source_partition_hash = ");
            let source_partition_hash = probe.source.partition_digest();
            builder.push_bind(source_partition_hash.as_slice());
            builder.push(" AND checkpoint.generation = ");
            builder.push_bind(to_i64::<DB>(
                probe.generation.get(),
                "projection generation",
            )?);
            builder.push(")");
        }
        builder.push(")");
    }
    builder.push(
        " ORDER BY checkpoint.source_hash, checkpoint.source_partition_hash, \
         checkpoint.generation",
    );

    let rows = builder.build().fetch_all(executor).await.map_err(|error| {
        protocol_storage_error::<DB>("read atomic projection query snapshot", error)
    })?;
    let first = rows.first().ok_or_else(|| {
        corrupt_storage("atomic projection query snapshot returned no anchor row")
    })?;

    let row_version: Option<i64> = first.try_get("ps_row_version").map_err(|error| {
        protocol_storage_error::<DB>("decode projection query row presence", error)
    })?;
    let physical_row = if row_version.is_some() {
        let mut values = RowValues::new();
        for (index, column) in schema.columns.iter().enumerate() {
            let mut aliased = column.clone();
            aliased.column_name = format!("ps_row_{index}");
            values.insert(column.column_name.clone(), DB::row_value(first, &aliased)?);
        }
        validate_row_values(schema, &values, true)?;
        validate_values_match_key(schema, &request.key, &values)?;
        Some(values)
    } else {
        None
    };

    let registered_topology: Option<Vec<u8>> = first
        .try_get("ps_registered_topology_bytes")
        .map_err(|error| {
            protocol_storage_error::<DB>("decode projection query registered topology", error)
        })?;
    let Some(registered_topology) = registered_topology else {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection query model `{}` has no registered topology owner",
            request.scope.model()
        )));
    };
    verify_bytes(
        &registered_topology,
        &request.scope.topology().canonical_bytes(),
        "projection query registered topology",
    )?;
    let registered_table: Option<String> =
        first.try_get("ps_registered_table").map_err(|error| {
            protocol_storage_error::<DB>("decode projection query registered table", error)
        })?;
    if registered_table.as_deref() != Some(schema.table_name.as_str()) {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection query model `{}` is registered to table `{}`, not `{}`",
            request.scope.model(),
            registered_table.as_deref().unwrap_or("<missing>"),
            schema.table_name
        )));
    }

    let active_generation: Option<i64> =
        first.try_get("ps_active_generation").map_err(|error| {
            protocol_storage_error::<DB>("decode projection query partition presence", error)
        })?;
    let partition = match active_generation {
        Some(active_generation) => {
            let topology_bytes: Option<Vec<u8>> = first
                .try_get("ps_partition_topology_bytes")
                .map_err(|error| {
                    protocol_storage_error::<DB>(
                        "decode projection query partition topology",
                        error,
                    )
                })?;
            let partition_bytes: Option<Vec<u8>> =
                first.try_get("ps_partition_bytes").map_err(|error| {
                    protocol_storage_error::<DB>("decode projection query partition bytes", error)
                })?;
            verify_bytes(
                topology_bytes
                    .as_deref()
                    .ok_or_else(|| corrupt_storage("partition topology bytes are missing"))?,
                &request.scope.topology().canonical_bytes(),
                "projection query partition topology",
            )?;
            verify_bytes(
                partition_bytes
                    .as_deref()
                    .ok_or_else(|| corrupt_storage("partition bytes are missing"))?,
                request.scope.projection_partition().canonical_bytes(),
                "projection query partition",
            )?;
            let change_epoch: Option<String> =
                first.try_get("ps_change_epoch").map_err(|error| {
                    protocol_storage_error::<DB>("decode projection query change epoch", error)
                })?;
            let change_head: Option<i64> = first.try_get("ps_change_head").map_err(|error| {
                protocol_storage_error::<DB>("decode projection query change head", error)
            })?;
            let compacted_through: Option<i64> =
                first.try_get("ps_compacted").map_err(|error| {
                    protocol_storage_error::<DB>(
                        "decode projection query compaction watermark",
                        error,
                    )
                })?;
            let state = PartitionState {
                active_generation: ProjectionGeneration::new(from_i64::<DB>(
                    active_generation,
                    "projection active generation",
                )?)?,
                change_epoch: ProjectionEpoch::new(
                    change_epoch
                        .ok_or_else(|| corrupt_storage("partition change epoch is missing"))?,
                )?,
                change_head: from_i64::<DB>(
                    change_head
                        .ok_or_else(|| corrupt_storage("partition change head is missing"))?,
                    "projection change head",
                )?,
                compacted_through: from_i64::<DB>(
                    compacted_through.ok_or_else(|| {
                        corrupt_storage("partition compaction watermark is missing")
                    })?,
                    "projection compaction watermark",
                )?,
                pending_retry_failure_id: None,
                stopped_failure_id: None,
            };
            if state.compacted_through > state.change_head {
                return Err(corrupt_storage(
                    "projection compaction watermark exceeds change head",
                ));
            }
            Some(state)
        }
        None => None,
    };

    let record_incarnation: Option<i64> =
        first.try_get("ps_record_incarnation").map_err(|error| {
            protocol_storage_error::<DB>("decode projection query record presence", error)
        })?;
    let record = match record_incarnation {
        Some(incarnation) => {
            let Some(partition) = partition.as_ref() else {
                return Err(corrupt_storage(
                    "projection record exists without partition state",
                ));
            };
            let key_bytes: Option<Vec<u8>> =
                first.try_get("ps_record_key_bytes").map_err(|error| {
                    protocol_storage_error::<DB>("decode projection query record key bytes", error)
                })?;
            let stored_key_hash: Option<Vec<u8>> =
                first.try_get("ps_record_key_hash").map_err(|error| {
                    protocol_storage_error::<DB>("decode projection query record key hash", error)
                })?;
            verify_bytes(
                key_bytes
                    .as_deref()
                    .ok_or_else(|| corrupt_storage("projection record key bytes are missing"))?,
                request.scope.canonical_key_bytes(),
                "projection query record key",
            )?;
            verify_digest(
                stored_key_hash
                    .as_deref()
                    .ok_or_else(|| corrupt_storage("projection record key hash is missing"))?,
                request.scope.key_digest(),
                "projection query record key",
            )?;
            let revision: Option<i64> = first.try_get("ps_record_revision").map_err(|error| {
                protocol_storage_error::<DB>("decode projection query record revision", error)
            })?;
            let tombstone: Option<i64> = first.try_get("ps_record_tombstone").map_err(|error| {
                protocol_storage_error::<DB>("decode projection query record tombstone", error)
            })?;
            let tombstone = match tombstone
                .ok_or_else(|| corrupt_storage("projection record tombstone is missing"))?
            {
                0 => false,
                1 => true,
                value => {
                    return Err(corrupt_storage(format!(
                        "record tombstone contains invalid value {value}"
                    )));
                }
            };
            let record_change_epoch: Option<String> =
                first.try_get("ps_record_change_epoch").map_err(|error| {
                    protocol_storage_error::<DB>(
                        "decode projection query record change epoch",
                        error,
                    )
                })?;
            let record_change_epoch = ProjectionEpoch::new(
                record_change_epoch
                    .ok_or_else(|| corrupt_storage("record change epoch is missing"))?,
            )?;
            if record_change_epoch != partition.change_epoch {
                return Err(corrupt_storage(
                    "projection record change epoch differs from its partition",
                ));
            }
            let record_change_position: Option<i64> = first
                .try_get("ps_record_change_position")
                .map_err(|error| {
                    protocol_storage_error::<DB>(
                        "decode projection query record change position",
                        error,
                    )
                })?;
            let record_change_position = from_i64::<DB>(
                record_change_position
                    .ok_or_else(|| corrupt_storage("record change position is missing"))?,
                "projection record change position",
            )?;
            if record_change_position > partition.change_head {
                return Err(corrupt_storage(
                    "projection record change exceeds its partition head",
                ));
            }
            Some(ProjectionRecordMetadata {
                revision: RecordRevision::new(
                    request.scope.clone(),
                    from_i64::<DB>(incarnation, "projection record incarnation")?,
                    from_i64::<DB>(
                        revision.ok_or_else(|| corrupt_storage("record revision is missing"))?,
                        "projection record revision",
                    )?,
                )?,
                tombstone,
                source_snapshot: decode_source_snapshot(
                    first.try_get("ps_source_snapshot").map_err(|error| {
                        protocol_storage_error::<DB>("decode source snapshot", error)
                    })?,
                )?,
                change: ProjectionChangeCursor::new(
                    request.scope.topology().clone(),
                    request.scope.projection_partition().clone(),
                    record_change_epoch,
                    record_change_position,
                )?,
            })
        }
        None => None,
    };

    match (physical_row.is_some(), record.as_ref()) {
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

    let mut checkpoint_values = vec![None; request.checkpoint_probes.len()];
    for row in &rows {
        let stored_generation: Option<i64> =
            row.try_get("ps_cursor_generation").map_err(|error| {
                protocol_storage_error::<DB>("decode projection query checkpoint presence", error)
            })?;
        let Some(stored_generation) = stored_generation else {
            continue;
        };
        let Some(partition) = partition.as_ref() else {
            return Err(corrupt_storage(
                "projection checkpoint exists without partition state",
            ));
        };
        let generation = ProjectionGeneration::new(from_i64::<DB>(
            stored_generation,
            "projection checkpoint generation",
        )?)?;
        let source_bytes: Option<Vec<u8>> =
            row.try_get("ps_cursor_source_bytes").map_err(|error| {
                protocol_storage_error::<DB>(
                    "decode projection query checkpoint source bytes",
                    error,
                )
            })?;
        let source_hash: Option<Vec<u8>> =
            row.try_get("ps_cursor_source_hash").map_err(|error| {
                protocol_storage_error::<DB>(
                    "decode projection query checkpoint source hash",
                    error,
                )
            })?;
        let source_partition_bytes: Option<Vec<u8>> =
            row.try_get("ps_cursor_partition_bytes").map_err(|error| {
                protocol_storage_error::<DB>(
                    "decode projection query checkpoint source partition bytes",
                    error,
                )
            })?;
        let source_partition_hash: Option<Vec<u8>> =
            row.try_get("ps_cursor_partition_hash").map_err(|error| {
                protocol_storage_error::<DB>(
                    "decode projection query checkpoint source partition hash",
                    error,
                )
            })?;
        let source_hash = source_hash
            .as_deref()
            .ok_or_else(|| corrupt_storage("checkpoint source hash is missing"))?;
        let source_partition_hash = source_partition_hash
            .as_deref()
            .ok_or_else(|| corrupt_storage("checkpoint source partition hash is missing"))?;
        let Some(probe_index) = request.checkpoint_probes.iter().position(|probe| {
            probe.generation == generation
                && source_hash == probe.source.digest().as_slice()
                && source_partition_hash == probe.source.partition_digest().as_slice()
        }) else {
            return Err(corrupt_storage(
                "checkpoint row does not match an explicit query probe",
            ));
        };
        let probe = &request.checkpoint_probes[probe_index];
        verify_bytes(
            source_bytes
                .as_deref()
                .ok_or_else(|| corrupt_storage("checkpoint source bytes are missing"))?,
            &probe.source.canonical_name_bytes(),
            "projection query checkpoint source",
        )?;
        verify_digest(
            source_hash,
            probe.source.digest(),
            "projection query checkpoint source",
        )?;
        verify_bytes(
            source_partition_bytes
                .as_deref()
                .ok_or_else(|| corrupt_storage("checkpoint source partition bytes are missing"))?,
            probe.source.canonical_partition_bytes(),
            "projection query checkpoint source partition",
        )?;
        verify_digest(
            source_partition_hash,
            probe.source.partition_digest(),
            "projection query checkpoint source partition",
        )?;
        let source_epoch: Option<String> =
            row.try_get("ps_cursor_source_epoch").map_err(|error| {
                protocol_storage_error::<DB>(
                    "decode projection query checkpoint source epoch",
                    error,
                )
            })?;
        let source_epoch = ProjectionEpoch::new(
            source_epoch.ok_or_else(|| corrupt_storage("checkpoint source epoch is missing"))?,
        )?;
        if source_epoch != probe.epoch {
            return Err(ProjectionProtocolError::IncomparableInput);
        }
        let source_position: Option<i64> =
            row.try_get("ps_cursor_source_position").map_err(|error| {
                protocol_storage_error::<DB>(
                    "decode projection query checkpoint source position",
                    error,
                )
            })?;
        let gap_free: Option<i64> = row.try_get("ps_cursor_gap_free").map_err(|error| {
            protocol_storage_error::<DB>("decode projection query checkpoint gap-free flag", error)
        })?;
        let gap_free =
            match gap_free.ok_or_else(|| corrupt_storage("checkpoint gap-free flag is missing"))? {
                0 => false,
                1 => true,
                value => {
                    return Err(corrupt_storage(format!(
                        "cursor gap-free flag contains invalid value {value}"
                    )));
                }
            };
        let checkpoint_change_epoch: Option<String> =
            row.try_get("ps_cursor_change_epoch").map_err(|error| {
                protocol_storage_error::<DB>(
                    "decode projection query checkpoint change epoch",
                    error,
                )
            })?;
        let checkpoint_change_epoch = ProjectionEpoch::new(
            checkpoint_change_epoch
                .ok_or_else(|| corrupt_storage("checkpoint change epoch is missing"))?,
        )?;
        let checkpoint_change_position: Option<i64> =
            row.try_get("ps_cursor_change_position").map_err(|error| {
                protocol_storage_error::<DB>(
                    "decode projection query checkpoint change position",
                    error,
                )
            })?;
        let checkpoint_change_position = from_i64::<DB>(
            checkpoint_change_position
                .ok_or_else(|| corrupt_storage("checkpoint change position is missing"))?,
            "projection checkpoint change position",
        )?;
        if checkpoint_change_epoch != partition.change_epoch
            || checkpoint_change_position > partition.change_head
        {
            return Err(corrupt_storage(
                "projection checkpoint change lies outside its partition head",
            ));
        }
        let checkpoint = ProjectionCheckpoint::new(
            ProjectionInputCursor::new(
                probe.topology.clone(),
                probe.partition.clone(),
                probe.source.clone(),
                source_epoch,
                from_i64::<DB>(
                    source_position
                        .ok_or_else(|| corrupt_storage("checkpoint source position is missing"))?,
                    "projection checkpoint source position",
                )?,
            )?,
            ProjectionChangeCursor::new(
                probe.topology.clone(),
                probe.partition.clone(),
                checkpoint_change_epoch,
                checkpoint_change_position,
            )?,
            gap_free,
        )?;
        if checkpoint_values[probe_index].replace(checkpoint).is_some() {
            return Err(corrupt_storage(
                "projection query returned duplicate checkpoint rows",
            ));
        }
    }

    let checkpoints = request
        .checkpoint_probes
        .iter()
        .cloned()
        .zip(checkpoint_values)
        .map(
            |(probe, checkpoint)| crate::projection_protocol::ProjectionCheckpointSnapshot {
                probe,
                checkpoint,
            },
        )
        .collect();
    let (change_head, compacted_through) = match partition {
        Some(partition) => {
            let head = if partition.change_head == 0 {
                None
            } else {
                Some(ProjectionChangeCursor::new(
                    request.scope.topology().clone(),
                    request.scope.projection_partition().clone(),
                    partition.change_epoch,
                    partition.change_head,
                )?)
            };
            (head, partition.compacted_through)
        }
        None => (None, 0),
    };

    Ok(ProjectionQuerySnapshot {
        row: physical_row,
        record,
        checkpoints,
        change_head,
        compacted_through,
    })
}

pub(crate) async fn read_projection_execution_snapshot_batch_in_executor<DB>(
    connection: &mut DB::Connection,
    request: &ProjectionExecutionSnapshotBatchRequest,
) -> Result<ProjectionExecutionSnapshotBatch, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    request.validate()?;
    let mut snapshots = Vec::with_capacity(request.requests.len());
    for row_request in &request.requests {
        let snapshot =
            read_projection_query_snapshot_in_executor::<DB, _>(&mut *connection, row_request)
                .await?;
        snapshots.push(ProjectionScopedRowSnapshot {
            scope: row_request.scope.clone(),
            row: snapshot.row,
            record: snapshot.record,
        });
    }
    Ok(ProjectionExecutionSnapshotBatch { snapshots })
}

pub(crate) async fn read_projection_graph_snapshot_in_executor<DB>(
    connection: &mut DB::Connection,
    request: &ProjectionGraphSnapshotRequest,
) -> Result<ProjectionGraphSnapshot, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    validate_projection_graph_snapshot_request(request)?;
    let root_snapshot =
        read_projection_query_snapshot_in_executor::<DB, _>(&mut *connection, &request.root)
            .await?;
    let root = ProjectionScopedRowSnapshot {
        scope: request.root.scope.clone(),
        row: root_snapshot.row,
        record: root_snapshot.record,
    };
    let mut includes = std::collections::BTreeMap::new();
    let mut unique_scopes = std::collections::HashSet::from([root.scope.clone()]);
    let mut materialized_snapshots = 1;
    for (name, include) in &request.includes {
        let mut snapshots = Vec::new();
        if let Some(root_row) = root.row.as_ref() {
            let keys = read_projection_relationship_keys_in_executor::<DB>(
                connection,
                &request.root.schema,
                root_row,
                &include.relationship,
                &include.target_schema,
                request.max_unique_record_scopes,
            )
            .await?;
            let codec = ProjectionScopeCodec::with_models(
                request.root.scope.topology().clone(),
                [(
                    include.target_schema.model_name.as_str(),
                    include.target_schema.as_ref(),
                )],
            )
            .map_err(|error| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "invalid projection graph target schema: {error}"
                ))
            })?;
            for key in keys {
                materialized_snapshots =
                    checked_projection_graph_materialization(materialized_snapshots)?;
                let scope = codec
                    .encode_row_scope_in_partition(
                        &include.target_schema.model_name,
                        request.root.scope.projection_partition().clone(),
                        &key,
                    )
                    .map_err(|error| {
                        ProjectionProtocolError::InvalidBatch(format!(
                            "invalid projection graph included key: {error}"
                        ))
                    })?;
                let is_new_scope = unique_scopes.insert(scope.clone());
                if is_new_scope && unique_scopes.len() > request.max_unique_record_scopes {
                    return Err(projection_graph_budget_error(
                        &request.root.schema,
                        unique_scopes.len(),
                        request.max_unique_record_scopes,
                    ));
                }
                let row_request = ProjectionQuerySnapshotRequest {
                    schema: Arc::clone(&include.target_schema),
                    key,
                    scope: scope.clone(),
                    checkpoint_probes: Vec::new(),
                };
                let snapshot = read_projection_query_snapshot_in_executor::<DB, _>(
                    &mut *connection,
                    &row_request,
                )
                .await?;
                snapshots.push(ProjectionScopedRowSnapshot {
                    scope,
                    row: snapshot.row,
                    record: snapshot.record,
                });
            }
            snapshots.sort_by(|left, right| {
                left.scope
                    .canonical_key_bytes()
                    .cmp(right.scope.canonical_key_bytes())
            });
        }
        includes.insert(
            name.clone(),
            ProjectionGraphIncludeSnapshot {
                relationship: include.relationship.clone(),
                target_schema: include.target_schema.as_ref().clone(),
                rows: snapshots,
            },
        );
    }
    Ok(ProjectionGraphSnapshot { root, includes })
}

#[allow(clippy::too_many_arguments)]
async fn read_projection_relationship_keys_in_executor<DB>(
    connection: &mut DB::Connection,
    root_schema: &TableSchema,
    root_row: &RowValues,
    relationship: &crate::table::RelationshipDef,
    target_schema: &TableSchema,
    max_unique: usize,
) -> Result<Vec<RowKey>, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let foreign_key = relationship.foreign_key.as_deref().ok_or_else(|| {
        ProjectionProtocolError::InvalidBatch(format!(
            "projection graph relationship `{}` has no foreign key",
            relationship.field_name
        ))
    })?;
    let (target_column, value) = match relationship.kind {
        RelationshipKind::HasMany => {
            let (target_column, root_column) =
                projection_has_many_columns(root_schema, relationship, target_schema)?;
            let value = root_row.get(&root_column).cloned().ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph root `{}` is missing relationship key `{root_column}`",
                    root_schema.model_name
                ))
            })?;
            (target_column, value)
        }
        RelationshipKind::BelongsTo => {
            let source_column = column_name_for(root_schema, foreign_key).ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph relationship `{}` foreign key `{foreign_key}` is not a source column",
                    relationship.field_name
                ))
            })?;
            let [target_column] = target_schema.primary_key.columns.as_slice() else {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph belongs-to target `{}` must have one primary-key column",
                    target_schema.model_name
                )));
            };
            let value = root_row.get(&source_column).cloned().ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph root `{}` is missing relationship key `{source_column}`",
                    root_schema.model_name
                ))
            })?;
            (target_column.clone(), value)
        }
        RelationshipKind::ManyToMany => {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection graph relationship `{}` is many-to-many; project an explicit join read model instead",
                relationship.field_name
            )));
        }
    };
    if value == RowValue::Null {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<DB>::new("SELECT ");
    for (index, primary_key) in target_schema.primary_key.columns.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        DB::push_select_column(&mut builder, column_by_name(target_schema, primary_key)?);
    }
    builder.push(" FROM ");
    builder.push(quote_identifier(&target_schema.table_name));
    builder.push(" WHERE ");
    builder.push(quote_identifier(&target_column));
    builder.push(" = ");
    DB::push_row_value_bind(
        &mut builder,
        value,
        column_by_name(target_schema, &target_column)?,
    )?;
    push_order_by_primary_key(&mut builder, target_schema);
    builder.push(" LIMIT ");
    builder.push(max_unique.saturating_add(1).to_string());

    let rows = builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("load projection graph relationship keys", error)
        })?;
    if rows.len() > max_unique {
        return Err(projection_graph_budget_error(
            root_schema,
            max_unique.saturating_add(1),
            max_unique,
        ));
    }
    rows.iter()
        .map(|row| {
            let values = target_schema
                .primary_key
                .columns
                .iter()
                .map(|primary_key| {
                    let column = column_by_name(target_schema, primary_key)?;
                    Ok((primary_key.clone(), DB::row_value(row, column)?))
                })
                .collect::<Result<Vec<_>, TableStoreError>>()?;
            Ok(RowKey::new(values))
        })
        .collect()
}

fn projection_graph_budget_error(
    root_schema: &TableSchema,
    returned: usize,
    maximum: usize,
) -> ProjectionProtocolError {
    ProjectionProtocolError::InvalidBatch(format!(
        "projection graph model `{}` returned {returned} unique record scopes; request budget is {maximum}",
        root_schema.model_name
    ))
}

/// Resolve a bounded command-obligation set from one existing SQL snapshot.
///
/// Observations and failures are read in two set queries on the same borrowed
/// connection. A durable failure is immutable and therefore wins even when an
/// observation for the same causation also exists or its change entry has been
/// compacted.
pub(crate) async fn read_projection_obligation_evidence_batch_in_executor<DB>(
    connection: &mut DB::Connection,
    request: &ProjectionObligationEvidenceBatchRequest,
) -> Result<ProjectionObligationEvidenceBatch, ProjectionProtocolError>
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
    request.validate()?;
    if request.requests.is_empty() {
        return Ok(ProjectionObligationEvidenceBatch::default());
    }

    let mut observations = vec![None; request.requests.len()];
    let mut observation_query = QueryBuilder::<DB>::new(
        "SELECT observation.topology_hash AS evidence_topology_hash, \
         observation.partition_hash AS evidence_partition_hash, \
         observation.causation_id AS evidence_causation_id, \
         observation.model_name AS evidence_model_name, \
         observation.scope_kind AS evidence_scope_kind, \
         observation.canonical_key_bytes, observation.canonical_key_hash, \
         observation.incarnation, observation.revision, observation.change_epoch, \
         observation.change_position, partition.topology_bytes AS evidence_topology_bytes, \
         partition.partition_bytes AS evidence_partition_bytes, \
         partition.change_epoch AS evidence_partition_epoch, \
         partition.change_head AS evidence_partition_head \
         FROM projection_observations AS observation \
         INNER JOIN projection_partitions AS partition \
         ON partition.topology_hash = observation.topology_hash \
         AND partition.partition_hash = observation.partition_hash WHERE ",
    );
    for (index, probe) in request.requests.iter().enumerate() {
        if index > 0 {
            observation_query.push(" OR ");
        }
        let topology_hash = probe.scope.topology().digest();
        let partition_hash = probe.scope.projection_partition().digest();
        let key_hash = probe.scope.key_digest();
        observation_query.push("(observation.topology_hash = ");
        observation_query.push_bind(topology_hash.as_slice());
        observation_query.push(" AND observation.partition_hash = ");
        observation_query.push_bind(partition_hash.as_slice());
        observation_query.push(" AND observation.causation_id = ");
        observation_query.push_bind(probe.causation_id.as_str());
        observation_query.push(" AND observation.model_name = ");
        observation_query.push_bind(probe.scope.model());
        observation_query.push(" AND observation.scope_kind = ");
        observation_query.push_bind(probe.kind.as_storage_str());
        observation_query.push(" AND observation.canonical_key_hash = ");
        observation_query.push_bind(key_hash.as_slice());
        observation_query.push(")");
    }
    let observation_rows = observation_query
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("read projection obligation observations", error)
        })?;
    for row in observation_rows {
        let topology_hash = decode_digest(
            row.try_get("evidence_topology_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode evidence topology hash", error)
            })?,
            "projection evidence topology",
        )?;
        let partition_hash = decode_digest(
            row.try_get("evidence_partition_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode evidence partition hash", error)
            })?,
            "projection evidence partition",
        )?;
        let causation_id: String = row
            .try_get("evidence_causation_id")
            .map_err(|error| protocol_storage_error::<DB>("decode evidence causation ID", error))?;
        let model: String = row
            .try_get("evidence_model_name")
            .map_err(|error| protocol_storage_error::<DB>("decode evidence model", error))?;
        let kind_value: String = row.try_get("evidence_scope_kind").map_err(|error| {
            protocol_storage_error::<DB>("decode evidence observation kind", error)
        })?;
        let kind = decode_observation_kind(&kind_value)?;
        let key_hash = decode_digest(
            row.try_get("canonical_key_hash")
                .map_err(|error| protocol_storage_error::<DB>("decode evidence key hash", error))?,
            "projection evidence key",
        )?;
        let key_bytes: Vec<u8> = row
            .try_get("canonical_key_bytes")
            .map_err(|error| protocol_storage_error::<DB>("decode evidence key bytes", error))?;
        let digest_candidates = request
            .requests
            .iter()
            .enumerate()
            .filter(|(_, probe)| {
                probe.scope.topology().digest() == topology_hash
                    && probe.scope.projection_partition().digest() == partition_hash
                    && probe.causation_id == causation_id
                    && probe.scope.model() == model
                    && probe.kind == kind
                    && probe.scope.key_digest() == key_hash
            })
            .collect::<Vec<_>>();
        let exact = digest_candidates
            .iter()
            .filter(|(_, probe)| probe.scope.canonical_key_bytes() == key_bytes.as_slice())
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let [index] = exact.as_slice() else {
            return Err(corrupt_storage(if digest_candidates.is_empty() {
                "projection observation escaped its bounded evidence predicate".to_string()
            } else {
                "projection observation canonical key does not match its digest lookup".to_string()
            }));
        };
        let probe = &request.requests[*index];
        let topology_bytes: Vec<u8> = row.try_get("evidence_topology_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode evidence topology bytes", error)
        })?;
        verify_bytes(
            &topology_bytes,
            &probe.scope.topology().canonical_bytes(),
            "projection evidence topology",
        )?;
        let partition_bytes: Vec<u8> =
            row.try_get("evidence_partition_bytes").map_err(|error| {
                protocol_storage_error::<DB>("decode evidence partition bytes", error)
            })?;
        verify_bytes(
            &partition_bytes,
            probe.scope.projection_partition().canonical_bytes(),
            "projection evidence partition",
        )?;
        let partition_epoch = ProjectionEpoch::new(
            row.try_get::<String, _>("evidence_partition_epoch")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode evidence partition epoch", error)
                })?,
        )?;
        let partition_head = from_i64::<DB>(
            row.try_get("evidence_partition_head").map_err(|error| {
                protocol_storage_error::<DB>("decode evidence partition head", error)
            })?,
            "projection evidence partition head",
        )?;
        let observation = decode_observation_row::<DB>(
            &row,
            &probe.causation_id,
            &probe.scope,
            probe.kind,
            &partition_epoch,
        )?;
        if observation.change.position() > partition_head {
            return Err(corrupt_storage(
                "projection observation change exceeds its partition head",
            ));
        }
        if observations[*index].replace(observation).is_some() {
            return Err(corrupt_storage(
                "projection obligation evidence returned duplicate observations",
            ));
        }
    }

    let mut failures = vec![None; request.requests.len()];
    let mut failure_query = QueryBuilder::<DB>::new(
        "SELECT failure.topology_hash AS evidence_topology_hash, \
         failure.partition_hash AS evidence_partition_hash, failure.failure_id, \
         failure.source_bytes, failure.source_hash, failure.source_partition_bytes, \
         failure.source_partition_hash, failure.source_epoch, failure.source_position, \
         failure.input_hash, failure.message_id, failure.causation_id, failure.gap_free, \
         failure.generation, failure.failure_code, failure.failure_bytes, \
         failure.failure_hash, failure.change_epoch, failure.change_position, \
         partition.topology_bytes AS evidence_topology_bytes, \
         partition.partition_bytes AS evidence_partition_bytes, \
         partition.change_epoch AS evidence_partition_epoch, \
         partition.change_head AS evidence_partition_head \
         FROM projection_failures AS failure \
         INNER JOIN projection_partitions AS partition \
         ON partition.topology_hash = failure.topology_hash \
         AND partition.partition_hash = failure.partition_hash WHERE ",
    );
    for (index, probe) in request.requests.iter().enumerate() {
        if index > 0 {
            failure_query.push(" OR ");
        }
        let topology_hash = probe.scope.topology().digest();
        let partition_hash = probe.scope.projection_partition().digest();
        failure_query.push("(failure.topology_hash = ");
        failure_query.push_bind(topology_hash.as_slice());
        failure_query.push(" AND failure.partition_hash = ");
        failure_query.push_bind(partition_hash.as_slice());
        failure_query.push(" AND failure.causation_id = ");
        failure_query.push_bind(probe.causation_id.as_str());
        failure_query.push(")");
    }
    failure_query.push(
        " ORDER BY failure.topology_hash, failure.partition_hash, \
         failure.causation_id, failure.change_position, failure.failure_id",
    );
    let failure_rows = failure_query
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("read projection obligation failures", error)
        })?;
    for row in failure_rows {
        let topology_hash = decode_digest(
            row.try_get("evidence_topology_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode failure evidence topology hash", error)
            })?,
            "projection failure evidence topology",
        )?;
        let partition_hash = decode_digest(
            row.try_get("evidence_partition_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode failure evidence partition hash", error)
            })?,
            "projection failure evidence partition",
        )?;
        let causation_id: String = row.try_get("causation_id").map_err(|error| {
            protocol_storage_error::<DB>("decode failure evidence causation ID", error)
        })?;
        let matching = request
            .requests
            .iter()
            .enumerate()
            .filter(|(_, probe)| {
                probe.scope.topology().digest() == topology_hash
                    && probe.scope.projection_partition().digest() == partition_hash
                    && probe.causation_id == causation_id
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let Some(first_index) = matching.first().copied() else {
            return Err(corrupt_storage(
                "projection failure escaped its bounded evidence predicate",
            ));
        };
        let first = &request.requests[first_index];
        let topology_bytes: Vec<u8> = row.try_get("evidence_topology_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode failure evidence topology bytes", error)
        })?;
        verify_bytes(
            &topology_bytes,
            &first.scope.topology().canonical_bytes(),
            "projection failure evidence topology",
        )?;
        let partition_bytes: Vec<u8> =
            row.try_get("evidence_partition_bytes").map_err(|error| {
                protocol_storage_error::<DB>("decode failure evidence partition bytes", error)
            })?;
        verify_bytes(
            &partition_bytes,
            first.scope.projection_partition().canonical_bytes(),
            "projection failure evidence partition",
        )?;
        let partition_epoch = ProjectionEpoch::new(
            row.try_get::<String, _>("evidence_partition_epoch")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode failure evidence partition epoch", error)
                })?,
        )?;
        let partition_head = from_i64::<DB>(
            row.try_get("evidence_partition_head").map_err(|error| {
                protocol_storage_error::<DB>("decode failure evidence partition head", error)
            })?,
            "projection failure evidence partition head",
        )?;
        let failure = decode_failure_row::<DB>(
            &row,
            first.scope.topology(),
            first.scope.projection_partition(),
            &partition_epoch,
        )?
        .failure;
        if failure.causation_id != causation_id || failure.change.position() > partition_head {
            return Err(corrupt_storage(
                "projection failure evidence lies outside its exact partition/causation",
            ));
        }
        // Rows are ordered by change position, so retaining the first durable
        // failure preserves the command promise's original terminal outcome.
        for index in matching {
            if failures[index].is_none() {
                failures[index] = Some(failure.clone());
            }
        }
    }

    let evidence = failures
        .into_iter()
        .zip(observations)
        .map(|(failure, observation)| match (failure, observation) {
            (Some(failure), _) => ProjectionObligationEvidence::TerminalFailure(failure),
            (None, Some(observation)) => ProjectionObligationEvidence::Observed(observation),
            (None, None) => ProjectionObligationEvidence::Pending,
        })
        .collect();
    Ok(ProjectionObligationEvidenceBatch { evidence })
}

/// Discover the bounded durable evidence for one modeled command causation.
///
/// The command ledger stores only opaque scope tokens. This read returns
/// server-internal candidates from one SQL snapshot; the authenticated GraphQL
/// authority later remints each token and accepts exact byte equality only.
pub(crate) async fn read_projection_causation_evidence_in_executor<DB>(
    connection: &mut DB::Connection,
    request: &ProjectionCausationEvidenceRequest,
) -> Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>
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
    request.validate()?;
    let overflow_limit = MAX_PROJECTION_EVIDENCE_BATCH_ITEMS + 1;
    let mut observation_query = QueryBuilder::<DB>::new(
        "SELECT observation.topology_hash, observation.partition_hash, \
         observation.causation_id, observation.model_name, observation.scope_kind, \
         observation.canonical_key_bytes, observation.canonical_key_hash, \
         observation.incarnation, observation.revision, observation.change_epoch, \
         observation.change_position, partition.topology_bytes, \
         partition.partition_bytes, partition.change_epoch AS partition_change_epoch, \
         partition.change_head AS partition_change_head \
         FROM projection_observations observation \
         INNER JOIN projection_partitions partition \
         ON partition.topology_hash = observation.topology_hash \
         AND partition.partition_hash = observation.partition_hash \
         WHERE observation.causation_id = ",
    );
    observation_query.push_bind(request.causation_id.as_str());
    observation_query.push(" AND (");
    for (index, topology) in request.topologies.iter().enumerate() {
        if index > 0 {
            observation_query.push(" OR ");
        }
        let topology_hash = topology.digest();
        observation_query.push("observation.topology_hash = ");
        observation_query.push_bind(topology_hash.as_slice());
    }
    observation_query.push(")");
    observation_query.push(
        " ORDER BY observation.topology_hash, observation.partition_hash, \
         observation.model_name, observation.scope_kind, observation.canonical_key_hash \
         LIMIT ",
    );
    observation_query.push(overflow_limit.to_string());
    let rows = observation_query
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("read modeled projection causation observations", error)
        })?;
    if rows.len() > MAX_PROJECTION_EVIDENCE_BATCH_ITEMS {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection causation has more than {} observations",
            MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
        )));
    }

    let mut observations = Vec::with_capacity(rows.len());
    for row in rows {
        let stored_causation_id: String = row.try_get("causation_id").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence causation ID", error)
        })?;
        if stored_causation_id != request.causation_id {
            return Err(corrupt_storage(
                "modeled projection observation escaped its causation predicate",
            ));
        }
        let topology_bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence topology bytes", error)
        })?;
        let topology = ProjectorTopologyId::from_canonical_bytes(&topology_bytes)?;
        if !request
            .topologies
            .iter()
            .any(|allowed| allowed == &topology)
        {
            return Err(corrupt_storage(
                "modeled projection observation escaped its topology predicate",
            ));
        }
        let topology_hash: Vec<u8> = row.try_get("topology_hash").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence topology hash", error)
        })?;
        verify_digest(
            &topology_hash,
            topology.digest(),
            "modeled projection evidence topology",
        )?;
        let partition_bytes: Vec<u8> = row.try_get("partition_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence partition bytes", error)
        })?;
        let partition = ProjectionPartition::new(partition_bytes)?;
        let partition_hash: Vec<u8> = row.try_get("partition_hash").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence partition hash", error)
        })?;
        verify_digest(
            &partition_hash,
            partition.digest(),
            "modeled projection evidence partition",
        )?;
        let model: String = row.try_get("model_name").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence model", error)
        })?;
        let key_bytes: Vec<u8> = row.try_get("canonical_key_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence key bytes", error)
        })?;
        let scope = ProjectionRecordScope::new(topology, partition, model, key_bytes)?;
        let key_hash: Vec<u8> = row.try_get("canonical_key_hash").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence key hash", error)
        })?;
        verify_digest(
            &key_hash,
            scope.key_digest(),
            "modeled projection evidence key",
        )?;
        let kind_value: String = row.try_get("scope_kind").map_err(|error| {
            protocol_storage_error::<DB>("decode modeled evidence observation kind", error)
        })?;
        let kind = decode_observation_kind(&kind_value)?;
        let partition_epoch = ProjectionEpoch::new(
            row.try_get::<String, _>("partition_change_epoch")
                .map_err(|error| {
                    protocol_storage_error::<DB>("decode modeled evidence partition epoch", error)
                })?,
        )?;
        let partition_head = from_i64::<DB>(
            row.try_get("partition_change_head").map_err(|error| {
                protocol_storage_error::<DB>("decode modeled evidence partition head", error)
            })?,
            "modeled projection evidence partition head",
        )?;
        let observation = decode_observation_row::<DB>(
            &row,
            &request.causation_id,
            &scope,
            kind,
            &partition_epoch,
        )?;
        if observation.change.position() > partition_head
            || observations.iter().any(|existing: &ProjectionObservation| {
                existing.kind == observation.kind && existing.scope == observation.scope
            })
        {
            return Err(corrupt_storage(
                "modeled projection observation is duplicated or exceeds its partition head",
            ));
        }
        observations.push(observation);
    }

    let mut failure_query = QueryBuilder::<DB>::new(
        "SELECT DISTINCT partition.topology_bytes, partition.topology_hash \
         FROM projection_failures failure \
         INNER JOIN projection_partitions partition \
         ON partition.topology_hash = failure.topology_hash \
         AND partition.partition_hash = failure.partition_hash \
         AND partition.stopped_failure_id = failure.failure_id \
         WHERE failure.causation_id = ",
    );
    failure_query.push_bind(request.causation_id.as_str());
    failure_query.push(" AND (");
    for (index, topology) in request.topologies.iter().enumerate() {
        if index > 0 {
            failure_query.push(" OR ");
        }
        let topology_hash = topology.digest();
        failure_query.push("failure.topology_hash = ");
        failure_query.push_bind(topology_hash.as_slice());
    }
    failure_query.push(")");
    failure_query.push(" ORDER BY partition.topology_hash LIMIT ");
    failure_query.push(overflow_limit.to_string());
    let rows = failure_query
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>(
                "read modeled projection causation terminal failures",
                error,
            )
        })?;
    if rows.len() > MAX_PROJECTION_EVIDENCE_BATCH_ITEMS {
        return Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection causation has more than {} stopped topologies",
            MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
        )));
    }
    let mut terminal_failure_topologies = Vec::with_capacity(rows.len());
    for row in rows {
        let topology_bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode stopped evidence topology bytes", error)
        })?;
        let topology = ProjectorTopologyId::from_canonical_bytes(&topology_bytes)?;
        if !request
            .topologies
            .iter()
            .any(|allowed| allowed == &topology)
        {
            return Err(corrupt_storage(
                "stopped modeled projection escaped its topology predicate",
            ));
        }
        let topology_hash: Vec<u8> = row.try_get("topology_hash").map_err(|error| {
            protocol_storage_error::<DB>("decode stopped evidence topology hash", error)
        })?;
        verify_digest(
            &topology_hash,
            topology.digest(),
            "stopped modeled projection topology",
        )?;
        if terminal_failure_topologies
            .iter()
            .any(|existing| existing == &topology)
        {
            return Err(corrupt_storage(
                "modeled projection causation returned duplicate stopped topologies",
            ));
        }
        terminal_failure_topologies.push(topology);
    }
    Ok(ProjectionCausationEvidenceBatch {
        observations,
        terminal_failure_topologies,
    })
}

/// Recover exact live record scopes for typed physical-row keys without
/// requiring the caller to know hidden projection partitions.
pub(crate) async fn read_projection_live_record_batch_in_executor<DB>(
    connection: &mut DB::Connection,
    request: &ProjectionLiveRecordBatchRequest,
) -> Result<ProjectionLiveRecordBatch, ProjectionProtocolError>
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
    request.validate()?;
    if request.requests.is_empty() {
        return Ok(ProjectionLiveRecordBatch::default());
    }

    let mut registered = vec![false; request.requests.len()];
    let mut registration_query = QueryBuilder::<DB>::new(
        "SELECT topology_bytes, topology_hash, model_name, table_name \
         FROM projection_registered_models WHERE ",
    );
    for (index, probe) in request.requests.iter().enumerate() {
        if index > 0 {
            registration_query.push(" OR ");
        }
        let topology_hash = probe.topology.digest();
        registration_query.push("(topology_hash = ");
        registration_query.push_bind(topology_hash.as_slice());
        registration_query.push(" AND model_name = ");
        registration_query.push_bind(probe.model());
        registration_query.push(")");
    }
    let registration_rows = registration_query
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("read projection live-record registrations", error)
        })?;
    for row in registration_rows {
        let topology_hash = decode_digest(
            row.try_get("topology_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record topology hash", error)
            })?,
            "projection live-record topology",
        )?;
        let model: String = row.try_get("model_name").map_err(|error| {
            protocol_storage_error::<DB>("decode live-record registered model", error)
        })?;
        let topology_bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode live-record topology bytes", error)
        })?;
        let table: String = row.try_get("table_name").map_err(|error| {
            protocol_storage_error::<DB>("decode live-record registered table", error)
        })?;
        let matching = request
            .requests
            .iter()
            .enumerate()
            .filter(|(_, probe)| probe.topology.digest() == topology_hash && probe.model() == model)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Err(corrupt_storage(
                "projection registration escaped its bounded live-record predicate",
            ));
        }
        for index in matching {
            let probe = &request.requests[index];
            verify_bytes(
                &topology_bytes,
                &probe.topology.canonical_bytes(),
                "projection live-record topology",
            )?;
            if table != probe.schema.table_name {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection live-record model `{}` is registered to table `{table}`, not `{}`",
                    probe.model(),
                    probe.schema.table_name
                )));
            }
            if std::mem::replace(&mut registered[index], true) {
                return Err(corrupt_storage(
                    "projection live-record model has duplicate registrations",
                ));
            }
        }
    }
    for (index, is_registered) in registered.into_iter().enumerate() {
        if !is_registered {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection live-record model `{}` has no registered topology owner",
                request.requests[index].model()
            )));
        }
    }

    let mut record_query = QueryBuilder::<DB>::new(
        "SELECT record.topology_hash AS live_topology_hash, record.partition_hash AS \
         live_partition_hash, record.model_name AS live_model_name, \
         record.canonical_key_bytes, record.canonical_key_hash, record.incarnation, \
         record.revision, record.tombstone, record.change_epoch, record.change_position, record.source_snapshot, \
         partition.topology_bytes AS live_topology_bytes, \
         partition.partition_bytes AS live_partition_bytes, \
         partition.change_epoch AS live_partition_epoch, \
         partition.change_head AS live_partition_head \
         FROM projection_records AS record \
         INNER JOIN projection_partitions AS partition \
         ON partition.topology_hash = record.topology_hash \
         AND partition.partition_hash = record.partition_hash \
         WHERE record.tombstone = 0 AND (",
    );
    for (index, probe) in request.requests.iter().enumerate() {
        if index > 0 {
            record_query.push(" OR ");
        }
        let topology_hash = probe.topology.digest();
        record_query.push("(record.topology_hash = ");
        record_query.push_bind(topology_hash.as_slice());
        record_query.push(" AND record.model_name = ");
        record_query.push_bind(probe.model());
        record_query.push(" AND record.canonical_key_hash = ");
        record_query.push_bind(probe.canonical_key_hash.as_slice());
        record_query.push(")");
    }
    record_query.push(")");
    let rows = record_query
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            protocol_storage_error::<DB>("read projection live-record evidence", error)
        })?;
    let mut records = vec![None; request.requests.len()];
    for row in rows {
        let topology_hash = decode_digest(
            row.try_get("live_topology_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record topology hash", error)
            })?,
            "projection live-record topology",
        )?;
        let model: String = row
            .try_get("live_model_name")
            .map_err(|error| protocol_storage_error::<DB>("decode live-record model", error))?;
        let key_hash = decode_digest(
            row.try_get("canonical_key_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record key hash", error)
            })?,
            "projection live-record key",
        )?;
        let key_bytes: Vec<u8> = row
            .try_get("canonical_key_bytes")
            .map_err(|error| protocol_storage_error::<DB>("decode live-record key bytes", error))?;
        let digest_candidates = request
            .requests
            .iter()
            .enumerate()
            .filter(|(_, probe)| {
                probe.topology.digest() == topology_hash
                    && probe.model() == model
                    && probe.canonical_key_hash == key_hash
            })
            .collect::<Vec<_>>();
        let exact = digest_candidates
            .iter()
            .filter(|(_, probe)| probe.canonical_key_bytes == key_bytes)
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let [index] = exact.as_slice() else {
            return Err(corrupt_storage(if digest_candidates.is_empty() {
                "projection record escaped its bounded live-record predicate".to_string()
            } else {
                "projection live-record canonical key does not match its digest lookup".to_string()
            }));
        };
        let probe = &request.requests[*index];
        let topology_bytes: Vec<u8> = row.try_get("live_topology_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode live-record topology bytes", error)
        })?;
        verify_bytes(
            &topology_bytes,
            &probe.topology.canonical_bytes(),
            "projection live-record topology",
        )?;
        let partition_bytes: Vec<u8> = row.try_get("live_partition_bytes").map_err(|error| {
            protocol_storage_error::<DB>("decode live-record partition bytes", error)
        })?;
        let partition = ProjectionPartition::new(partition_bytes)?;
        let stored_partition_hash: Vec<u8> =
            row.try_get("live_partition_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record partition hash", error)
            })?;
        verify_digest(
            &stored_partition_hash,
            partition.digest(),
            "projection live-record partition",
        )?;
        let scope = ProjectionRecordScope::new(
            probe.topology.clone(),
            partition,
            probe.model().to_string(),
            key_bytes,
        )?;
        verify_digest(&key_hash, scope.key_digest(), "projection live-record key")?;
        let incarnation = from_i64::<DB>(
            row.try_get("incarnation").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record incarnation", error)
            })?,
            "projection live-record incarnation",
        )?;
        let revision = from_i64::<DB>(
            row.try_get("revision").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record revision", error)
            })?,
            "projection live-record revision",
        )?;
        let tombstone: i64 = row
            .try_get("tombstone")
            .map_err(|error| protocol_storage_error::<DB>("decode live-record tombstone", error))?;
        if tombstone != 0 {
            return Err(corrupt_storage(
                "projection live-record lookup returned a tombstone",
            ));
        }
        let partition_epoch =
            ProjectionEpoch::new(row.try_get::<String, _>("live_partition_epoch").map_err(
                |error| protocol_storage_error::<DB>("decode live-record partition epoch", error),
            )?)?;
        let change_epoch =
            ProjectionEpoch::new(row.try_get::<String, _>("change_epoch").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record change epoch", error)
            })?)?;
        if change_epoch != partition_epoch {
            return Err(corrupt_storage(
                "projection live-record change epoch differs from its partition",
            ));
        }
        let change_position = from_i64::<DB>(
            row.try_get("change_position").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record change position", error)
            })?,
            "projection live-record change position",
        )?;
        let partition_head = from_i64::<DB>(
            row.try_get("live_partition_head").map_err(|error| {
                protocol_storage_error::<DB>("decode live-record partition head", error)
            })?,
            "projection live-record partition head",
        )?;
        if change_position > partition_head {
            return Err(corrupt_storage(
                "projection live-record change exceeds its partition head",
            ));
        }
        let metadata =
            ProjectionRecordMetadata {
                source_snapshot: decode_source_snapshot(row.try_get("source_snapshot").map_err(
                    |error| protocol_storage_error::<DB>("decode source snapshot", error),
                )?)?,
                revision: RecordRevision::new(scope.clone(), incarnation, revision)?,
                tombstone: false,
                change: ProjectionChangeCursor::new(
                    scope.topology().clone(),
                    scope.projection_partition().clone(),
                    change_epoch,
                    change_position,
                )?,
            };
        if records[*index].replace(metadata).is_some() {
            return Err(corrupt_storage(format!(
                "projection live-record identity for model `{}` is ambiguous across partitions",
                probe.model()
            )));
        }
    }
    Ok(ProjectionLiveRecordBatch { records })
}

/// Boxed operation run inside one framework-owned projection read snapshot.
///
/// The boxed lifetime prevents callers from leaking the borrowed connection
/// beyond the transaction boundary.
pub(crate) type ProjectionReadSnapshotFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProjectionProtocolError>> + Send + 'a>>;

/// Run a physical query plan and all of its causal-evidence probes in one
/// repeatable database snapshot.
///
/// PostgreSQL uses `REPEATABLE READ READ ONLY`; its default READ COMMITTED
/// isolation would let each metadata statement observe a newer commit than the
/// physical GraphQL statement. SQLite holds one ordinary read transaction,
/// whose first read establishes the snapshot for the remaining plan.
pub(crate) async fn with_projection_read_snapshot<DB, T, F>(
    pool: &Pool<DB>,
    operation: F,
) -> Result<T, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    T: Send,
    F: for<'connection> FnOnce(
            &'connection mut DB::Connection,
        ) -> ProjectionReadSnapshotFuture<'connection, T>
        + Send,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
{
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| protocol_storage_error::<DB>("begin projection read snapshot", error))?;
    if DB::BACKEND == "postgres" {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                protocol_storage_error::<DB>("configure projection read snapshot", error)
            })?;
    }

    let result = operation(&mut *tx).await;
    match result {
        Ok(value) => {
            tx.commit().await.map_err(|error| {
                protocol_storage_error::<DB>("commit projection read snapshot", error)
            })?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await.map_err(|rollback_error| {
                protocol_storage_error::<DB>(
                    "roll back failed projection read snapshot",
                    rollback_error,
                )
            })?;
            Err(error)
        }
    }
}

pub(super) async fn read_projection_changes_in_executor_after_state<DB, AfterState>(
    connection: &mut DB::Connection,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    after: Option<&ProjectionChangeCursor>,
    limit: usize,
    after_state: AfterState,
) -> Result<ProjectionChangeRead, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    AfterState: Future<Output = ()> + Send,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    let state = load_partition_in_connection(connection, topology, partition).await?;
    after_state.await;
    let Some(state) = state else {
        return Ok(match after {
            Some(_) => ProjectionChangeRead::ResetRequired {
                head: None,
                compacted_through: 0,
            },
            None => ProjectionChangeRead::Changes {
                head: None,
                compacted_through: 0,
                changes: Vec::new(),
            },
        });
    };
    let head = if state.change_head == 0 {
        None
    } else {
        Some(ProjectionChangeCursor::new(
            topology.clone(),
            partition.clone(),
            state.change_epoch.clone(),
            state.change_head,
        )?)
    };
    if after.is_none() && state.compacted_through > 0 {
        return Ok(ProjectionChangeRead::ResetRequired {
            head,
            compacted_through: state.compacted_through,
        });
    }
    let start = match after {
        Some(cursor)
            if cursor.topology() != topology
                || cursor.projection_partition() != partition
                || cursor.epoch() != &state.change_epoch
                || cursor.position() > state.change_head
                || cursor.position() < state.compacted_through =>
        {
            return Ok(ProjectionChangeRead::ResetRequired {
                head,
                compacted_through: state.compacted_through,
            });
        }
        Some(cursor) => cursor.position(),
        None => state.compacted_through,
    };
    if limit == 0 || start == state.change_head {
        return Ok(ProjectionChangeRead::Changes {
            head,
            compacted_through: state.compacted_through,
            changes: Vec::new(),
        });
    }
    let topology_hash = topology.digest();
    let partition_hash = partition.digest();
    let mut builder = QueryBuilder::<DB>::new(
        "SELECT change_epoch, change_position, change_kind, causation_id, model_name, \
         scope_kind, canonical_key_bytes, canonical_key_hash, incarnation, revision, \
         failure_id FROM projection_changes WHERE topology_hash = ",
    );
    builder.push_bind(topology_hash.as_slice());
    builder.push(" AND partition_hash = ");
    builder.push_bind(partition_hash.as_slice());
    builder.push(" AND change_epoch = ");
    builder.push_bind(state.change_epoch.as_str());
    builder.push(" AND change_position > ");
    builder.push_bind(to_i64::<DB>(start, "projection change read position")?);
    builder.push(" ORDER BY change_position ASC LIMIT ");
    builder.push_bind(i64::try_from(limit).unwrap_or(i64::MAX));
    let rows = builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| protocol_storage_error::<DB>("read projection changes", error))?;
    let mut changes = Vec::with_capacity(rows.len());
    let mut expected = checked_next(start, "projection change read")?;
    for row in rows {
        let change = decode_change_row::<DB>(&row, topology, partition, &state.change_epoch)?;
        if change.cursor.position() != expected {
            return Err(corrupt_storage(format!(
                "projection change log expected position {expected} but found {}",
                change.cursor.position()
            )));
        }
        expected = if change.cursor.position() == state.change_head {
            state.change_head
        } else {
            checked_next(change.cursor.position(), "projection change read")?
        };
        changes.push(change);
    }
    if changes.is_empty() {
        return Err(corrupt_storage(format!(
            "projection change log is missing retained position {}",
            checked_next(start, "projection change read")?
        )));
    }
    Ok(ProjectionChangeRead::Changes {
        head,
        compacted_through: state.compacted_through,
        changes,
    })
}

/// Read one durable resumable projection-change page using an existing
/// database executor and therefore the caller's already-established snapshot.
pub(crate) async fn read_projection_changes_in_executor<DB>(
    connection: &mut DB::Connection,
    topology: &ProjectorTopologyId,
    partition: &ProjectionPartition,
    after: Option<&ProjectionChangeCursor>,
    limit: usize,
) -> Result<ProjectionChangeRead, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    read_projection_changes_in_executor_after_state::<DB, _>(
        connection,
        topology,
        partition,
        after,
        limit,
        std::future::ready(()),
    )
    .await
}

/// Read one resumable projection-change page from a new database snapshot.
///
/// `after_state` is normally an immediately-ready future. Tests use it to
/// commit compaction after the partition watermark has been observed but
/// before retained rows are read, proving both statements remain one view.
#[allow(dead_code)]
pub(super) async fn read_projection_changes_in_snapshot<DB, AfterState>(
    pool: &Pool<DB>,
    topology: ProjectorTopologyId,
    partition: ProjectionPartition,
    after: Option<ProjectionChangeCursor>,
    limit: usize,
    after_state: AfterState,
) -> Result<ProjectionChangeRead, ProjectionProtocolError>
where
    DB: SqlxRepoBackend,
    AfterState: Future<Output = ()> + Send + 'static,
    DB::Arguments: IntoArguments<DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    with_projection_read_snapshot(pool, move |connection| {
        Box::pin(async move {
            read_projection_changes_in_executor_after_state::<DB, _>(
                connection,
                &topology,
                &partition,
                after.as_ref(),
                limit,
                after_state,
            )
            .await
        })
    })
    .await
}
