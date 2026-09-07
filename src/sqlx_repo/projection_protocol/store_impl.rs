use super::*;

impl<DB> ProjectionProtocolStore for SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> String: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> Vec<u8>: Type<DB> + sqlx::Decode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB> + Type<DB>,
    for<'q> &'q [u8]: Encode<'q, DB> + Type<DB>,
    for<'r> &'r str: sqlx::ColumnIndex<DB::Row>,
{
    #[cfg(feature = "graphql")]
    async fn projection_rebuild_records(
        &self,
        context: &crate::projection::rebuild::RebuildContext,
    ) -> Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError> {
        self.snapshot_rebuild_records(context).await
    }

    #[cfg(feature = "graphql")]
    async fn commit_projection_rebuild(
        &self,
        plan: crate::projection::rebuild::SnapshotProjectionRebuildPlan,
    ) -> Result<usize, ProjectionProtocolError> {
        self.apply_snapshot_rebuild(plan).await
    }

    fn register_projection_models<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        ownership: &'a [ProjectionModelOwnership],
    ) -> impl Future<Output = Result<(), ProjectionProtocolError>> + Send + 'a {
        async move {
            if ownership.is_empty() {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "projection startup registration requires at least one owned model".into(),
                ));
            }
            crate::projection_protocol::validate_ownership_batch(ownership)?;

            let mut tx = self.pool().begin().await.map_err(|error| {
                protocol_storage_error::<DB>("begin projection registration", error)
            })?;
            let ownership_tables = ownership
                .iter()
                .map(|declaration| declaration.table.clone())
                .collect::<BTreeSet<_>>();
            lock_projection_table_ownership_fences_in_tx(&mut tx, &ownership_tables).await?;
            let topology_hash = topology.digest();
            let topology_bytes = topology.canonical_bytes();
            for declaration in ownership {
                let mut global = QueryBuilder::<DB>::new(
                    "SELECT model_name FROM projection_causal_tables WHERE table_name = ",
                );
                global.push_bind(declaration.table.as_str());
                let existing = global
                    .build()
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|error| {
                        protocol_storage_error::<DB>(
                            "load global causal projection registration",
                            error,
                        )
                    })?;
                if let Some(row) = existing {
                    let model: String = row.try_get("model_name").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode global causal projection registration",
                            error,
                        )
                    })?;
                    if model != declaration.model {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "projection table `{}` is globally registered for model `{model}`",
                            declaration.table
                        )));
                    }
                } else {
                    let mut legacy_rows = QueryBuilder::<DB>::new("SELECT 1 FROM ");
                    legacy_rows.push(quote_identifier(&declaration.table));
                    legacy_rows.push(" LIMIT 1");
                    if legacy_rows
                        .build()
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|error| {
                            protocol_storage_error::<DB>(
                                "verify empty table before causal registration",
                                error,
                            )
                        })?
                        .is_some()
                    {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "cannot causally register table `{}` because it already contains unverified legacy rows",
                            declaration.table
                        )));
                    }
                    let mut insert = QueryBuilder::<DB>::new(
                        "INSERT INTO projection_causal_tables (table_name, model_name) VALUES (",
                    );
                    insert.push_bind(declaration.table.as_str());
                    insert.push(", ");
                    insert.push_bind(declaration.model.as_str());
                    insert.push(") ON CONFLICT (table_name) DO NOTHING");
                    insert.build().execute(&mut *tx).await.map_err(|error| {
                        protocol_storage_error::<DB>(
                            "insert global causal projection registration",
                            error,
                        )
                    })?;
                    let mut verify = QueryBuilder::<DB>::new(
                        "SELECT model_name FROM projection_causal_tables WHERE table_name = ",
                    );
                    verify.push_bind(declaration.table.as_str());
                    let row = verify
                        .build()
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|error| {
                            protocol_storage_error::<DB>(
                                "verify global causal projection registration",
                                error,
                            )
                        })?
                        .ok_or_else(|| {
                            corrupt_storage(
                                "global causal projection registration disappeared after insert",
                            )
                        })?;
                    let model: String = row.try_get("model_name").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode global causal projection registration",
                            error,
                        )
                    })?;
                    if model != declaration.model {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "projection table `{}` is globally registered for model `{model}`",
                            declaration.table
                        )));
                    }
                }

                let mut physical_owner = QueryBuilder::<DB>::new(
                    "SELECT topology_bytes, topology_hash, model_name \
                     FROM projection_registered_models WHERE table_name = ",
                );
                physical_owner.push_bind(declaration.table.as_str());
                if let Some(row) = physical_owner
                    .build()
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|error| {
                        protocol_storage_error::<DB>(
                            "load authoritative projection table owner",
                            error,
                        )
                    })?
                {
                    let owner_hash: Vec<u8> = row.try_get("topology_hash").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode authoritative projection table owner",
                            error,
                        )
                    })?;
                    let owner_model: String = row.try_get("model_name").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode authoritative projection table owner",
                            error,
                        )
                    })?;
                    if owner_hash != topology_hash || owner_model != declaration.model {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "projection table `{}` is authoritatively owned by another topology/model",
                            declaration.table
                        )));
                    }
                    let owner_bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode authoritative projection topology bytes",
                            error,
                        )
                    })?;
                    verify_bytes(
                        &owner_bytes,
                        &topology_bytes,
                        "authoritative projector topology",
                    )?;
                    continue;
                }

                let mut registered = QueryBuilder::<DB>::new(
                    "SELECT topology_bytes, table_name FROM projection_registered_models \
                     WHERE topology_hash = ",
                );
                registered.push_bind(topology_hash.as_slice());
                registered.push(" AND model_name = ");
                registered.push_bind(declaration.model.as_str());
                if let Some(row) =
                    registered
                        .build()
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|error| {
                            protocol_storage_error::<DB>(
                                "load topology projection registration",
                                error,
                            )
                        })?
                {
                    let bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode topology projection registration",
                            error,
                        )
                    })?;
                    verify_bytes(&bytes, &topology_bytes, "registered projector topology")?;
                    let table: String = row.try_get("table_name").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode topology projection registration",
                            error,
                        )
                    })?;
                    if table != declaration.table {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "projection model `{}` is already registered for table `{table}`",
                            declaration.model
                        )));
                    }
                    continue;
                }

                let mut table_registration = QueryBuilder::<DB>::new(
                    "SELECT topology_bytes, model_name FROM projection_registered_models \
                     WHERE topology_hash = ",
                );
                table_registration.push_bind(topology_hash.as_slice());
                table_registration.push(" AND table_name = ");
                table_registration.push_bind(declaration.table.as_str());
                if let Some(row) = table_registration
                    .build()
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|error| {
                        protocol_storage_error::<DB>("load topology table registration", error)
                    })?
                {
                    let bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
                        protocol_storage_error::<DB>("decode topology table registration", error)
                    })?;
                    verify_bytes(&bytes, &topology_bytes, "registered projector topology")?;
                    let model: String = row.try_get("model_name").map_err(|error| {
                        protocol_storage_error::<DB>("decode topology table registration", error)
                    })?;
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection table `{}` is already registered for model `{model}`",
                        declaration.table
                    )));
                }

                let mut insert = QueryBuilder::<DB>::new(
                    "INSERT INTO projection_registered_models \
                     (topology_bytes, topology_hash, model_name, table_name) VALUES (",
                );
                insert.push_bind(topology_bytes.as_slice());
                insert.push(", ");
                insert.push_bind(topology_hash.as_slice());
                insert.push(", ");
                insert.push_bind(declaration.model.as_str());
                insert.push(", ");
                insert.push_bind(declaration.table.as_str());
                insert.push(") ON CONFLICT DO NOTHING");
                let result = insert.build().execute(&mut *tx).await.map_err(|error| {
                    protocol_storage_error::<DB>("insert topology projection registration", error)
                })?;
                if DB::rows_affected(&result) != 1 {
                    let mut verify = QueryBuilder::<DB>::new(
                        "SELECT topology_bytes, table_name FROM projection_registered_models \
                         WHERE topology_hash = ",
                    );
                    verify.push_bind(topology_hash.as_slice());
                    verify.push(" AND model_name = ");
                    verify.push_bind(declaration.model.as_str());
                    let row = verify
                        .build()
                        .fetch_optional(&mut *tx)
                        .await
                        .map_err(|error| {
                            protocol_storage_error::<DB>(
                                "verify concurrent topology projection registration",
                                error,
                            )
                        })?
                        .ok_or_else(|| {
                            ProjectionProtocolError::InvalidBatch(format!(
                                "projection table `{}` was concurrently registered to another model",
                                declaration.table
                            ))
                        })?;
                    let bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode concurrent topology projection registration",
                            error,
                        )
                    })?;
                    verify_bytes(&bytes, &topology_bytes, "registered projector topology")?;
                    let table: String = row.try_get("table_name").map_err(|error| {
                        protocol_storage_error::<DB>(
                            "decode concurrent topology projection registration",
                            error,
                        )
                    })?;
                    if table != declaration.table {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "projection model `{}` was concurrently registered for table `{table}`",
                            declaration.model
                        )));
                    }
                }
            }
            tx.commit().await.map_err(|error| {
                protocol_storage_error::<DB>("commit projection registration", error)
            })
        }
    }

    fn commit_projection(
        &self,
        batch: ProjectionCommitBatch,
    ) -> impl Future<Output = Result<ProjectionCommitResult, ProjectionProtocolError>> + Send + '_
    {
        async move {
            batch.validate()?;
            let write_plan = TableWritePlan::new(
                batch
                    .mutations
                    .iter()
                    .map(|mutation| mutation.mutation.clone())
                    .collect(),
            );
            validate_sql_write_plan(&write_plan)?;

            let topology = batch.input.cursor.topology().clone();
            let partition = batch.input.cursor.projection_partition().clone();
            let mut tx =
                self.pool().begin().await.map_err(|error| {
                    protocol_storage_error::<DB>("begin projection commit", error)
                })?;
            verify_registered_topology_in_tx(&mut tx, &topology).await?;
            let mut state =
                lock_partition_in_tx(&mut tx, &topology, &partition, &batch.change_epoch).await?;
            validate_input_identity_in_tx(&mut tx, &batch.input).await?;
            ensure_active_input(&state, &batch.input)?;
            match classify_validated_input_in_tx(&mut tx, &batch.input, &state).await? {
                InputDisposition::Duplicate(checkpoint) => {
                    return Ok(ProjectionCommitResult::not_applied(
                        ProjectionCommitOutcome::Duplicate,
                        Some(checkpoint),
                    ));
                }
                InputDisposition::Stale(checkpoint) => {
                    return Ok(ProjectionCommitResult::not_applied(
                        ProjectionCommitOutcome::StaleInput,
                        Some(checkpoint),
                    ));
                }
                InputDisposition::New => {
                    ensure_pending_retry_input_in_tx(&mut tx, &state, &batch.input).await?;
                }
            }
            ensure_inbox_available_in_tx(&mut tx, &batch.input).await?;
            ensure_partition_ownership_in_tx(&mut tx, &topology, &partition, &batch.ownership)
                .await?;

            for mutation in &batch.mutations {
                if mutation.mutation.table_name()
                    != batch
                        .ownership
                        .iter()
                        .find(|owned| owned.model == mutation.scope.model())
                        .map(|owned| owned.table.as_str())
                        .unwrap_or_default()
                    || table_model_name(&mutation.mutation) != mutation.scope.model()
                {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection mutation for model `{}` does not target its registered table",
                        mutation.scope.model()
                    )));
                }
            }

            let mut records = Vec::with_capacity(batch.mutations.len());
            let mut records_by_scope = HashMap::with_capacity(batch.mutations.len());
            let mut changes =
                Vec::with_capacity(batch.mutations.len() + batch.observations.len().max(1));
            for mutation in &batch.mutations {
                let current = record_in_tx(&mut tx, &mutation.scope, &state.change_epoch).await?;
                let physical_exists =
                    physical_row_exists_in_tx(&mut tx, &mutation.mutation).await?;
                match current.as_ref().map(|record| &record.metadata) {
                    None if physical_exists => {
                        return Err(ProjectionProtocolError::RecordAlreadyExists {
                            model: mutation.scope.model().to_string(),
                        });
                    }
                    Some(metadata) if metadata.tombstone && physical_exists => {
                        return Err(ProjectionProtocolError::RecordAlreadyExists {
                            model: mutation.scope.model().to_string(),
                        });
                    }
                    Some(metadata) if !metadata.tombstone && !physical_exists => {
                        return Err(ProjectionProtocolError::RecordMissing {
                            model: mutation.scope.model().to_string(),
                        });
                    }
                    _ => {}
                }
                let (revision, tombstone) = next_record(
                    &mutation.scope,
                    &mutation.expectation,
                    mutation.kind,
                    current.as_ref(),
                    mutation.source_snapshot.is_some(),
                )?;
                crate::projection_protocol::validate_snapshot_write(
                    current.as_ref().map(|record| &record.metadata),
                    mutation.source_snapshot.as_ref(),
                )?;
                let change = allocate_change(
                    &mut state,
                    &topology,
                    &partition,
                    change_kind_for_mutation(mutation.kind),
                    batch.input.causation_id.clone(),
                    None,
                    Some(mutation.scope.clone()),
                    Some(revision.clone()),
                    None,
                )?;
                let metadata = ProjectionRecordMetadata {
                    revision,
                    tombstone,
                    change: change.cursor.clone(),
                    source_snapshot: mutation.source_snapshot.clone(),
                };
                records_by_scope.insert(mutation.scope.clone(), metadata.clone());
                records.push(metadata);
                changes.push(change);
            }

            let mut observations = Vec::with_capacity(batch.observations.len());
            for request in &batch.observations {
                let (scope, revision, staged_change) = match &request.target {
                    ProjectionObservationTarget::StagedRecord(scope) => {
                        let metadata = records_by_scope.get(scope).ok_or_else(|| {
                            ProjectionProtocolError::InvalidBatch(format!(
                                "projection observation references unstaged model `{}`",
                                scope.model()
                            ))
                        })?;
                        (
                            scope.clone(),
                            Some(metadata.revision.clone()),
                            Some(metadata.change.clone()),
                        )
                    }
                    ProjectionObservationTarget::ExistingRecord(expected) => {
                        let metadata =
                            if let Some(metadata) = records_by_scope.get(expected.scope()) {
                                metadata.clone()
                            } else {
                                record_in_tx(&mut tx, expected.scope(), &state.change_epoch)
                                    .await?
                                    .map(|record| record.metadata)
                                    .ok_or_else(|| ProjectionProtocolError::RecordMissing {
                                        model: expected.scope().model().to_string(),
                                    })?
                            };
                        if metadata.revision != *expected {
                            return Err(ProjectionProtocolError::RecordRevisionConflict {
                                model: expected.scope().model().to_string(),
                                expected_incarnation: expected.incarnation(),
                                expected_revision: expected.revision(),
                                actual_incarnation: metadata.revision.incarnation(),
                                actual_revision: metadata.revision.revision(),
                            });
                        }
                        if metadata.tombstone && metadata.source_snapshot.is_none() {
                            return Err(ProjectionProtocolError::RecordTombstoned {
                                model: expected.scope().model().to_string(),
                            });
                        }
                        (expected.scope().clone(), Some(expected.clone()), None)
                    }
                    ProjectionObservationTarget::Dependency(scope) => (scope.clone(), None, None),
                };
                if observation_in_tx(
                    &mut tx,
                    &batch.input.causation_id,
                    &scope,
                    request.kind,
                    &state.change_epoch,
                )
                .await?
                .is_some()
                {
                    // Causation observations are immutable earliest evidence.
                    // A later input carrying the same exact observation key
                    // neither rewrites its revision nor emits a duplicate
                    // change.
                    continue;
                }

                let change_cursor = if let Some(change) = staged_change {
                    change
                } else {
                    let change = allocate_change(
                        &mut state,
                        &topology,
                        &partition,
                        ProjectionChangeKind::Observation,
                        batch.input.causation_id.clone(),
                        Some(request.kind),
                        Some(scope.clone()),
                        revision.clone(),
                        None,
                    )?;
                    let cursor = change.cursor.clone();
                    changes.push(change);
                    cursor
                };
                observations.push(ProjectionObservation {
                    causation_id: batch.input.causation_id.clone(),
                    kind: request.kind,
                    revision,
                    scope,
                    change: change_cursor,
                });
            }

            if changes.is_empty() {
                changes.push(allocate_change(
                    &mut state,
                    &topology,
                    &partition,
                    ProjectionChangeKind::Checkpoint,
                    batch.input.causation_id.clone(),
                    None,
                    None,
                    None,
                    None,
                )?);
            }
            let final_change = changes
                .last()
                .expect("a successful projection commit always allocates a change")
                .cursor
                .clone();
            let checkpoint = ProjectionCheckpoint::new(
                batch.input.cursor.clone(),
                final_change.clone(),
                batch.input.gap_free,
            )?;

            apply_read_model_write_plan_in_tx(&mut tx, write_plan).await?;
            for change in &changes {
                insert_change_in_tx(&mut tx, change).await?;
            }
            for metadata in &records {
                upsert_record_in_tx(&mut tx, metadata).await?;
            }
            for observation in &observations {
                insert_observation_in_tx(&mut tx, observation).await?;
            }
            store_input_cursor_in_tx(&mut tx, &batch.input, &final_change).await?;
            insert_input_identity_in_tx(&mut tx, &batch.input).await?;
            insert_input_receipt_in_tx(&mut tx, &batch.input, "applied", None, &final_change)
                .await?;
            insert_inbox_in_tx(&mut tx, &batch.input).await?;
            update_partition_head_in_tx(
                &mut tx,
                &topology,
                &partition,
                state.change_head,
                state.pending_retry_failure_id.as_deref(),
            )
            .await?;
            retain_projection_change_suffix_in_tx(
                &mut tx,
                &topology,
                &partition,
                &state,
                self.projection_change_retention(),
            )
            .await?;

            let mut changed_tables = batch
                .mutations
                .iter()
                .map(|mutation| mutation.mutation.table_name().to_string())
                .collect::<BTreeSet<_>>();
            changed_tables.insert(PROJECTION_CHANGE_NOTIFY_TABLE.to_string());
            if self.projection_notify_enabled() {
                DB::push_change_notify(&mut *tx, &changed_tables).await?;
            }
            tx.commit().await.map_err(|error| {
                protocol_storage_error::<DB>("commit projection transaction", error)
            })?;
            self.publish_read_model_change(crate::ReadModelChange {
                tables: changed_tables,
            });
            Ok(ProjectionCommitResult {
                outcome: ProjectionCommitOutcome::Applied,
                checkpoint: Some(checkpoint),
                records,
                changes,
            })
        }
    }

    fn record_projection_failure(
        &self,
        batch: ProjectionFailureBatch,
    ) -> impl Future<Output = Result<ProjectionFailure, ProjectionProtocolError>> + Send + '_ {
        async move {
            batch.validate()?;
            let topology = batch.input.cursor.topology().clone();
            let partition = batch.input.cursor.projection_partition().clone();
            let mut tx =
                self.pool().begin().await.map_err(|error| {
                    protocol_storage_error::<DB>("begin projection failure", error)
                })?;
            verify_registered_topology_in_tx(&mut tx, &topology).await?;
            let mut state =
                lock_partition_in_tx(&mut tx, &topology, &partition, &batch.change_epoch).await?;
            validate_input_identity_in_tx(&mut tx, &batch.input).await?;
            if state.active_generation != batch.input.generation {
                return Err(ProjectionProtocolError::GenerationFenced {
                    expected: state.active_generation.get(),
                    actual: batch.input.generation.get(),
                });
            }
            if let Some(stopped_failure_id) = &state.stopped_failure_id {
                if stopped_failure_id == &batch.failure_id {
                    let existing = failure_in_tx(
                        &mut tx,
                        &topology,
                        &partition,
                        stopped_failure_id,
                        &state.change_epoch,
                    )
                    .await?
                    .ok_or_else(|| {
                        corrupt_storage(format!(
                            "stopped projection failure `{stopped_failure_id}` is missing"
                        ))
                    })?;
                    if failure_matches_batch(&existing, &batch) {
                        return Ok(existing.failure);
                    }
                    if existing.failure.input == batch.input.cursor {
                        if existing.failure.input_fingerprint != batch.input.fingerprint
                            || existing.failure.message_id != batch.input.message_id
                            || existing.failure.causation_id != batch.input.causation_id
                            || existing.failure.gap_free != batch.input.gap_free
                        {
                            return Err(ProjectionProtocolError::InputCorruption);
                        }
                    } else if existing.failure.message_id == batch.input.message_id {
                        return Err(ProjectionProtocolError::MessageIdReuse {
                            message_id: batch.input.message_id.clone(),
                        });
                    }
                }
                return Err(ProjectionProtocolError::PartitionStopped {
                    failure_id: stopped_failure_id.clone(),
                });
            }
            let mut failure_id_query =
                QueryBuilder::<DB>::new("SELECT 1 FROM projection_failures WHERE failure_id = ");
            failure_id_query.push_bind(batch.failure_id.as_str());
            failure_id_query.push(" LIMIT 1");
            if failure_id_query
                .build()
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| {
                    protocol_storage_error::<DB>("check projection failure ID", error)
                })?
                .is_some()
            {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection failure ID `{}` is already bound to another failure",
                    batch.failure_id
                )));
            }

            match classify_validated_input_in_tx(&mut tx, &batch.input, &state).await? {
                InputDisposition::New => {
                    ensure_pending_retry_input_in_tx(&mut tx, &state, &batch.input).await?;
                }
                InputDisposition::Duplicate(_) | InputDisposition::Stale(_) => {
                    return Err(ProjectionProtocolError::InvalidBatch(
                        "cannot record terminal failure for an already processed input".into(),
                    ));
                }
            }
            ensure_inbox_available_in_tx(&mut tx, &batch.input).await?;
            let change = allocate_change(
                &mut state,
                &topology,
                &partition,
                ProjectionChangeKind::Failure,
                batch.input.causation_id.clone(),
                None,
                None,
                None,
                Some(batch.failure_id.clone()),
            )?;
            insert_change_in_tx(&mut tx, &change).await?;
            insert_failure_in_tx(&mut tx, &batch, &change.cursor).await?;
            insert_input_identity_in_tx(&mut tx, &batch.input).await?;
            insert_input_receipt_in_tx(
                &mut tx,
                &batch.input,
                "failed",
                Some(&batch.failure_id),
                &change.cursor,
            )
            .await?;
            insert_inbox_in_tx(&mut tx, &batch.input).await?;
            stop_partition_in_tx(&mut tx, &batch, state.change_head).await?;
            retain_projection_change_suffix_in_tx(
                &mut tx,
                &topology,
                &partition,
                &state,
                self.projection_change_retention(),
            )
            .await?;

            let changed_tables = BTreeSet::from([PROJECTION_CHANGE_NOTIFY_TABLE.to_string()]);
            if self.projection_notify_enabled() {
                DB::push_change_notify(&mut *tx, &changed_tables).await?;
            }
            let failure = ProjectionFailure {
                failure_id: batch.failure_id,
                input: batch.input.cursor,
                input_fingerprint: batch.input.fingerprint,
                message_id: batch.input.message_id,
                causation_id: batch.input.causation_id,
                generation: batch.input.generation,
                gap_free: batch.input.gap_free,
                failure_code: batch.failure_code,
                failure_bytes: batch.failure_bytes,
                failure_digest: batch.failure_digest,
                change: change.cursor,
            };
            tx.commit().await.map_err(|error| {
                protocol_storage_error::<DB>("commit projection failure", error)
            })?;
            self.publish_read_model_change(crate::ReadModelChange {
                tables: changed_tables,
            });
            Ok(failure)
        }
    }

    fn projection_checkpoint<'a>(
        &'a self,
        cursor_scope: &'a ProjectionInputCursor,
        generation: ProjectionGeneration,
    ) -> impl Future<Output = Result<Option<ProjectionCheckpoint>, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let Some(state) = load_partition(
                self.pool(),
                cursor_scope.topology(),
                cursor_scope.projection_partition(),
            )
            .await?
            else {
                return Ok(None);
            };
            let probe = TrustedProjectionInput {
                cursor: cursor_scope.clone(),
                fingerprint: ProjectionInputFingerprint::from_digest([0; 32]),
                message_id: String::new(),
                causation_id: String::new(),
                generation,
                gap_free: false,
            };
            let mut tx = self.pool().begin().await.map_err(|error| {
                protocol_storage_error::<DB>("begin projection checkpoint read", error)
            })?;
            let Some(stored) = current_input_cursor_in_tx(&mut tx, &probe).await? else {
                return Ok(None);
            };
            verify_stored_change(&state, &stored.change)?;
            if stored.source_epoch != *cursor_scope.epoch() {
                return Err(ProjectionProtocolError::IncomparableInput);
            }
            Ok(Some(checkpoint_from_stored(
                cursor_scope,
                stored.source_epoch,
                stored.source_position,
                stored.change,
                stored.gap_free,
            )?))
        }
    }

    fn projection_record<'a>(
        &'a self,
        scope: &'a ProjectionRecordScope,
    ) -> impl Future<Output = Result<Option<ProjectionRecordMetadata>, ProjectionProtocolError>>
           + Send
           + 'a {
        async move {
            let Some(state) =
                load_partition(self.pool(), scope.topology(), scope.projection_partition()).await?
            else {
                return Ok(None);
            };
            let mut tx = self.pool().begin().await.map_err(|error| {
                protocol_storage_error::<DB>("begin projection record read", error)
            })?;
            Ok(record_in_tx(&mut tx, scope, &state.change_epoch)
                .await?
                .map(|record| record.metadata))
        }
    }

    fn projection_input_disposition<'a>(
        &'a self,
        input: &'a TrustedProjectionInput,
    ) -> impl Future<Output = Result<ProjectionInputDisposition, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let mut tx = self.pool().begin().await.map_err(|error| {
                protocol_storage_error::<DB>("begin projection input disposition read", error)
            })?;
            if DB::BACKEND == "postgres" {
                sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| {
                        protocol_storage_error::<DB>(
                            "configure projection input disposition read",
                            error,
                        )
                    })?;
            }

            let result = async {
                verify_registered_topology_in_tx(&mut tx, input.cursor.topology()).await?;
                // Durable cursor/message/capability corruption wins over a
                // generation fence, matching commit/failure validation.
                validate_input_identity_read_only_in_tx(&mut tx, input).await?;
                let Some(state) = load_partition_in_tx(
                    &mut tx,
                    input.cursor.topology(),
                    input.cursor.projection_partition(),
                )
                .await?
                else {
                    if input.generation != ProjectionGeneration::initial() {
                        return Err(ProjectionProtocolError::GenerationFenced {
                            expected: ProjectionGeneration::initial().get(),
                            actual: input.generation.get(),
                        });
                    }
                    return Ok(ProjectionInputDisposition::Pending);
                };
                if state.active_generation != input.generation {
                    return Err(ProjectionProtocolError::GenerationFenced {
                        expected: state.active_generation.get(),
                        actual: input.generation.get(),
                    });
                }
                verify_generation_exists_in_tx(
                    &mut tx,
                    input.cursor.topology(),
                    input.cursor.projection_partition(),
                    input.generation,
                )
                .await?;
                if let Some(failure_id) = &state.stopped_failure_id {
                    return Err(ProjectionProtocolError::PartitionStopped {
                        failure_id: failure_id.clone(),
                    });
                }
                match classify_validated_input_in_tx(&mut tx, input, &state).await? {
                    InputDisposition::New => {
                        ensure_pending_retry_input_in_tx(&mut tx, &state, input).await?;
                        Ok(ProjectionInputDisposition::Pending)
                    }
                    InputDisposition::Duplicate(checkpoint) => {
                        Ok(ProjectionInputDisposition::Duplicate(checkpoint))
                    }
                    InputDisposition::Stale(checkpoint) => {
                        Ok(ProjectionInputDisposition::Stale(checkpoint))
                    }
                }
            }
            .await;

            match result {
                Ok(disposition) => {
                    tx.commit().await.map_err(|error| {
                        protocol_storage_error::<DB>(
                            "commit projection input disposition read",
                            error,
                        )
                    })?;
                    Ok(disposition)
                }
                Err(error) => {
                    tx.rollback().await.map_err(|rollback_error| {
                        protocol_storage_error::<DB>(
                            "roll back failed projection input disposition read",
                            rollback_error,
                        )
                    })?;
                    Err(error)
                }
            }
        }
    }

    fn projection_query_snapshot<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshot, ProjectionProtocolError>> + Send + 'a
    {
        read_projection_query_snapshot_in_executor(self.pool(), request)
    }

    fn projection_query_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshotBatch, ProjectionProtocolError>> + Send + 'a
    {
        let request = request.clone();
        async move {
            request.validate()?;
            with_projection_read_snapshot(self.pool(), move |connection| {
                Box::pin(async move {
                    let mut snapshots = Vec::with_capacity(request.requests.len());
                    for row_request in &request.requests {
                        snapshots.push(
                            read_projection_query_snapshot_in_executor::<DB, _>(
                                &mut *connection,
                                row_request,
                            )
                            .await?,
                        );
                    }
                    Ok(ProjectionQuerySnapshotBatch { snapshots })
                })
            })
            .await
        }
    }

    fn projection_execution_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionExecutionSnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionExecutionSnapshotBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        let request = request.clone();
        async move {
            request.validate()?;
            with_projection_read_snapshot(self.pool(), move |connection| {
                Box::pin(async move {
                    read_projection_execution_snapshot_batch_in_executor::<DB>(connection, &request)
                        .await
                })
            })
            .await
        }
    }

    fn projection_graph_snapshot<'a>(
        &'a self,
        request: &'a ProjectionGraphSnapshotRequest,
    ) -> impl Future<Output = Result<ProjectionGraphSnapshot, ProjectionProtocolError>> + Send + 'a
    {
        let request = request.clone();
        async move {
            validate_projection_graph_snapshot_request(&request)?;
            with_projection_read_snapshot(self.pool(), move |connection| {
                Box::pin(async move {
                    read_projection_graph_snapshot_in_executor::<DB>(connection, &request).await
                })
            })
            .await
        }
    }

    fn projection_obligation_evidence_batch<'a>(
        &'a self,
        request: &'a ProjectionObligationEvidenceBatchRequest,
    ) -> impl Future<Output = Result<ProjectionObligationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        let request = request.clone();
        async move {
            request.validate()?;
            with_projection_read_snapshot(self.pool(), move |connection| {
                Box::pin(async move {
                    read_projection_obligation_evidence_batch_in_executor::<DB>(
                        connection, &request,
                    )
                    .await
                })
            })
            .await
        }
    }

    fn projection_causation_evidence<'a>(
        &'a self,
        request: &'a ProjectionCausationEvidenceRequest,
    ) -> impl Future<Output = Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        let request = request.clone();
        async move {
            request.validate()?;
            with_projection_read_snapshot(self.pool(), move |connection| {
                Box::pin(async move {
                    read_projection_causation_evidence_in_executor::<DB>(connection, &request).await
                })
            })
            .await
        }
    }

    fn projection_live_record_batch<'a>(
        &'a self,
        request: &'a ProjectionLiveRecordBatchRequest,
    ) -> impl Future<Output = Result<ProjectionLiveRecordBatch, ProjectionProtocolError>> + Send + 'a
    {
        let request = request.clone();
        async move {
            request.validate()?;
            with_projection_read_snapshot(self.pool(), move |connection| {
                Box::pin(async move {
                    read_projection_live_record_batch_in_executor::<DB>(connection, &request).await
                })
            })
            .await
        }
    }

    fn projection_partition_runtime_state<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
    ) -> impl Future<Output = Result<Option<ProjectionPartitionRuntimeState>, ProjectionProtocolError>>
           + Send
           + 'a {
        load_partition_runtime_state(self.pool(), topology, partition)
    }

    fn projection_observation<'a>(
        &'a self,
        causation_id: &'a str,
        scope: &'a ProjectionRecordScope,
        kind: ProjectionObservationKind,
    ) -> impl Future<Output = Result<Option<ProjectionObservation>, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let Some(state) =
                load_partition(self.pool(), scope.topology(), scope.projection_partition()).await?
            else {
                return Ok(None);
            };
            let mut tx = self.pool().begin().await.map_err(|error| {
                protocol_storage_error::<DB>("begin projection observation read", error)
            })?;
            observation_in_tx(&mut tx, causation_id, scope, kind, &state.change_epoch).await
        }
    }

    fn projection_changes<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        after: Option<&'a ProjectionChangeCursor>,
        limit: usize,
    ) -> impl Future<Output = Result<ProjectionChangeRead, ProjectionProtocolError>> + Send + 'a
    {
        let topology = topology.clone();
        let partition = partition.clone();
        let after = after.cloned();
        async move {
            read_projection_changes_in_snapshot(
                self.pool(),
                topology,
                partition,
                after,
                limit,
                std::future::ready(()),
            )
            .await
        }
    }

    fn repair_projection<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<ProjectionGeneration, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let mut tx =
                self.pool().begin().await.map_err(|error| {
                    protocol_storage_error::<DB>("begin projection repair", error)
                })?;
            let Some(state) = lock_existing_partition_in_tx(&mut tx, topology, partition).await?
            else {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "cannot repair an unknown projection partition".into(),
                ));
            };
            match &state.stopped_failure_id {
                Some(stopped) if stopped == failure_id => {}
                Some(stopped) => {
                    return Err(ProjectionProtocolError::PartitionStopped {
                        failure_id: stopped.clone(),
                    });
                }
                None => {
                    return Err(ProjectionProtocolError::InvalidBatch(
                        "projection partition is not stopped".into(),
                    ));
                }
            }
            verify_generation_exists_in_tx(&mut tx, topology, partition, state.active_generation)
                .await?;
            let failure = failure_in_tx(
                &mut tx,
                topology,
                partition,
                failure_id,
                &state.change_epoch,
            )
            .await?
            .ok_or_else(|| {
                corrupt_storage(format!(
                    "stopped projection failure `{failure_id}` is missing"
                ))
            })?;
            if failure.failure.generation != state.active_generation {
                return Err(corrupt_storage(format!(
                    "stopped failure generation {} differs from active generation {}",
                    failure.failure.generation.get(),
                    state.active_generation.get()
                )));
            }
            let next_generation = state.active_generation.checked_next()?;
            let topology_hash = topology.digest();
            let partition_hash = partition.digest();
            let old_generation =
                to_i64::<DB>(state.active_generation.get(), "projection generation")?;
            let next_generation_value =
                to_i64::<DB>(next_generation.get(), "projection generation")?;

            let mut existing = QueryBuilder::<DB>::new(
                "SELECT 1 FROM projection_generations WHERE topology_hash = ",
            );
            existing.push_bind(topology_hash.as_slice());
            existing.push(" AND partition_hash = ");
            existing.push_bind(partition_hash.as_slice());
            existing.push(" AND generation = ");
            existing.push_bind(next_generation_value);
            existing.push(" LIMIT 1");
            if existing
                .build()
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| {
                    protocol_storage_error::<DB>("check projection repair generation", error)
                })?
                .is_some()
            {
                return Err(corrupt_storage(format!(
                    "projection repair generation {} already exists",
                    next_generation.get()
                )));
            }
            let mut retry_link = QueryBuilder::<DB>::new(
                "SELECT generation FROM projection_generations WHERE topology_hash = ",
            );
            retry_link.push_bind(topology_hash.as_slice());
            retry_link.push(" AND partition_hash = ");
            retry_link.push_bind(partition_hash.as_slice());
            retry_link.push(" AND retry_of_failure_id = ");
            retry_link.push_bind(failure_id);
            retry_link.push(" LIMIT 1");
            if retry_link
                .build()
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| {
                    protocol_storage_error::<DB>("check projection repair failure link", error)
                })?
                .is_some()
            {
                return Err(corrupt_storage(format!(
                    "stopped failure `{failure_id}` already has a repair generation"
                )));
            }

            let mut insert_generation = QueryBuilder::<DB>::new(
                "INSERT INTO projection_generations \
                 (topology_hash, partition_hash, generation, retry_of_generation, \
                 retry_of_failure_id) VALUES (",
            );
            insert_generation.push_bind(topology_hash.as_slice());
            insert_generation.push(", ");
            insert_generation.push_bind(partition_hash.as_slice());
            insert_generation.push(", ");
            insert_generation.push_bind(next_generation_value);
            insert_generation.push(", ");
            insert_generation.push_bind(old_generation);
            insert_generation.push(", ");
            insert_generation.push_bind(failure_id);
            insert_generation.push(")");
            insert_generation
                .build()
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    protocol_storage_error::<DB>("insert projection repair generation", error)
                })?;

            let mut copy = QueryBuilder::<DB>::new(
                "INSERT INTO projection_input_cursors \
                 (topology_hash, partition_hash, source_bytes, source_hash, source_partition_bytes, \
                 source_partition_hash, source_epoch, source_position, input_hash, message_id, \
                 causation_id, gap_free, generation, change_epoch, change_position) \
                 SELECT topology_hash, partition_hash, source_bytes, source_hash, \
                 source_partition_bytes, source_partition_hash, source_epoch, source_position, \
                 input_hash, message_id, causation_id, gap_free, ",
            );
            copy.push_bind(next_generation_value);
            copy.push(
                ", change_epoch, change_position FROM projection_input_cursors \
                 WHERE topology_hash = ",
            );
            copy.push_bind(topology_hash.as_slice());
            copy.push(" AND partition_hash = ");
            copy.push_bind(partition_hash.as_slice());
            copy.push(" AND generation = ");
            copy.push_bind(old_generation);
            copy.build().execute(&mut *tx).await.map_err(|error| {
                protocol_storage_error::<DB>("copy projection repair checkpoints", error)
            })?;

            let mut activate =
                QueryBuilder::<DB>::new("UPDATE projection_partitions SET active_generation = ");
            activate.push_bind(next_generation_value);
            activate.push(", pending_retry_failure_id = ");
            activate.push_bind(failure_id);
            activate.push(
                ", stopped_failure_id = NULL, stopped_source_bytes = NULL, \
                 stopped_source_hash = NULL, stopped_source_partition_bytes = NULL, \
                 stopped_source_partition_hash = NULL, stopped_source_epoch = NULL, \
                 stopped_source_position = NULL, stopped_generation = NULL, \
                 stopped_input_hash = NULL, stopped_message_id = NULL, \
                 stopped_causation_id = NULL, stopped_gap_free = NULL WHERE topology_hash = ",
            );
            activate.push_bind(topology_hash.as_slice());
            activate.push(" AND partition_hash = ");
            activate.push_bind(partition_hash.as_slice());
            activate.push(" AND active_generation = ");
            activate.push_bind(old_generation);
            activate.push(" AND stopped_failure_id = ");
            activate.push_bind(failure_id);
            let result = activate.build().execute(&mut *tx).await.map_err(|error| {
                protocol_storage_error::<DB>("activate projection repair generation", error)
            })?;
            if DB::rows_affected(&result) != 1 {
                return Err(corrupt_storage(
                    "projection stop fence changed while its partition lock was held",
                ));
            }
            tx.commit()
                .await
                .map_err(|error| protocol_storage_error::<DB>("commit projection repair", error))?;
            Ok(next_generation)
        }
    }

    fn compact_projection_changes<'a>(
        &'a self,
        through: &'a ProjectionChangeCursor,
    ) -> impl Future<Output = Result<u64, ProjectionProtocolError>> + Send + 'a {
        async move {
            let topology = through.topology();
            let partition = through.projection_partition();
            let mut tx = self.pool().begin().await.map_err(|error| {
                protocol_storage_error::<DB>("begin projection change compaction", error)
            })?;
            let Some(state) = lock_existing_partition_in_tx(&mut tx, topology, partition).await?
            else {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "cannot compact an unknown projection partition".into(),
                ));
            };
            if through.epoch() != &state.change_epoch {
                return Err(ProjectionProtocolError::ScopeMismatch {
                    field: "projection compaction epoch",
                });
            }
            if through.position() > state.change_head {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "cannot compact beyond the projection change head".into(),
                ));
            }
            if through.position() <= state.compacted_through {
                return Ok(state.compacted_through);
            }
            let topology_hash = topology.digest();
            let partition_hash = partition.digest();
            let through_position =
                to_i64::<DB>(through.position(), "projection compaction position")?;
            let mut exact =
                QueryBuilder::<DB>::new("SELECT 1 FROM projection_changes WHERE topology_hash = ");
            exact.push_bind(topology_hash.as_slice());
            exact.push(" AND partition_hash = ");
            exact.push_bind(partition_hash.as_slice());
            exact.push(" AND change_epoch = ");
            exact.push_bind(state.change_epoch.as_str());
            exact.push(" AND change_position = ");
            exact.push_bind(through_position);
            exact.push(" LIMIT 1");
            if exact
                .build()
                .fetch_optional(&mut *tx)
                .await
                .map_err(|error| {
                    protocol_storage_error::<DB>("verify projection compaction cursor", error)
                })?
                .is_none()
            {
                return Err(corrupt_storage(format!(
                    "projection compaction cursor {} is missing",
                    through.position()
                )));
            }

            let mut delete =
                QueryBuilder::<DB>::new("DELETE FROM projection_changes WHERE topology_hash = ");
            delete.push_bind(topology_hash.as_slice());
            delete.push(" AND partition_hash = ");
            delete.push_bind(partition_hash.as_slice());
            delete.push(" AND change_epoch = ");
            delete.push_bind(state.change_epoch.as_str());
            delete.push(" AND change_position <= ");
            delete.push_bind(through_position);
            let result = delete.build().execute(&mut *tx).await.map_err(|error| {
                protocol_storage_error::<DB>("compact projection changes", error)
            })?;
            let expected_removed = through.position() - state.compacted_through;
            if DB::rows_affected(&result) != expected_removed {
                return Err(corrupt_storage(format!(
                    "projection compaction expected to remove {expected_removed} changes but removed {}",
                    DB::rows_affected(&result)
                )));
            }

            let mut watermark =
                QueryBuilder::<DB>::new("UPDATE projection_partitions SET compacted_through = ");
            watermark.push_bind(through_position);
            watermark.push(" WHERE topology_hash = ");
            watermark.push_bind(topology_hash.as_slice());
            watermark.push(" AND partition_hash = ");
            watermark.push_bind(partition_hash.as_slice());
            let result = watermark.build().execute(&mut *tx).await.map_err(|error| {
                protocol_storage_error::<DB>("advance projection compaction watermark", error)
            })?;
            if DB::rows_affected(&result) != 1 {
                return Err(corrupt_storage(
                    "projection partition disappeared during compaction",
                ));
            }
            tx.commit().await.map_err(|error| {
                protocol_storage_error::<DB>("commit projection change compaction", error)
            })?;
            Ok(through.position())
        }
    }

    fn projection_failure<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailure>, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let Some(state) = load_partition(self.pool(), topology, partition).await? else {
                return Ok(None);
            };
            let mut tx = self.pool().begin().await.map_err(|error| {
                protocol_storage_error::<DB>("begin projection failure read", error)
            })?;
            Ok(failure_in_tx(
                &mut tx,
                topology,
                partition,
                failure_id,
                &state.change_epoch,
            )
            .await?
            .map(|stored| stored.failure))
        }
    }

    fn projection_failure_location<'a>(
        &'a self,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailureLocation>, ProjectionProtocolError>>
           + Send
           + 'a {
        async move {
            let mut builder = QueryBuilder::<DB>::new(
                "SELECT partition.topology_bytes, partition.topology_hash, \
                 partition.partition_bytes, partition.partition_hash \
                 FROM projection_failures failure \
                 INNER JOIN projection_partitions partition \
                 ON partition.topology_hash = failure.topology_hash \
                 AND partition.partition_hash = failure.partition_hash \
                 WHERE failure.failure_id = ",
            );
            builder.push_bind(failure_id);
            let Some(row) = builder
                .build()
                .fetch_optional(self.pool())
                .await
                .map_err(|error| {
                    protocol_storage_error::<DB>("resolve projection failure location", error)
                })?
            else {
                return Ok(None);
            };

            let topology_bytes: Vec<u8> = row.try_get("topology_bytes").map_err(|error| {
                protocol_storage_error::<DB>("decode repair topology bytes", error)
            })?;
            let topology = ProjectorTopologyId::from_canonical_bytes(&topology_bytes)?;
            let topology_hash: Vec<u8> = row.try_get("topology_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode repair topology hash", error)
            })?;
            verify_digest(
                &topology_hash,
                topology.digest(),
                "projection repair topology",
            )?;

            let partition_bytes: Vec<u8> = row.try_get("partition_bytes").map_err(|error| {
                protocol_storage_error::<DB>("decode repair partition bytes", error)
            })?;
            let partition = ProjectionPartition::new(partition_bytes)?;
            let partition_hash: Vec<u8> = row.try_get("partition_hash").map_err(|error| {
                protocol_storage_error::<DB>("decode repair partition hash", error)
            })?;
            verify_digest(
                &partition_hash,
                partition.digest(),
                "projection repair partition",
            )?;

            Ok(Some(ProjectionFailureLocation {
                topology,
                partition,
            }))
        }
    }
}
