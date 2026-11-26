use super::*;

impl ProjectionProtocolStore for InMemoryRepository {
    async fn projection_rebuild_records(
        &self,
        context: &crate::projection::rebuild::RebuildContext,
    ) -> Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError> {
        self.snapshot_rebuild_records(context).await
    }

    async fn commit_projection_rebuild(
        &self,
        plan: crate::projection::rebuild::SnapshotProjectionRebuildPlan,
    ) -> Result<usize, ProjectionProtocolError> {
        self.apply_snapshot_rebuild(plan).await
    }

    fn register_projection_models<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        ownership: &'a [crate::projection_protocol::ProjectionModelOwnership],
    ) -> impl Future<Output = Result<(), ProjectionProtocolError>> + Send + 'a {
        async move {
            if ownership.is_empty() {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "projection topology bootstrap requires at least one model/table owner".into(),
                ));
            }
            crate::projection_protocol::validate_ownership_batch(ownership)?;
            // Serialize bootstrap against raw row writers. Once this returns,
            // every repository-level legacy path observes the causal marker
            // before it can acquire the row map for mutation.
            let rows = self
                .model_store
                .relational_rows
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection ownership row fence"))?;
            let mut protocol = self
                .projection_protocol
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection ownership write"))?;
            // `validate_ownership_batch` rejected declarations that disagree
            // with each other, so each one is checked against stored state.
            for declaration in ownership {
                let key = RegisteredModelKey {
                    topology: topology.clone(),
                    model: declaration.model.clone(),
                };
                if let Some(previous) = protocol.authoritative_table_owners.get(&declaration.table)
                {
                    if previous != &key {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "projection table `{}` already has authoritative owner `{}` in another topology",
                            declaration.table, previous.model
                        )));
                    }
                } else if rows.keys().any(|storage_key| {
                    storage_key_belongs_to_table(storage_key, &declaration.table)
                }) {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection table `{}` contains rows without causal metadata; rebuild or verified import is required before registration",
                        declaration.table
                    )));
                }
                if let Some(previous) = protocol.registered_models.get(&key) {
                    if previous != &declaration.table {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "projection model `{}` is already registered for table `{previous}`",
                            declaration.model
                        )));
                    }
                }
                if let Some((other, _)) =
                    protocol.registered_models.iter().find(|(other, table)| {
                        other.topology == *topology
                            && *table == &declaration.table
                            && other.model != declaration.model
                    })
                {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection table `{}` is already registered for model `{}`",
                        declaration.table, other.model
                    )));
                }
            }
            let mut causal_tables = self
                .causal_tables
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("causal table marker write"))?;
            // Nothing below can fail.
            protocol.registered_topologies.insert(topology.clone());
            for declaration in ownership {
                let key = RegisteredModelKey {
                    topology: topology.clone(),
                    model: declaration.model.clone(),
                };
                protocol
                    .registered_models
                    .insert(key.clone(), declaration.table.clone());
                protocol
                    .authoritative_table_owners
                    .insert(declaration.table.clone(), key);
            }
            causal_tables.extend(
                ownership
                    .iter()
                    .map(|declaration| declaration.table.clone()),
            );
            Ok(())
        }
    }

    fn commit_projection(
        &self,
        mut batch: ProjectionCommitBatch,
    ) -> impl Future<Output = Result<ProjectionCommitResult, ProjectionProtocolError>> + Send + '_
    {
        async move {
            batch.validate()?;
            let partition_key = PartitionKey::from_input(&batch.input.cursor);

            // All projection commits use one lock order: rows, protocol, inbox.
            // Protocol/inbox guards therefore drop before the row guard, so a
            // reader can never observe a row before its revision metadata.
            let mut rows = self
                .model_store
                .relational_rows
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection rows write"))?;
            let mut protocol = self
                .projection_protocol
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection protocol write"))?;
            let mut inbox = self
                .inbox_store
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection inbox write"))?;

            let change_epoch = protocol.change_epoch(&partition_key);
            let redelivery = match protocol.input_disposition(&batch.input)? {
                ProjectionInputDisposition::Duplicate(checkpoint) => {
                    return Ok(ProjectionCommitResult::not_applied(
                        ProjectionCommitOutcome::Duplicate,
                        Some(checkpoint),
                    ));
                }
                ProjectionInputDisposition::Stale(checkpoint) => {
                    return Ok(ProjectionCommitResult::not_applied(
                        ProjectionCommitOutcome::StaleInput,
                        Some(checkpoint),
                    ));
                }
                ProjectionInputDisposition::Pending => false,
                ProjectionInputDisposition::Redelivery => {
                    batch.mutations.clear();
                    batch.observations.clear();
                    true
                }
            };

            let receipt = batch.input.inbox_receipt();
            receipt.validate()?;
            if !redelivery
                && inbox.contains(&(receipt.consumer.clone(), receipt.message_id.clone()))
            {
                return Err(ProjectionProtocolError::MessageIdReuse {
                    message_id: batch.input.message_id.clone(),
                });
            }

            // Everything fallible runs against the stored state and stages
            // what this commit adds; the stores change only after it all passed.
            let partition = protocol.partitions.get(&partition_key);
            let mut head = partition.map_or(0, |partition| partition.change_head);
            protocol.check_ownership(&partition_key, &batch)?;
            let mut staged_records = HashMap::with_capacity(batch.mutations.len());
            let mut staged_observations = HashMap::new();

            let mut touched_rows = HashSet::with_capacity(batch.mutations.len());
            for mutation in &batch.mutations {
                let lock_key = mutation.mutation.lock_key();
                if !touched_rows.insert(lock_key.clone()) {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection batch repeats table row `{lock_key}`"
                    )));
                }
            }
            let mut staged_rows = HashMap::with_capacity(touched_rows.len());
            for key in &touched_rows {
                if let Some(row) = rows.get(key) {
                    staged_rows.insert(key.clone(), row.clone());
                }
            }

            let mut records = Vec::with_capacity(batch.mutations.len());
            let mut changes =
                Vec::with_capacity(batch.mutations.len() + batch.observations.len().max(1));
            for mutation in &batch.mutations {
                let current = staged_records
                    .get(&mutation.scope)
                    .or_else(|| protocol.records.get(&mutation.scope));
                validate_physical_row(
                    &mutation.scope,
                    current,
                    staged_rows.contains_key(&mutation.mutation.lock_key()),
                )?;
                let (revision, tombstone) = next_record(
                    &mutation.scope,
                    &mutation.expectation,
                    mutation.kind,
                    current,
                    mutation.source_snapshot.is_some(),
                )?;
                crate::projection_protocol::validate_snapshot_write(
                    current,
                    mutation.source_snapshot.as_ref(),
                )?;
                let change = next_change(
                    &partition_key,
                    &change_epoch,
                    &mut head,
                    PendingChange {
                        kind: change_kind_for_mutation(mutation.kind),
                        causation_id: batch.input.causation_id.clone(),
                        observation_kind: None,
                        scope: Some(mutation.scope.clone()),
                        revision: Some(revision.clone()),
                        failure_id: None,
                        program_id: program_id_for_model(&batch.ownership, mutation.scope.model()),
                    },
                )?;
                let metadata = ProjectionRecordMetadata {
                    revision,
                    tombstone,
                    change: change.cursor.clone(),
                    source_snapshot: mutation.source_snapshot.clone(),
                };
                ensure_live_record_identity_available(
                    &protocol.records,
                    &staged_records,
                    &metadata,
                )?;
                staged_records.insert(mutation.scope.clone(), metadata.clone());
                records.push(metadata);
                changes.push(change);
            }

            apply_read_model_write_plan(
                TableWritePlan::new(
                    batch
                        .mutations
                        .iter()
                        .map(|mutation| mutation.mutation.clone())
                        .collect(),
                ),
                &mut staged_rows,
            )?;
            for mutation in &batch.mutations {
                let row_exists = staged_rows.contains_key(&mutation.mutation.lock_key());
                let should_exist = !matches!(mutation.kind, RecordTransition::Delete);
                if row_exists != should_exist {
                    return Err(corrupt_storage(format!(
                        "projection table mutation left model `{}` physical row {}",
                        mutation.scope.model(),
                        if row_exists {
                            "present when deletion required absence"
                        } else {
                            "absent when persistence required presence"
                        }
                    )));
                }
            }

            for request in &batch.observations {
                let scope = request.scope().clone();
                let (revision, staged_change) = resolve_observation(
                    &request.target,
                    staged_records
                        .get(&scope)
                        .or_else(|| protocol.records.get(&scope)),
                )?;
                let observation_key = ObservationKey {
                    causation_id: batch.input.causation_id.clone(),
                    scope: scope.clone(),
                    kind: request.kind,
                };
                if protocol.observations.contains_key(&observation_key)
                    || staged_observations.contains_key(&observation_key)
                {
                    continue;
                }
                let program_id = program_id_for_model(&batch.ownership, scope.model());
                let change_cursor = match staged_change {
                    Some(cursor) => cursor,
                    None => {
                        let change = next_change(
                            &partition_key,
                            &change_epoch,
                            &mut head,
                            PendingChange {
                                kind: ProjectionChangeKind::Observation,
                                causation_id: batch.input.causation_id.clone(),
                                observation_kind: Some(request.kind),
                                scope: Some(scope.clone()),
                                revision: revision.clone(),
                                failure_id: None,
                                program_id,
                            },
                        )?;
                        let cursor = change.cursor.clone();
                        changes.push(change);
                        cursor
                    }
                };
                let observation = ProjectionObservation {
                    causation_id: batch.input.causation_id.clone(),
                    kind: request.kind,
                    revision,
                    scope,
                    change: change_cursor,
                    program_id,
                };
                staged_observations.insert(observation_key, observation);
            }

            if changes.is_empty() {
                changes.push(next_change(
                    &partition_key,
                    &change_epoch,
                    &mut head,
                    PendingChange {
                        kind: ProjectionChangeKind::Checkpoint,
                        causation_id: batch.input.causation_id.clone(),
                        observation_kind: None,
                        scope: None,
                        revision: None,
                        failure_id: None,
                        program_id: None,
                    },
                )?);
            }
            let checkpoint = ProjectionCheckpoint::new(
                batch.input.cursor.clone(),
                changes
                    .last()
                    .expect("every successful projection input emits a change")
                    .cursor
                    .clone(),
                batch.input.gap_free,
            )?;
            let through = retained_through(partition, head, self.projection_change_retention)?;

            // Nothing below can fail. The row write guard is declared first
            // and releases last.
            for key in touched_rows {
                match staged_rows.remove(&key) {
                    Some(row) => {
                        rows.insert(key, row);
                    }
                    None => {
                        rows.remove(&key);
                    }
                }
            }
            let protocol = &mut *protocol;
            let partition = protocol.partition_mut(&partition_key, &change_epoch);
            partition.append(changes.iter().cloned());
            partition.compact(through);
            partition.pending_retry_failure_id = None;
            protocol
                .gap_free_capabilities
                .entry(SourceCapabilityKey::new(&batch.input.cursor))
                .or_insert(batch.input.gap_free);
            protocol
                .ownership
                .extend(batch.ownership.iter().map(|declaration| {
                    (
                        OwnershipKey {
                            partition: partition_key.clone(),
                            model: declaration.model.clone(),
                        },
                        declaration.table.clone(),
                    )
                }));
            protocol.records.extend(staged_records);
            protocol.observations.extend(staged_observations);
            let applied = (batch.input.clone(), checkpoint.change().clone());
            protocol.inputs.insert(
                InputKey::new(&batch.input.cursor, batch.input.generation),
                applied.clone(),
            );
            protocol.persist_input_identity(&batch.input);
            protocol.applied_receipts.insert(
                CursorReceiptKey::new(&batch.input.cursor, batch.input.generation),
                applied,
            );
            inbox.insert((receipt.consumer, receipt.message_id));

            Ok(ProjectionCommitResult {
                outcome: if redelivery {
                    ProjectionCommitOutcome::Duplicate
                } else {
                    ProjectionCommitOutcome::Applied
                },
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
            let partition_key = PartitionKey::from_input(&batch.input.cursor);

            // Keep the same global lock order as successful commits even though
            // failures intentionally do not touch rows.
            let _rows = self
                .model_store
                .relational_rows
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection rows failure fence"))?;
            let mut protocol = self
                .projection_protocol
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection failure write"))?;
            let mut inbox = self
                .inbox_store
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection failure inbox write"))?;

            protocol.require_registered_topology(batch.input.cursor.topology())?;
            let partition = protocol.partitions.get(&partition_key);
            protocol.validate_input_identity(&batch.input)?;
            ensure_active_input(
                &batch.input,
                protocol.active_generation(&partition_key),
                None,
            )?;
            if let Some(stopped) =
                partition.and_then(|partition| partition.stopped_failure_id.as_ref())
            {
                return match protocol.failures.get(stopped) {
                    Some(existing) if failure_matches_batch(existing, &batch) => {
                        Ok(existing.clone())
                    }
                    Some(_) => Err(ProjectionProtocolError::PartitionStopped {
                        failure_id: stopped.clone(),
                    }),
                    None => Err(corrupt_storage(format!(
                        "stopped projection failure `{stopped}` is missing"
                    ))),
                };
            }
            if protocol.failures.contains_key(&batch.failure_id) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection failure ID `{}` is already bound to another failure",
                    batch.failure_id
                )));
            }
            if protocol.classify_input(&batch.input)? != ProjectionInputDisposition::Pending {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "cannot record terminal failure for an already processed input".into(),
                ));
            }

            let receipt = batch.input.inbox_receipt();
            receipt.validate()?;
            if inbox.contains(&(receipt.consumer.clone(), receipt.message_id.clone())) {
                return Err(ProjectionProtocolError::MessageIdReuse {
                    message_id: batch.input.message_id.clone(),
                });
            }

            // As in a commit: compute against stored state, then apply.
            let change_epoch = protocol.change_epoch(&partition_key);
            let mut head = partition.map_or(0, |partition| partition.change_head);
            let change = next_change(
                &partition_key,
                &change_epoch,
                &mut head,
                PendingChange {
                    kind: ProjectionChangeKind::Failure,
                    causation_id: batch.input.causation_id.clone(),
                    observation_kind: None,
                    scope: None,
                    revision: None,
                    failure_id: Some(batch.failure_id.clone()),
                    program_id: None,
                },
            )?;
            let failure = ProjectionFailure {
                failure_id: batch.failure_id.clone(),
                input: batch.input.cursor.clone(),
                input_fingerprint: batch.input.fingerprint,
                message_id: batch.input.message_id.clone(),
                causation_id: batch.input.causation_id.clone(),
                generation: batch.input.generation,
                gap_free: batch.input.gap_free,
                failure_code: batch.failure_code,
                failure_bytes: batch.failure_bytes,
                failure_digest: batch.failure_digest,
                change: change.cursor.clone(),
            };
            let through = retained_through(partition, head, self.projection_change_retention)?;

            // Nothing below can fail.
            let protocol = &mut *protocol;
            let partition = protocol.partition_mut(&partition_key, &change_epoch);
            partition.append([change]);
            partition.compact(through);
            partition.stopped_failure_id = Some(batch.failure_id.clone());
            partition.pending_retry_failure_id = None;
            protocol
                .gap_free_capabilities
                .entry(SourceCapabilityKey::new(&batch.input.cursor))
                .or_insert(batch.input.gap_free);
            protocol.persist_input_identity(&batch.input);
            protocol.failures.insert(batch.failure_id, failure.clone());
            inbox.insert((receipt.consumer, receipt.message_id));
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
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection checkpoint read"))?;
            let Some((input, change)) = protocol
                .inputs
                .get(&InputKey::new(cursor_scope, generation))
            else {
                return Ok(None);
            };
            if matches!(
                cursor_scope.compare_position(&input.cursor),
                RevisionComparison::Incomparable
            ) {
                return Err(ProjectionProtocolError::IncomparableInput);
            }
            Ok(Some(ProjectionCheckpoint::new(
                input.cursor.clone(),
                change.clone(),
                input.gap_free,
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
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection record read"))?;
            Ok(protocol.records.get(scope).cloned())
        }
    }

    fn projection_input_disposition<'a>(
        &'a self,
        input: &'a TrustedProjectionInput,
    ) -> impl Future<Output = Result<ProjectionInputDisposition, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection input disposition read"))?;
            protocol.input_disposition(input)
        }
    }

    fn projection_query_snapshot<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshot, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            request.validate()?;

            // Projection writers acquire rows before protocol. Holding both
            // read guards in that same order prevents a writer from publishing
            // either half of a row/revision/checkpoint snapshot independently.
            let rows = self
                .model_store
                .relational_rows
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection query rows read"))?;
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection query protocol read"))?;

            read_projection_query_snapshot_from_state(&rows, &protocol, request)
        }
    }

    fn projection_execution_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionExecutionSnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionExecutionSnapshotBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        async move {
            request.validate()?;
            let rows = self.model_store.relational_rows.read().map_err(|_| {
                RepositoryError::LockPoisoned("projection execution snapshot rows read")
            })?;
            let protocol = self.projection_protocol.read().map_err(|_| {
                RepositoryError::LockPoisoned("projection execution snapshot protocol read")
            })?;
            read_projection_execution_snapshot_batch_from_state(&rows, &protocol, request)
        }
    }

    fn projection_causation_evidence<'a>(
        &'a self,
        request: &'a ProjectionCausationEvidenceRequest,
    ) -> impl Future<Output = Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        async move {
            request.validate()?;
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection causation evidence read"))?;
            let observations = protocol
                .observations
                .values()
                .filter(|observation| {
                    observation.causation_id == request.causation_id
                        && request
                            .topologies
                            .iter()
                            .any(|topology| topology == observation.scope.topology())
                })
                .cloned()
                .collect::<Vec<_>>();
            if observations.len() > MAX_PROJECTION_EVIDENCE_BATCH_ITEMS {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection causation has {} observations; maximum is {}",
                    observations.len(),
                    MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
                )));
            }
            for observation in &observations {
                let probe = ProjectionObligationEvidenceRequest::new(
                    request.causation_id.clone(),
                    observation.scope.clone(),
                    observation.kind,
                )?;
                validate_observation_from_state(&protocol, &probe, observation)?;
            }
            let mut terminal_failure_topologies = Vec::new();
            for failure in protocol.failures.values().filter(|failure| {
                failure.causation_id == request.causation_id
                    && request
                        .topologies
                        .iter()
                        .any(|topology| topology == failure.input.topology())
            }) {
                let key = PartitionKey::new(
                    failure.input.topology(),
                    failure.input.projection_partition(),
                );
                let partition = protocol.partitions.get(&key).ok_or_else(|| {
                    corrupt_storage("stored projection failure has no partition state")
                })?;
                if partition.stopped_failure_id.as_deref() != Some(&failure.failure_id) {
                    continue;
                }
                if failure.change.topology() != failure.input.topology()
                    || failure.change.projection_partition() != failure.input.projection_partition()
                    || failure.change.epoch() != &partition.change_epoch
                    || failure.change.position() > partition.change_head
                {
                    return Err(corrupt_storage(
                        "stored stopped projection failure lies outside its partition",
                    ));
                }
                if !terminal_failure_topologies
                    .iter()
                    .any(|topology| topology == failure.input.topology())
                {
                    terminal_failure_topologies.push(failure.input.topology().clone());
                }
            }
            if terminal_failure_topologies.len() > MAX_PROJECTION_EVIDENCE_BATCH_ITEMS {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection causation has {} stopped topologies; maximum is {}",
                    terminal_failure_topologies.len(),
                    MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
                )));
            }
            let mut evidence = ProjectionCausationEvidenceBatch {
                observations,
                terminal_failure_topologies,
            };
            sort_causation_evidence(&mut evidence);
            Ok(evidence)
        }
    }

    fn projection_partition_runtime_state<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
    ) -> impl Future<Output = Result<Option<ProjectionPartitionRuntimeState>, ProjectionProtocolError>>
           + Send
           + 'a {
        async move {
            let protocol = self.projection_protocol.read().map_err(|_| {
                RepositoryError::LockPoisoned("projection partition runtime state read")
            })?;
            let Some(state) = protocol
                .partitions
                .get(&PartitionKey::new(topology, partition))
            else {
                return Ok(None);
            };
            let pending_retry = match &state.pending_retry_failure_id {
                Some(failure_id) => {
                    let failure = protocol.failures.get(failure_id).ok_or_else(|| {
                        corrupt_storage(format!(
                            "pending projection retry failure `{failure_id}` is missing"
                        ))
                    })?;
                    if state.stopped_failure_id.is_some()
                        || failure.failure_id != *failure_id
                        || failure.input.topology() != topology
                        || failure.input.projection_partition() != partition
                        || failure.generation.checked_next()? != state.active_generation
                    {
                        return Err(corrupt_storage(format!(
                            "pending projection retry failure `{failure_id}` is corrupt"
                        )));
                    }
                    Some(ProjectionPendingRetry {
                        failure_id: failure_id.clone(),
                        input: failure.input.clone(),
                        input_fingerprint: failure.input_fingerprint,
                        message_id: failure.message_id.clone(),
                        causation_id: failure.causation_id.clone(),
                        failed_generation: failure.generation,
                        gap_free: failure.gap_free,
                    })
                }
                None => None,
            };
            Ok(Some(ProjectionPartitionRuntimeState {
                active_generation: state.active_generation,
                stopped_failure_id: state.stopped_failure_id.clone(),
                pending_retry,
            }))
        }
    }

    fn projection_observation<'a>(
        &'a self,
        causation_id: &'a str,
        scope: &'a ProjectionRecordScope,
        kind: ProjectionObservationKind,
    ) -> impl Future<Output = Result<Option<ProjectionObservation>, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection observation read"))?;
            Ok(protocol
                .observations
                .get(&ObservationKey {
                    causation_id: causation_id.to_string(),
                    scope: scope.clone(),
                    kind,
                })
                .cloned())
        }
    }

    fn projection_changes<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
        after: Option<&'a ProjectionChangeCursor>,
        limit: usize,
    ) -> impl Future<Output = Result<ProjectionChangeRead, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection changes read"))?;
            let state = protocol
                .partitions
                .get(&PartitionKey::new(topology, partition));
            let window = state.map(|state| {
                (
                    &state.change_epoch,
                    state.change_head,
                    state.compacted_through,
                )
            });
            let start = match change_read_window(topology, partition, window, after, limit)? {
                std::ops::ControlFlow::Break(read) => return Ok(read),
                std::ops::ControlFlow::Continue(start) => start,
            };
            let (state, window) = state
                .zip(window)
                .expect("a change read past its window decision has a partition");
            let changes = state
                .changes
                .range((std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded))
                .take(limit)
                .map(|(_, change)| change.clone())
                .collect();
            change_page(topology, partition, window, start, changes)
        }
    }

    fn repair_projection<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<ProjectionGeneration, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let mut protocol = self
                .projection_protocol
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection repair write"))?;
            let key = PartitionKey::new(topology, partition);
            let Some(state) = protocol.partitions.get(&key) else {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "cannot repair an unknown projection partition".into(),
                ));
            };
            let current_generation = state.active_generation;
            let next_generation = plan_repair(
                topology,
                partition,
                current_generation,
                state.stopped_failure_id.as_deref(),
                failure_id,
                protocol.failures.get(failure_id),
            )?;
            if protocol.generations.contains_key(&GenerationKey {
                partition: key.clone(),
                generation: next_generation,
            }) {
                return Err(corrupt_storage(format!(
                    "projection repair generation {} already exists",
                    next_generation.get()
                )));
            }
            if protocol
                .generations
                .iter()
                .any(|(generation_key, lineage)| {
                    generation_key.partition == key
                        && lineage.retry_of_failure_id.as_deref() == Some(failure_id)
                })
            {
                return Err(corrupt_storage(format!(
                    "stopped failure `{failure_id}` already has a repair generation"
                )));
            }
            let copied_inputs = protocol
                .inputs
                .iter()
                .filter(|(input_key, _)| {
                    input_key.partition == key && input_key.generation == current_generation
                })
                .map(|(input_key, applied)| {
                    (
                        InputKey {
                            partition: input_key.partition.clone(),
                            source: input_key.source.clone(),
                            generation: next_generation,
                        },
                        applied.clone(),
                    )
                })
                .collect::<Vec<_>>();

            // Nothing below can fail.
            let protocol = &mut *protocol;
            let state = protocol
                .partitions
                .get_mut(&key)
                .expect("the repaired partition was read above under this guard");
            state.active_generation = next_generation;
            state.stopped_failure_id = None;
            state.pending_retry_failure_id = Some(failure_id.to_string());
            protocol.inputs.extend(copied_inputs);
            protocol.generations.insert(
                GenerationKey {
                    partition: key.clone(),
                    generation: next_generation,
                },
                GenerationLineage::retry_of(current_generation, failure_id.to_string()),
            );
            Ok(next_generation)
        }
    }

    fn compact_projection_changes<'a>(
        &'a self,
        through: &'a ProjectionChangeCursor,
    ) -> impl Future<Output = Result<u64, ProjectionProtocolError>> + Send + 'a {
        async move {
            let mut protocol = self
                .projection_protocol
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("projection changes compaction"))?;
            let key = PartitionKey::new(through.topology(), through.projection_partition());
            let Some(partition) = protocol.partitions.get(&key) else {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "cannot compact an unknown projection partition".into(),
                ));
            };
            if through.epoch() != &partition.change_epoch {
                return Err(ProjectionProtocolError::ScopeMismatch {
                    field: "projection compaction epoch",
                });
            }
            if through.position() > partition.change_head {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "cannot compact beyond the projection change head".into(),
                ));
            }
            protocol.compact_changes_through(&key, through.position())
        }
    }

    fn projection_failure<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailure>, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection failure read"))?;
            Ok(protocol
                .failures
                .get(failure_id)
                .filter(|failure| {
                    failure.input.topology() == topology
                        && failure.input.projection_partition() == partition
                })
                .cloned())
        }
    }

    fn projection_failure_location<'a>(
        &'a self,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailureLocation>, ProjectionProtocolError>>
           + Send
           + 'a {
        async move {
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection failure location read"))?;
            Ok(protocol
                .failures
                .get(failure_id)
                .map(|failure| ProjectionFailureLocation {
                    topology: failure.input.topology().clone(),
                    partition: failure.input.projection_partition().clone(),
                }))
        }
    }
}

#[cfg(test)]
impl InMemoryRepository {
    /// Create a projection partition the way an earlier release did, with
    /// the change epoch its application declared.
    pub(crate) fn insert_projection_partition(
        &self,
        topology: &ProjectorTopologyId,
        partition: &PartitionId,
        epoch: &ProjectionEpoch,
    ) {
        self.projection_protocol
            .write()
            .unwrap()
            .partition_mut(&PartitionKey::new(topology, partition), epoch);
    }
}
