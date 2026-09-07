use super::*;

impl ProjectionProtocolStore for InMemoryRepository {
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
            let mut staged = protocol.clone();
            staged.registered_topologies.insert(topology.clone());
            for declaration in ownership {
                let key = RegisteredModelKey {
                    topology: topology.clone(),
                    model: declaration.model.clone(),
                };
                if let Some(previous) = staged.authoritative_table_owners.get(&declaration.table) {
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
                if let Some(previous) = staged.registered_models.get(&key) {
                    if previous != &declaration.table {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
                            "projection model `{}` is already registered for table `{previous}`",
                            declaration.model
                        )));
                    }
                }
                if let Some((other, _)) = staged.registered_models.iter().find(|(other, table)| {
                    other.topology == *topology
                        && *table == &declaration.table
                        && other.model != declaration.model
                }) {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection table `{}` is already registered for model `{}`",
                        declaration.table, other.model
                    )));
                }
                staged
                    .registered_models
                    .insert(key.clone(), declaration.table.clone());
                staged
                    .authoritative_table_owners
                    .insert(declaration.table.clone(), key);
            }
            let mut causal_tables = self
                .causal_tables
                .write()
                .map_err(|_| RepositoryError::LockPoisoned("causal table marker write"))?;
            *protocol = staged;
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
        batch: ProjectionCommitBatch,
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

            protocol.validate_partition(&partition_key, &batch.input, &batch.change_epoch)?;
            match protocol.classify_input(
                &batch.input.cursor,
                batch.input.fingerprint,
                &batch.input.message_id,
                &batch.input.causation_id,
                batch.input.generation,
                batch.input.gap_free,
            )? {
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
                    protocol.validate_pending_retry(&partition_key, &batch.input)?;
                }
            }

            let receipt = batch.input.inbox_receipt();
            receipt.validate()?;
            if inbox.contains(&(receipt.consumer.clone(), receipt.message_id.clone())) {
                return Err(ProjectionProtocolError::MessageIdReuse {
                    message_id: batch.input.message_id.clone(),
                });
            }

            let mut staged_protocol = protocol.clone();
            let mut staged_inbox = inbox.clone();
            staged_protocol.ensure_partition(&partition_key, &batch.change_epoch)?;
            staged_protocol
                .gap_free_capabilities
                .entry(SourceCapabilityKey::new(&batch.input.cursor))
                .or_insert(batch.input.gap_free);
            staged_protocol.register_ownership(&partition_key, &batch)?;

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
                let (revision, tombstone) = staged_protocol.next_record(
                    &mutation.scope,
                    &mutation.expectation,
                    mutation.kind,
                    mutation.source_snapshot.is_some(),
                )?;
                crate::projection_protocol::validate_snapshot_write(
                    staged_protocol.records.get(&mutation.scope),
                    mutation.source_snapshot.as_ref(),
                )?;
                staged_protocol.validate_physical_record(
                    &mutation.scope,
                    &mutation.expectation,
                    mutation.kind,
                    staged_rows.contains_key(&mutation.mutation.lock_key()),
                )?;
                let change = staged_protocol.append_change(
                    &partition_key,
                    PendingChange {
                        kind: change_kind_for_mutation(mutation.kind),
                        causation_id: batch.input.causation_id.clone(),
                        observation_kind: None,
                        scope: Some(mutation.scope.clone()),
                        revision: Some(revision.clone()),
                        failure_id: None,
                    },
                )?;
                let metadata = ProjectionRecordMetadata {
                    revision,
                    tombstone,
                    change: change.cursor.clone(),
                    source_snapshot: mutation.source_snapshot.clone(),
                };
                staged_protocol.ensure_live_record_identity_available(&metadata)?;
                staged_protocol
                    .records
                    .insert(mutation.scope.clone(), metadata.clone());
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
                let should_exist = !matches!(mutation.kind, ProjectionMutationKind::Delete);
                if row_exists != should_exist {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
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
                let (scope, revision, staged_change) = match &request.target {
                    ProjectionObservationTarget::StagedRecord(scope) => {
                        let metadata = staged_protocol
                            .records
                            .get(scope)
                            .expect("batch validation requires a staged record");
                        (
                            scope.clone(),
                            Some(metadata.revision.clone()),
                            Some(metadata.change.clone()),
                        )
                    }
                    ProjectionObservationTarget::ExistingRecord(expected) => {
                        let Some(metadata) = staged_protocol.records.get(expected.scope()) else {
                            return Err(ProjectionProtocolError::RecordMissing {
                                model: expected.scope().model().to_string(),
                            });
                        };
                        if &metadata.revision != expected {
                            return Err(ProjectionProtocolError::RecordRevisionConflict {
                                model: expected.scope().model().to_string(),
                                expected_incarnation: expected.incarnation(),
                                expected_revision: expected.revision(),
                                actual_incarnation: metadata.revision.incarnation(),
                                actual_revision: metadata.revision.revision(),
                            });
                        }
                        (expected.scope().clone(), Some(expected.clone()), None)
                    }
                    ProjectionObservationTarget::Dependency(scope) => (scope.clone(), None, None),
                };
                let observation_key = ObservationKey {
                    causation_id: batch.input.causation_id.clone(),
                    scope: scope.clone(),
                    kind: request.kind,
                };
                if staged_protocol.observations.contains_key(&observation_key) {
                    continue;
                }
                let change_cursor = match staged_change {
                    Some(cursor) => cursor,
                    None => {
                        let change = staged_protocol.append_change(
                            &partition_key,
                            PendingChange {
                                kind: ProjectionChangeKind::Observation,
                                causation_id: batch.input.causation_id.clone(),
                                observation_kind: Some(request.kind),
                                scope: Some(scope.clone()),
                                revision: revision.clone(),
                                failure_id: None,
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
                };
                staged_protocol
                    .observations
                    .insert(observation_key, observation);
            }

            if changes.is_empty() {
                changes.push(staged_protocol.append_change(
                    &partition_key,
                    PendingChange {
                        kind: ProjectionChangeKind::Checkpoint,
                        causation_id: batch.input.causation_id.clone(),
                        observation_kind: None,
                        scope: None,
                        revision: None,
                        failure_id: None,
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
            let input_key = InputKey::new(&batch.input.cursor, batch.input.generation);
            staged_protocol.inputs.insert(
                input_key,
                StoredInput {
                    cursor: batch.input.cursor.clone(),
                    fingerprint: batch.input.fingerprint,
                    message_id: batch.input.message_id.clone(),
                    causation_id: batch.input.causation_id.clone(),
                    checkpoint: checkpoint.clone(),
                    gap_free: batch.input.gap_free,
                },
            );
            staged_protocol.persist_input_identity(&batch.input);
            staged_protocol.applied_receipts.insert(
                CursorReceiptKey::new(&batch.input.cursor, batch.input.generation),
                AppliedInputReceipt {
                    fingerprint: batch.input.fingerprint,
                    message_id: batch.input.message_id.clone(),
                    causation_id: batch.input.causation_id.clone(),
                    gap_free: batch.input.gap_free,
                    checkpoint: checkpoint.clone(),
                },
            );
            staged_protocol
                .partitions
                .get_mut(&partition_key)
                .expect("successful projection partition is initialized")
                .pending_retry_failure_id = None;
            staged_protocol
                .retain_change_suffix(&partition_key, self.projection_change_retention)?;
            staged_inbox.insert((receipt.consumer, receipt.message_id));

            // No fallible operations below this point. Merge only touched rows,
            // then publish the staged protocol/inbox maps while all guards remain
            // held. The row write guard is declared first and releases last.
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
            *protocol = staged_protocol;
            *inbox = staged_inbox;

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
            protocol.validate_known_input_identity(&batch.input)?;
            if let Some(partition) = protocol.partitions.get(&partition_key) {
                if batch.input.generation != partition.active_generation {
                    return Err(ProjectionProtocolError::GenerationFenced {
                        expected: partition.active_generation.get(),
                        actual: batch.input.generation.get(),
                    });
                }
                protocol.validate_gap_free(&batch.input.cursor, batch.input.gap_free)?;
                protocol.validate_input_identity(&batch.input)?;
                if let Some(stopped_failure_id) = &partition.stopped_failure_id {
                    if stopped_failure_id == &batch.failure_id {
                        if let Some(existing) = protocol.failures.get(stopped_failure_id) {
                            if failure_matches_batch(existing, &batch)
                                && protocol.failure_inputs.get(stopped_failure_id).is_some_and(
                                    |fence| {
                                        fence.generation == batch.input.generation
                                            && fence.matches_retry(&batch.input)
                                    },
                                )
                                && protocol.has_exact_message_identity(&batch.input)
                            {
                                return Ok(existing.clone());
                            }
                        }
                    }
                    return Err(ProjectionProtocolError::PartitionStopped {
                        failure_id: stopped_failure_id.clone(),
                    });
                }
            }
            if protocol.failures.contains_key(&batch.failure_id) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection failure ID `{}` is already bound to another failure",
                    batch.failure_id
                )));
            }

            protocol.validate_partition(&partition_key, &batch.input, &batch.change_epoch)?;
            match protocol.classify_input(
                &batch.input.cursor,
                batch.input.fingerprint,
                &batch.input.message_id,
                &batch.input.causation_id,
                batch.input.generation,
                batch.input.gap_free,
            )? {
                InputDisposition::New => {
                    protocol.validate_pending_retry(&partition_key, &batch.input)?;
                }
                InputDisposition::Duplicate(_) | InputDisposition::Stale(_) => {
                    return Err(ProjectionProtocolError::InvalidBatch(
                        "cannot record terminal failure for an already processed input".into(),
                    ));
                }
            }

            let receipt = batch.input.inbox_receipt();
            receipt.validate()?;
            if inbox.contains(&(receipt.consumer.clone(), receipt.message_id.clone())) {
                return Err(ProjectionProtocolError::MessageIdReuse {
                    message_id: batch.input.message_id.clone(),
                });
            }

            let mut staged_protocol = protocol.clone();
            let mut staged_inbox = inbox.clone();
            staged_protocol.ensure_partition(&partition_key, &batch.change_epoch)?;
            staged_protocol
                .gap_free_capabilities
                .entry(SourceCapabilityKey::new(&batch.input.cursor))
                .or_insert(batch.input.gap_free);
            let change = staged_protocol.append_change(
                &partition_key,
                PendingChange {
                    kind: ProjectionChangeKind::Failure,
                    causation_id: batch.input.causation_id.clone(),
                    observation_kind: None,
                    scope: None,
                    revision: None,
                    failure_id: Some(batch.failure_id.clone()),
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
                change: change.cursor,
            };
            staged_protocol
                .partitions
                .get_mut(&partition_key)
                .expect("failure partition was initialized")
                .stopped_failure_id = Some(batch.failure_id.clone());
            staged_protocol
                .partitions
                .get_mut(&partition_key)
                .expect("failure partition was initialized")
                .pending_retry_failure_id = None;
            staged_protocol.persist_input_identity(&batch.input);
            staged_protocol.failure_inputs.insert(
                batch.failure_id.clone(),
                FailedInputFence::from_input(&batch.input),
            );
            staged_protocol
                .failures
                .insert(batch.failure_id, failure.clone());
            staged_protocol
                .retain_change_suffix(&partition_key, self.projection_change_retention)?;
            staged_inbox.insert((receipt.consumer, receipt.message_id));

            *protocol = staged_protocol;
            *inbox = staged_inbox;
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
            let Some(input) = protocol
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
            Ok(Some(input.checkpoint.clone()))
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
            protocol.require_registered_topology(input.cursor.topology())?;
            protocol.validate_known_input_identity(input)?;
            let partition_key = PartitionKey::from_input(&input.cursor);
            let Some(partition) = protocol.partitions.get(&partition_key) else {
                if input.generation != ProjectionGeneration::initial() {
                    return Err(ProjectionProtocolError::GenerationFenced {
                        expected: ProjectionGeneration::initial().get(),
                        actual: input.generation.get(),
                    });
                }
                protocol.validate_gap_free(&input.cursor, input.gap_free)?;
                protocol.validate_input_identity(input)?;
                return Ok(ProjectionInputDisposition::Pending);
            };
            if input.generation != partition.active_generation {
                return Err(ProjectionProtocolError::GenerationFenced {
                    expected: partition.active_generation.get(),
                    actual: input.generation.get(),
                });
            }
            if !protocol.generations.contains_key(&GenerationKey {
                partition: partition_key.clone(),
                generation: input.generation,
            }) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "active projection generation {} has no durable lineage",
                    input.generation.get()
                )));
            }
            protocol.validate_gap_free(&input.cursor, input.gap_free)?;
            protocol.validate_input_identity(input)?;
            if let Some(failure_id) = &partition.stopped_failure_id {
                return Err(ProjectionProtocolError::PartitionStopped {
                    failure_id: failure_id.clone(),
                });
            }
            match protocol.classify_input(
                &input.cursor,
                input.fingerprint,
                &input.message_id,
                &input.causation_id,
                input.generation,
                input.gap_free,
            )? {
                InputDisposition::Duplicate(checkpoint) => {
                    Ok(ProjectionInputDisposition::Duplicate(checkpoint))
                }
                InputDisposition::Stale(checkpoint) => {
                    Ok(ProjectionInputDisposition::Stale(checkpoint))
                }
                InputDisposition::New => {
                    protocol.validate_pending_retry(&partition_key, input)?;
                    Ok(ProjectionInputDisposition::Pending)
                }
            }
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

    fn projection_query_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshotBatch, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            request.validate()?;
            // Use the same writer lock order as the one-row primitive and hold
            // both guards for the entire plan, not once per row.
            let rows =
                self.model_store.relational_rows.read().map_err(|_| {
                    RepositoryError::LockPoisoned("projection query batch rows read")
                })?;
            let protocol = self.projection_protocol.read().map_err(|_| {
                RepositoryError::LockPoisoned("projection query batch protocol read")
            })?;
            let snapshots = request
                .requests
                .iter()
                .map(|row_request| {
                    read_projection_query_snapshot_from_state(&rows, &protocol, row_request)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ProjectionQuerySnapshotBatch { snapshots })
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

    fn projection_graph_snapshot<'a>(
        &'a self,
        request: &'a ProjectionGraphSnapshotRequest,
    ) -> impl Future<Output = Result<ProjectionGraphSnapshot, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            validate_projection_graph_snapshot_request(request)?;
            let rows = self.model_store.relational_rows.read().map_err(|_| {
                RepositoryError::LockPoisoned("projection graph snapshot rows read")
            })?;
            let protocol = self.projection_protocol.read().map_err(|_| {
                RepositoryError::LockPoisoned("projection graph snapshot protocol read")
            })?;
            read_projection_graph_snapshot_from_state(&rows, &protocol, request)
        }
    }

    fn projection_obligation_evidence_batch<'a>(
        &'a self,
        request: &'a ProjectionObligationEvidenceBatchRequest,
    ) -> impl Future<Output = Result<ProjectionObligationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        async move {
            request.validate()?;
            let protocol = self.projection_protocol.read().map_err(|_| {
                RepositoryError::LockPoisoned("projection obligation evidence read")
            })?;
            let evidence = request
                .requests
                .iter()
                .map(|probe| read_projection_obligation_evidence_from_state(&protocol, probe))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ProjectionObligationEvidenceBatch { evidence })
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
            let mut observations = protocol
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
            observations.sort_by(|left, right| {
                (
                    left.scope.topology().canonical_bytes(),
                    left.scope.projection_partition().canonical_bytes(),
                    left.scope.model(),
                    left.kind.as_storage_str(),
                    left.scope.canonical_key_bytes(),
                )
                    .cmp(&(
                        right.scope.topology().canonical_bytes(),
                        right.scope.projection_partition().canonical_bytes(),
                        right.scope.model(),
                        right.kind.as_storage_str(),
                        right.scope.canonical_key_bytes(),
                    ))
            });

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
                    ProjectionProtocolError::InvalidBatch(
                        "stored projection failure has no partition state".into(),
                    )
                })?;
                if partition.stopped_failure_id.as_deref() != Some(&failure.failure_id) {
                    continue;
                }
                if failure.change.topology() != failure.input.topology()
                    || failure.change.projection_partition() != failure.input.projection_partition()
                    || failure.change.epoch() != &partition.change_epoch
                    || failure.change.position() > partition.change_head
                {
                    return Err(ProjectionProtocolError::InvalidBatch(
                        "stored stopped projection failure lies outside its partition".into(),
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
            terminal_failure_topologies.sort_by_key(ProjectorTopologyId::canonical_bytes);
            Ok(ProjectionCausationEvidenceBatch {
                observations,
                terminal_failure_topologies,
            })
        }
    }

    fn projection_live_record_batch<'a>(
        &'a self,
        request: &'a ProjectionLiveRecordBatchRequest,
    ) -> impl Future<Output = Result<ProjectionLiveRecordBatch, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            request.validate()?;
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection live-record read"))?;
            let records = request
                .requests
                .iter()
                .map(|probe| read_projection_live_record_from_state(&protocol, probe))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ProjectionLiveRecordBatch { records })
        }
    }

    fn projection_partition_runtime_state<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
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
                        ProjectionProtocolError::InvalidBatch(format!(
                            "pending projection retry failure `{failure_id}` is missing"
                        ))
                    })?;
                    let fence = protocol.failure_inputs.get(failure_id).ok_or_else(|| {
                        ProjectionProtocolError::InvalidBatch(format!(
                            "pending projection retry failure `{failure_id}` has no input fence"
                        ))
                    })?;
                    if state.stopped_failure_id.is_some()
                        || failure.failure_id != *failure_id
                        || failure.input.topology() != topology
                        || failure.input.projection_partition() != partition
                        || failure.generation.checked_next()? != state.active_generation
                        || fence.cursor != failure.input
                        || fence.fingerprint != failure.input_fingerprint
                        || fence.message_id != failure.message_id
                        || fence.causation_id != failure.causation_id
                        || fence.generation != failure.generation
                        || fence.gap_free != failure.gap_free
                    {
                        return Err(ProjectionProtocolError::InvalidBatch(format!(
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
        partition: &'a ProjectionPartition,
        after: Option<&'a ProjectionChangeCursor>,
        limit: usize,
    ) -> impl Future<Output = Result<ProjectionChangeRead, ProjectionProtocolError>> + Send + 'a
    {
        async move {
            let protocol = self
                .projection_protocol
                .read()
                .map_err(|_| RepositoryError::LockPoisoned("projection changes read"))?;
            let key = PartitionKey::new(topology, partition);
            let Some(state) = protocol.partitions.get(&key) else {
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
            let after_position = match after {
                None if state.compacted_through > 0 => {
                    return Ok(ProjectionChangeRead::ResetRequired {
                        head,
                        compacted_through: state.compacted_through,
                    });
                }
                None => 0,
                Some(cursor)
                    if cursor.topology() != topology
                        || cursor.projection_partition() != partition
                        || cursor.epoch() != &state.change_epoch
                        || cursor.position() > state.change_head
                        // `compacted_through` is the last removed change. An
                        // exact cursor at that watermark is therefore the safe
                        // boundary for resuming at the first retained change.
                        || cursor.position() < state.compacted_through =>
                {
                    return Ok(ProjectionChangeRead::ResetRequired {
                        head,
                        compacted_through: state.compacted_through,
                    });
                }
                Some(cursor) => cursor.position(),
            };
            let changes = if limit == 0 {
                Vec::new()
            } else {
                state
                    .changes
                    .range((
                        std::ops::Bound::Excluded(after_position),
                        std::ops::Bound::Unbounded,
                    ))
                    .take(limit)
                    .map(|(_, change)| change.clone())
                    .collect()
            };
            Ok(ProjectionChangeRead::Changes {
                head,
                compacted_through: state.compacted_through,
                changes,
            })
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
            let Some(failure) = protocol.failures.get(failure_id) else {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "stopped projection failure `{failure_id}` is missing"
                )));
            };
            if failure.input.topology() != topology
                || failure.input.projection_partition() != partition
            {
                return Err(ProjectionProtocolError::ScopeMismatch {
                    field: "projection repair failure",
                });
            }
            if failure.generation != state.active_generation {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "stopped failure generation {} differs from active generation {}",
                    failure.generation.get(),
                    state.active_generation.get()
                )));
            }
            let Some(failure_fence) = protocol.failure_inputs.get(failure_id) else {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "stopped projection failure `{failure_id}` has no exact input fence"
                )));
            };
            if failure_fence.generation != state.active_generation
                || failure_fence.cursor != failure.input
                || failure_fence.fingerprint != failure.input_fingerprint
                || failure_fence.message_id != failure.message_id
                || failure_fence.causation_id != failure.causation_id
            {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "stopped projection failure `{failure_id}` input fence is corrupt"
                )));
            }

            let current_generation = state.active_generation;
            let next_generation = current_generation.checked_next()?;
            if protocol.generations.contains_key(&GenerationKey {
                partition: key.clone(),
                generation: next_generation,
            }) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
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
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "stopped failure `{failure_id}` already has a repair generation"
                )));
            }
            let copied_inputs = protocol
                .inputs
                .iter()
                .filter(|(input_key, _)| {
                    input_key.partition == key && input_key.generation == current_generation
                })
                .map(|(input_key, input)| {
                    (
                        InputKey {
                            partition: input_key.partition.clone(),
                            source: input_key.source.clone(),
                            generation: next_generation,
                        },
                        input.clone(),
                    )
                })
                .collect::<Vec<_>>();

            let mut staged = protocol.clone();
            for (input_key, input) in copied_inputs {
                staged.inputs.insert(input_key, input);
            }
            staged.generations.insert(
                GenerationKey {
                    partition: key.clone(),
                    generation: next_generation,
                },
                GenerationLineage::retry_of(current_generation, failure_id.to_string()),
            );
            let state = staged
                .partitions
                .get_mut(&key)
                .expect("repair partition exists in staged state");
            state.active_generation = next_generation;
            state.stopped_failure_id = None;
            state.pending_retry_failure_id = Some(failure_id.to_string());
            *protocol = staged;
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
        partition: &'a ProjectionPartition,
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
