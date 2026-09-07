use super::*;

impl InMemoryProjectionProtocolState {
    pub(super) fn ensure_live_record_identity_available(
        &self,
        metadata: &ProjectionRecordMetadata,
    ) -> Result<(), ProjectionProtocolError> {
        if metadata.tombstone {
            return Ok(());
        }
        let candidate = metadata.revision.scope();
        for (scope, stored) in &self.records {
            if stored.tombstone
                || scope == candidate
                || scope.topology() != candidate.topology()
                || scope.model() != candidate.model()
                || scope.key_digest() != candidate.key_digest()
            {
                continue;
            }
            if scope.canonical_key_bytes() != candidate.canonical_key_bytes() {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection live-record key digest collision for model `{}`",
                    candidate.model()
                )));
            }
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection live record for model `{}` is already owned by another partition",
                candidate.model()
            )));
        }
        Ok(())
    }

    pub(super) fn require_registered_topology(
        &self,
        topology: &ProjectorTopologyId,
    ) -> Result<(), ProjectionProtocolError> {
        if self.registered_topologies.contains(topology) {
            Ok(())
        } else {
            Err(ProjectionProtocolError::InvalidBatch(
                "projector topology was not bootstrapped before projector traffic".into(),
            ))
        }
    }

    pub(super) fn ensure_partition(
        &mut self,
        key: &PartitionKey,
        change_epoch: &ProjectionEpoch,
    ) -> Result<(), ProjectionProtocolError> {
        match self.partitions.get(key) {
            Some(partition) if &partition.change_epoch != change_epoch => {
                return Err(ProjectionProtocolError::ScopeMismatch {
                    field: "projection change epoch",
                });
            }
            Some(_) => {}
            None => {
                self.partitions
                    .insert(key.clone(), PartitionState::new(change_epoch.clone()));
                self.generations.insert(
                    GenerationKey {
                        partition: key.clone(),
                        generation: ProjectionGeneration::initial(),
                    },
                    GenerationLineage::initial(),
                );
            }
        }
        Ok(())
    }

    pub(super) fn validate_partition(
        &self,
        key: &PartitionKey,
        input: &TrustedProjectionInput,
        change_epoch: &ProjectionEpoch,
    ) -> Result<(), ProjectionProtocolError> {
        self.require_registered_topology(input.cursor.topology())?;
        self.validate_known_input_identity(input)?;
        let Some(partition) = self.partitions.get(key) else {
            if input.generation != ProjectionGeneration::initial() {
                return Err(ProjectionProtocolError::GenerationFenced {
                    expected: ProjectionGeneration::initial().get(),
                    actual: input.generation.get(),
                });
            }
            self.validate_gap_free(&input.cursor, input.gap_free)?;
            self.validate_input_identity(input)?;
            return Ok(());
        };
        if &partition.change_epoch != change_epoch {
            return Err(ProjectionProtocolError::ScopeMismatch {
                field: "projection change epoch",
            });
        }
        if input.generation != partition.active_generation {
            return Err(ProjectionProtocolError::GenerationFenced {
                expected: partition.active_generation.get(),
                actual: input.generation.get(),
            });
        }
        if !self.generations.contains_key(&GenerationKey {
            partition: key.clone(),
            generation: input.generation,
        }) {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "active projection generation {} has no durable lineage",
                input.generation.get()
            )));
        }
        self.validate_gap_free(&input.cursor, input.gap_free)?;
        self.validate_input_identity(input)?;
        if let Some(failure_id) = &partition.stopped_failure_id {
            return Err(ProjectionProtocolError::PartitionStopped {
                failure_id: failure_id.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn validate_known_input_identity(
        &self,
        input: &TrustedProjectionInput,
    ) -> Result<(), ProjectionProtocolError> {
        let cursor_known = self
            .input_identities
            .contains_key(&CursorIdentityKey::new(&input.cursor));
        let message_known = self.messages.contains_key(&MessageKey {
            topology: input.cursor.topology().clone(),
            message_id: input.message_id.clone(),
        });
        let source_capability_known = self
            .gap_free_capabilities
            .contains_key(&SourceCapabilityKey::new(&input.cursor));
        if cursor_known || message_known || source_capability_known {
            // Exact cursor corruption wins over message reuse; both are durable
            // generation-independent identities. The source's fixed gap-free
            // capability has the same precedence. An entirely unknown
            // old-generation input is still rejected as GenerationFenced below.
            self.validate_input_identity(input)?;
            self.validate_gap_free(&input.cursor, input.gap_free)?;
        }
        Ok(())
    }

    pub(super) fn validate_pending_retry(
        &self,
        key: &PartitionKey,
        input: &TrustedProjectionInput,
    ) -> Result<(), ProjectionProtocolError> {
        let Some(partition) = self.partitions.get(key) else {
            return Ok(());
        };
        if let Some(failure_id) = &partition.pending_retry_failure_id {
            let Some(fence) = self.failure_inputs.get(failure_id) else {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "pending projection retry `{failure_id}` has no exact input fence"
                )));
            };
            if fence.cursor != input.cursor {
                return Err(ProjectionProtocolError::IncomparableInput);
            }
            if !fence.matches_retry(input) {
                return Err(ProjectionProtocolError::InputCorruption);
            }
        }
        Ok(())
    }

    pub(super) fn validate_input_identity(
        &self,
        input: &TrustedProjectionInput,
    ) -> Result<(), ProjectionProtocolError> {
        if self
            .input_identities
            .get(&CursorIdentityKey::new(&input.cursor))
            .is_some_and(|identity| {
                identity.fingerprint != input.fingerprint
                    || identity.message_id != input.message_id
                    || identity.causation_id != input.causation_id
                    || identity.gap_free != input.gap_free
            })
        {
            return Err(ProjectionProtocolError::InputCorruption);
        }
        self.reject_reused_message(
            &input.cursor,
            input.fingerprint,
            &input.message_id,
            &input.causation_id,
            input.gap_free,
        )
    }

    pub(super) fn persist_input_identity(&mut self, input: &TrustedProjectionInput) {
        self.input_identities
            .entry(CursorIdentityKey::new(&input.cursor))
            .or_insert_with(|| InputIdentity {
                fingerprint: input.fingerprint,
                message_id: input.message_id.clone(),
                causation_id: input.causation_id.clone(),
                gap_free: input.gap_free,
            });
        self.messages
            .entry(MessageKey {
                topology: input.cursor.topology().clone(),
                message_id: input.message_id.clone(),
            })
            .or_insert_with(|| MessageIdentity {
                cursor: input.cursor.clone(),
                fingerprint: input.fingerprint,
                causation_id: input.causation_id.clone(),
                gap_free: input.gap_free,
            });
    }

    pub(super) fn classify_input(
        &self,
        cursor: &ProjectionInputCursor,
        fingerprint: ProjectionInputFingerprint,
        message_id: &str,
        causation_id: &str,
        generation: ProjectionGeneration,
        gap_free: bool,
    ) -> Result<InputDisposition, ProjectionProtocolError> {
        self.validate_gap_free(cursor, gap_free)?;
        let candidate = TrustedProjectionInput {
            cursor: cursor.clone(),
            fingerprint,
            message_id: message_id.to_string(),
            causation_id: causation_id.to_string(),
            generation,
            gap_free,
        };
        self.validate_input_identity(&candidate)?;
        if let Some(receipt) = self
            .applied_receipts
            .get(&CursorReceiptKey::new(cursor, generation))
        {
            if receipt.fingerprint == fingerprint
                && receipt.message_id == message_id
                && receipt.causation_id == causation_id
                && receipt.gap_free == gap_free
            {
                return Ok(InputDisposition::Duplicate(receipt.checkpoint.clone()));
            }
            return Err(ProjectionProtocolError::InputCorruption);
        }
        let input_key = InputKey::new(cursor, generation);
        let Some(previous) = self.inputs.get(&input_key) else {
            self.reject_reused_message(cursor, fingerprint, message_id, causation_id, gap_free)?;
            return Ok(InputDisposition::New);
        };

        match cursor.compare_position(&previous.cursor) {
            RevisionComparison::Equal
                if fingerprint != previous.fingerprint
                    || message_id != previous.message_id
                    || causation_id != previous.causation_id
                    || gap_free != previous.gap_free =>
            {
                Err(ProjectionProtocolError::InputCorruption)
            }
            RevisionComparison::Equal => {
                Ok(InputDisposition::Duplicate(previous.checkpoint.clone()))
            }
            RevisionComparison::Older => {
                self.reject_reused_message(
                    cursor,
                    fingerprint,
                    message_id,
                    causation_id,
                    gap_free,
                )?;
                Ok(InputDisposition::Stale(previous.checkpoint.clone()))
            }
            RevisionComparison::Incomparable => Err(ProjectionProtocolError::IncomparableInput),
            RevisionComparison::Newer => {
                self.reject_reused_message(
                    cursor,
                    fingerprint,
                    message_id,
                    causation_id,
                    gap_free,
                )?;
                if gap_free
                    && cursor.position()
                        != checked_next(previous.cursor.position(), "gap-free projection input")?
                {
                    return Err(ProjectionProtocolError::IncomparableInput);
                }
                Ok(InputDisposition::New)
            }
        }
    }

    pub(super) fn reject_reused_message(
        &self,
        cursor: &ProjectionInputCursor,
        fingerprint: ProjectionInputFingerprint,
        message_id: &str,
        causation_id: &str,
        gap_free: bool,
    ) -> Result<(), ProjectionProtocolError> {
        let key = MessageKey {
            topology: cursor.topology().clone(),
            message_id: message_id.to_string(),
        };
        if let Some(previous) = self.messages.get(&key) {
            if previous.cursor != *cursor
                || previous.fingerprint != fingerprint
                || previous.causation_id != causation_id
                || previous.gap_free != gap_free
            {
                return Err(ProjectionProtocolError::MessageIdReuse {
                    message_id: message_id.to_string(),
                });
            }
        }
        Ok(())
    }

    pub(super) fn validate_gap_free(
        &self,
        cursor: &ProjectionInputCursor,
        gap_free: bool,
    ) -> Result<(), ProjectionProtocolError> {
        if self
            .gap_free_capabilities
            .get(&SourceCapabilityKey::new(cursor))
            .is_some_and(|registered| *registered != gap_free)
        {
            return Err(ProjectionProtocolError::InputCorruption);
        }
        Ok(())
    }

    pub(super) fn has_exact_message_identity(&self, input: &TrustedProjectionInput) -> bool {
        self.messages
            .get(&MessageKey {
                topology: input.cursor.topology().clone(),
                message_id: input.message_id.clone(),
            })
            .is_some_and(|identity| {
                identity.cursor == input.cursor
                    && identity.fingerprint == input.fingerprint
                    && identity.causation_id == input.causation_id
                    && identity.gap_free == input.gap_free
            })
    }

    pub(super) fn register_ownership(
        &mut self,
        partition: &PartitionKey,
        batch: &ProjectionCommitBatch,
    ) -> Result<(), ProjectionProtocolError> {
        for declaration in &batch.ownership {
            let registered_key = RegisteredModelKey {
                topology: partition.topology.clone(),
                model: declaration.model.clone(),
            };
            match self.registered_models.get(&registered_key) {
                Some(table) if table == &declaration.table => {}
                Some(table) => {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection model `{}` was bootstrapped for table `{table}`, not `{}`",
                        declaration.model, declaration.table
                    )));
                }
                None => {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection model `{}` was not registered before projector traffic",
                        declaration.model
                    )));
                }
            }
            if self.authoritative_table_owners.get(&declaration.table) != Some(&registered_key) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection table `{}` does not have model `{}` in this topology as its global authoritative owner",
                    declaration.table, declaration.model
                )));
            }
        }

        crate::projection_protocol::validate_ownership_batch(&batch.ownership)?;

        for declaration in &batch.ownership {
            let key = OwnershipKey {
                partition: partition.clone(),
                model: declaration.model.clone(),
            };
            if let Some(previous) = self.ownership.get(&key) {
                if previous != &declaration.table {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection model `{}` is already bound to table `{previous}`",
                        declaration.model
                    )));
                }
            }
            if let Some((other, _)) = self.ownership.iter().find(|(other, table)| {
                other.partition == *partition
                    && *table == &declaration.table
                    && other.model != declaration.model
            }) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection table `{}` is already bound to model `{}`",
                    declaration.table, other.model
                )));
            }
            self.ownership.insert(key, declaration.table.clone());
        }

        for mutation in &batch.mutations {
            let table = self.owned_table(partition, &mutation.scope)?;
            if table != mutation.mutation.table_name() {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection model `{}` owns table `{table}`, but its mutation targets `{}`",
                    mutation.scope.model(),
                    mutation.mutation.table_name()
                )));
            }
            if table_model_name(&mutation.mutation) != mutation.scope.model() {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection record model `{}` does not match table schema model `{}`",
                    mutation.scope.model(),
                    table_model_name(&mutation.mutation)
                )));
            }
        }
        for observation in &batch.observations {
            self.owned_table(partition, observation.scope())?;
        }
        Ok(())
    }

    pub(super) fn register_same_transaction_ownership(
        &mut self,
        partition: &PartitionKey,
        batch: &SameTransactionProjectionBatch,
    ) -> Result<(), ProjectionProtocolError> {
        let declaration = batch
            .ownership
            .first()
            .expect("direct projection batch validation requires one owner");
        let registered_key = RegisteredModelKey {
            topology: partition.topology.clone(),
            model: declaration.model.clone(),
        };
        match self.registered_models.get(&registered_key) {
            Some(table) if table == &declaration.table => {}
            Some(table) => {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection model `{}` was bootstrapped for table `{table}`, not `{}`",
                    declaration.model, declaration.table
                )));
            }
            None => {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection model `{}` was not registered before direct projection traffic",
                    declaration.model
                )));
            }
        }
        if self.authoritative_table_owners.get(&declaration.table) != Some(&registered_key) {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection table `{}` does not have model `{}` in this topology as its global authoritative owner",
                declaration.table, declaration.model
            )));
        }

        let ownership_key = OwnershipKey {
            partition: partition.clone(),
            model: declaration.model.clone(),
        };
        if let Some(previous) = self.ownership.get(&ownership_key) {
            if previous != &declaration.table {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection model `{}` is already bound to table `{previous}`",
                    declaration.model
                )));
            }
        }
        if let Some((other, _)) = self.ownership.iter().find(|(other, table)| {
            other.partition == *partition
                && *table == &declaration.table
                && other.model != declaration.model
        }) {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection table `{}` is already bound to model `{}`",
                declaration.table, other.model
            )));
        }
        self.ownership
            .insert(ownership_key, declaration.table.clone());

        let mutation = batch
            .mutations
            .first()
            .expect("direct projection batch validation requires one mutation");
        let table = self.owned_table(partition, &mutation.scope)?;
        if table != mutation.mutation.table_name()
            || table_model_name(&mutation.mutation) != mutation.scope.model()
        {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "direct projection record model `{}` does not own staged table `{}`",
                mutation.scope.model(),
                mutation.mutation.table_name()
            )));
        }
        Ok(())
    }

    pub(super) fn owned_table<'a>(
        &'a self,
        partition: &PartitionKey,
        scope: &ProjectionRecordScope,
    ) -> Result<&'a str, ProjectionProtocolError> {
        self.ownership
            .get(&OwnershipKey {
                partition: partition.clone(),
                model: scope.model().to_string(),
            })
            .map(String::as_str)
            .ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "projection model `{}` has no causal ownership declaration",
                    scope.model()
                ))
            })
    }

    pub(super) fn next_record(
        &self,
        scope: &ProjectionRecordScope,
        expectation: &ProjectionRecordExpectation,
        kind: ProjectionMutationKind,
        source_snapshot: bool,
    ) -> Result<(RecordRevision, bool), ProjectionProtocolError> {
        let current = self.records.get(scope);
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

    pub(super) fn validate_physical_record(
        &self,
        scope: &ProjectionRecordScope,
        expectation: &ProjectionRecordExpectation,
        kind: ProjectionMutationKind,
        row_exists: bool,
    ) -> Result<(), ProjectionProtocolError> {
        let should_exist = match (expectation, kind) {
            (
                ProjectionRecordExpectation::Missing,
                ProjectionMutationKind::Upsert | ProjectionMutationKind::Delete,
            ) => false,
            (ProjectionRecordExpectation::Exact(_), ProjectionMutationKind::Recreate) => false,
            (ProjectionRecordExpectation::Exact(_), ProjectionMutationKind::Delete) => !self
                .records
                .get(scope)
                .is_some_and(|record| record.tombstone),
            (ProjectionRecordExpectation::Exact(_), ProjectionMutationKind::Upsert) => true,
            _ => {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "projection mutation has no valid physical-row expectation".into(),
                ));
            }
        };
        if row_exists == should_exist {
            return Ok(());
        }
        let expected = if should_exist { "present" } else { "absent" };
        let actual = if row_exists { "present" } else { "absent" };
        Err(ProjectionProtocolError::InvalidBatch(format!(
            "projection record `{}` metadata requires its physical row to be {expected}, but it is {actual}",
            scope.model()
        )))
    }

    pub(super) fn append_change(
        &mut self,
        partition_key: &PartitionKey,
        pending: PendingChange,
    ) -> Result<ProjectionChange, ProjectionProtocolError> {
        let partition = self
            .partitions
            .get_mut(partition_key)
            .expect("partition is initialized before appending changes");
        let position = partition.change_head.checked_add(1).ok_or(
            ProjectionProtocolError::PositionOverflow {
                domain: "projection change",
            },
        )?;
        let cursor = ProjectionChangeCursor::new(
            partition_key.topology.clone(),
            partition_key.partition.clone(),
            partition.change_epoch.clone(),
            position,
        )?;
        let change = ProjectionChange {
            cursor,
            kind: pending.kind,
            causation_id: pending.causation_id,
            observation_kind: pending.observation_kind,
            scope: pending.scope,
            revision: pending.revision,
            failure_id: pending.failure_id,
        };
        partition.change_head = position;
        partition.changes.insert(position, change.clone());
        Ok(change)
    }

    pub(super) fn compact_changes_through(
        &mut self,
        partition_key: &PartitionKey,
        through: u64,
    ) -> Result<u64, ProjectionProtocolError> {
        let partition = self.partitions.get_mut(partition_key).ok_or_else(|| {
            ProjectionProtocolError::InvalidBatch(
                "cannot compact an unknown projection partition".into(),
            )
        })?;
        if through > partition.change_head {
            return Err(ProjectionProtocolError::InvalidBatch(
                "cannot compact beyond the projection change head".into(),
            ));
        }
        if through <= partition.compacted_through {
            return Ok(partition.compacted_through);
        }
        let expected_removed = through - partition.compacted_through;
        let actual_removed = u64::try_from(
            partition
                .changes
                .range((
                    std::ops::Bound::Excluded(partition.compacted_through),
                    std::ops::Bound::Included(through),
                ))
                .count(),
        )
        .map_err(|_| {
            ProjectionProtocolError::InvalidBatch(
                "projection change compaction count exceeds u64".into(),
            )
        })?;
        if actual_removed != expected_removed {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection change compaction expected to remove {expected_removed} contiguous entries but found {actual_removed}"
            )));
        }
        partition.changes.retain(|position, _| *position > through);
        partition.compacted_through = through;
        Ok(through)
    }

    pub(super) fn retain_change_suffix(
        &mut self,
        partition_key: &PartitionKey,
        retention: ProjectionChangeRetention,
    ) -> Result<u64, ProjectionProtocolError> {
        let head = self
            .partitions
            .get(partition_key)
            .ok_or_else(|| {
                ProjectionProtocolError::InvalidBatch(
                    "cannot retain changes for an unknown projection partition".into(),
                )
            })?
            .change_head;
        self.compact_changes_through(
            partition_key,
            head.saturating_sub(retention.max_retained_changes()),
        )
    }
}
