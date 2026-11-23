use super::*;

/// A live record's key may belong to one partition only. `staged` holds the
/// records a write is about to store over `stored`.
pub(super) fn ensure_live_record_identity_available(
    stored: &HashMap<ProjectionRecordScope, ProjectionRecordMetadata>,
    staged: &HashMap<ProjectionRecordScope, ProjectionRecordMetadata>,
    metadata: &ProjectionRecordMetadata,
) -> Result<(), ProjectionProtocolError> {
    if metadata.tombstone {
        return Ok(());
    }
    let candidate = metadata.revision.scope();
    let records = staged.iter().chain(
        stored
            .iter()
            .filter(|(scope, _)| !staged.contains_key(*scope)),
    );
    for (scope, stored) in records {
        if stored.tombstone
            || scope == candidate
            || scope.topology() != candidate.topology()
            || scope.model() != candidate.model()
            || scope.key_digest() != candidate.key_digest()
        {
            continue;
        }
        if scope.canonical_key_bytes() != candidate.canonical_key_bytes() {
            return Err(corrupt_storage(format!(
                "projection live-record key digest collision for model `{}`",
                candidate.model()
            )));
        }
        return Err(corrupt_storage(format!(
            "projection live record for model `{}` is already owned by another partition",
            candidate.model()
        )));
    }
    Ok(())
}

/// The compaction point that keeps `retention` once a partition's log has
/// grown to `head`, proven against the stored log. Changes above the stored
/// head are the caller's own contiguous appends and need no proof.
pub(super) fn retained_through(
    partition: Option<&PartitionState>,
    head: u64,
    retention: ProjectionChangeRetention,
) -> Result<u64, ProjectionProtocolError> {
    let through = head.saturating_sub(retention.max_retained_changes());
    if let Some(partition) = partition {
        partition.check_compaction(through.min(partition.change_head))?;
    }
    Ok(through)
}

/// The change that follows `head` in a partition's log. Advances `head`;
/// storing the change is the caller's.
pub(super) fn next_change(
    partition_key: &PartitionKey,
    change_epoch: &ProjectionEpoch,
    head: &mut u64,
    pending: PendingChange,
) -> Result<ProjectionChange, ProjectionProtocolError> {
    let position = head
        .checked_add(1)
        .ok_or(ProjectionProtocolError::PositionOverflow {
            domain: "projection change",
        })?;
    let cursor = ProjectionChangeCursor::new(
        partition_key.topology.clone(),
        partition_key.partition.clone(),
        change_epoch.clone(),
        position,
    )?;
    *head = position;
    Ok(ProjectionChange {
        cursor,
        kind: pending.kind,
        causation_id: pending.causation_id,
        observation_kind: pending.observation_kind,
        scope: pending.scope,
        revision: pending.revision,
        failure_id: pending.failure_id,
        program_id: pending.program_id,
    })
}

impl InMemoryProjectionProtocolState {
    pub(super) fn ensure_live_record_identity_available(
        &self,
        metadata: &ProjectionRecordMetadata,
    ) -> Result<(), ProjectionProtocolError> {
        ensure_live_record_identity_available(&self.records, &HashMap::new(), metadata)
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
        if let Some(partition) = self.partitions.get(key) {
            ensure_change_epoch(&partition.change_epoch, change_epoch)?;
        }
        self.partition_mut(key, change_epoch);
        Ok(())
    }

    /// The partition's state, created at the initial generation on first use.
    pub(super) fn partition_mut(
        &mut self,
        key: &PartitionKey,
        change_epoch: &ProjectionEpoch,
    ) -> &mut PartitionState {
        if !self.partitions.contains_key(key) {
            self.generations.insert(
                GenerationKey {
                    partition: key.clone(),
                    generation: ProjectionGeneration::initial(),
                },
                GenerationLineage::initial(),
            );
        }
        self.partitions
            .entry(key.clone())
            .or_insert_with(|| PartitionState::new(change_epoch.clone()))
    }

    /// Check an input's durable identities and partition fences, then
    /// classify it. `change_epoch` is absent for a read that stages nothing.
    pub(super) fn input_disposition(
        &self,
        input: &TrustedProjectionInput,
        change_epoch: Option<&ProjectionEpoch>,
    ) -> Result<ProjectionInputDisposition, ProjectionProtocolError> {
        self.require_registered_topology(input.cursor.topology())?;
        let key = PartitionKey::from_input(&input.cursor);
        let partition = self.partitions.get(&key);
        if let (Some(partition), Some(change_epoch)) = (partition, change_epoch) {
            ensure_change_epoch(&partition.change_epoch, change_epoch)?;
        }
        self.validate_input_identity(input)?;
        ensure_active_input(
            input,
            self.active_generation(&key),
            partition.and_then(|partition| partition.stopped_failure_id.as_deref()),
        )?;
        self.classify_input(input)
    }

    pub(super) fn active_generation(&self, key: &PartitionKey) -> ProjectionGeneration {
        self.partitions
            .get(key)
            .map_or(ProjectionGeneration::initial(), |partition| {
                partition.active_generation
            })
    }

    pub(super) fn validate_input_identity(
        &self,
        input: &TrustedProjectionInput,
    ) -> Result<(), ProjectionProtocolError> {
        validate_input_identity(
            input,
            self.gap_free_capabilities
                .get(&SourceCapabilityKey::new(&input.cursor))
                .copied(),
            self.input_identities
                .get(&CursorIdentityKey::new(&input.cursor)),
            self.messages.get(&MessageKey {
                topology: input.cursor.topology().clone(),
                message_id: input.message_id.clone(),
            }),
        )
    }

    /// Classify an input that passed its identity and partition fences. An
    /// unapplied input must also be the one a pending repair retries.
    pub(super) fn classify_input(
        &self,
        input: &TrustedProjectionInput,
    ) -> Result<ProjectionInputDisposition, ProjectionProtocolError> {
        let key = PartitionKey::from_input(&input.cursor);
        if self.partitions.contains_key(&key)
            && !self.generations.contains_key(&GenerationKey {
                partition: key.clone(),
                generation: input.generation,
            })
        {
            return Err(corrupt_storage(format!(
                "active projection generation {} has no durable lineage",
                input.generation.get()
            )));
        }
        let disposition = classify_input(
            input,
            self.applied_receipts
                .get(&CursorReceiptKey::new(&input.cursor, input.generation))
                .map(|(input, change)| (input, change)),
            self.applied_receipts.iter().any(|(receipt, (applied, _))| {
                receipt.generation == input.generation
                    && receipt.partition == key
                    && applied.message_id == input.message_id
            }),
            self.inputs
                .get(&InputKey::new(&input.cursor, input.generation))
                .map(|(input, change)| (input, change)),
        )?;
        let pending_retry = self
            .partitions
            .get(&key)
            .and_then(|partition| partition.pending_retry_failure_id.as_ref());
        if let (
            ProjectionInputDisposition::Pending | ProjectionInputDisposition::Redelivery,
            Some(failure_id),
        ) = (&disposition, pending_retry)
        {
            let failure = self.failures.get(failure_id).ok_or_else(|| {
                corrupt_storage(format!(
                    "pending projection retry failure `{failure_id}` is missing"
                ))
            })?;
            validate_pending_retry(input, self.active_generation(&key), failure)?;
        }
        Ok(disposition)
    }

    pub(super) fn persist_input_identity(&mut self, input: &TrustedProjectionInput) {
        self.input_identities
            .entry(CursorIdentityKey::new(&input.cursor))
            .or_insert_with(|| input.clone());
        self.messages
            .entry(MessageKey {
                topology: input.cursor.topology().clone(),
                message_id: input.message_id.clone(),
            })
            .or_insert_with(|| input.clone());
    }

    /// Validate a commit's ownership declarations and targets. The caller
    /// stores the declarations once the whole commit is known to apply.
    pub(super) fn check_ownership(
        &self,
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
        }
        let owned_table = |scope: &ProjectionRecordScope| match batch
            .ownership
            .iter()
            .find(|declaration| declaration.model == scope.model())
        {
            Some(declaration) => Ok(declaration.table.as_str()),
            None => self.owned_table(partition, scope),
        };

        for mutation in &batch.mutations {
            let table = owned_table(&mutation.scope)?;
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
            owned_table(observation.scope())?;
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

    pub(super) fn append_change(
        &mut self,
        partition_key: &PartitionKey,
        pending: PendingChange,
    ) -> Result<ProjectionChange, ProjectionProtocolError> {
        let partition = self
            .partitions
            .get_mut(partition_key)
            .expect("partition is initialized before appending changes");
        let mut head = partition.change_head;
        let change = next_change(partition_key, &partition.change_epoch, &mut head, pending)?;
        partition.append([change.clone()]);
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
        partition.check_compaction(through)?;
        partition.compact(through);
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

impl PartitionState {
    /// Store changes that continue the log at `change_head + 1`.
    pub(super) fn append(&mut self, changes: impl IntoIterator<Item = ProjectionChange>) {
        for change in changes {
            self.change_head = change.cursor.position();
            self.changes.insert(self.change_head, change);
        }
    }

    /// Prove the stored log holds every change a compaction through
    /// `through` (at most the head) would remove.
    pub(super) fn check_compaction(&self, through: u64) -> Result<(), ProjectionProtocolError> {
        if through <= self.compacted_through {
            return Ok(());
        }
        let expected_removed = through - self.compacted_through;
        let actual_removed = self
            .changes
            .range((
                std::ops::Bound::Excluded(self.compacted_through),
                std::ops::Bound::Included(through),
            ))
            .count() as u64;
        if actual_removed != expected_removed {
            return Err(corrupt_storage(format!(
                "projection change compaction expected to remove {expected_removed} contiguous entries but found {actual_removed}"
            )));
        }
        Ok(())
    }

    /// Drop every change at or below `through`. Never moves backwards.
    pub(super) fn compact(&mut self, through: u64) {
        if through > self.compacted_through {
            self.changes.retain(|position, _| *position > through);
            self.compacted_through = through;
        }
    }
}
