use super::*;

/// One closed asynchronous projector transaction.
#[derive(Debug)]
pub(crate) struct ProjectionCommitBatch {
    pub(crate) input: TrustedProjectionInput,
    pub(crate) change_epoch: ProjectionEpoch,
    pub(crate) ownership: Vec<ProjectionModelOwnership>,
    pub(crate) mutations: Vec<ProjectionRecordMutation>,
    pub(crate) observations: Vec<ProjectionObservationRequest>,
}

impl ProjectionCommitBatch {
    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        let topology = self.input.cursor.topology();
        let partition = self.input.cursor.projection_partition();
        self.input.inbox_receipt().validate()?;

        let mut owned_models = std::collections::HashMap::new();
        let mut owned_tables = std::collections::HashSet::new();
        for ownership in &self.ownership {
            if owned_models
                .insert(ownership.model.as_str(), ownership.table.as_str())
                .is_some()
                || !owned_tables.insert(ownership.table.as_str())
            {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection ownership repeats model `{}` or table `{}`",
                    ownership.model, ownership.table
                )));
            }
        }

        let mut scopes = std::collections::HashSet::new();
        for mutation in &self.mutations {
            validate_scope(topology, partition, &mutation.scope)?;
            if mutation.kind == ProjectionMutationKind::Delete
                && matches!(mutation.expectation, ProjectionRecordExpectation::Missing)
                && mutation.source_snapshot.is_none()
            {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "delete of an unseen row requires an authoritative source snapshot".into(),
                ));
            }
            let schema = match &mutation.mutation {
                TableMutation::UpsertRow(mutation) => mutation.schema,
                TableMutation::PatchRow(mutation) => mutation.schema,
                TableMutation::DeleteRow(mutation) => mutation.schema,
            };
            if mutation.scope.model() != schema.model_name
                || owned_models.get(mutation.scope.model()).copied()
                    != Some(schema.table_name.as_str())
            {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection record scope `{}` is not registered to table `{}`",
                    mutation.scope.model(),
                    schema.table_name
                )));
            }
            if !scopes.insert(mutation.scope.clone()) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection batch repeats model `{}` record scope",
                    mutation.scope.model()
                )));
            }
        }
        TableWritePlan::new(
            self.mutations
                .iter()
                .map(|mutation| mutation.mutation.clone())
                .collect(),
        )
        .validate()?;

        let mut observations = std::collections::HashSet::new();
        for observation in &self.observations {
            validate_scope(topology, partition, observation.scope())?;
            if !owned_models.contains_key(observation.scope().model()) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection observation scope `{}` is not registered",
                    observation.scope().model()
                )));
            }
            if !observations.insert((observation.kind, observation.scope().clone())) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection batch repeats {:?} observation for model `{}`",
                    observation.kind,
                    observation.scope().model()
                )));
            }
            match (&observation.kind, &observation.target) {
                (
                    ProjectionObservationKind::Record,
                    ProjectionObservationTarget::StagedRecord(scope),
                ) if !scopes.contains(scope) => {
                    return Err(ProjectionProtocolError::InvalidBatch(format!(
                        "projection observation for model `{}` references an unstaged record",
                        scope.model()
                    )));
                }
                (
                    ProjectionObservationKind::Record,
                    ProjectionObservationTarget::StagedRecord(_)
                    | ProjectionObservationTarget::ExistingRecord(_),
                )
                | (
                    ProjectionObservationKind::Dependency,
                    ProjectionObservationTarget::Dependency(_),
                ) => {}
                _ => {
                    return Err(ProjectionProtocolError::InvalidBatch(
                        "projection observation kind and target disagree".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// One exact row mutation sealed out of a same-transaction command workspace.
///
/// Direct command projection deliberately has no caller-supplied record
/// expectation. The adapter must inspect the authoritative record metadata
/// while holding the registered projector partition lock: a missing row and
/// missing metadata creates revision one, a live row advances its revision,
/// and a tombstone or row/metadata disagreement fails closed.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SameTransactionProjectionMutation {
    pub(crate) scope: ProjectionRecordScope,
    pub(crate) mutation: TableMutation,
}

/// A sealed direct projection participant in one ledger-fenced domain commit.
///
/// This is intentionally a different type from [`ProjectionCommitBatch`].
/// Same-transaction projection has no source cursor, input fingerprint,
/// consumer inbox receipt, repair generation, or async checkpoint.
#[derive(Debug)]
pub(crate) struct SameTransactionProjectionBatch {
    pub(crate) topology: ProjectorTopologyId,
    pub(crate) partition: ProjectionPartition,
    pub(crate) change_epoch: ProjectionEpoch,
    pub(crate) ownership: Vec<ProjectionModelOwnership>,
    pub(crate) causation_id: String,
    pub(crate) mutations: Vec<SameTransactionProjectionMutation>,
    pub(crate) observations: Vec<ProjectionObservationRequest>,
}

impl SameTransactionProjectionBatch {
    pub(crate) fn single_upsert(
        topology: ProjectorTopologyId,
        partition: ProjectionPartition,
        change_epoch: ProjectionEpoch,
        ownership: ProjectionModelOwnership,
        scope: ProjectionRecordScope,
        mutation: TableMutation,
        causation_id: impl Into<String>,
    ) -> Result<Self, ProjectionProtocolError> {
        let observations = vec![ProjectionObservationRequest {
            kind: ProjectionObservationKind::Record,
            target: ProjectionObservationTarget::StagedRecord(scope.clone()),
        }];
        let batch = Self {
            topology,
            partition,
            change_epoch,
            ownership: vec![ownership],
            causation_id: bounded_opaque(
                "projection causation ID",
                causation_id,
                MAX_CAUSATION_ID_BYTES,
            )?,
            mutations: vec![SameTransactionProjectionMutation { scope, mutation }],
            observations,
        };
        batch.validate()?;
        Ok(batch)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        if self.mutations.len() != 1 || self.observations.len() != 1 {
            return Err(ProjectionProtocolError::InvalidBatch(
                "a direct projected command must contain exactly one row upsert and one exact record observation"
                    .into(),
            ));
        }
        if self.ownership.len() != 1 {
            return Err(ProjectionProtocolError::InvalidBatch(
                "a direct projected command must declare exactly one output model owner".into(),
            ));
        }
        bounded_opaque(
            "projection causation ID",
            self.causation_id.clone(),
            MAX_CAUSATION_ID_BYTES,
        )?;

        let ownership = &self.ownership[0];
        let staged = &self.mutations[0];
        validate_scope(&self.topology, &self.partition, &staged.scope)?;
        let TableMutation::UpsertRow(row) = &staged.mutation else {
            return Err(ProjectionProtocolError::InvalidBatch(
                "a direct projected command must seal one full-row upsert".into(),
            ));
        };
        if row.mode != crate::table::RowWriteMode::Upsert
            || row.expected_version != crate::table::ExpectedVersion::Any
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "a direct projected command requires an unfenced full-row upsert; the projection protocol owns its revision fence"
                    .into(),
            ));
        }
        if staged.scope.model() != row.schema.model_name
            || ownership.model != row.schema.model_name
            || ownership.table != row.schema.table_name
        {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "direct projection scope/ownership does not match model `{}` table `{}`",
                row.schema.model_name, row.schema.table_name
            )));
        }
        TableWritePlan::new(vec![staged.mutation.clone()]).validate()?;

        let observation = &self.observations[0];
        match (&observation.kind, &observation.target) {
            (
                ProjectionObservationKind::Record,
                ProjectionObservationTarget::StagedRecord(scope),
            ) if scope == &staged.scope => Ok(()),
            _ => Err(ProjectionProtocolError::InvalidBatch(
                "a direct projected command must observe the exact staged record scope".into(),
            )),
        }
    }
}

/// Closed terminal-failure transaction for one exact input/generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionFailureBatch {
    pub(crate) input: TrustedProjectionInput,
    pub(crate) change_epoch: ProjectionEpoch,
    pub(crate) failure_id: String,
    pub(crate) failure_code: String,
    pub(crate) failure_bytes: Vec<u8>,
    pub(crate) failure_digest: [u8; 32],
}

impl ProjectionFailureBatch {
    /// Recompute the protocol fingerprint for persisted failure bytes.
    ///
    /// Storage adapters use this while decoding rows so corrupt bytes cannot
    /// be returned with an otherwise well-formed stored digest.
    pub(crate) fn fingerprint_bytes(failure_bytes: &[u8]) -> [u8; 32] {
        domain_separated_digest(FAILURE_FINGERPRINT_DOMAIN, failure_bytes)
    }

    pub(crate) fn new(
        input: TrustedProjectionInput,
        change_epoch: ProjectionEpoch,
        failure_id: impl Into<String>,
        failure_code: impl Into<String>,
        failure_bytes: impl Into<Vec<u8>>,
    ) -> Result<Self, ProjectionProtocolError> {
        let failure_bytes = failure_bytes.into();
        let failure_digest = Self::fingerprint_bytes(&failure_bytes);
        let batch = Self {
            input,
            change_epoch,
            failure_id: failure_id.into(),
            failure_code: failure_code.into(),
            failure_bytes,
            failure_digest,
        };
        batch.validate()?;
        Ok(batch)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        bounded_opaque(
            "projection failure ID",
            self.failure_id.clone(),
            MAX_FAILURE_ID_BYTES,
        )?;
        bounded_name(
            "projection failure code",
            self.failure_code.clone(),
            MAX_FAILURE_CODE_BYTES,
        )?;
        if self.failure_bytes.is_empty() || self.failure_bytes.len() > MAX_FAILURE_DETAIL_BYTES {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection failure details must contain 1..={MAX_FAILURE_DETAIL_BYTES} bytes"
            )));
        }
        if Self::fingerprint_bytes(&self.failure_bytes) != self.failure_digest {
            return Err(ProjectionProtocolError::InvalidBatch(
                "projection failure digest does not match its exact bytes".into(),
            ));
        }
        bounded_opaque(
            "projection message ID",
            self.input.message_id.clone(),
            MAX_MESSAGE_ID_BYTES,
        )?;
        bounded_opaque(
            "projection causation ID",
            self.input.causation_id.clone(),
            MAX_CAUSATION_ID_BYTES,
        )?;
        self.input.inbox_receipt().validate()?;
        Ok(())
    }
}
