use super::*;

/// Durable record metadata returned by projection stores and query snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionRecordMetadata {
    pub revision: RecordRevision,
    pub tombstone: bool,
    pub change: ProjectionChangeCursor,
    /// Authoritative source fence for explicitly declared snapshot projections.
    pub source_snapshot: Option<super::super::SourceSnapshotVersion>,
}

/// One exact input-source checkpoint requested alongside a physical query row.
///
/// The topology and partition are retained in every probe so a caller cannot
/// accidentally combine source progress from another projector scope. The
/// enclosing snapshot request validates those values before any adapter read.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProjectionCheckpointProbe {
    pub(crate) topology: ProjectorTopologyId,
    pub(crate) partition: ProjectionPartition,
    pub(crate) source: ProjectionSource,
    pub(crate) epoch: ProjectionEpoch,
    pub(crate) generation: ProjectionGeneration,
}

impl ProjectionCheckpointProbe {
    pub(crate) fn new(
        topology: ProjectorTopologyId,
        partition: ProjectionPartition,
        source: ProjectionSource,
        epoch: ProjectionEpoch,
        generation: ProjectionGeneration,
    ) -> Self {
        Self {
            topology,
            partition,
            source,
            epoch,
            generation,
        }
    }
}

/// Sealed adapter-neutral request for one causally versioned physical row.
///
/// Construction derives `scope` from the topology's registered key codec.
/// Consequently the SQL/in-memory row key and projection metadata key cannot be
/// supplied independently. Task 16 can add its wire envelope around this
/// internal primitive without gaining authority to forge causal evidence.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionQuerySnapshotRequest {
    pub(crate) schema: Arc<TableSchema>,
    pub(crate) key: RowKey,
    pub(crate) scope: ProjectionRecordScope,
    pub(crate) checkpoint_probes: Vec<ProjectionCheckpointProbe>,
}

impl ProjectionQuerySnapshotRequest {
    pub(crate) fn new(
        codec: &ProjectionScopeCodec,
        partition: Option<&serde_json::Value>,
        model: &str,
        key: RowKey,
        checkpoint_probes: Vec<ProjectionCheckpointProbe>,
    ) -> Result<Self, ProjectionProtocolError> {
        let schema = codec.registered_schema_owned(model).map_err(|error| {
            ProjectionProtocolError::InvalidBatch(format!(
                "invalid projection query snapshot model: {error}"
            ))
        })?;
        let partition = codec.encode_partition(partition).map_err(|error| {
            ProjectionProtocolError::InvalidBatch(format!(
                "invalid projection query snapshot partition: {error}"
            ))
        })?;
        let scope = codec
            .encode_row_scope_in_partition(model, partition, &key)
            .map_err(|error| {
                ProjectionProtocolError::InvalidBatch(format!(
                    "invalid projection query snapshot key: {error}"
                ))
            })?;
        let request = Self {
            schema,
            key,
            scope,
            checkpoint_probes,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        if self.checkpoint_probes.len() > MAX_PROJECTION_QUERY_CHECKPOINT_PROBES {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection query snapshot has {} checkpoint probes; maximum is {}",
                self.checkpoint_probes.len(),
                MAX_PROJECTION_QUERY_CHECKPOINT_PROBES
            )));
        }
        if self.scope.model() != self.schema.model_name {
            return Err(ProjectionProtocolError::ScopeMismatch {
                field: "projection query model",
            });
        }
        crate::table::validate_key(&self.schema, &self.key)?;

        let mut probes = std::collections::HashSet::new();
        for probe in &self.checkpoint_probes {
            if &probe.topology != self.scope.topology() {
                return Err(ProjectionProtocolError::ScopeMismatch {
                    field: "projection query checkpoint topology",
                });
            }
            if &probe.partition != self.scope.projection_partition() {
                return Err(ProjectionProtocolError::ScopeMismatch {
                    field: "projection query checkpoint partition",
                });
            }
            if !probes.insert((probe.source.clone(), probe.generation)) {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "projection query snapshot repeats one source/generation checkpoint probe"
                        .into(),
                ));
            }
        }
        Ok(())
    }
}

/// Result for one explicit checkpoint probe in a query snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionCheckpointSnapshot {
    pub(crate) probe: ProjectionCheckpointProbe,
    pub(crate) checkpoint: Option<ProjectionCheckpoint>,
}

/// Physical row and every causal fence needed to consume it safely.
///
/// All fields come from one adapter snapshot. In particular, `change_head` is
/// the live-resume boundary for this partition and must never be fetched after
/// the row/revision in a second independent read.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionQuerySnapshot {
    pub(crate) row: Option<RowValues>,
    pub(crate) record: Option<ProjectionRecordMetadata>,
    pub(crate) checkpoints: Vec<ProjectionCheckpointSnapshot>,
    pub(crate) change_head: Option<ProjectionChangeCursor>,
    pub(crate) compacted_through: u64,
}

/// Sanitized live boundary for one exact projector partition, read inside the
/// same adapter snapshot as a GraphQL query when used by Task 16.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionPartitionSnapshot {
    pub(crate) head: Option<ProjectionChangeCursor>,
    pub(crate) compacted_through: u64,
}

/// A bounded set of row/evidence probes that must share one adapter snapshot.
///
/// This is the adapter-neutral convenience for known keys. Dynamic GraphQL
/// list/relationship plans use the SQL repeatable-snapshot session so their
/// physical membership query runs before these probes on the same connection.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionQuerySnapshotBatchRequest {
    pub(crate) requests: Vec<ProjectionQuerySnapshotRequest>,
}

impl ProjectionQuerySnapshotBatchRequest {
    pub(crate) fn new(
        requests: Vec<ProjectionQuerySnapshotRequest>,
    ) -> Result<Self, ProjectionProtocolError> {
        let request = Self { requests };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        if self.requests.len() > MAX_PROJECTION_QUERY_BATCH_ROWS {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection query snapshot batch has {} rows; maximum is {}",
                self.requests.len(),
                MAX_PROJECTION_QUERY_BATCH_ROWS
            )));
        }
        let checkpoint_probes = self.requests.iter().try_fold(0usize, |total, request| {
            total
                .checked_add(request.checkpoint_probes.len())
                .ok_or_else(|| {
                    ProjectionProtocolError::InvalidBatch(
                        "projection query snapshot batch checkpoint-probe count overflowed".into(),
                    )
                })
        })?;
        if checkpoint_probes > MAX_PROJECTION_QUERY_BATCH_CHECKPOINT_PROBES {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection query snapshot batch has {checkpoint_probes} aggregate checkpoint probes; maximum is {}",
                MAX_PROJECTION_QUERY_BATCH_CHECKPOINT_PROBES
            )));
        }
        for request in &self.requests {
            request.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ProjectionQuerySnapshotBatch {
    pub(crate) snapshots: Vec<ProjectionQuerySnapshot>,
}

/// One explicitly scoped physical-row/protocol snapshot.
///
/// Execution paths use this wrapper instead of trusting positional alignment
/// from an adapter batch. Missing rows still retain the requested scope, so a
/// store cannot accidentally answer one key with another key's absence.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionScopedRowSnapshot {
    pub(crate) scope: ProjectionRecordScope,
    pub(crate) row: Option<RowValues>,
    pub(crate) record: Option<ProjectionRecordMetadata>,
}

/// One bounded, duplicate-free execution preflight.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionExecutionSnapshotBatchRequest {
    pub(crate) requests: Vec<ProjectionQuerySnapshotRequest>,
}

impl ProjectionExecutionSnapshotBatchRequest {
    pub(crate) fn new(
        requests: Vec<ProjectionQuerySnapshotRequest>,
    ) -> Result<Self, ProjectionProtocolError> {
        let request = Self { requests };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        if self.requests.len() > MAX_PROJECTION_QUERY_BATCH_ROWS {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection execution snapshot batch has {} scopes; maximum is {}",
                self.requests.len(),
                MAX_PROJECTION_QUERY_BATCH_ROWS
            )));
        }
        let mut scopes = std::collections::HashSet::new();
        for request in &self.requests {
            request.validate()?;
            if !scopes.insert(request.scope.clone()) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection execution snapshot batch repeats model `{}` record scope",
                    request.scope.model()
                )));
            }
        }
        Ok(())
    }
}

/// Explicitly scoped results for one execution preflight.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ProjectionExecutionSnapshotBatch {
    pub(crate) snapshots: Vec<ProjectionScopedRowSnapshot>,
}

/// One validated relationship include in a coherent graph query.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionGraphIncludeRequest {
    pub(crate) relationship: crate::table::RelationshipDef,
    pub(crate) target_schema: std::sync::Arc<TableSchema>,
}

/// One coherent root/include graph query.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionGraphSnapshotRequest {
    pub(crate) root: ProjectionQuerySnapshotRequest,
    pub(crate) includes: std::collections::BTreeMap<String, ProjectionGraphIncludeRequest>,
    /// Maximum unique root/included record scopes the adapter may return.
    ///
    /// This is the caller's remaining execution budget, not a pagination hint:
    /// adapters must reject a result that would exceed it.
    pub(crate) max_unique_record_scopes: usize,
}

impl ProjectionGraphSnapshotRequest {
    pub(crate) fn new(
        root: ProjectionQuerySnapshotRequest,
        includes: impl IntoIterator<Item = (String, std::sync::Arc<TableSchema>)>,
        max_unique_record_scopes: usize,
    ) -> Result<Self, ProjectionProtocolError> {
        root.validate()?;
        if max_unique_record_scopes == 0
            || max_unique_record_scopes > MAX_PROJECTION_QUERY_BATCH_ROWS
        {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection graph snapshot record-scope budget is {max_unique_record_scopes}; expected 1..={MAX_PROJECTION_QUERY_BATCH_ROWS}"
            )));
        }
        let includes = includes.into_iter().collect::<Vec<_>>();
        let query_scopes = includes.len().checked_add(1).ok_or_else(|| {
            ProjectionProtocolError::InvalidBatch(
                "projection graph snapshot query-scope count overflowed".into(),
            )
        })?;
        if query_scopes > MAX_PROJECTION_QUERY_BATCH_ROWS {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection graph snapshot has {query_scopes} query scopes; maximum is {}",
                MAX_PROJECTION_QUERY_BATCH_ROWS
            )));
        }
        let mut validated = std::collections::BTreeMap::new();
        for (include, target_schema) in includes {
            let relationship = root
                .schema
                .relationships
                .iter()
                .find(|relationship| relationship.field_name == include)
                .cloned()
                .ok_or_else(|| {
                    ProjectionProtocolError::InvalidBatch(format!(
                        "projection graph snapshot model `{}` has no relationship `{include}`",
                        root.schema.model_name
                    ))
                })?;
            if matches!(
                relationship.kind,
                crate::table::RelationshipKind::ManyToMany
            ) {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph relationship `{include}` is many-to-many; project an explicit join read model instead"
                )));
            }
            target_schema.validate()?;
            if relationship.target_model != target_schema.model_name {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph relationship `{include}` targets `{}` but registered schema is `{}`",
                    relationship.target_model, target_schema.model_name
                )));
            }
            if validated
                .insert(
                    include,
                    ProjectionGraphIncludeRequest {
                        relationship,
                        target_schema,
                    },
                )
                .is_some()
            {
                return Err(ProjectionProtocolError::InvalidBatch(format!(
                    "projection graph snapshot repeats an include for model `{}`",
                    root.schema.model_name
                )));
            }
        }
        Ok(Self {
            root,
            includes: validated,
            max_unique_record_scopes,
        })
    }
}

/// Included rows and exact protocol revisions returned by one graph snapshot.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionGraphIncludeSnapshot {
    pub(crate) relationship: crate::table::RelationshipDef,
    pub(crate) target_schema: TableSchema,
    pub(crate) rows: Vec<ProjectionScopedRowSnapshot>,
}

/// Root and included rows read with protocol metadata from one adapter
/// snapshot.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionGraphSnapshot {
    pub(crate) root: ProjectionScopedRowSnapshot,
    pub(crate) includes: std::collections::BTreeMap<String, ProjectionGraphIncludeSnapshot>,
}

/// One exact command obligation evidence probe.
///
/// The scope is already compiler-bound and canonical. A store must compare all
/// of topology, partition, causation, model, observation kind, key bytes, and
/// key digest; a digest lookup alone is never evidence.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProjectionObligationEvidenceRequest {
    pub(crate) causation_id: String,
    pub(crate) scope: ProjectionRecordScope,
    pub(crate) kind: ProjectionObservationKind,
}

impl ProjectionObligationEvidenceRequest {
    pub(crate) fn new(
        causation_id: impl Into<String>,
        scope: ProjectionRecordScope,
        kind: ProjectionObservationKind,
    ) -> Result<Self, ProjectionProtocolError> {
        let request = Self {
            causation_id: bounded_opaque(
                "projection causation ID",
                causation_id,
                MAX_CAUSATION_ID_BYTES,
            )?,
            scope,
            kind,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        bounded_opaque(
            "projection causation ID",
            self.causation_id.clone(),
            MAX_CAUSATION_ID_BYTES,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionObligationEvidence {
    Pending,
    Observed(ProjectionObservation),
    TerminalFailure(ProjectionFailure),
}

/// Bounded obligation probes evaluated from one adapter snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionObligationEvidenceBatchRequest {
    pub(crate) requests: Vec<ProjectionObligationEvidenceRequest>,
}

impl ProjectionObligationEvidenceBatchRequest {
    pub(crate) fn new(
        requests: Vec<ProjectionObligationEvidenceRequest>,
    ) -> Result<Self, ProjectionProtocolError> {
        let request = Self { requests };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        if self.requests.len() > MAX_PROJECTION_EVIDENCE_BATCH_ITEMS {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection obligation evidence batch has {} probes; maximum is {}",
                self.requests.len(),
                MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
            )));
        }
        let mut exact = std::collections::HashSet::new();
        for request in &self.requests {
            request.validate()?;
            if !exact.insert((request.causation_id.as_str(), &request.scope, request.kind)) {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "projection obligation evidence batch repeats an exact probe".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProjectionObligationEvidenceBatch {
    pub(crate) evidence: Vec<ProjectionObligationEvidence>,
}

/// Bounded durable evidence discovered from one framework-minted causation.
///
/// This server-only read exists for modeled projection receipts, whose ledger
/// representation deliberately persists opaque scope tokens rather than raw
/// physical row identities. The authenticated GraphQL authority remints tokens
/// from these candidates and accepts only exact byte matches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionCausationEvidenceRequest {
    pub(crate) causation_id: String,
    pub(crate) topologies: Vec<ProjectorTopologyId>,
}

impl ProjectionCausationEvidenceRequest {
    pub(crate) fn new(
        causation_id: impl Into<String>,
        topologies: Vec<ProjectorTopologyId>,
    ) -> Result<Self, ProjectionProtocolError> {
        let request = Self {
            causation_id: bounded_opaque(
                "projection causation ID",
                causation_id,
                MAX_CAUSATION_ID_BYTES,
            )?,
            topologies,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        bounded_opaque(
            "projection causation ID",
            self.causation_id.clone(),
            MAX_CAUSATION_ID_BYTES,
        )?;
        if self.topologies.is_empty() || self.topologies.len() > MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
        {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection causation evidence has {} topology filters; expected 1..={}",
                self.topologies.len(),
                MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
            )));
        }
        let mut exact = std::collections::HashSet::new();
        if self
            .topologies
            .iter()
            .any(|topology| !exact.insert(topology))
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "projection causation evidence repeats an exact topology filter".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProjectionCausationEvidenceBatch {
    pub(crate) observations: Vec<ProjectionObservation>,
    /// Only failures that still stop their exact partition are returned.
    ///
    /// Repair clears that stop fence, so an immutable historical failure does
    /// not permanently poison later status reads.
    pub(crate) terminal_failure_topologies: Vec<ProjectorTopologyId>,
}

/// A typed physical-row key whose causal partition is deliberately unknown.
///
/// The registered codec supplies the topology, schema, and canonical key. The
/// store may only return a live record after recovering its exact partition
/// from durable metadata and proving that identity is unique.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionLiveRecordRequest {
    pub(crate) schema: Arc<TableSchema>,
    pub(crate) topology: ProjectorTopologyId,
    pub(crate) key: RowKey,
    pub(crate) canonical_key_bytes: Vec<u8>,
    pub(crate) canonical_key_hash: [u8; 32],
}

impl ProjectionLiveRecordRequest {
    pub(crate) fn new(
        codec: &ProjectionScopeCodec,
        model: &str,
        key: RowKey,
    ) -> Result<Self, ProjectionProtocolError> {
        let schema = codec.registered_schema_owned(model).map_err(|error| {
            ProjectionProtocolError::InvalidBatch(format!(
                "invalid projection live-record model: {error}"
            ))
        })?;
        crate::table::validate_key(&schema, &key)?;
        let canonical_key_bytes =
            codec
                .encode_unpartitioned_row_key(model, &key)
                .map_err(|error| {
                    ProjectionProtocolError::InvalidBatch(format!(
                        "invalid projection live-record key: {error}"
                    ))
                })?;
        let canonical_key_hash = ProjectionRecordScope::key_digest_for(&canonical_key_bytes);
        Ok(Self {
            schema,
            topology: codec.topology().clone(),
            key,
            canonical_key_bytes,
            canonical_key_hash,
        })
    }

    pub(crate) fn model(&self) -> &str {
        &self.schema.model_name
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        crate::table::validate_key(&self.schema, &self.key)?;
        if self.canonical_key_bytes.is_empty()
            || self.canonical_key_bytes.len() > super::MAX_PROJECTION_RECORD_KEY_BYTES
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "projection live-record canonical key is empty or oversized".into(),
            ));
        }
        if ProjectionRecordScope::key_digest_for(&self.canonical_key_bytes)
            != self.canonical_key_hash
        {
            return Err(ProjectionProtocolError::InvalidBatch(
                "projection live-record canonical key bytes and digest disagree".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectionLiveRecordBatchRequest {
    pub(crate) requests: Vec<ProjectionLiveRecordRequest>,
}

impl ProjectionLiveRecordBatchRequest {
    pub(crate) fn new(
        requests: Vec<ProjectionLiveRecordRequest>,
    ) -> Result<Self, ProjectionProtocolError> {
        let request = Self { requests };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn validate(&self) -> Result<(), ProjectionProtocolError> {
        if self.requests.len() > MAX_PROJECTION_EVIDENCE_BATCH_ITEMS {
            return Err(ProjectionProtocolError::InvalidBatch(format!(
                "projection live-record batch has {} rows; maximum is {}",
                self.requests.len(),
                MAX_PROJECTION_EVIDENCE_BATCH_ITEMS
            )));
        }
        let mut identities = std::collections::HashSet::new();
        for request in &self.requests {
            request.validate()?;
            if !identities.insert((
                &request.topology,
                request.model(),
                request.canonical_key_hash,
            )) {
                return Err(ProjectionProtocolError::InvalidBatch(
                    "projection live-record batch repeats a topology/model/key identity".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Results are aligned positionally with the validated request batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProjectionLiveRecordBatch {
    pub(crate) records: Vec<Option<ProjectionRecordMetadata>>,
}

/// Exact immutable identity that a repaired partition must retry first.
///
/// `failed_generation` identifies the generation that stopped. The enclosing
/// runtime state carries the newly active generation under which this exact
/// input must be retried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionPendingRetry {
    pub(crate) failure_id: String,
    pub(crate) input: ProjectionInputCursor,
    pub(crate) input_fingerprint: ProjectionInputFingerprint,
    pub(crate) message_id: String,
    pub(crate) causation_id: String,
    pub(crate) failed_generation: ProjectionGeneration,
    pub(crate) gap_free: bool,
}

/// Runtime-visible fences for one exact projector partition.
///
/// A missing value means the partition has never been bootstrapped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionPartitionRuntimeState {
    pub(crate) active_generation: ProjectionGeneration,
    pub(crate) stopped_failure_id: Option<String>,
    pub(crate) pending_retry: Option<ProjectionPendingRetry>,
}

/// Exact durable observation of one command causation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionObservation {
    pub causation_id: String,
    pub kind: ProjectionObservationKind,
    /// Present for record observations; absent for embedded/dependency scopes.
    pub revision: Option<RecordRevision>,
    /// Canonical dependency scope when no record revision exists.
    pub scope: ProjectionRecordScope,
    pub change: ProjectionChangeCursor,
}

/// Durable terminal failure for one exact input and repair generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionFailure {
    pub failure_id: String,
    pub input: ProjectionInputCursor,
    pub input_fingerprint: ProjectionInputFingerprint,
    pub message_id: String,
    pub causation_id: String,
    pub generation: ProjectionGeneration,
    /// Whether the failed input source promised gap-free ordered delivery.
    pub gap_free: bool,
    pub failure_code: String,
    pub failure_bytes: Vec<u8>,
    pub failure_digest: [u8; 32],
    pub change: ProjectionChangeCursor,
}

/// Adapter-resolved exact scope for one globally unique projection failure ID.
///
/// This is crate-private so an operator supplies only the opaque failure handle;
/// the durable store, not the caller, reconstructs canonical topology and
/// partition authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectionFailureLocation {
    pub(crate) topology: ProjectorTopologyId,
    pub(crate) partition: ProjectionPartition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProjectionChangeKind {
    /// A successful input with no row/observation change. This makes the exact
    /// checkpoint resumable without pretending an old change head was newly
    /// allocated and allows gap-free barriers to advance.
    Checkpoint,
    RecordUpsert,
    RecordDelete,
    RecordRecreate,
    Observation,
    Failure,
}

impl ProjectionChangeKind {
    pub(crate) fn as_storage_str(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::RecordUpsert => "record_upsert",
            Self::RecordDelete => "record_delete",
            Self::RecordRecreate => "record_recreate",
            Self::Observation => "observation",
            Self::Failure => "failure",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionChange {
    pub cursor: ProjectionChangeCursor,
    pub kind: ProjectionChangeKind,
    pub causation_id: String,
    pub observation_kind: Option<ProjectionObservationKind>,
    /// Canonical record/dependency identity for row and observation changes.
    /// Checkpoint-only and failure entries deliberately carry no model scope.
    pub scope: Option<ProjectionRecordScope>,
    pub revision: Option<RecordRevision>,
    pub failure_id: Option<String>,
}

/// Result and exact evidence produced by an asynchronous projection commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionCommitResult {
    pub outcome: ProjectionCommitOutcome,
    pub checkpoint: Option<ProjectionCheckpoint>,
    pub records: Vec<ProjectionRecordMetadata>,
    pub changes: Vec<ProjectionChange>,
}

impl ProjectionCommitResult {
    pub(crate) fn not_applied(
        outcome: ProjectionCommitOutcome,
        checkpoint: Option<ProjectionCheckpoint>,
    ) -> Self {
        debug_assert!(outcome != ProjectionCommitOutcome::Applied);
        Self {
            outcome,
            checkpoint,
            records: Vec::new(),
            changes: Vec::new(),
        }
    }
}
