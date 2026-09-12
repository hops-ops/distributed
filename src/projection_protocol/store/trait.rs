use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionChangeRead {
    Changes {
        head: Option<ProjectionChangeCursor>,
        compacted_through: u64,
        changes: Vec<ProjectionChange>,
    },
    ResetRequired {
        head: Option<ProjectionChangeCursor>,
        compacted_through: u64,
    },
}

/// Adapter contract for atomic causal projection persistence.
pub(crate) trait ProjectionProtocolStore: Send + Sync {
    #[cfg(feature = "graphql")]
    fn projection_rebuild_records<'a>(
        &'a self,
        _context: &'a crate::projection::rebuild::RebuildContext,
    ) -> impl Future<Output = Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError>> + Send + 'a
    {
        async {
            Err(ProjectionProtocolError::InvalidBatch(
                "this store does not support snapshot projection rebuilds".into(),
            ))
        }
    }

    #[cfg(feature = "graphql")]
    fn commit_projection_rebuild(
        &self,
        _plan: crate::projection::rebuild::SnapshotProjectionRebuildPlan,
    ) -> impl Future<Output = Result<usize, ProjectionProtocolError>> + Send + '_ {
        async {
            Err(ProjectionProtocolError::InvalidBatch(
                "this store does not support snapshot projection rebuilds".into(),
            ))
        }
    }

    /// Install model-wide causal ownership before projector traffic begins.
    /// This bootstrap marker closes the absent-row race with legacy/raw write
    /// plans; per-partition ownership is still verified inside each commit.
    fn register_projection_models<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        ownership: &'a [ProjectionModelOwnership],
    ) -> impl Future<Output = Result<(), ProjectionProtocolError>> + Send + 'a;

    fn commit_projection(
        &self,
        batch: ProjectionCommitBatch,
    ) -> impl Future<Output = Result<ProjectionCommitResult, ProjectionProtocolError>> + Send + '_;

    fn record_projection_failure(
        &self,
        batch: ProjectionFailureBatch,
    ) -> impl Future<Output = Result<ProjectionFailure, ProjectionProtocolError>> + Send + '_;

    fn projection_checkpoint<'a>(
        &'a self,
        cursor_scope: &'a ProjectionInputCursor,
        generation: ProjectionGeneration,
    ) -> impl Future<Output = Result<Option<ProjectionCheckpoint>, ProjectionProtocolError>> + Send + 'a;

    fn projection_record<'a>(
        &'a self,
        scope: &'a ProjectionRecordScope,
    ) -> impl Future<Output = Result<Option<ProjectionRecordMetadata>, ProjectionProtocolError>>
           + Send
           + 'a;

    fn projection_input_disposition<'a>(
        &'a self,
        input: &'a TrustedProjectionInput,
    ) -> impl Future<Output = Result<ProjectionInputDisposition, ProjectionProtocolError>> + Send + 'a;

    /// Read one physical row, its record metadata, requested source
    /// checkpoints, and the partition live-resume boundary from one atomic
    /// adapter snapshot.
    fn projection_query_snapshot<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshot, ProjectionProtocolError>> + Send + 'a;

    fn projection_query_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionQuerySnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionQuerySnapshotBatch, ProjectionProtocolError>> + Send + 'a;

    /// Read every execution target from one adapter snapshot and echo each
    /// exact requested scope in the result.
    ///
    /// Adapters opt into this contract in their projection-protocol
    /// implementation. The default keeps older adapters fail-closed until they
    /// can prove a coherent batch snapshot.
    fn projection_execution_snapshot_batch<'a>(
        &'a self,
        _request: &'a ProjectionExecutionSnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionExecutionSnapshotBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        async {
            Err(ProjectionProtocolError::InvalidBatch(
                "projection adapter does not support coherent execution snapshots".into(),
            ))
        }
    }

    /// Read one root graph, its includes, and every exact protocol revision
    /// from one adapter snapshot.
    ///
    /// Implementations must enforce `request.max_unique_record_scopes` before
    /// materializing or returning an oversized root/include result.
    fn projection_graph_snapshot<'a>(
        &'a self,
        _request: &'a ProjectionGraphSnapshotRequest,
    ) -> impl Future<Output = Result<ProjectionGraphSnapshot, ProjectionProtocolError>> + Send + 'a
    {
        async {
            Err(ProjectionProtocolError::InvalidBatch(
                "projection adapter does not support coherent graph snapshots".into(),
            ))
        }
    }

    fn projection_obligation_evidence_batch<'a>(
        &'a self,
        request: &'a ProjectionObligationEvidenceBatchRequest,
    ) -> impl Future<Output = Result<ProjectionObligationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a;

    fn projection_causation_evidence<'a>(
        &'a self,
        _request: &'a ProjectionCausationEvidenceRequest,
    ) -> impl Future<Output = Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a {
        async {
            Err(ProjectionProtocolError::InvalidBatch(
                "projection adapter does not support modeled causation evidence".into(),
            ))
        }
    }

    fn projection_live_record_batch<'a>(
        &'a self,
        request: &'a ProjectionLiveRecordBatchRequest,
    ) -> impl Future<Output = Result<ProjectionLiveRecordBatch, ProjectionProtocolError>> + Send + 'a;

    fn projection_partition_runtime_state<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
    ) -> impl Future<Output = Result<Option<ProjectionPartitionRuntimeState>, ProjectionProtocolError>>
           + Send
           + 'a;

    fn projection_observation<'a>(
        &'a self,
        causation_id: &'a str,
        scope: &'a ProjectionRecordScope,
        kind: ProjectionObservationKind,
    ) -> impl Future<Output = Result<Option<ProjectionObservation>, ProjectionProtocolError>> + Send + 'a;

    fn projection_changes<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        after: Option<&'a ProjectionChangeCursor>,
        limit: usize,
    ) -> impl Future<Output = Result<ProjectionChangeRead, ProjectionProtocolError>> + Send + 'a;

    /// Start an explicitly linked repair generation for the immutable failure
    /// currently stopping this exact partition. Implementations copy every
    /// last-good source checkpoint, atomically switch the active generation,
    /// and only then clear the stop fence.
    fn repair_projection<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<ProjectionGeneration, ProjectionProtocolError>> + Send + 'a;

    /// Compact durable changes through the supplied exact cursor. The returned
    /// watermark is the last removed position; adapters never advertise a
    /// larger window than they actually retain.
    fn compact_projection_changes<'a>(
        &'a self,
        through: &'a ProjectionChangeCursor,
    ) -> impl Future<Output = Result<u64, ProjectionProtocolError>> + Send + 'a;

    fn projection_failure<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a ProjectionPartition,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailure>, ProjectionProtocolError>> + Send + 'a;

    /// Resolve a globally unique durable failure ID to its exact stored scope.
    ///
    /// Adapters must reconstruct and validate the canonical topology/partition
    /// bytes they own. This is the safe basis for the public opaque repair
    /// handle; callers never provide tenant-bearing partition bytes.
    fn projection_failure_location<'a>(
        &'a self,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<Option<ProjectionFailureLocation>, ProjectionProtocolError>>
           + Send
           + 'a;
}
