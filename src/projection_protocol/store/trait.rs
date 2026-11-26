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
///
/// `projection_checkpoint`, `projection_record`, `projection_observation`,
/// `projection_changes`, `projection_failure` and `compact_projection_changes`
/// have no production caller. They are the contract's observable state: the
/// conformance suite asserts every commit, failure, repair and compaction
/// through them on each backend. That is why they stay, each allowed as dead
/// code outside tests.
pub(crate) trait ProjectionProtocolStore: Send + Sync {
    fn projection_rebuild_records<'a>(
        &'a self,
        context: &'a crate::projection::rebuild::RebuildContext,
    ) -> impl Future<Output = Result<Vec<ProjectionRecordMetadata>, ProjectionProtocolError>> + Send + 'a;

    fn commit_projection_rebuild(
        &self,
        plan: crate::projection::rebuild::SnapshotProjectionRebuildPlan,
    ) -> impl Future<Output = Result<usize, ProjectionProtocolError>> + Send + '_;

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

    #[cfg_attr(not(test), allow(dead_code))]
    fn projection_checkpoint<'a>(
        &'a self,
        cursor_scope: &'a ProjectionInputCursor,
        generation: ProjectionGeneration,
    ) -> impl Future<Output = Result<Option<ProjectionCheckpoint>, ProjectionProtocolError>> + Send + 'a;

    #[cfg_attr(not(test), allow(dead_code))]
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

    /// Read every execution target from one adapter snapshot and echo each
    /// exact requested scope in the result.
    // Its caller, the modeled projector context, exists only with `graphql`.
    #[cfg_attr(not(feature = "graphql"), allow(dead_code))]
    fn projection_execution_snapshot_batch<'a>(
        &'a self,
        request: &'a ProjectionExecutionSnapshotBatchRequest,
    ) -> impl Future<Output = Result<ProjectionExecutionSnapshotBatch, ProjectionProtocolError>>
           + Send
           + 'a;

    // Its caller, the causal command host, asks for evidence only with
    // `graphql`.
    #[cfg_attr(not(feature = "graphql"), allow(dead_code))]
    fn projection_causation_evidence<'a>(
        &'a self,
        request: &'a ProjectionCausationEvidenceRequest,
    ) -> impl Future<Output = Result<ProjectionCausationEvidenceBatch, ProjectionProtocolError>>
           + Send
           + 'a;

    fn projection_partition_runtime_state<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
    ) -> impl Future<Output = Result<Option<ProjectionPartitionRuntimeState>, ProjectionProtocolError>>
           + Send
           + 'a;

    #[cfg_attr(not(test), allow(dead_code))]
    fn projection_observation<'a>(
        &'a self,
        causation_id: &'a str,
        scope: &'a ProjectionRecordScope,
        kind: ProjectionObservationKind,
    ) -> impl Future<Output = Result<Option<ProjectionObservation>, ProjectionProtocolError>> + Send + 'a;

    #[cfg_attr(not(test), allow(dead_code))]
    fn projection_changes<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
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
        partition: &'a PartitionId,
        failure_id: &'a str,
    ) -> impl Future<Output = Result<ProjectionGeneration, ProjectionProtocolError>> + Send + 'a;

    /// Compact durable changes through the supplied exact cursor. The returned
    /// watermark is the last removed position; adapters never advertise a
    /// larger window than they actually retain.
    #[cfg_attr(not(test), allow(dead_code))]
    fn compact_projection_changes<'a>(
        &'a self,
        through: &'a ProjectionChangeCursor,
    ) -> impl Future<Output = Result<u64, ProjectionProtocolError>> + Send + 'a;

    #[cfg_attr(not(test), allow(dead_code))]
    fn projection_failure<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
        partition: &'a PartitionId,
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

    /// The failure IDs currently stopping partitions of `topology`, in
    /// ascending order. A partition leaves the list when its failure is
    /// repaired.
    fn stopped_projection_failures<'a>(
        &'a self,
        topology: &'a ProjectorTopologyId,
    ) -> impl Future<Output = Result<Vec<String>, ProjectionProtocolError>> + Send + 'a;
}
