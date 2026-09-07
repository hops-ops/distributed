#![expect(
    clippy::manual_async_fn,
    reason = "trait impls preserve the ProjectionProtocolStore Send bounds"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use super::InMemoryRepository;
use crate::projection_protocol::{
    change_kind_for_mutation, checked_next, checked_projection_graph_materialization,
    failure_matches_batch, projection_has_many_columns, table_model_name,
    validate_projection_graph_snapshot_request, ProjectionCausationEvidenceBatch,
    ProjectionCausationEvidenceRequest, ProjectionChange, ProjectionChangeCursor,
    ProjectionChangeKind, ProjectionChangeRead, ProjectionChangeRetention, ProjectionCheckpoint,
    ProjectionCommitBatch, ProjectionCommitOutcome, ProjectionCommitResult, ProjectionEpoch,
    ProjectionExecutionSnapshotBatch, ProjectionExecutionSnapshotBatchRequest, ProjectionFailure,
    ProjectionFailureBatch, ProjectionFailureLocation, ProjectionGeneration,
    ProjectionGraphIncludeSnapshot, ProjectionGraphSnapshot, ProjectionGraphSnapshotRequest,
    ProjectionInputCursor, ProjectionInputDisposition, ProjectionInputFingerprint,
    ProjectionLiveRecordBatch, ProjectionLiveRecordBatchRequest, ProjectionMutationKind,
    ProjectionObligationEvidence, ProjectionObligationEvidenceBatch,
    ProjectionObligationEvidenceBatchRequest, ProjectionObligationEvidenceRequest,
    ProjectionObservation, ProjectionObservationKind, ProjectionObservationTarget,
    ProjectionPartition, ProjectionPartitionRuntimeState, ProjectionPendingRetry,
    ProjectionProtocolError, ProjectionProtocolStore, ProjectionQuerySnapshot,
    ProjectionQuerySnapshotBatch, ProjectionQuerySnapshotBatchRequest,
    ProjectionQuerySnapshotRequest, ProjectionRecordExpectation, ProjectionRecordMetadata,
    ProjectionRecordScope, ProjectionScopeCodec, ProjectionScopedRowSnapshot, ProjectionSource,
    ProjectorTopologyId, RecordRevision, RevisionComparison, SameTransactionProjectionBatch,
    SameTransactionProjectionEvidence, TrustedProjectionInput, MAX_PROJECTION_EVIDENCE_BATCH_ITEMS,
};
use crate::read_model::in_memory::{
    apply_read_model_write_plan, relational_storage_key, StoredRow,
};
use crate::repository::RepositoryError;
use crate::table::{
    column_name_for, key_from_row, RelationshipKind, RowKey, RowValues, TableMutation, TableSchema,
    TableStoreError, TableWritePlan,
};

mod direct_projection;
mod read_helpers;
#[cfg(feature = "graphql")]
mod rebuild;
mod state;
mod state_impl;
mod store_impl;
mod util;

pub(super) use direct_projection::stage_same_transaction_projection;
pub(super) use state::{reject_causal_owned_plans, InMemoryProjectionProtocolState};

use read_helpers::{
    read_projection_execution_snapshot_batch_from_state, read_projection_graph_snapshot_from_state,
    read_projection_live_record_from_state, read_projection_obligation_evidence_from_state,
    read_projection_query_snapshot_from_state, validate_observation_from_state,
};
use state::*;
use util::storage_key_belongs_to_table;

#[cfg(test)]
mod tests;
