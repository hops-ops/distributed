#![expect(
    clippy::manual_async_fn,
    reason = "trait impls preserve the ProjectionProtocolStore Send bounds"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;

use super::InMemoryRepository;
use crate::projection_protocol::{
    change_kind_for_mutation, change_page, change_read_window, classify_input, corrupt_storage,
    ensure_active_input, failure_matches_batch, next_record, plan_repair, program_id_for_model,
    resolve_observation, sort_causation_evidence, table_model_name, validate_input_identity,
    validate_pending_retry, validate_physical_row, PartitionId, ProjectionCausationEvidenceBatch,
    ProjectionCausationEvidenceRequest, ProjectionChange, ProjectionChangeCursor,
    ProjectionChangeKind, ProjectionChangeRead, ProjectionChangeRetention, ProjectionCheckpoint,
    ProjectionCommitBatch, ProjectionCommitOutcome, ProjectionCommitResult, ProjectionEpoch,
    ProjectionExecutionSnapshotBatch, ProjectionExecutionSnapshotBatchRequest, ProjectionFailure,
    ProjectionFailureBatch, ProjectionFailureLocation, ProjectionGeneration, ProjectionInputCursor,
    ProjectionInputDisposition, ProjectionObligationEvidenceRequest, ProjectionObservation,
    ProjectionObservationKind, ProjectionPartitionRuntimeState, ProjectionPendingRetry,
    ProjectionProtocolError, ProjectionProtocolStore, ProjectionQuerySnapshot,
    ProjectionQuerySnapshotRequest, ProjectionRecordExpectation, ProjectionRecordMetadata,
    ProjectionRecordScope, ProjectionScopedRowSnapshot, ProjectionSource, ProjectorTopologyId,
    RecordRevision, RecordTransition, RevisionComparison, SameTransactionProjectionBatch,
    SameTransactionProjectionEvidence, TrustedProjectionInput, MAX_PROJECTION_EVIDENCE_BATCH_ITEMS,
};
use crate::read_model::in_memory::{
    apply_read_model_write_plan, relational_storage_key, StoredRow,
};
use crate::repository::RepositoryError;
use crate::table::TableWritePlan;

mod direct_projection;
mod read_helpers;
mod rebuild;
mod state;
mod state_impl;
mod store_impl;
mod util;

pub(super) use direct_projection::stage_same_transaction_projection;
pub(super) use state::InMemoryProjectionProtocolState;

use read_helpers::{
    read_projection_execution_snapshot_batch_from_state, read_projection_query_snapshot_from_state,
    validate_observation_from_state,
};
use state::*;
use state_impl::{ensure_live_record_identity_available, next_change, retained_through};
use util::storage_key_belongs_to_table;

#[cfg(test)]
mod tests;
