//! Sealed projection commit vocabulary shared by repository adapters.
//!
//! Application projectors never construct these batches directly. A
//! framework-owned workspace validates scopes and stages row mutations, then
//! hands one closed batch to a repository. This keeps row data, dedupe,
//! revisions, observations, checkpoints, and change publication inside one
//! adapter transaction.

// The store trait + query vocabulary is wider than the production call graph:
// adapters implement the full surface, and many paths are only exercised by
// adapter tests today. Keep the contract complete without drowning default
// builds in dead_code noise.
#![allow(dead_code)]

use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ProjectionChangeCursor, ProjectionCheckpoint, ProjectionCommitOutcome, ProjectionEpoch,
    ProjectionInputCursor, ProjectionPartition, ProjectionProtocolValidationError,
    ProjectionRecordScope, ProjectionScopeCodec, ProjectionSource, ProjectorTopologyId,
    RecordRevision, MAX_PROJECTION_PARTITION_BYTES, MAX_PROJECTION_POSITION,
    MAX_PROJECTION_RECORD_KEY_BYTES,
};
use crate::repository::{InboxReceipt, RepositoryError};
use crate::table::{
    RowKey, RowValues, TableMutation, TableSchema, TableStoreError, TableWritePlan,
};
use crate::ProjectionProgramId;

mod backend_helpers;
mod commit;
mod error;
mod helpers;
mod identity;
mod kind_codec;
mod ownership;
mod query;
mod replay;
mod r#trait;

#[cfg(test)]
pub(crate) mod scenario_tests;
#[cfg(test)]
mod tests;

use helpers::{bounded_name, bounded_opaque, digest_hex, validate_scope};
use identity::{
    FAILURE_FINGERPRINT_DOMAIN, MAX_CAUSATION_ID_BYTES, MAX_FAILURE_CODE_BYTES,
    MAX_FAILURE_DETAIL_BYTES, MAX_FAILURE_ID_BYTES, MAX_MESSAGE_ID_BYTES,
};

pub(crate) use backend_helpers::{
    change_kind_for_mutation, checked_next, checked_projection_graph_materialization,
    failure_matches_batch, projection_relationship_values, table_model_name,
    validate_projection_graph_snapshot_request,
};
pub(crate) use commit::{
    ProjectionCommitBatch, ProjectionFailureBatch, SameTransactionProjectionBatch,
};
pub use error::ProjectionProtocolError;
pub(super) use helpers::domain_separated_digest;
pub use identity::{
    ProjectionChangeRetention, ProjectionGeneration, ProjectionInputFingerprint,
    ProjectionObservationKind, DEFAULT_MAX_RETAINED_PROJECTION_CHANGES,
};
pub(crate) use identity::{
    ProjectionInputDisposition, ProjectionModelOwnership, ProjectionMutationKind,
    ProjectionObservationRequest, ProjectionObservationTarget, ProjectionRecordExpectation,
    ProjectionRecordMutation, TrustedProjectionInput, MAX_PROJECTION_EVIDENCE_BATCH_ITEMS,
    MAX_PROJECTION_QUERY_BATCH_CHECKPOINT_PROBES, MAX_PROJECTION_QUERY_BATCH_ROWS,
    MAX_PROJECTION_QUERY_CHECKPOINT_PROBES,
};
pub(crate) use ownership::validate_ownership_batch;
pub use query::{
    ProjectionChange, ProjectionChangeKind, ProjectionCommitResult, ProjectionFailure,
    ProjectionObservation, ProjectionRecordMetadata,
};
// Several of these are only constructed from adapter unit tests; re-export for the
// store surface and silence unused_imports on non-test lib builds.
#[allow(unused_imports)]
pub(crate) use query::{
    ProjectionCausationEvidenceBatch, ProjectionCausationEvidenceRequest,
    ProjectionCheckpointProbe, ProjectionCheckpointSnapshot, ProjectionExecutionSnapshotBatch,
    ProjectionExecutionSnapshotBatchRequest, ProjectionFailureLocation,
    ProjectionGraphIncludeRequest, ProjectionGraphIncludeSnapshot, ProjectionGraphSnapshot,
    ProjectionGraphSnapshotRequest, ProjectionLiveRecordBatch, ProjectionLiveRecordBatchRequest,
    ProjectionLiveRecordRequest, ProjectionObligationEvidence, ProjectionObligationEvidenceBatch,
    ProjectionObligationEvidenceBatchRequest, ProjectionObligationEvidenceRequest,
    ProjectionPartitionRuntimeState, ProjectionPartitionSnapshot, ProjectionPendingRetry,
    ProjectionQuerySnapshot, ProjectionQuerySnapshotBatch, ProjectionQuerySnapshotBatchRequest,
    ProjectionQuerySnapshotRequest, ProjectionScopedRowSnapshot,
};
pub use r#trait::ProjectionChangeRead;
pub(crate) use r#trait::ProjectionProtocolStore;
pub(crate) use replay::SameTransactionProjectionEvidence;
