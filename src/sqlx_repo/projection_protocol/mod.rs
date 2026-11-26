//! Generic SQLx persistence for the durable projection protocol.
//!
//! SQLite and PostgreSQL share the same state machine and SQL shape. Every
//! mutating protocol operation first takes a portable partition lock by
//! performing a no-op upsert on `projection_partitions`; all semantic
//! decisions then happen before statements that could raise a constraint
//! error, because PostgreSQL aborts a transaction after such an error.

#![expect(
    clippy::manual_async_fn,
    reason = "trait impls return impl Future + Send to preserve Send bounds"
)]

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;

use sqlx::{Encode, Executor, IntoArguments, Pool, QueryBuilder, Row, Transaction, Type};

use crate::projection::ProjectionProgramId;
use crate::projection_protocol::{
    change_kind_for_mutation, change_page, change_read_window, checked_next, classify_input,
    corrupt_storage, ensure_active_input, failure_matches_batch, next_record, plan_repair,
    program_id_for_model, resolve_observation, same_input, sort_causation_evidence,
    table_model_name, validate_input_identity, validate_pending_retry, validate_physical_row,
    PartitionId, ProjectionCausationEvidenceBatch, ProjectionCausationEvidenceRequest,
    ProjectionChange, ProjectionChangeCursor, ProjectionChangeKind, ProjectionChangeRead,
    ProjectionChangeRetention, ProjectionCheckpoint, ProjectionCommitBatch,
    ProjectionCommitOutcome, ProjectionCommitResult, ProjectionEpoch,
    ProjectionExecutionSnapshotBatch, ProjectionExecutionSnapshotBatchRequest, ProjectionFailure,
    ProjectionFailureBatch, ProjectionFailureLocation, ProjectionGeneration, ProjectionInputCursor,
    ProjectionInputDisposition, ProjectionInputFingerprint, ProjectionLiveRecordBatch,
    ProjectionLiveRecordBatchRequest, ProjectionModelOwnership, ProjectionObservation,
    ProjectionObservationKind, ProjectionObservationTarget, ProjectionPartitionRuntimeState,
    ProjectionPartitionSnapshot, ProjectionPendingRetry, ProjectionProtocolError,
    ProjectionProtocolStore, ProjectionQuerySnapshot, ProjectionQuerySnapshotRequest,
    ProjectionRecordExpectation, ProjectionRecordMetadata, ProjectionRecordScope,
    ProjectionScopedRowSnapshot, ProjectionSource, ProjectorTopologyId, RecordRevision,
    RecordTransition, SameTransactionProjectionBatch, SameTransactionProjectionEvidence,
    TrustedProjectionInput, MAX_PROJECTION_EVIDENCE_BATCH_ITEMS,
};
use crate::repository::RepositoryError;
use crate::sqlx_repo::read_model::{
    apply_read_model_write_plan_in_tx, push_key_predicates, quote_identifier, row_version_in_tx,
    validate_values_match_key, version_column,
};
use crate::sqlx_repo::repo::{repository_storage_error, SqlxRepoBackend, SqlxRepository};
use crate::table::{
    validate_row_values, RowValues, TableMutation, TableStoreError, TableWritePlan,
};

mod helpers;
mod identity;
mod locks;
mod partitions;
mod reads;
mod rebuild;
mod store_impl;
mod types;
mod writes;

use helpers::*;
use identity::*;
use locks::*;
pub(crate) use locks::{reject_causal_table_writes_in_tx, PROJECTION_CHANGE_NOTIFY_TABLE};
pub(crate) use partitions::read_projection_partition_snapshot_in_executor;
use partitions::*;
use reads::*;
// The GraphQL layer is the only caller of several of these.
#[cfg_attr(not(feature = "graphql"), allow(unused_imports))]
pub(crate) use reads::{
    apply_same_transaction_projection_in_tx, read_projection_causation_evidence_in_executor,
    read_projection_changes_in_executor, read_projection_live_record_batch_in_executor,
    read_projection_query_snapshot_in_executor, with_projection_read_snapshot,
};
pub(crate) use types::VerifiedRegistrations;
use types::*;
use writes::*;

#[cfg(all(test, feature = "sqlite"))]
include!("tests.rs");

#[cfg(all(test, feature = "postgres"))]
include!("postgres_tests.rs");
