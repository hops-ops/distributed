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
use std::sync::Arc;

use sqlx::{Encode, Executor, IntoArguments, Pool, QueryBuilder, Row, Transaction, Type};

use crate::projection_protocol::{
    change_kind_for_mutation, checked_next, checked_projection_graph_materialization,
    projection_has_many_columns, table_model_name, validate_projection_graph_snapshot_request,
    ProjectionCausationEvidenceBatch, ProjectionCausationEvidenceRequest, ProjectionChange,
    ProjectionChangeCursor, ProjectionChangeKind, ProjectionChangeRead, ProjectionChangeRetention,
    ProjectionCheckpoint, ProjectionCommitBatch, ProjectionCommitOutcome, ProjectionCommitResult,
    ProjectionEpoch, ProjectionExecutionSnapshotBatch, ProjectionExecutionSnapshotBatchRequest,
    ProjectionFailure, ProjectionFailureBatch, ProjectionFailureLocation, ProjectionGeneration,
    ProjectionGraphIncludeSnapshot, ProjectionGraphSnapshot, ProjectionGraphSnapshotRequest,
    ProjectionInputCursor, ProjectionInputDisposition, ProjectionInputFingerprint,
    ProjectionLiveRecordBatch, ProjectionLiveRecordBatchRequest, ProjectionModelOwnership,
    ProjectionMutationKind, ProjectionObligationEvidence, ProjectionObligationEvidenceBatch,
    ProjectionObligationEvidenceBatchRequest, ProjectionObservation, ProjectionObservationKind,
    ProjectionObservationTarget, ProjectionPartition, ProjectionPartitionRuntimeState,
    ProjectionPartitionSnapshot, ProjectionPendingRetry, ProjectionProtocolError,
    ProjectionProtocolStore, ProjectionQuerySnapshot, ProjectionQuerySnapshotBatch,
    ProjectionQuerySnapshotBatchRequest, ProjectionQuerySnapshotRequest,
    ProjectionRecordExpectation, ProjectionRecordMetadata, ProjectionRecordScope,
    ProjectionScopeCodec, ProjectionScopedRowSnapshot, ProjectionSource, ProjectorTopologyId,
    RecordRevision, SameTransactionProjectionBatch, SameTransactionProjectionEvidence,
    TrustedProjectionInput, MAX_PROJECTION_EVIDENCE_BATCH_ITEMS,
};
use crate::repository::RepositoryError;
use crate::sqlx_repo::read_model::{
    apply_read_model_write_plan_in_tx, column_by_name, push_key_predicates,
    push_order_by_primary_key, quote_identifier, row_version_in_tx, validate_sql_write_plan,
    validate_values_match_key, version_column,
};
use crate::sqlx_repo::repo::{repository_storage_error, SqlxRepoBackend, SqlxRepository};
use crate::table::{
    column_name_for, validate_row_values, RelationshipKind, RowKey, RowValue, RowValues,
    TableMutation, TableSchema, TableStoreError, TableWritePlan,
};

mod helpers;
mod identity;
mod locks;
mod partitions;
mod reads;
#[cfg(feature = "graphql")]
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
pub(crate) use reads::{
    apply_same_transaction_projection_in_tx, read_projection_causation_evidence_in_executor,
    read_projection_changes_in_executor, read_projection_live_record_batch_in_executor,
    read_projection_obligation_evidence_batch_in_executor,
    read_projection_query_snapshot_in_executor, with_projection_read_snapshot,
};
use types::*;
use writes::*;

#[cfg(all(test, feature = "sqlite"))]
include!("tests.rs");

#[cfg(all(test, feature = "postgres"))]
include!("postgres_tests.rs");
