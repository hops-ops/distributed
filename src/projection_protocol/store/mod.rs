//! Sealed projection commit vocabulary shared by repository adapters.
//!
//! Application projectors never construct these batches directly. A
//! framework-owned workspace validates scopes and stages row mutations, then
//! hands one closed batch to a repository. This keeps row data, dedupe,
//! revisions, observations, checkpoints, and change publication inside one
//! adapter transaction.

use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    PartitionId, ProjectionChangeCursor, ProjectionCheckpoint, ProjectionCommitOutcome,
    ProjectionEpoch, ProjectionInputCursor, ProjectionProtocolValidationError,
    ProjectionRecordScope, ProjectionScopeCodec, ProjectionSource, ProjectorTopologyId,
    RecordRevision, MAX_PROJECTION_PARTITION_BYTES, MAX_PROJECTION_POSITION,
    MAX_PROJECTION_RECORD_KEY_BYTES,
};
use crate::projection::ProjectionProgramId;
use crate::repository::{InboxReceipt, RepositoryError};
use crate::table::{
    RowKey, RowValues, TableMutation, TableSchema, TableStoreError, TableWritePlan,
};

mod backend_helpers;
mod commit;
mod decisions;
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
    change_kind_for_mutation, checked_next, failure_matches_batch, table_model_name,
};
pub(crate) use commit::{
    ProjectionCommitBatch, ProjectionFailureBatch, SameTransactionProjectionBatch,
};
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub(crate) use decisions::same_input;
pub(crate) use decisions::{
    change_page, change_read_window, classify_input, corrupt_storage, ensure_active_input,
    ensure_change_epoch, next_record, plan_repair, program_id_for_model, resolve_observation,
    sort_causation_evidence, validate_input_identity, validate_pending_retry,
    validate_physical_row,
};
pub use error::ProjectionProtocolError;
pub(super) use helpers::domain_separated_digest;
pub use identity::{
    ProjectionChangeRetention, ProjectionGeneration, ProjectionInputFingerprint,
    ProjectionObservationKind, DEFAULT_MAX_RETAINED_PROJECTION_CHANGES,
};
pub(crate) use identity::{
    ProjectionInputDisposition, ProjectionModelOwnership, ProjectionObservationRequest,
    ProjectionObservationTarget, ProjectionRecordExpectation, ProjectionRecordMutation,
    RecordTransition, TrustedProjectionInput, MAX_PROJECTION_EVIDENCE_BATCH_ITEMS,
    MAX_PROJECTION_QUERY_BATCH_ROWS, MAX_PROJECTION_QUERY_CHECKPOINT_PROBES,
};
pub(crate) use ownership::validate_ownership_batch;
pub use query::*;
pub use r#trait::ProjectionChangeRead;
pub(crate) use r#trait::ProjectionProtocolStore;
pub(crate) use replay::SameTransactionProjectionEvidence;

/// Every table the projection store writes that a query's causal evidence
/// (snapshot, records, observations, cursors, failures) can read. A cache that
/// stores a response with its evidence must invalidate on writes to all of
/// them, including no-op commits.
///
/// `projection_input_delivery_aliases` is deliberately absent: it maps
/// redelivered message ids onto inputs at ingestion and no query evidence
/// reads it. `proof_tables_cover_every_projection_table` keeps this list
/// in step with the migrations.
pub const PROOF_TABLES: &[&str] = &[
    "projection_partitions",
    "projection_generations",
    "projection_source_capabilities",
    "projection_input_identities",
    "projection_input_cursors",
    "projection_input_receipts",
    "projection_table_ownership_fences",
    "projection_causal_tables",
    "projection_registered_models",
    "projection_model_ownership",
    "projection_records",
    "projection_observations",
    "projection_failures",
    "projection_changes",
];

/// Projection-store tables that are not query evidence, with the reason.
#[cfg(test)]
const NOT_PROOF_TABLES: &[(&str, &str)] = &[(
    "projection_input_delivery_aliases",
    "ingestion-side message id aliasing; no query evidence reads it",
)];

#[cfg(test)]
mod proof_table_tests {
    use super::{NOT_PROOF_TABLES, PROOF_TABLES};
    use std::collections::BTreeSet;

    fn created_projection_tables(dialect: &str) -> BTreeSet<String> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("migrations")
            .join(dialect);
        let mut tables = BTreeSet::new();
        for entry in std::fs::read_dir(&dir).expect("migrations directory") {
            let sql = std::fs::read_to_string(entry.expect("migration").path()).expect("migration");
            for statement in sql.split("CREATE TABLE").skip(1) {
                let name = statement
                    .trim_start()
                    .trim_start_matches("IF NOT EXISTS")
                    .trim_start()
                    .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .next()
                    .unwrap_or_default();
                if name.starts_with("projection_") {
                    tables.insert(name.to_owned());
                }
            }
        }
        tables
    }

    #[test]
    fn proof_tables_cover_every_projection_table() {
        let listed: BTreeSet<String> = PROOF_TABLES
            .iter()
            .chain(NOT_PROOF_TABLES.iter().map(|(table, _)| table))
            .map(|table| (*table).to_owned())
            .collect();
        assert_eq!(listed.len(), PROOF_TABLES.len() + NOT_PROOF_TABLES.len());
        for dialect in ["postgres", "sqlite"] {
            assert_eq!(
                created_projection_tables(dialect),
                listed,
                "{dialect}: a projection_* table must be in PROOF_TABLES or NOT_PROOF_TABLES"
            );
        }
    }
}
