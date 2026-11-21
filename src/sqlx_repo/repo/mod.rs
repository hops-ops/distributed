//! Backend-agnostic event-store, snapshot, outbox, and inbox logic shared by
//! the Postgres and SQLite repositories.
//!
//! This extends the [`SqlxReadModelBackend`](super::read_model::SqlxReadModelBackend)
//! pattern to the whole repository surface: the SQL statements and row codecs
//! are identical across the two backends because `QueryBuilder<DB>` renders the
//! right placeholder dialect. What genuinely differs — schema SQL, bind-param
//! chunking, the timestamp codec (Postgres epoch-`f64`/`to_timestamp()` vs
//! SQLite `"secs.nanos"` text), the unique-violation predicate, conflict
//! recovery (SQLite can re-read in the failed transaction; a failed Postgres
//! statement aborts it), and the outbox `claim` strategy — lives behind the
//! [`SqlxRepoBackend`] trait, implemented once per backend.

#![expect(
    clippy::manual_async_fn,
    reason = "async trait impls return impl Future + Send to preserve public Send bounds"
)]

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::migrate::{Migrate, Migration, MigrationType, Migrator};
use sqlx::pool::PoolOptions;
use sqlx::query_builder::Separated;
use sqlx::{Encode, Executor, IntoArguments, Pool, QueryBuilder, Row, Transaction, Type};

use crate::command_ledger::{
    AttemptFence, CausalCommitBatch, CausalGetStream, CausalRepositoryIdentity,
    CausalStorageIdentity, CausalTransactionalCommit, CommandCompletion, CommandLedgerError,
    CommandLedgerKey, CommandLedgerStore, CommandLookup, CommandLookupScope, CommandReservation,
    ExternalCommandCompletion, ReservationOutcome,
};
use crate::entity::{Entity, EventRecord};
use crate::outbox::{OutboxMessage, OutboxMessageStatus};
use crate::outbox_worker::{ClaimOutboxMessages, OutboxBacklogStats, OutboxClaimRef, OutboxStore};
use crate::projection_protocol::{ProjectionChangeRetention, SameTransactionProjectionBatch};
use crate::read_model::{ReadModelLoadGraph, ReadModelLoadRequest, ReadModelQueryCapabilities};
use crate::repository::{
    validate_commit_batch, CommitBatch, GetStream, InboxReceipt, InboxStore, PreparedEventAppend,
    ReadModelWritePlanStore, RelationalReadModelQueryStore, RepositoryError, SnapshotStore,
    SnapshotWrite, StreamIdentity, TransactionalCommit,
};
use crate::snapshot::SnapshotRecord;
use crate::sqlx_repo::projection_protocol::{
    apply_same_transaction_projection_in_tx, reject_causal_table_writes_in_tx,
    PROJECTION_CHANGE_NOTIFY_TABLE,
};
use crate::sqlx_repo::read_model::{
    apply_read_model_write_plan_in_tx, begin_read_model_tx, commit_read_model_tx,
    load_read_model_graph, remember_read_model_schemas, sql_read_model_capabilities,
    validate_sql_write_plan, SqlxReadModelBackend,
};
use crate::sqlx_repo::{audited_table_schema_sql, repository_u64_from_i64};
use crate::table::{
    generate_table_migration_artifacts, table_schema_bootstrap_result, table_schema_statements,
    TableMigrationArtifact, TableSchemaBootstrap, TableSchemaRegistry, TableSqlDialect,
    TableSqlSchemaAdapter, TableStoreError,
};
use crate::table::{
    TableAdapterCapabilities as ReadModelAdapterCapabilities,
    TableCommitOutcome as ReadModelCommitOutcome, TableStoreError as ReadModelError,
    TableWritePlan as ReadModelWritePlan,
};

mod backend;
mod commit;
mod errors;
mod events;
mod executor;
pub(crate) use executor::ConnectionExecutor;
mod inbox;
mod outbox;
mod read_models;
mod snapshots;
mod streams;
mod types;

use backend::*;
use events::*;
use outbox::*;
use snapshots::*;

pub(crate) use backend::embedded_migrator;
pub use backend::SqlxRepoBackend;
#[cfg(feature = "postgres")]
pub(crate) use backend::POSTGRES_MIGRATIONS;
#[cfg(feature = "sqlite")]
pub(crate) use backend::SQLITE_MIGRATIONS;
pub(crate) use errors::{repository_storage_error, system_time_epoch_secs};
#[cfg(feature = "postgres")]
pub(crate) use outbox::outbox_message_from_row;
pub use types::{SqlxOutboxStore, SqlxRepository};
