use std::future::Future;

use crate::entity::{Entity, EventRecord};
use crate::outbox::OutboxMessage;
use crate::read_model::{ReadModelLoadGraph, ReadModelLoadRequest, ReadModelQueryCapabilities};
use crate::snapshot::SnapshotRecord;
use crate::table::{TableAdapterCapabilities, TableCommitOutcome, TableStoreError, TableWritePlan};

use super::inbox::InboxReceipt;
use super::{RepositoryError, StreamIdentity};

/// One aggregate event stream staged for an async transactional commit.
pub struct StreamWrite<'a> {
    pub identity: StreamIdentity,
    pub entity: &'a mut Entity,
}

impl<'a> StreamWrite<'a> {
    pub fn new(identity: StreamIdentity, entity: &'a mut Entity) -> Self {
        Self { identity, entity }
    }
}

/// Snapshot writes staged in an async transactional commit.
#[derive(Clone, Debug)]
pub enum SnapshotWrite {
    Save {
        identity: StreamIdentity,
        record: SnapshotRecord,
    },
}

/// A structured async write batch that must commit under one backend transaction.
pub struct CommitBatch<'a> {
    pub streams: Vec<StreamWrite<'a>>,
    pub outbox_messages: Vec<OutboxMessage>,
    pub read_model_plans: Vec<TableWritePlan>,
    pub snapshots: Vec<SnapshotWrite>,
    /// Consumer inbox receipts to record in the same transaction (the optional
    /// effectively-once effect fence). Empty for the default idempotent path.
    pub inbox_receipts: Vec<InboxReceipt>,
}

impl<'a> CommitBatch<'a> {
    pub fn new(streams: Vec<StreamWrite<'a>>) -> Self {
        Self {
            streams,
            outbox_messages: Vec::new(),
            read_model_plans: Vec::new(),
            snapshots: Vec::new(),
            inbox_receipts: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }
}

/// Append data prepared from a borrowed stream write before async I/O. Events
/// are borrowed from the staged entity — backends bind them by reference, so
/// preparing a batch never clones event payloads.
#[derive(Clone, Debug)]
pub struct PreparedEventAppend<'a> {
    pub identity: StreamIdentity,
    pub expected_version: u64,
    pub events: &'a [EventRecord],
}

impl<'a> PreparedEventAppend<'a> {
    pub fn from_stream_write(write: &'a StreamWrite<'_>) -> Self {
        Self {
            identity: write.identity.clone(),
            expected_version: write.entity.committed_version(),
            events: write.entity.new_events(),
        }
    }
}

/// Stream-aware aggregate loading.
pub trait GetStream: Send + Sync {
    fn get_stream<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a;

    /// Load the streams for the provided identities, skipping missing ones.
    ///
    /// The default loads each stream with [`get_stream`], which is always
    /// correct, just one round trip per identity. Backends with a queryable
    /// store (Postgres, SQLite) override this with a single grouped query.
    /// Backends may return entities in storage order rather than input order.
    ///
    /// [`get_stream`]: GetStream::get_stream
    fn get_streams<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<Entity>, RepositoryError>> + Send + 'a {
        async move {
            let mut entities = Vec::with_capacity(identities.len());
            for identity in identities {
                if let Some(entity) = self.get_stream(identity).await? {
                    entities.push(entity);
                }
            }
            Ok(entities)
        }
    }

    /// Load only the events with `sequence > after_version` as a tail-only
    /// [`Entity`] (see [`Entity::load_tail_from_history`]).
    ///
    /// This is the I/O half of snapshot loading: a snapshot covers events up to
    /// its version, so only the tail must be fetched and decoded. Backends with
    /// a queryable store (Postgres, SQLite) override this with a
    /// `WHERE sequence > $after_version` read; the default delegates to
    /// [`get_stream`], which loads the full history and is always correct, just
    /// not optimized.
    ///
    /// The returned entity's `version`/`committed_version` reflect the true
    /// persisted stream position (`max(sequence)`), not the tail length.
    /// When `after_version` is past the stream (stale or planted snapshot
    /// cache), prefix is clamped so hydrate treats that snapshot as a miss.
    ///
    /// [`get_stream`]: GetStream::get_stream
    fn get_stream_tail<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        after_version: u64,
    ) -> impl Future<Output = Result<Option<Entity>, RepositoryError>> + Send + 'a {
        // Default: no tail optimization. Loading the full history yields the
        // same hydrated aggregate; only the I/O is heavier.
        let _ = after_version;
        self.get_stream(identity)
    }
}

/// Transactional commit capability for durable persistence backends.
pub trait TransactionalCommit: Send + Sync {
    fn commit_batch<'a>(
        &'a self,
        batch: CommitBatch<'a>,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a;
}

/// Consumer inbox read capability: check whether a `(consumer, message_id)`
/// receipt has already been recorded.
///
/// The pre-check lets a consumer skip re-running a handler for an already-processed
/// message (and ack the redelivery) before opening a transaction. The
/// authoritative dedupe is still the receipt's `(consumer, message_id)` primary
/// key written in [`commit_batch`](TransactionalCommit::commit_batch),
/// which fences the race where two deliveries both pass the pre-check.
pub trait InboxStore: Send + Sync {
    fn inbox_contains<'a>(
        &'a self,
        consumer: &'a str,
        message_id: &'a str,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a;

    /// Purge inbox receipts older than `age`, returning the number removed.
    ///
    /// The consumer inbox grows by one row per processed message and has no
    /// built-in TTL: **retention is the operator's responsibility.** Once a
    /// transport's own redelivery window has passed, an old receipt can never
    /// gate a replay again, so it is safe to delete. Call this periodically
    /// (e.g. a cron/maintenance task) with an `age` comfortably larger than the
    /// broker's maximum redelivery/visibility window.
    ///
    /// Age is evaluated against the **database clock**, not the caller's, so
    /// there is no client/server skew. SQL backends issue a single bounded
    /// `DELETE`; the in-memory store keeps no timestamps and treats any positive
    /// `age` as a no-op (see [`InMemoryRepository`](crate::InMemoryRepository),
    /// whose inbox is dev-only).
    fn purge_inbox_older_than(
        &self,
        age: std::time::Duration,
    ) -> impl Future<Output = Result<u64, RepositoryError>> + Send;
}

/// Repository trait for types that implement stream reads and commits.
pub trait Repository: GetStream + TransactionalCommit {}

impl<T> Repository for T where T: GetStream + TransactionalCommit {}

/// Adapter contract for committing read-model write plans.
pub trait ReadModelWritePlanStore: Send + Sync {
    fn read_model_capabilities(&self) -> TableAdapterCapabilities;

    fn commit_write_plan(
        &self,
        plan: TableWritePlan,
    ) -> impl Future<Output = Result<TableCommitOutcome, TableStoreError>> + Send + '_;
}

/// Primary-key relational read-model query contract.
pub trait RelationalReadModelQueryStore: Send + Sync {
    fn read_model_query_capabilities(&self) -> ReadModelQueryCapabilities;

    /// Optional bounded normalized-row scan. Unsupported adapters fail closed.
    fn scan_read_model(
        &self,
        _request: crate::read_model::ReadModelScanRequest,
    ) -> impl Future<Output = Result<crate::read_model::ReadModelScanPage, TableStoreError>> + Send + '_ {
        async { Err(TableStoreError::Metadata("bounded read-model scan is unsupported by this adapter".into())) }
    }

    fn load_graph(
        &self,
        request: ReadModelLoadRequest,
    ) -> impl Future<Output = Result<ReadModelLoadGraph, TableStoreError>> + Send + '_;
}

/// Snapshot persistence keyed by full stream identity.
pub trait SnapshotStore: Send + Sync {
    fn get_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, RepositoryError>> + Send + 'a;

    /// Load the snapshots for the provided identities, skipping identities
    /// without one. Each returned record carries its own aggregate type/id, so
    /// callers can pair records back to identities.
    ///
    /// The default loads each snapshot with [`get_snapshot`], which is always
    /// correct, just one round trip per identity. Backends with a queryable
    /// store (Postgres, SQLite) override this with a single grouped query.
    ///
    /// [`get_snapshot`]: SnapshotStore::get_snapshot
    fn get_snapshots<'a>(
        &'a self,
        identities: &'a [StreamIdentity],
    ) -> impl Future<Output = Result<Vec<SnapshotRecord>, RepositoryError>> + Send + 'a {
        async move {
            let mut records = Vec::with_capacity(identities.len());
            for identity in identities {
                if let Some(record) = self.get_snapshot(identity).await? {
                    records.push(record);
                }
            }
            Ok(records)
        }
    }

    fn save_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
        record: SnapshotRecord,
    ) -> impl Future<Output = Result<(), RepositoryError>> + Send + 'a;

    fn delete_snapshot<'a>(
        &'a self,
        identity: &'a StreamIdentity,
    ) -> impl Future<Output = Result<bool, RepositoryError>> + Send + 'a;
}
