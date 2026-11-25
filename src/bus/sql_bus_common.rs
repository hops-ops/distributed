//! Shared machinery for the SQL-backed buses (`PostgresBus`, `SqliteBus`).
//!
//! Postgres and SQLite implement the same bus model — a claim-lease work queue
//! (`bus_queue`) for point-to-point commands, and an append-only log plus a
//! durable generation identity and per-consumer offset table (`bus_log` +
//! `bus_log_identity` + `bus_offset`) for fan-out events.
//! They differ only in SQL dialect (placeholder style, database clock,
//! name-list binding, claim-token minting) and pool type. Mirroring
//! [`lock::sqlx_common`](crate::lock), everything else — the builders, the
//! [`Bus`]/[`BusConsumer`] impls, the queue/log sources, row decoding, and
//! settlement — lives here, generic over a [`SqlBusDialect`].
//!
//! ## Settlement model
//!
//! - **Queue (`listen`):** a claim mints a fresh `claim_token` under a lease;
//!   every settlement is fenced by `seq AND claim_token`, so a worker whose
//!   lease expired (and whose row was reclaimed under a new token) cannot
//!   settle the newer claim. `ack`/`dead_letter`/`park` delete the row; `nack`
//!   releases it for redelivery.
//! - **Log (`subscribe`):** `ack` advances the consumer's offset to the entry's
//!   `seq` (the effectively-once point); `nack` leaves the offset unmoved so the
//!   entry is re-read; `dead_letter`/`park` advance past poison entries.
//!
//! ## Corruption handling
//!
//! A row that fails to decode a required column (`name`, `kind`, `payload`,
//! `content_type`, `metadata`) is permanent corruption. Returning a placeholder
//! message would route it to the runner's ack-and-ignore path, silently deleting
//! the row (queue) or advancing the offset past it (log) with no trace. Instead
//! the decode failure is surfaced through [`ReceivedMessage::decode_error`] so
//! the runner settles the claim through the configured failure policy
//! (dead-letter by default), like any other permanent failure.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sqlx::{ColumnIndex, Decode, Row, Type};

use crate::projection_protocol::{ProjectionEpoch, ProjectionSource};
use crate::sqlx_repo::is_sqlx_transient;

use super::source::{MessageSource, ReceivedMessage};
use super::{
    run_source, Bus, BusConsumer, BusTopologyConfig, MessageRouter, OrderedDelivery,
    PublishDeadLetters, RunOptions, TransportError, TransportErrorKind,
};
use super::{Message, MessageKind, WireEnvelope};

pub(crate) const DEFAULT_LEASE: Duration = Duration::from_secs(30);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// `bus_log` entries fetched per log-source refill. Buffering amortizes the
/// offset subquery across a batch instead of re-running it for every message.
/// The queue source claims one row per `recv` instead, so no claimed row waits
/// behind another's handler.
const SOURCE_BATCH: i64 = 16;

/// Classify a database error transient vs permanent (via [`is_sqlx_transient`])
/// and wrap it as a `"{backend} bus {context}"` transport error. Deterministic
/// failures reach the failure policy instead of redelivering forever.
pub(crate) fn db_err(backend: &str, context: &str, err: sqlx::Error) -> TransportError {
    let kind = if is_sqlx_transient(&err) {
        TransportErrorKind::Retryable
    } else {
        TransportErrorKind::Permanent
    };
    TransportError::new(kind, format!("{backend} bus {context}: {err}")).with_source(err)
}

pub(crate) fn metadata_json(message: &Message) -> String {
    serde_json::to_string(&message.metadata).unwrap_or_else(|_| "[]".into())
}

fn corrupt_row(backend: &str, message: impl Into<String>) -> TransportError {
    TransportError::permanent(format!("{backend} bus corrupt row: {}", message.into()))
}

fn decode_err(backend: &str, column: &str, err: sqlx::Error) -> TransportError {
    corrupt_row(
        backend,
        format!("required column '{column}' failed to decode: {err}"),
    )
}

fn parse_message_kind(backend: &str, value: &str) -> Result<MessageKind, TransportError> {
    match value {
        "command" => Ok(MessageKind::Command),
        "event" => Ok(MessageKind::Event),
        _ => Err(corrupt_row(
            backend,
            format!("required column 'kind' has unsupported value {value:?}"),
        )),
    }
}

fn parse_metadata(backend: &str, value: &str) -> Result<Vec<(String, String)>, TransportError> {
    serde_json::from_str(value).map_err(|err| {
        corrupt_row(
            backend,
            format!("required column 'metadata' failed to parse as JSON metadata: {err}"),
        )
    })
}

/// Reconstruct a [`Message`] from a claimed `bus_queue`/`bus_log` row.
///
/// Required-column decode/parsing failures are permanent corruption. The row has
/// already been selected or claimed, so callers surface the error through
/// [`ReceivedMessage::decode_error`] and let the runner settle it through the
/// configured failure policy.
pub(crate) fn message_from_row<R>(backend: &str, row: &R) -> Result<Message, TransportError>
where
    R: Row,
    for<'a> &'a str: ColumnIndex<R>,
    for<'r> String: Decode<'r, R::Database> + Type<R::Database>,
    for<'r> Vec<u8>: Decode<'r, R::Database> + Type<R::Database>,
{
    let name: String = row
        .try_get("name")
        .map_err(|err| decode_err(backend, "name", err))?;
    let kind: String = row
        .try_get("kind")
        .map_err(|err| decode_err(backend, "kind", err))?;
    let payload: Vec<u8> = row
        .try_get("payload")
        .map_err(|err| decode_err(backend, "payload", err))?;
    let content_type: String = row
        .try_get("content_type")
        .map_err(|err| decode_err(backend, "content_type", err))?;
    if content_type.is_empty() {
        return Err(corrupt_row(
            backend,
            "required column 'content_type' is empty",
        ));
    }
    let metadata_json: String = row
        .try_get("metadata")
        .map_err(|err| decode_err(backend, "metadata", err))?;
    let metadata = parse_metadata(backend, &metadata_json)?;

    Ok(Message {
        id: row
            .try_get::<Option<String>, _>("message_id")
            .unwrap_or(None),
        name,
        kind: parse_message_kind(backend, &kind)?,
        payload,
        content_type,
        metadata,
    })
}

/// Verify that a retry using an existing stable message ID still names the
/// exact envelope a causal projector observes.
///
/// Trace metadata is deliberately excluded: an outbox retry may acquire a new
/// span after an ambiguous publish acknowledgement. Causation is included
/// because it is part of projection receipt identity. The first committed row
/// remains authoritative for all other metadata.
pub(crate) fn validate_log_retry(
    backend: &str,
    existing: &Message,
    retry: &Message,
) -> Result<(), TransportError> {
    let matches = existing.name == retry.name
        && existing.kind == retry.kind
        && existing.payload == retry.payload
        && existing.content_type == retry.content_type
        && existing.causation_id() == retry.causation_id();
    if matches {
        return Ok(());
    }

    Err(TransportError::permanent(format!(
        "{backend} bus ordered-log message ID {:?} was reused with a different \
         name, kind, payload, content type, or causation ID",
        retry.id()
    )))
}

fn fresh_log_epoch() -> ProjectionEpoch {
    ProjectionEpoch::new(format!("sql-log-{}", uuid::Uuid::now_v7()))
        .expect("a UUID-backed SQL log epoch is valid")
}

/// A decoded `bus_queue`/`bus_log` row: its `seq` plus either the message or the
/// decode failure (with an empty placeholder message the runner never
/// dispatches — it sees `decode_error()` first).
pub struct ReceivedRow {
    seq: i64,
    message: Message,
    decode_error: Option<TransportError>,
}

impl ReceivedRow {
    /// Decode a row, capturing a required-column failure as `decode_error`
    /// instead of losing the claim. The placeholder message's name is
    /// deliberately empty — the decode error carries the diagnostic.
    pub(crate) fn from_row<R>(backend: &str, row: &R) -> Result<Self, TransportError>
    where
        R: Row,
        for<'a> &'a str: ColumnIndex<R>,
        for<'r> String: Decode<'r, R::Database> + Type<R::Database>,
        for<'r> Vec<u8>: Decode<'r, R::Database> + Type<R::Database>,
        for<'r> i64: Decode<'r, R::Database> + Type<R::Database>,
    {
        let seq = row
            .try_get("seq")
            .map_err(|err| decode_err(backend, "seq", err))?;
        let (message, decode_error) = match message_from_row(backend, row) {
            Ok(message) => (message, None),
            Err(error) => (
                Message::new("", MessageKind::Event, Vec::new()),
                Some(error),
            ),
        };
        Ok(Self {
            seq,
            message,
            decode_error,
        })
    }
}

/// A claimed `bus_queue` row: the decoded row plus the claim token that fences
/// its settlement.
pub struct ClaimedRow {
    pub(crate) row: ReceivedRow,
    pub(crate) claim_token: String,
    /// Deliveries of this row so far, counting this claim.
    pub(crate) attempts: u32,
}

/// The dialect-specific SQL a [`SqlBus`] runs. Implemented by the per-backend
/// dialects; the shared bus, sources, and settle handles drive it.
pub trait SqlBusDialect: Clone + Send + Sync + 'static {
    /// Backend name used in error messages and consumer-group resolution.
    const BACKEND: &'static str;
    /// Idempotent DDL for the queue, log, log identity, and offsets.
    const SCHEMA: &'static str;

    /// Execute one DDL statement from [`SCHEMA`](Self::SCHEMA) (hence
    /// `'static`: statements are always slices of the schema const).
    fn execute_ddl(
        &self,
        statement: &'static str,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Insert a command into `bus_queue`.
    fn insert_queue(
        &self,
        message: &Message,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Append an event to `bus_log`.
    fn insert_log(
        &self,
        message: &Message,
        epoch_candidate: &ProjectionEpoch,
        expected_epoch: Option<&ProjectionEpoch>,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Load the epoch durably paired with `bus_log`, creating it from
    /// `epoch_candidate` when this is a new log.
    ///
    /// Implementations also detect one specific continuity violation: an
    /// observed `MAX(seq)` below the persisted high-water mark. That condition
    /// fails closed; a caller-provided candidate is never authorization to
    /// rotate an existing cursor domain.
    fn prepare_log_epoch(
        &self,
        epoch_candidate: &ProjectionEpoch,
        expected_epoch: Option<&ProjectionEpoch>,
    ) -> impl Future<Output = Result<ProjectionEpoch, TransportError>> + Send;

    /// Compare-and-swap reset of the ordered log and its cursor domain.
    ///
    /// Implementations lock the current identity, verify `expected_epoch`,
    /// clear the log and every offset, reset the backend sequence, and install
    /// the distinct `next_epoch` with generation incremented and high-water
    /// zero in one transaction.
    fn reset_log(
        &self,
        expected_epoch: &ProjectionEpoch,
        next_epoch: &ProjectionEpoch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Atomically claim up to `limit` available `bus_queue` rows (lowest `seq`
    /// first) whose `name` matches one of `names` **or is NULL** (un-routable
    /// corruption that must be claimed so the failure policy can settle it, not
    /// left as a poison row blocking the drain), minting fresh claim tokens
    /// under `lease_secs`. Returned rows need not be sorted; the source sorts.
    fn claim(
        &self,
        names: &[String],
        lease_secs: f64,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<ClaimedRow>, TransportError>> + Send;

    /// Read up to `limit` `bus_log` entries past `consumer`'s offset, in `seq`
    /// order, whose `name` matches one of `names` **or is NULL** (surfaced, not
    /// silently skipped, so the failure policy advances the offset past poison
    /// entries).
    fn log_read(
        &self,
        names: &[String],
        consumer: &str,
        limit: i64,
        expected_epoch: &ProjectionEpoch,
    ) -> impl Future<Output = Result<Vec<ReceivedRow>, TransportError>> + Send;

    /// Fail unless `expected_epoch` is still the durable identity paired with
    /// the current log generation.
    fn verify_log_epoch(
        &self,
        expected_epoch: &ProjectionEpoch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Delete a claimed queue row, fenced by its claim token.
    fn delete_claimed(
        &self,
        seq: i64,
        claim_token: &str,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Extend a claimed queue row's lease to `lease_secs` from now, fenced by
    /// its claim token. `false` when the claim is no longer held.
    fn renew_claim(
        &self,
        seq: i64,
        claim_token: &str,
        lease_secs: f64,
    ) -> impl Future<Output = Result<bool, TransportError>> + Send;

    /// Release a claimed queue row for redelivery, fenced by its claim token.
    fn release_claim(
        &self,
        seq: i64,
        claim_token: &str,
        delay_secs: f64,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Advance `consumer`'s log offset to `seq` (monotonic upsert).
    fn advance_offset(
        &self,
        consumer: &str,
        seq: i64,
        expected_epoch: &ProjectionEpoch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;
}

/// SQL-backed [`Bus`] + [`BusConsumer`], generic over a [`SqlBusDialect`].
/// Cheap to clone (the dialect wraps a pool, which is an `Arc`).
#[derive(Clone)]
pub struct SqlBus<B> {
    dialect: B,
    topology: BusTopologyConfig,
    lease: Duration,
    source_epoch: Option<ProjectionEpoch>,
    /// How long an idle consumer waits before it looks for work again.
    poll_interval: Duration,
}

impl<B: SqlBusDialect> SqlBus<B> {
    pub(crate) fn from_dialect(dialect: B) -> Self {
        Self {
            dialect,
            topology: BusTopologyConfig::default(),
            lease: DEFAULT_LEASE,
            source_epoch: None,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Set an explicit durable event subscription group.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    /// Override the claim lease for `listen`: how long a claimed command stays
    /// invisible to other workers after its worker stops renewing it. A worker
    /// renews the lease every third of it while the handler runs, so a handler
    /// may take longer than the lease; the lease bounds how long a crashed
    /// worker's command waits for redelivery.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Require an operator-controlled epoch for this append-only bus log.
    ///
    /// On a new empty log this value becomes its durable identity. It is also
    /// required to explicitly adopt a retained nonempty log whose identity was
    /// lost; adoption invalidates offsets that cannot be bound to the supplied
    /// epoch. On an existing generation it must exactly match the persisted
    /// identity. A builder can never relabel an identified generation or
    /// authorize a reset.
    pub fn with_source_epoch(mut self, epoch: ProjectionEpoch) -> Self {
        self.source_epoch = Some(epoch);
        self
    }

    /// How long an idle `listen`/`subscribe` waits before it looks for work
    /// again. Default 25 ms.
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Create the bus tables (queue, log, log identity, and offsets) if absent.
    ///
    /// Called by `listen`/`subscribe`; producers must ensure the tables exist
    /// before `send`/`publish`, either by calling this or through migrations.
    pub async fn ensure_tables(&self) -> Result<(), TransportError> {
        for statement in B::SCHEMA.split(';') {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            self.dialect.execute_ddl(statement).await?;
        }
        // Persist the identity beside the log even for producer-only setups.
        // Backups and explicit resets must treat `bus_log` and
        // `bus_log_identity` as one unit.
        let epoch_candidate = self.source_epoch.clone().unwrap_or_else(fresh_log_epoch);
        self.dialect
            .prepare_log_epoch(&epoch_candidate, self.source_epoch.as_ref())
            .await?;
        Ok(())
    }

    /// Destructively begin a new ordered-log cursor generation.
    ///
    /// This is the only supported way to reuse numeric `bus_log` positions.
    /// It is fenced like compare-and-swap: `expected_epoch` must still be the
    /// durable identity, and `next_epoch` must be distinct. On success the log,
    /// backend sequence, and every consumer offset are cleared atomically while
    /// the generation is incremented and the high-water mark returns to zero.
    ///
    /// A handler already dispatched from the retired generation cannot be
    /// cancelled or have its application effects rolled back by this call.
    /// Its later offset settlement is epoch-fenced and fails permanently; the
    /// operator must stop consumers before reset and restart them against the
    /// new generation. Projection handlers should still commit effects and
    /// their own idempotency/checkpoint state transactionally.
    pub async fn reset_ordered_log(
        &self,
        expected_epoch: &ProjectionEpoch,
        next_epoch: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        if expected_epoch == next_epoch {
            return Err(TransportError::permanent(
                "ordered-log reset requires a distinct next epoch",
            ));
        }
        self.dialect.reset_log(expected_epoch, next_epoch).await
    }
}

impl<B: SqlBusDialect> SqlBus<B> {
    /// Dead letters are published to this bus's log unless the run names a
    /// sink.
    fn with_dead_letters(&self, options: RunOptions) -> RunOptions {
        options.or_dead_letters(|| Arc::new(PublishDeadLetters(self.clone())))
    }
}

impl<B: SqlBusDialect> Bus for SqlBus<B> {
    async fn send_message(&self, message: Message) -> Result<(), TransportError> {
        WireEnvelope::validate(&message)?;
        self.dialect.insert_queue(&message).await
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        WireEnvelope::validate(&message)?;
        let epoch_candidate = self.source_epoch.clone().unwrap_or_else(fresh_log_epoch);
        self.dialect
            .insert_log(&message, &epoch_candidate, self.source_epoch.as_ref())
            .await
    }
}

impl<B: SqlBusDialect> BusConsumer for SqlBus<B> {
    async fn listen<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        self.ensure_tables().await?;
        let names = router.subscription_plan().commands;
        if names.is_empty() {
            return Ok(());
        }
        let source = SqlQueueSource {
            dialect: self.dialect.clone(),
            names,
            lease: self.lease,
            poll_interval: self.poll_interval,
        };
        run_source(router, source, self.with_dead_letters(options)).await
    }

    async fn subscribe<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        self.ensure_tables().await?;
        let names = router.subscription_plan().events;
        if names.is_empty() {
            return Ok(());
        }
        let group = self
            .topology
            .resolve_consumer_group(router.as_ref(), B::BACKEND)?;
        let epoch_candidate = self.source_epoch.clone().unwrap_or_else(fresh_log_epoch);
        let source_epoch = self
            .dialect
            .prepare_log_epoch(&epoch_candidate, self.source_epoch.as_ref())
            .await?;
        let source = SqlLogSource {
            dialect: self.dialect.clone(),
            names,
            consumer: group,
            buffer: VecDeque::new(),
            last_delivered: None,
            settled_seq: Arc::new(AtomicI64::new(0)),
            source_epoch,
            poll_interval: self.poll_interval,
        };
        run_source(router, source, self.with_dead_letters(options)).await
    }
}

/// Competing-consumer source over `bus_queue` (atomic claim under a lease).
///
/// One row is claimed per `recv`, and its lease is renewed while the delivery
/// is held, so a handler longer than the lease keeps its claim and no other
/// worker runs the row concurrently. Settlement is fenced by the claim token:
/// a worker that lost its claim (its renewals stopped reaching the database
/// and the row was reclaimed under a new token) cannot settle the newer claim.
struct SqlQueueSource<B> {
    dialect: B,
    names: Vec<String>,
    lease: Duration,
    poll_interval: Duration,
}

impl<B: SqlBusDialect> MessageSource for SqlQueueSource<B> {
    type Received = SqlQueueReceived<B>;

    fn transport_name(&self) -> &'static str {
        B::BACKEND
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        let claimed = self
            .dialect
            .claim(&self.names, self.lease.as_secs_f64(), 1)
            .await?;
        Ok(claimed.into_iter().next().map(|claimed| {
            let renewal = tokio::spawn(renew_lease(
                self.dialect.clone(),
                claimed.row.seq,
                claimed.claim_token.clone(),
                self.lease,
            ))
            .abort_handle();
            SqlQueueReceived {
                dialect: self.dialect.clone(),
                row: claimed.row,
                claim_token: claimed.claim_token,
                attempts: claimed.attempts,
                renewal,
            }
        }))
    }

    async fn ready(&mut self) {
        tokio::time::sleep(self.poll_interval).await
    }
}

/// Keep a claim alive: extend its lease every third of the lease until the
/// claim is lost. Runs until the delivery is settled or dropped, which aborts
/// it. A failed renewal is retried at the next tick; the claim is lost only
/// once the lease runs out with no renewal landing.
async fn renew_lease<B: SqlBusDialect>(dialect: B, seq: i64, claim_token: String, lease: Duration) {
    let every = (lease / 3).max(Duration::from_millis(10));
    loop {
        tokio::time::sleep(every).await;
        match dialect
            .renew_claim(seq, &claim_token, lease.as_secs_f64())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    backend = B::BACKEND,
                    seq,
                    "bus_queue claim was lost while its handler ran; another worker may run it"
                );
                return;
            }
            Err(error) => tracing::warn!(
                backend = B::BACKEND,
                seq,
                %error,
                "renewing a bus_queue claim failed; retrying"
            ),
        }
    }
}

/// A claimed `bus_queue` row: `ack` deletes it; `nack` makes it claimable
/// again after the delay. Its lease is renewed until it is settled or dropped.
pub struct SqlQueueReceived<B> {
    dialect: B,
    row: ReceivedRow,
    claim_token: String,
    attempts: u32,
    renewal: tokio::task::AbortHandle,
}

impl<B> Drop for SqlQueueReceived<B> {
    fn drop(&mut self) {
        self.renewal.abort();
    }
}

impl<B: SqlBusDialect> ReceivedMessage for SqlQueueReceived<B> {
    fn message(&self) -> &Message {
        &self.row.message
    }

    fn attempts(&self) -> u32 {
        self.attempts
    }

    fn decode_error(&self) -> Option<&TransportError> {
        self.row.decode_error.as_ref()
    }

    async fn ack(self) -> Result<(), TransportError> {
        // Stop renewing first. A renewal already in flight is fenced by the
        // claim token, so it cannot revive the row once it is settled.
        self.renewal.abort();
        self.dialect
            .delete_claimed(self.row.seq, &self.claim_token)
            .await
    }

    async fn nack(self, delay: Duration) -> Result<(), TransportError> {
        self.renewal.abort();
        self.dialect
            .release_claim(self.row.seq, &self.claim_token, delay.as_secs_f64())
            .await
    }
}

/// Fan-out source over `bus_log`: reads the next entry past this consumer's
/// offset for its subscribed names, in `seq` order.
/// Reads [`SOURCE_BATCH`] entries past the offset per query and buffers them.
///
/// Buffered read-ahead must not break the offset contract: entries advance the
/// offset **in order**, and a nacked entry is re-read before anything after it.
/// The handles report forward settlement (ack) through the
/// shared `settled_seq`; when the previously delivered entry was *not* settled
/// forward (a nack — offset unmoved), the buffer is discarded so the next read
/// starts again from the durable offset. The runner settles each message before
/// the next `recv`, so this check is race-free.
struct SqlLogSource<B> {
    dialect: B,
    names: Vec<String>,
    consumer: String,
    buffer: VecDeque<ReceivedRow>,
    /// `seq` of the entry most recently handed to the runner.
    last_delivered: Option<i64>,
    /// Highest `seq` settled forward by this source's handles.
    settled_seq: Arc<AtomicI64>,
    source_epoch: ProjectionEpoch,
    poll_interval: Duration,
}

impl<B: SqlBusDialect> MessageSource for SqlLogSource<B> {
    type Received = SqlLogReceived<B>;

    fn transport_name(&self) -> &'static str {
        B::BACKEND
    }

    async fn ready(&mut self) {
        tokio::time::sleep(self.poll_interval).await
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        // Validate even when `buffer` already contains read-ahead. A log reset
        // retires every cached row from the prior generation immediately.
        self.dialect.verify_log_epoch(&self.source_epoch).await?;
        if let Some(last) = self.last_delivered {
            if self.settled_seq.load(Ordering::Acquire) < last {
                // The last entry was nacked: the offset did not move, so the
                // buffered read-ahead would skip past it. Drop it and re-read
                // from the durable offset (re-delivering the nacked entry).
                self.buffer.clear();
            }
        }
        if self.buffer.is_empty() {
            let rows = self
                .dialect
                .log_read(
                    &self.names,
                    &self.consumer,
                    SOURCE_BATCH,
                    &self.source_epoch,
                )
                .await?;
            self.buffer.extend(rows);
        }
        let Some(row) = self.buffer.pop_front() else {
            return Ok(None);
        };
        let position = u64::try_from(row.seq).map_err(|_| {
            corrupt_row(
                B::BACKEND,
                format!(
                    "bus_log seq {} is outside the projection cursor domain",
                    row.seq
                ),
            )
        })?;
        let source = ProjectionSource::new(format!("{}.bus_log", B::BACKEND), b"global".to_vec())
            .map_err(|error| corrupt_row(B::BACKEND, error.to_string()))?;
        let ordered = OrderedDelivery::new(source, self.source_epoch.clone(), position, false)
            .map_err(|error| corrupt_row(B::BACKEND, error.to_string()))?;
        self.last_delivered = Some(row.seq);
        Ok(Some(SqlLogReceived {
            dialect: self.dialect.clone(),
            consumer: self.consumer.clone(),
            settled_seq: self.settled_seq.clone(),
            row,
            ordered,
        }))
    }
}

/// A `bus_log` entry: `ack` advances this consumer's offset to its `seq` (the
/// effectively-once point); `nack` leaves the offset, so the entry is read
/// again and nothing after it is delivered first.
pub struct SqlLogReceived<B> {
    dialect: B,
    consumer: String,
    settled_seq: Arc<AtomicI64>,
    row: ReceivedRow,
    ordered: OrderedDelivery,
}

impl<B: SqlBusDialect> SqlLogReceived<B> {
    /// Advance the durable offset, then record the forward settlement so the
    /// source knows its buffered read-ahead is still valid.
    async fn settle_forward(self) -> Result<(), TransportError> {
        self.dialect
            .advance_offset(&self.consumer, self.row.seq, self.ordered.epoch())
            .await?;
        self.settled_seq.store(self.row.seq, Ordering::Release);
        Ok(())
    }
}

impl<B: SqlBusDialect> ReceivedMessage for SqlLogReceived<B> {
    fn message(&self) -> &Message {
        &self.row.message
    }

    fn ordered_delivery(&self) -> Option<&OrderedDelivery> {
        Some(&self.ordered)
    }

    fn decode_error(&self) -> Option<&TransportError> {
        self.row.decode_error.as_ref()
    }

    async fn ack(self) -> Result<(), TransportError> {
        self.settle_forward().await
    }

    async fn nack(self, delay: Duration) -> Result<(), TransportError> {
        // The offset stays where it is. A log cannot deliver a later entry
        // first, so the delay is spent here, holding the position.
        tokio::time::sleep(delay).await;
        Ok(())
    }
}
