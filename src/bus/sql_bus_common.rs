//! The SQL-backed bus (`PostgresBus`, `SqliteBus`), written once.
//!
//! Postgres and SQLite run the same bus model — a claim-lease work queue
//! (`bus_queue`) for point-to-point commands, and an append-only log plus a
//! durable generation identity, per-group offsets and per-group leases
//! (`bus_log` + `bus_log_identity` + `bus_offset` + `bus_consumer_lease`) for
//! fan-out events. Every statement and all control flow live here, written
//! with [`QueryBuilder`] so each backend renders its own placeholders. A
//! backend contributes only a [`SqlBusDialect`]: its schema, how it opens a
//! writing transaction and locks a row, its clock, name-list binding,
//! claim-token minting, `GREATEST`, and how it empties the log.
//!
//! ## Settlement model
//!
//! - **Queue (`listen`):** a claim mints a fresh `claim_token` under a lease;
//!   every settlement is fenced by `seq AND claim_token`, so a worker whose
//!   lease expired (and whose row was reclaimed under a new token) cannot
//!   settle the newer claim. `ack` deletes the row; `nack` releases it for
//!   redelivery after the delay.
//! - **Log (`subscribe`):** one consumer of a group reads at a time. It holds
//!   the group's lease in `bus_consumer_lease` while it reads, renews it while
//!   it works, and releases it when its run ends; the other consumers of the
//!   group wait (a crashed holder's lease runs out after the bus lease). `ack`
//!   records the entry as done; the group's offset is written once per batch,
//!   in the transaction that reads the next batch, and when the run ends. A
//!   `nack` leaves the offset where it is, so the entry is read again before
//!   anything after it.
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

use sqlx::{
    ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Pool, QueryBuilder, Row,
    Transaction, Type,
};

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

/// `bus_log` entries read per batch. The group's offset is written once per
/// batch, so this also bounds how many acknowledged entries a crash
/// redelivers. The queue source claims one row per `recv` instead, so no
/// claimed row waits behind another's handler.
const LOG_BATCH: i64 = 16;

const ROW_COLUMNS: &str = "seq, name, message_id, kind, payload, content_type, metadata";

/// What differs between the SQL backends. Implemented on the `sqlx`
/// database type; everything else is [`SqlBus`].
pub trait SqlBusDialect: Database {
    /// Backend name used in error messages and consumer-group resolution.
    const BACKEND: &'static str;
    /// Idempotent DDL for the queue, log, log identity, offsets and leases,
    /// statements separated by `;`.
    const SCHEMA: &'static str;
    /// Opens a transaction that reads before it writes. SQLite takes its
    /// writer lock up front so the transaction waits for it instead of being
    /// refused when another writer committed in between.
    const BEGIN_WRITE: &'static str;
    /// Reads `source_epoch, high_water` of the log identity, locked against
    /// every other writer of the log (appends, resets, settlement).
    const LOCK_IDENTITY: &'static str;
    /// Reads `source_epoch` of the log identity, locked against a reset but
    /// not against other settlement.
    const SHARE_IDENTITY: &'static str;
    /// Row-lock suffix that skips rows another claim holds.
    const SKIP_LOCKED: &'static str;
    /// The larger of two integers (`GREATEST` or `MAX`).
    const GREATEST: &'static str;
    /// The database clock.
    const CLOCK: &'static str;
    /// An expression minting a fresh claim token for each row it is
    /// evaluated for.
    const NEW_CLAIM_TOKEN: &'static str;
    /// Statements that empty `bus_log` and restart its sequence.
    const CLEAR_LOG: &'static [&'static str];
    /// `SELECT CAST(count(*) AS BIGINT) AS present FROM ... WHERE ` over the
    /// tables and indexes visible to the bus, each with a `name` column: a
    /// catalog read that takes no lock on the tables it finds.
    const COUNT_SCHEMA_OBJECTS: &'static str;

    /// Push the database clock plus `seconds`.
    fn push_clock_plus(query: &mut QueryBuilder<Self>, seconds: f64);

    /// Push a condition that `name` is one of `names`.
    fn push_name_in(query: &mut QueryBuilder<Self>, names: &[String]);
}

/// The tables and indexes `schema` creates: the name after each
/// `IF NOT EXISTS`.
fn schema_objects(schema: &str) -> Vec<String> {
    schema
        .split("IF NOT EXISTS")
        .skip(1)
        .filter_map(|rest| {
            rest.split(|c: char| c.is_whitespace() || c == '(')
                .find(|word| !word.is_empty())
        })
        .map(str::to_string)
        .collect()
}

/// Classify a database error transient vs permanent (via [`is_sqlx_transient`])
/// and wrap it as a `"{backend} bus {context}"` transport error. Deterministic
/// failures reach the failure policy instead of redelivering forever.
fn db_err<DB: SqlBusDialect>(context: &str, err: sqlx::Error) -> TransportError {
    let kind = if is_sqlx_transient(&err) {
        TransportErrorKind::Retryable
    } else {
        TransportErrorKind::Permanent
    };
    TransportError::new(kind, format!("{} bus {context}: {err}", DB::BACKEND)).with_source(err)
}

fn metadata_json(message: &Message) -> String {
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

fn parse_epoch(backend: &str, value: String) -> Result<ProjectionEpoch, TransportError> {
    ProjectionEpoch::new(value).map_err(|error| {
        TransportError::permanent(format!("{backend} bus corrupt log identity epoch: {error}"))
    })
}

/// Verify that a retry using an existing stable message ID still names the
/// exact envelope a causal projector observes.
///
/// Trace metadata is deliberately excluded: an outbox retry may acquire a new
/// span after an ambiguous publish acknowledgement. Causation is included
/// because it is part of projection receipt identity. The first committed row
/// remains authoritative for all other metadata.
fn validate_log_retry(
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

fn check_epoch(
    backend: &str,
    actual: &ProjectionEpoch,
    expected: &ProjectionEpoch,
) -> Result<(), TransportError> {
    if actual == expected {
        return Ok(());
    }
    Err(TransportError::permanent(format!(
        "{backend} bus ordered-log epoch changed from {:?} to {:?}; the subscriber must restart",
        expected.as_str(),
        actual.as_str()
    )))
}

fn check_configured_epoch(
    backend: &str,
    durable: &ProjectionEpoch,
    configured: Option<&ProjectionEpoch>,
) -> Result<(), TransportError> {
    match configured {
        Some(configured) if configured != durable => Err(TransportError::permanent(format!(
            "{backend} bus configured source epoch {:?} does not match durable ordered-log \
             epoch {:?}",
            configured.as_str(),
            durable.as_str()
        ))),
        _ => Ok(()),
    }
}

/// A decoded `bus_queue`/`bus_log` row: its `seq` plus either the message or the
/// decode failure (with an empty placeholder message the runner never
/// dispatches — it sees `decode_error()` first).
pub struct ReceivedRow {
    seq: i64,
    message: Message,
    decode_error: Option<TransportError>,
}

/// A claimed `bus_queue` row: the decoded row plus the claim token that fences
/// its settlement.
pub struct ClaimedRow {
    row: ReceivedRow,
    claim_token: String,
    /// Deliveries of this row so far, counting this claim.
    attempts: u32,
}

/// One read of a group's log: write the acknowledged position, take or keep
/// the group's lease, and read what follows the offset.
pub struct LogBatch<'a> {
    names: &'a [String],
    consumer: &'a str,
    holder: &'a str,
    lease_secs: f64,
    flush: Option<i64>,
    epoch: &'a ProjectionEpoch,
}

/// Every statement the SQL bus runs, written once for every
/// [`SqlBusDialect`]. Implemented for each database whose types the bus can
/// bind; callers name only this trait.
pub trait SqlBusStore: SqlBusDialect {
    /// Whether every table and index [`SqlBusDialect::SCHEMA`] creates
    /// exists, read from the catalog without locking them.
    fn schema_is_complete(
        pool: &Pool<Self>,
    ) -> impl Future<Output = Result<bool, TransportError>> + Send;

    fn execute_ddl(
        pool: &Pool<Self>,
        statement: &str,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    fn enqueue(
        pool: &Pool<Self>,
        message: &Message,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Append an event to `bus_log`: lock the identity, insert, raise the
    /// high-water mark. A stable id already in the log keeps its position
    /// when the retry carries the same envelope.
    fn append(
        pool: &Pool<Self>,
        message: &Message,
        configured: Option<&ProjectionEpoch>,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Load the epoch durably paired with `bus_log`, creating it from
    /// `configured` (or a fresh epoch) when this is a new log, and fail closed
    /// when the log ends below the persisted high-water mark.
    fn prepare_log_epoch(
        pool: &Pool<Self>,
        configured: Option<&ProjectionEpoch>,
    ) -> impl Future<Output = Result<ProjectionEpoch, TransportError>> + Send;

    /// Compare-and-swap reset of the log and its cursor domain.
    fn reset_log(
        pool: &Pool<Self>,
        expected: &ProjectionEpoch,
        next: &ProjectionEpoch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Claim the lowest available `bus_queue` row whose `name` is one of
    /// `names`, minting a fresh claim token under `lease_secs`.
    fn claim(
        pool: &Pool<Self>,
        names: &[String],
        lease_secs: f64,
    ) -> impl Future<Output = Result<Option<ClaimedRow>, TransportError>> + Send;

    fn delete_claimed(
        pool: &Pool<Self>,
        seq: i64,
        claim_token: &str,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Extend a claim's lease; `false` when the claim is no longer held.
    fn renew_claim(
        pool: &Pool<Self>,
        seq: i64,
        claim_token: &str,
        lease_secs: f64,
    ) -> impl Future<Output = Result<bool, TransportError>> + Send;

    fn release_claim(
        pool: &Pool<Self>,
        seq: i64,
        claim_token: &str,
        delay_secs: f64,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Fail unless `expected` is still the durable log identity.
    fn verify_log_epoch(
        pool: &Pool<Self>,
        expected: &ProjectionEpoch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// `None` when another consumer of the group holds its lease.
    fn read_log(
        pool: &Pool<Self>,
        batch: LogBatch<'_>,
    ) -> impl Future<Output = Result<Option<Vec<ReceivedRow>>, TransportError>> + Send;

    /// Write the acknowledged position (epoch-fenced) and give up the lease.
    fn finish_log(
        pool: &Pool<Self>,
        consumer: &str,
        holder: &str,
        flush: Option<i64>,
        epoch: &ProjectionEpoch,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Extend a group lease; `false` when it is no longer held.
    fn renew_group_lease(
        pool: &Pool<Self>,
        consumer: &str,
        holder: &str,
        lease_secs: f64,
    ) -> impl Future<Output = Result<bool, TransportError>> + Send;
}

impl<DB> SqlBusStore for DB
where
    DB: SqlBusDialect,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
    for<'q> f64: Encode<'q, DB> + Type<DB>,
    for<'q> String: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
    for<'q> Option<String>: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
    for<'q> Vec<u8>: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
    for<'r> &'r str: ColumnIndex<DB::Row>,
{
    async fn schema_is_complete(pool: &Pool<Self>) -> Result<bool, TransportError> {
        let objects = schema_objects(DB::SCHEMA);
        let mut query = QueryBuilder::<DB>::new(DB::COUNT_SCHEMA_OBJECTS);
        DB::push_name_in(&mut query, &objects);
        let present: i64 = query
            .build()
            .fetch_one(pool)
            .await
            .map_err(|err| db_err::<DB>("read bus schema", err))?
            .try_get("present")
            .map_err(|err| db_err::<DB>("decode bus schema", err))?;
        Ok(usize::try_from(present).is_ok_and(|present| present == objects.len()))
    }

    async fn execute_ddl(pool: &Pool<Self>, statement: &str) -> Result<(), TransportError> {
        QueryBuilder::<DB>::new(statement)
            .build()
            .execute(pool)
            .await
            .map_err(|err| db_err::<DB>("ensure_tables", err))?;
        Ok(())
    }

    async fn enqueue(pool: &Pool<Self>, message: &Message) -> Result<(), TransportError> {
        let mut query = QueryBuilder::<DB>::new(
            "INSERT INTO bus_queue (name, message_id, kind, payload, content_type, metadata) ",
        );
        push_message_values(&mut query, message);
        query
            .build()
            .execute(pool)
            .await
            .map_err(|err| db_err::<DB>("enqueue", err))?;
        Ok(())
    }

    async fn append(
        pool: &Pool<Self>,
        message: &Message,
        configured: Option<&ProjectionEpoch>,
    ) -> Result<(), TransportError> {
        let mut tx = begin_write::<DB>(pool, "begin append").await?;
        // Every append takes this lock before allocating a sequence, so
        // commit order is `seq` order and `high_water` is an exact fence.
        let high_water = match lock_identity::<DB>(&mut tx).await? {
            Some((durable, high_water)) => {
                check_configured_epoch(DB::BACKEND, &durable, configured)?;
                high_water
            }
            // A producer that never ran `ensure_tables` writes the identity
            // on its first append.
            None => {
                reconcile_log_identity::<DB>(&mut tx, configured).await?;
                lock_identity::<DB>(&mut tx)
                    .await?
                    .map_or(0, |(_, high_water)| high_water)
            }
        };

        let mut insert = QueryBuilder::<DB>::new(
            "INSERT INTO bus_log (name, message_id, kind, payload, content_type, metadata) ",
        );
        push_message_values(&mut insert, message);
        insert.push(
            " ON CONFLICT (message_id) WHERE message_id IS NOT NULL DO NOTHING RETURNING seq",
        );
        let appended: Option<i64> = insert
            .build()
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| db_err::<DB>("append", err))?
            .map(|row| row.try_get("seq"))
            .transpose()
            .map_err(|err| db_err::<DB>("decode append position", err))?;

        match appended {
            // Positions only grow under the identity lock, so a new row at or
            // below the high-water mark means the log was rebuilt underneath
            // the bus. The transaction is dropped, so nothing is appended.
            Some(seq) if seq <= high_water => {
                return Err(TransportError::permanent(format!(
                    "{} bus appended position {seq} at or below durable high-water \
                     {high_water}; the log was rebuilt, use reset_ordered_log with the \
                     expected epoch",
                    DB::BACKEND
                )));
            }
            Some(seq) => {
                let mut fence = QueryBuilder::<DB>::new(format!(
                    "UPDATE bus_log_identity SET high_water = {}(high_water, ",
                    DB::GREATEST
                ));
                fence.push_bind(seq).push(") WHERE singleton = 1");
                fence
                    .build()
                    .execute(&mut *tx)
                    .await
                    .map_err(|err| db_err::<DB>("advance log high water", err))?;
            }
            None => {
                // Only a stable id conflicts: the retry must be the envelope
                // already in the log, which keeps its original position.
                let mut existing = QueryBuilder::<DB>::new(format!(
                    "SELECT {ROW_COLUMNS} FROM bus_log WHERE message_id = "
                ));
                existing.push_bind(message.id.clone());
                let row = existing
                    .build()
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|err| db_err::<DB>("read idempotent append", err))?
                    .ok_or_else(|| {
                        TransportError::permanent(format!(
                            "{} bus stable message ID conflict had no existing log row",
                            DB::BACKEND
                        ))
                    })?;
                validate_log_retry(DB::BACKEND, &message_from_row::<DB>(&row)?, message)?;
            }
        }
        tx.commit()
            .await
            .map_err(|err| db_err::<DB>("commit append", err))
    }

    async fn prepare_log_epoch(
        pool: &Pool<Self>,
        configured: Option<&ProjectionEpoch>,
    ) -> Result<ProjectionEpoch, TransportError> {
        let mut tx = begin_write::<DB>(pool, "begin log identity").await?;
        let epoch = reconcile_log_identity::<DB>(&mut tx, configured).await?;
        tx.commit()
            .await
            .map_err(|err| db_err::<DB>("commit log identity", err))?;
        Ok(epoch)
    }

    async fn reset_log(
        pool: &Pool<Self>,
        expected: &ProjectionEpoch,
        next: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        let mut tx = begin_write::<DB>(pool, "begin ordered-log reset").await?;
        let actual = lock_identity::<DB>(&mut tx)
            .await?
            .map(|(epoch, _)| epoch)
            .ok_or_else(|| {
                TransportError::permanent(format!(
                    "{} bus ordered-log reset found no log identity",
                    DB::BACKEND
                ))
            })?;
        if &actual != expected {
            return Err(TransportError::permanent(format!(
                "{} bus ordered-log reset expected epoch {:?} but durable epoch is {:?}",
                DB::BACKEND,
                expected.as_str(),
                actual.as_str()
            )));
        }
        for statement in DB::CLEAR_LOG
            .iter()
            .copied()
            .chain(["DELETE FROM bus_offset"])
        {
            QueryBuilder::<DB>::new(statement)
                .build()
                .execute(&mut *tx)
                .await
                .map_err(|err| db_err::<DB>("clear ordered log", err))?;
        }
        let mut install = QueryBuilder::<DB>::new("UPDATE bus_log_identity SET source_epoch = ");
        install
            .push_bind(next.as_str().to_string())
            .push(", generation = generation + 1, high_water = 0 WHERE singleton = 1");
        install
            .build()
            .execute(&mut *tx)
            .await
            .map_err(|err| db_err::<DB>("install next ordered-log epoch", err))?;
        tx.commit()
            .await
            .map_err(|err| db_err::<DB>("commit ordered-log reset", err))
    }

    async fn claim(
        pool: &Pool<Self>,
        names: &[String],
        lease_secs: f64,
    ) -> Result<Option<ClaimedRow>, TransportError> {
        let mut query = QueryBuilder::<DB>::new("UPDATE bus_queue SET locked_until = ");
        DB::push_clock_plus(&mut query, lease_secs);
        query.push(format!(
            ", claim_token = {}, attempts = attempts + 1 \
             WHERE seq IN (SELECT seq FROM bus_queue WHERE ",
            DB::NEW_CLAIM_TOKEN
        ));
        DB::push_name_in(&mut query, names);
        query.push(format!(
            " AND available_at <= {clock} \
              AND (locked_until IS NULL OR locked_until <= {clock}) \
              ORDER BY seq LIMIT 1{skip}) \
             RETURNING claim_token, CAST(attempts AS BIGINT) AS attempts, {ROW_COLUMNS}",
            clock = DB::CLOCK,
            skip = DB::SKIP_LOCKED,
        ));
        let Some(row) = query
            .build()
            .fetch_optional(pool)
            .await
            .map_err(|err| db_err::<DB>("claim", err))?
        else {
            return Ok(None);
        };
        let claim_token = row
            .try_get("claim_token")
            .map_err(|err| db_err::<DB>("claim token", err))?;
        let attempts: i64 = row
            .try_get("attempts")
            .map_err(|err| db_err::<DB>("claim attempts", err))?;
        Ok(Some(ClaimedRow {
            row: received_row::<DB>(&row)?,
            claim_token,
            attempts: u32::try_from(attempts).unwrap_or(u32::MAX),
        }))
    }

    async fn delete_claimed(
        pool: &Pool<Self>,
        seq: i64,
        claim_token: &str,
    ) -> Result<(), TransportError> {
        let mut query = QueryBuilder::<DB>::new("DELETE FROM bus_queue WHERE seq = ");
        query.push_bind(seq).push(" AND claim_token = ");
        query.push_bind(claim_token.to_string());
        query
            .build()
            .execute(pool)
            .await
            .map_err(|err| db_err::<DB>("delete", err))?;
        Ok(())
    }

    async fn renew_claim(
        pool: &Pool<Self>,
        seq: i64,
        claim_token: &str,
        lease_secs: f64,
    ) -> Result<bool, TransportError> {
        let mut query = QueryBuilder::<DB>::new("UPDATE bus_queue SET locked_until = ");
        DB::push_clock_plus(&mut query, lease_secs);
        query.push(" WHERE seq = ").push_bind(seq);
        query.push(" AND claim_token = ");
        query
            .push_bind(claim_token.to_string())
            .push(" RETURNING seq");
        let renewed = query
            .build()
            .fetch_optional(pool)
            .await
            .map_err(|err| db_err::<DB>("renew claim", err))?;
        Ok(renewed.is_some())
    }

    async fn release_claim(
        pool: &Pool<Self>,
        seq: i64,
        claim_token: &str,
        delay_secs: f64,
    ) -> Result<(), TransportError> {
        let mut query = QueryBuilder::<DB>::new(
            "UPDATE bus_queue SET locked_until = NULL, claim_token = NULL, available_at = ",
        );
        DB::push_clock_plus(&mut query, delay_secs);
        query.push(" WHERE seq = ").push_bind(seq);
        query.push(" AND claim_token = ");
        query.push_bind(claim_token.to_string());
        query
            .build()
            .execute(pool)
            .await
            .map_err(|err| db_err::<DB>("nack", err))?;
        Ok(())
    }

    async fn verify_log_epoch(
        pool: &Pool<Self>,
        expected: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        let actual: String = QueryBuilder::<DB>::new(
            "SELECT source_epoch FROM bus_log_identity WHERE singleton = 1",
        )
        .build()
        .fetch_one(pool)
        .await
        .map_err(|err| db_err::<DB>("read log identity", err))?
        .try_get("source_epoch")
        .map_err(|err| db_err::<DB>("decode log identity epoch", err))?;
        check_epoch(DB::BACKEND, &parse_epoch(DB::BACKEND, actual)?, expected)
    }

    async fn read_log(
        pool: &Pool<Self>,
        batch: LogBatch<'_>,
    ) -> Result<Option<Vec<ReceivedRow>>, TransportError> {
        let mut tx = begin_write::<DB>(pool, "begin log read").await?;
        share_identity::<DB>(&mut tx, batch.epoch).await?;
        if let Some(seq) = batch.flush {
            advance_offset::<DB>(&mut tx, batch.consumer, seq, batch.epoch).await?;
        }

        let mut lease = QueryBuilder::<DB>::new(
            "INSERT INTO bus_consumer_lease (consumer, holder, locked_until) VALUES (",
        );
        lease.push_bind(batch.consumer.to_string()).push(", ");
        lease.push_bind(batch.holder.to_string()).push(", ");
        DB::push_clock_plus(&mut lease, batch.lease_secs);
        lease.push(format!(
            ") ON CONFLICT (consumer) DO UPDATE SET holder = excluded.holder, \
                 locked_until = excluded.locked_until \
             WHERE bus_consumer_lease.holder = excluded.holder \
                OR bus_consumer_lease.locked_until <= {} \
             RETURNING holder",
            DB::CLOCK
        ));
        let held = lease
            .build()
            .fetch_optional(&mut *tx)
            .await
            .map_err(|err| db_err::<DB>("take group lease", err))?
            .is_some();

        let rows = if held {
            let mut read =
                QueryBuilder::<DB>::new(format!("SELECT {ROW_COLUMNS} FROM bus_log WHERE "));
            DB::push_name_in(&mut read, batch.names);
            read.push(" AND seq > COALESCE((SELECT last_seq FROM bus_offset WHERE consumer = ");
            read.push_bind(batch.consumer.to_string());
            read.push(" AND source_epoch = ");
            read.push_bind(batch.epoch.as_str().to_string());
            read.push("), 0) ORDER BY seq LIMIT ");
            read.push_bind(LOG_BATCH);
            let rows = read
                .build()
                .fetch_all(&mut *tx)
                .await
                .map_err(|err| db_err::<DB>("log read", err))?;
            Some(
                rows.iter()
                    .map(received_row::<DB>)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            None
        };
        tx.commit()
            .await
            .map_err(|err| db_err::<DB>("commit log read", err))?;
        Ok(rows)
    }

    async fn finish_log(
        pool: &Pool<Self>,
        consumer: &str,
        holder: &str,
        flush: Option<i64>,
        epoch: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        let flushed = match flush {
            Some(seq) => {
                async {
                    let mut tx = begin_write::<DB>(pool, "begin offset write").await?;
                    share_identity::<DB>(&mut tx, epoch).await?;
                    advance_offset::<DB>(&mut tx, consumer, seq, epoch).await?;
                    tx.commit()
                        .await
                        .map_err(|err| db_err::<DB>("commit offset write", err))
                }
                .await
            }
            None => Ok(()),
        };
        // Give the lease up even when the offset could not be written (a
        // reset retired this generation), so the group is not held until the
        // lease runs out.
        let mut release =
            QueryBuilder::<DB>::new("DELETE FROM bus_consumer_lease WHERE consumer = ");
        release
            .push_bind(consumer.to_string())
            .push(" AND holder = ");
        release.push_bind(holder.to_string());
        let released = release
            .build()
            .execute(pool)
            .await
            .map(|_| ())
            .map_err(|err| db_err::<DB>("release group lease", err));
        flushed.and(released)
    }

    async fn renew_group_lease(
        pool: &Pool<Self>,
        consumer: &str,
        holder: &str,
        lease_secs: f64,
    ) -> Result<bool, TransportError> {
        let mut query = QueryBuilder::<DB>::new("UPDATE bus_consumer_lease SET locked_until = ");
        DB::push_clock_plus(&mut query, lease_secs);
        query
            .push(" WHERE consumer = ")
            .push_bind(consumer.to_string());
        query.push(" AND holder = ").push_bind(holder.to_string());
        query.push(" RETURNING holder");
        let renewed = query
            .build()
            .fetch_optional(pool)
            .await
            .map_err(|err| db_err::<DB>("renew group lease", err))?;
        Ok(renewed.is_some())
    }
}

fn push_message_values<DB>(query: &mut QueryBuilder<DB>, message: &Message)
where
    DB: Database,
    for<'q> String: Encode<'q, DB> + Type<DB>,
    for<'q> Option<String>: Encode<'q, DB> + Type<DB>,
    for<'q> Vec<u8>: Encode<'q, DB> + Type<DB>,
{
    query.push("VALUES (");
    let mut values = query.separated(", ");
    values.push_bind(message.name.clone());
    values.push_bind(message.id.clone());
    values.push_bind(message.kind.as_str().to_string());
    values.push_bind(message.payload.clone());
    values.push_bind(message.content_type.clone());
    values.push_bind(metadata_json(message));
    query.push(")");
}

async fn begin_write<DB: SqlBusDialect>(
    pool: &Pool<DB>,
    context: &str,
) -> Result<Transaction<'static, DB>, TransportError> {
    pool.begin_with(DB::BEGIN_WRITE)
        .await
        .map_err(|err| db_err::<DB>(context, err))
}

/// The durable `(source_epoch, high_water)` of the log, locked; `None` when
/// the log has no identity yet.
async fn lock_identity<DB>(
    tx: &mut Transaction<'static, DB>,
) -> Result<Option<(ProjectionEpoch, i64)>, TransportError>
where
    DB: SqlBusDialect,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Decode<'q, DB> + Type<DB>,
    for<'q> String: Decode<'q, DB> + Type<DB>,
    for<'r> &'r str: ColumnIndex<DB::Row>,
{
    let Some(row) = QueryBuilder::<DB>::new(DB::LOCK_IDENTITY)
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| db_err::<DB>("lock log identity", err))?
    else {
        return Ok(None);
    };
    let epoch: String = row
        .try_get("source_epoch")
        .map_err(|err| db_err::<DB>("decode log identity epoch", err))?;
    let high_water: i64 = row
        .try_get("high_water")
        .map_err(|err| db_err::<DB>("decode log identity high water", err))?;
    Ok(Some((parse_epoch(DB::BACKEND, epoch)?, high_water)))
}

/// Fail unless `expected` is the durable identity, holding it against a
/// reset until the transaction ends.
async fn share_identity<DB>(
    tx: &mut Transaction<'static, DB>,
    expected: &ProjectionEpoch,
) -> Result<(), TransportError>
where
    DB: SqlBusDialect,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> String: Decode<'q, DB> + Type<DB>,
    for<'r> &'r str: ColumnIndex<DB::Row>,
{
    let actual: String = QueryBuilder::<DB>::new(DB::SHARE_IDENTITY)
        .build()
        .fetch_one(&mut **tx)
        .await
        .map_err(|err| db_err::<DB>("lock log identity for read", err))?
        .try_get("source_epoch")
        .map_err(|err| db_err::<DB>("decode log identity epoch", err))?;
    check_epoch(DB::BACKEND, &parse_epoch(DB::BACKEND, actual)?, expected)
}

/// Move `consumer`'s offset forward to `seq` in this epoch (monotonic).
async fn advance_offset<DB>(
    tx: &mut Transaction<'static, DB>,
    consumer: &str,
    seq: i64,
    epoch: &ProjectionEpoch,
) -> Result<(), TransportError>
where
    DB: SqlBusDialect,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Type<DB>,
    for<'q> String: Encode<'q, DB> + Type<DB>,
{
    let mut query = QueryBuilder::<DB>::new(
        "INSERT INTO bus_offset (consumer, source_epoch, last_seq) VALUES (",
    );
    query.push_bind(consumer.to_string()).push(", ");
    query.push_bind(epoch.as_str().to_string()).push(", ");
    query.push_bind(seq);
    query.push(format!(
        ") ON CONFLICT (consumer) DO UPDATE SET \
             source_epoch = excluded.source_epoch, \
             last_seq = CASE \
                 WHEN bus_offset.source_epoch = excluded.source_epoch \
                 THEN {}(bus_offset.last_seq, excluded.last_seq) \
                 ELSE excluded.last_seq \
             END",
        DB::GREATEST
    ));
    query
        .build()
        .execute(&mut **tx)
        .await
        .map_err(|err| db_err::<DB>("advance offset", err))?;
    Ok(())
}

/// Load (or create) the durable identity paired with `bus_log` and check the
/// log against it. The startup path: it runs when a consumer starts, when
/// tables are ensured, and on the first append to a log without an identity.
///
/// A lower log maximum than the persisted high-water mark proves only that
/// the observed log ends below a previously committed position. It fails
/// closed; only [`SqlBus::reset_ordered_log`] may retire that cursor domain.
async fn reconcile_log_identity<DB>(
    tx: &mut Transaction<'static, DB>,
    configured: Option<&ProjectionEpoch>,
) -> Result<ProjectionEpoch, TransportError>
where
    DB: SqlBusDialect,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
    for<'q> i64: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
    for<'q> String: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
    for<'r> &'r str: ColumnIndex<DB::Row>,
{
    let backend = DB::BACKEND;
    let candidate = configured.cloned().unwrap_or_else(fresh_log_epoch);
    let mut insert = QueryBuilder::<DB>::new(
        "INSERT INTO bus_log_identity (singleton, source_epoch, generation, high_water) \
         VALUES (1, ",
    );
    insert
        .push_bind(candidate.as_str().to_string())
        .push(", 1, 0) ON CONFLICT (singleton) DO NOTHING RETURNING singleton");
    let inserted = insert
        .build()
        .fetch_optional(&mut **tx)
        .await
        .map_err(|err| db_err::<DB>("prepare log identity", err))?
        .is_some();

    let (durable, high_water) = lock_identity::<DB>(tx)
        .await?
        .ok_or_else(|| TransportError::permanent(format!("{backend} bus log identity vanished")))?;
    let log_max: i64 = QueryBuilder::<DB>::new(
        "SELECT CAST(COALESCE(MAX(seq), 0) AS BIGINT) AS log_max FROM bus_log",
    )
    .build()
    .fetch_one(&mut **tx)
    .await
    .map_err(|err| db_err::<DB>("read log high water", err))?
    .try_get("log_max")
    .map_err(|err| db_err::<DB>("decode log high water", err))?;

    if inserted && log_max > 0 && configured.is_none() {
        return Err(TransportError::permanent(format!(
            "{backend} bus ordered-log identity is missing while the log still contains \
             positions through {log_max}; refusing to assign a random epoch, explicitly \
             adopt the retained log with with_source_epoch"
        )));
    }
    if inserted {
        // An identity recreated independently of its offsets cannot prove
        // which cursor generation those offsets belong to. Retire them in
        // the same transaction that installs the new identity.
        QueryBuilder::<DB>::new("DELETE FROM bus_offset")
            .build()
            .execute(&mut **tx)
            .await
            .map_err(|err| db_err::<DB>("clear unbound log offsets", err))?;
    }
    if log_max < high_water {
        return Err(TransportError::permanent(format!(
            "{backend} bus observed ordered-log maximum {log_max} below durable high-water \
             {high_water}; ordinary startup cannot rotate cursor identity, use \
             reset_ordered_log with the expected epoch"
        )));
    }
    if log_max > high_water {
        let mut raise = QueryBuilder::<DB>::new("UPDATE bus_log_identity SET high_water = ");
        raise.push_bind(log_max).push(" WHERE singleton = 1");
        raise
            .build()
            .execute(&mut **tx)
            .await
            .map_err(|err| db_err::<DB>("advance log high water", err))?;
    }
    check_configured_epoch(backend, &durable, configured)?;
    Ok(durable)
}

/// Reconstruct a [`Message`] from a `bus_queue`/`bus_log` row.
///
/// Required-column decode/parsing failures are permanent corruption. The row has
/// already been selected or claimed, so callers surface the error through
/// [`ReceivedMessage::decode_error`] and let the runner settle it through the
/// configured failure policy.
fn message_from_row<DB>(row: &DB::Row) -> Result<Message, TransportError>
where
    DB: SqlBusDialect,
    for<'r> &'r str: ColumnIndex<DB::Row>,
    for<'q> String: Decode<'q, DB> + Type<DB>,
    for<'q> Option<String>: Decode<'q, DB> + Type<DB>,
    for<'q> Vec<u8>: Decode<'q, DB> + Type<DB>,
{
    let backend = DB::BACKEND;
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
    let metadata: String = row
        .try_get("metadata")
        .map_err(|err| decode_err(backend, "metadata", err))?;

    Ok(Message {
        id: row
            .try_get::<Option<String>, _>("message_id")
            .unwrap_or(None),
        name,
        kind: parse_message_kind(backend, &kind)?,
        payload,
        content_type,
        metadata: parse_metadata(backend, &metadata)?,
    })
}

/// Decode a row, capturing a required-column failure as `decode_error`
/// instead of losing the claim. The placeholder message's name is
/// deliberately empty — the decode error carries the diagnostic.
fn received_row<DB>(row: &DB::Row) -> Result<ReceivedRow, TransportError>
where
    DB: SqlBusDialect,
    for<'r> &'r str: ColumnIndex<DB::Row>,
    for<'q> i64: Decode<'q, DB> + Type<DB>,
    for<'q> String: Decode<'q, DB> + Type<DB>,
    for<'q> Option<String>: Decode<'q, DB> + Type<DB>,
    for<'q> Vec<u8>: Decode<'q, DB> + Type<DB>,
{
    let seq = row
        .try_get("seq")
        .map_err(|err| decode_err(DB::BACKEND, "seq", err))?;
    let (message, decode_error) = match message_from_row::<DB>(row) {
        Ok(message) => (message, None),
        Err(error) => (
            Message::new("", MessageKind::Event, Vec::new()),
            Some(error),
        ),
    };
    Ok(ReceivedRow {
        seq,
        message,
        decode_error,
    })
}

/// SQL-backed [`Bus`] + [`BusConsumer`] over a pool of a [`SqlBusDialect`].
/// Cheap to clone (the pool is an `Arc`).
pub struct SqlBus<DB: Database> {
    pool: Pool<DB>,
    topology: BusTopologyConfig,
    lease: Duration,
    source_epoch: Option<ProjectionEpoch>,
    /// How long an idle consumer waits before it looks for work again.
    poll_interval: Duration,
}

impl<DB: Database> Clone for SqlBus<DB> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            topology: self.topology.clone(),
            lease: self.lease,
            source_epoch: self.source_epoch.clone(),
            poll_interval: self.poll_interval,
        }
    }
}

impl<DB: SqlBusStore> SqlBus<DB> {
    /// Build a bus over an existing pool.
    ///
    /// For event subscriptions, `subscribe` uses the router's consumer identity
    /// as the durable log offset. Service consumers usually get that identity
    /// from [`Service::named`](crate::microsvc::Service::named). Direct
    /// consumers can set it with [`group`](Self::group). Commands are claimed
    /// from `bus_queue` by message name, so command replicas compete by
    /// listening to the same registered command names.
    pub fn new(pool: Pool<DB>) -> Self {
        Self {
            pool,
            topology: BusTopologyConfig::default(),
            lease: DEFAULT_LEASE,
            source_epoch: None,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Build a bus with an explicit group for direct/low-level use.
    pub fn new_with_group(pool: Pool<DB>, group: impl Into<String>) -> Self {
        Self::new(pool).group(group)
    }

    /// Set an explicit durable event subscription group.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.topology = self.topology.group(group);
        self
    }

    /// Override the lease: how long a claimed command, or a group's hold on
    /// the event log, outlives a consumer that stopped renewing it. A
    /// consumer renews every third of the lease while it works, so a handler
    /// may take longer than the lease; the lease bounds how long a crashed
    /// consumer's command, or its group, waits for another consumer.
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

    /// Create the bus tables if absent and pair the log with its identity.
    ///
    /// Called by `listen`/`subscribe`; producers must ensure the tables exist
    /// before `send`/`publish`, either by calling this or through migrations.
    pub async fn ensure_tables(&self) -> Result<(), TransportError> {
        // `CREATE INDEX IF NOT EXISTS` takes a SHARE lock on the table even
        // when the index exists: it waits behind every in-flight write, and
        // every claim, renewal, ack and append waits behind it. Every
        // `listen`/`subscribe` starts here, so run the DDL only when an
        // object is missing.
        if !DB::schema_is_complete(&self.pool).await? {
            for statement in DB::SCHEMA.split(';') {
                let statement = statement.trim();
                if !statement.is_empty() {
                    DB::execute_ddl(&self.pool, statement).await?;
                }
            }
        }
        // Persist the identity beside the log even for producer-only setups.
        // Backups and explicit resets must treat `bus_log` and
        // `bus_log_identity` as one unit.
        DB::prepare_log_epoch(&self.pool, self.source_epoch.as_ref()).await?;
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
        DB::reset_log(&self.pool, expected_epoch, next_epoch).await
    }

    /// Dead letters are published to this bus's log unless the run names a
    /// sink.
    fn with_dead_letters(&self, options: RunOptions) -> RunOptions {
        options.or_dead_letters(|| Arc::new(PublishDeadLetters(self.clone())))
    }
}

impl<DB: SqlBusStore> Bus for SqlBus<DB> {
    async fn send_message(&self, message: Message) -> Result<(), TransportError> {
        WireEnvelope::validate(&message)?;
        DB::enqueue(&self.pool, &message).await
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        WireEnvelope::validate(&message)?;
        DB::append(&self.pool, &message, self.source_epoch.as_ref()).await
    }
}

impl<DB: SqlBusStore> BusConsumer for SqlBus<DB> {
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
            pool: self.pool.clone(),
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
            .resolve_consumer_group(router.as_ref(), DB::BACKEND)?;
        let source_epoch = DB::prepare_log_epoch(&self.pool, self.source_epoch.as_ref()).await?;
        let source = SqlLogSource {
            pool: self.pool.clone(),
            names,
            consumer: group,
            holder: uuid::Uuid::now_v7().to_string(),
            lease: self.lease,
            renewal: None,
            buffer: VecDeque::new(),
            last_delivered: None,
            settled_seq: Arc::new(AtomicI64::new(0)),
            flushed_seq: 0,
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
struct SqlQueueSource<DB: Database> {
    pool: Pool<DB>,
    names: Vec<String>,
    lease: Duration,
    poll_interval: Duration,
}

impl<DB: SqlBusStore> MessageSource for SqlQueueSource<DB> {
    type Received = SqlQueueReceived<DB>;

    fn transport_name(&self) -> &'static str {
        DB::BACKEND
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        let taken = tokio::time::Instant::now();
        let Some(claimed) = DB::claim(&self.pool, &self.names, self.lease.as_secs_f64()).await?
        else {
            return Ok(None);
        };
        let pool = self.pool.clone();
        let (seq, token, lease) = (claimed.row.seq, claimed.claim_token.clone(), self.lease);
        let renewal = keep_renewing(DB::BACKEND, "bus_queue claim", lease, taken, move || {
            let (pool, token) = (pool.clone(), token.clone());
            async move { DB::renew_claim(&pool, seq, &token, lease.as_secs_f64()).await }
        });
        Ok(Some(SqlQueueReceived {
            pool: self.pool.clone(),
            row: claimed.row,
            claim_token: claimed.claim_token,
            attempts: claimed.attempts,
            renewal,
        }))
    }

    async fn ready(&mut self) {
        tokio::time::sleep(self.poll_interval).await
    }
}

/// Keep a lease alive: renew it every third of `lease` until the lease is
/// lost or the returned handle is aborted.
///
/// `taken` is when the statement that took the lease was issued: the lease
/// runs from then on the database's clock, so the first renewal starts a
/// third of a lease after it. Each later renewal starts a third of a lease
/// after the previous one *started*, or as soon as it lands if it took
/// longer. Scheduling from landings instead would push every renewal back by
/// the previous one's latency, and a renewal taking a third of the lease
/// would already let the lease lapse between two of them. This way the lease
/// lapses only when a single renewal takes more than two thirds of it.
///
/// A failed renewal is retried at the next tick; the lease is lost only once
/// it runs out with no renewal landing.
fn keep_renewing<F, Fut>(
    backend: &'static str,
    what: &'static str,
    lease: Duration,
    taken: tokio::time::Instant,
    renew: F,
) -> tokio::task::AbortHandle
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = Result<bool, TransportError>> + Send + 'static,
{
    let every = (lease / 3).max(Duration::from_millis(10));
    tokio::spawn(async move {
        let mut next = taken + every;
        loop {
            tokio::time::sleep_until(next).await;
            next = tokio::time::Instant::now() + every;
            match renew().await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(
                        backend,
                        what,
                        "a lease was lost while its holder worked; another consumer may take over"
                    );
                    return;
                }
                Err(error) => {
                    tracing::warn!(backend, what, %error, "renewing a lease failed; retrying")
                }
            }
        }
    })
    .abort_handle()
}

/// A claimed `bus_queue` row: `ack` deletes it; `nack` makes it claimable
/// again after the delay. Its lease is renewed until it is settled or dropped.
pub struct SqlQueueReceived<DB: Database> {
    pool: Pool<DB>,
    row: ReceivedRow,
    claim_token: String,
    attempts: u32,
    renewal: tokio::task::AbortHandle,
}

impl<DB: Database> Drop for SqlQueueReceived<DB> {
    fn drop(&mut self) {
        self.renewal.abort();
    }
}

impl<DB: SqlBusStore> ReceivedMessage for SqlQueueReceived<DB> {
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
        DB::delete_claimed(&self.pool, self.row.seq, &self.claim_token).await
    }

    async fn nack(self, delay: Duration) -> Result<(), TransportError> {
        self.renewal.abort();
        DB::release_claim(
            &self.pool,
            self.row.seq,
            &self.claim_token,
            delay.as_secs_f64(),
        )
        .await
    }
}

/// Fan-out source over `bus_log` for one consumer group.
///
/// It reads [`LOG_BATCH`] entries past the group's offset at a time, holding
/// the group's lease while it reads so no other consumer of the group reads
/// the same entries. Acknowledgements are recorded in `settled_seq` and
/// written as the group's offset when the next batch is read and when the run
/// ends.
///
/// Read-ahead must not break the offset contract: a nacked entry is read
/// again before anything after it. When the entry delivered last was not
/// settled forward (a nack), the buffer is discarded so the next read starts
/// again from the acknowledged position. The runner settles each delivery
/// before the next `recv`, so this check is race-free.
struct SqlLogSource<DB: SqlBusStore> {
    pool: Pool<DB>,
    names: Vec<String>,
    consumer: String,
    /// This source's name in `bus_consumer_lease`.
    holder: String,
    lease: Duration,
    /// Renews the group lease while this source holds it.
    renewal: Option<tokio::task::AbortHandle>,
    buffer: VecDeque<ReceivedRow>,
    /// `seq` of the entry most recently handed to the runner.
    last_delivered: Option<i64>,
    /// Highest `seq` acknowledged by this source's deliveries.
    settled_seq: Arc<AtomicI64>,
    /// Highest `seq` written as the group's offset.
    flushed_seq: i64,
    source_epoch: ProjectionEpoch,
    poll_interval: Duration,
}

impl<DB: SqlBusStore> SqlLogSource<DB> {
    fn unflushed(&self) -> Option<i64> {
        let settled = self.settled_seq.load(Ordering::Acquire);
        (settled > self.flushed_seq).then_some(settled)
    }

    /// Renew the group lease the read issued at `taken` took, unless this
    /// source already does.
    fn hold_lease(&mut self, taken: tokio::time::Instant) {
        if self.renewal.is_some() {
            return;
        }
        let (pool, consumer, holder, lease) = (
            self.pool.clone(),
            self.consumer.clone(),
            self.holder.clone(),
            self.lease,
        );
        self.renewal = Some(keep_renewing(
            DB::BACKEND,
            "bus_consumer_lease",
            lease,
            taken,
            move || {
                let (pool, consumer, holder) = (pool.clone(), consumer.clone(), holder.clone());
                async move {
                    DB::renew_group_lease(&pool, &consumer, &holder, lease.as_secs_f64()).await
                }
            },
        ));
    }

    fn drop_lease(&mut self) {
        if let Some(renewal) = self.renewal.take() {
            renewal.abort();
        }
    }
}

impl<DB: SqlBusStore> Drop for SqlLogSource<DB> {
    fn drop(&mut self) {
        self.drop_lease();
    }
}

impl<DB: SqlBusStore> MessageSource for SqlLogSource<DB> {
    type Received = SqlLogReceived;

    fn transport_name(&self) -> &'static str {
        DB::BACKEND
    }

    async fn ready(&mut self) {
        tokio::time::sleep(self.poll_interval).await
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        if let Some(last) = self.last_delivered {
            if self.settled_seq.load(Ordering::Acquire) < last {
                // The last entry was nacked: the buffered read-ahead would
                // skip past it. Drop it and read again from the acknowledged
                // position (re-delivering the nacked entry).
                self.buffer.clear();
            }
        }
        if self.buffer.is_empty() {
            let flush = self.unflushed();
            let taken = tokio::time::Instant::now();
            let rows = DB::read_log(
                &self.pool,
                LogBatch {
                    names: &self.names,
                    consumer: &self.consumer,
                    holder: &self.holder,
                    lease_secs: self.lease.as_secs_f64(),
                    flush,
                    epoch: &self.source_epoch,
                },
            )
            .await?;
            if let Some(seq) = flush {
                self.flushed_seq = seq;
            }
            match rows {
                Some(rows) => {
                    self.hold_lease(taken);
                    self.buffer.extend(rows);
                }
                // Another consumer of the group is reading.
                None => {
                    self.drop_lease();
                    return Ok(None);
                }
            }
        } else {
            // A reset retires buffered rows of the old generation at once.
            DB::verify_log_epoch(&self.pool, &self.source_epoch).await?;
        }
        let Some(row) = self.buffer.pop_front() else {
            return Ok(None);
        };
        let position = u64::try_from(row.seq).map_err(|_| {
            corrupt_row(
                DB::BACKEND,
                format!(
                    "bus_log seq {} is outside the projection cursor domain",
                    row.seq
                ),
            )
        })?;
        let source = ProjectionSource::new(format!("{}.bus_log", DB::BACKEND), b"global".to_vec())
            .map_err(|error| corrupt_row(DB::BACKEND, error.to_string()))?;
        let ordered = OrderedDelivery::new(source, self.source_epoch.clone(), position, false)
            .map_err(|error| corrupt_row(DB::BACKEND, error.to_string()))?;
        self.last_delivered = Some(row.seq);
        Ok(Some(SqlLogReceived {
            settled_seq: self.settled_seq.clone(),
            row,
            ordered,
        }))
    }

    async fn finish(&mut self) -> Result<(), TransportError> {
        let flush = self.unflushed();
        self.drop_lease();
        DB::finish_log(
            &self.pool,
            &self.consumer,
            &self.holder,
            flush,
            &self.source_epoch,
        )
        .await?;
        if let Some(seq) = flush {
            self.flushed_seq = seq;
        }
        Ok(())
    }
}

/// A `bus_log` entry: `ack` records it as done (the group's offset moves past
/// it with the next batch, or when the run ends); `nack` leaves the offset, so
/// the entry is read again and nothing after it is delivered first.
pub struct SqlLogReceived {
    settled_seq: Arc<AtomicI64>,
    row: ReceivedRow,
    ordered: OrderedDelivery,
}

impl ReceivedMessage for SqlLogReceived {
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
        self.settled_seq.fetch_max(self.row.seq, Ordering::AcqRel);
        Ok(())
    }

    async fn nack(self, delay: Duration) -> Result<(), TransportError> {
        // The offset stays where it is. A log cannot deliver a later entry
        // first, so the delay is spent here, holding the position.
        tokio::time::sleep(delay).await;
        Ok(())
    }
}

#[cfg(test)]
mod schema_tests {
    #[test]
    fn the_schema_check_names_every_table_and_index() {
        let objects = super::schema_objects(
            "CREATE TABLE IF NOT EXISTS bus_queue (seq INTEGER); \
             CREATE INDEX IF NOT EXISTS bus_queue_claim_idx ON bus_queue (seq); \
             CREATE UNIQUE INDEX IF NOT EXISTS bus_log_id_idx ON bus_log (id); \
             CREATE TABLE IF NOT EXISTS bus_offset(consumer TEXT)",
        );
        assert_eq!(
            objects,
            [
                "bus_queue",
                "bus_queue_claim_idx",
                "bus_log_id_idx",
                "bus_offset"
            ]
        );
    }
}

/// The renewal schedule, driven on paused time: a renewal that "takes" a
/// latency sleeps it, so every start and landing is exact.
#[cfg(test)]
mod renewal_tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::time::Instant;

    const LEASE: Duration = Duration::from_secs(3);
    const EVERY: Duration = Duration::from_secs(1);

    /// When each renewal started and landed, relative to the lease being
    /// taken, over `window`. The `n`th renewal takes `latency(n)`.
    async fn renewals(
        window: Duration,
        latency: impl Fn(usize) -> Duration + Send + Sync + 'static,
    ) -> Vec<(Duration, Duration)> {
        let taken = Instant::now();
        let log = Arc::new(Mutex::new(Vec::new()));
        let latency = Arc::new(latency);
        let renewal = super::keep_renewing("test", "lease", LEASE, taken, {
            let log = log.clone();
            move || {
                let (log, latency) = (log.clone(), latency.clone());
                async move {
                    let started = taken.elapsed();
                    let n = log.lock().unwrap().len();
                    log.lock().unwrap().push((started, started));
                    tokio::time::sleep(latency(n)).await;
                    log.lock().unwrap()[n].1 = taken.elapsed();
                    Ok(true)
                }
            }
        });
        tokio::time::sleep(window).await;
        renewal.abort();
        let log = log.lock().unwrap().clone();
        log
    }

    /// Every renewal lands before the lease it extends runs out: the lease
    /// taken at 0 and each renewal's (it runs from the renewal's start).
    fn assert_never_lapses(renewals: &[(Duration, Duration)]) {
        let mut held_until = LEASE;
        for &(started, landed) in renewals {
            assert!(
                landed < held_until,
                "a renewal landing at {landed:?} came after the lease ran out at \
                 {held_until:?}: {renewals:?}"
            );
            held_until = started + LEASE;
        }
    }

    fn secs(seconds: f64) -> Duration {
        Duration::from_secs_f64(seconds)
    }

    #[tokio::test(start_paused = true)]
    async fn renewals_start_a_third_of_a_lease_apart_however_long_they_take() {
        let renewals = renewals(secs(4.5), |_| secs(0.6)).await;
        let starts: Vec<_> = renewals.iter().map(|&(started, _)| started).collect();
        // Scheduled from landings they would start at 1, 2.6, 4.2.
        assert_eq!(starts, vec![secs(1.0), secs(2.0), secs(3.0), secs(4.0)]);
        assert_never_lapses(&renewals);
    }

    /// Renewals that each take up to half the lease follow one another
    /// back to back, and the lease holds. Scheduled from landings, any
    /// renewal taking more than a third of the lease let it lapse.
    #[tokio::test(start_paused = true)]
    async fn the_lease_holds_while_every_renewal_takes_under_half_of_it() {
        let renewals = renewals(secs(10.0), |_| secs(1.45)).await;
        assert!(renewals.len() >= 6, "{renewals:?}");
        assert_never_lapses(&renewals);
    }

    /// One renewal taking up to two thirds of the lease, among quick ones.
    #[tokio::test(start_paused = true)]
    async fn the_lease_holds_through_one_renewal_taking_under_two_thirds_of_it() {
        let renewals = renewals(secs(10.0), |n| if n == 2 { secs(1.95) } else { secs(0.1) }).await;
        assert_never_lapses(&renewals);
    }

    /// The schedule runs from when the lease was taken, not from when the
    /// renewal task got going.
    #[tokio::test(start_paused = true)]
    async fn the_first_renewal_is_a_third_of_a_lease_after_the_lease_was_taken() {
        let taken = Instant::now();
        tokio::time::sleep(secs(0.4)).await;
        let started = Arc::new(Mutex::new(None));
        let renewal = super::keep_renewing("test", "lease", LEASE, taken, {
            let started = started.clone();
            move || {
                started.lock().unwrap().get_or_insert(taken.elapsed());
                std::future::ready(Ok(true))
            }
        });
        tokio::time::sleep(secs(1.0)).await;
        renewal.abort();
        assert_eq!(*started.lock().unwrap(), Some(EVERY));
    }
}

/// Pinned statement counts of the bus hot paths. `BEGIN` and `COMMIT` are
/// not counted.
#[cfg(test)]
mod statement_count_tests {
    use std::sync::Arc;

    use crate::bus::{Bus, BusConsumer, Handlers, Message, MessageKind, RunOptions};
    use crate::sqlx_repo::statement_count_tests::{assert_at_most, statements, COUNTING};

    fn event(id: String) -> Message {
        Message::new("count.shipped", MessageKind::Event, b"{}".to_vec()).with_id(id)
    }

    /// Publishing an event locks the log identity, inserts the row and
    /// raises the high-water mark: three statements, whatever the log holds.
    /// (It was five: an identity upsert and a `MAX(seq)` scan as well.)
    ///
    /// Consuming 17 events for a group: per read transaction an epoch check,
    /// the offset write (when acks are pending), the group lease and the rows;
    /// an epoch check per further buffered row; the lease release at the end.
    /// Two transactions per event before.
    async fn pin<DB>(pool: sqlx::Pool<DB>)
    where
        DB: super::SqlBusStore,
        for<'c> &'c sqlx::Pool<DB>: sqlx::Executor<'c, Database = DB>,
    {
        let bus = super::SqlBus::new(pool.clone());
        bus.ensure_tables().await.unwrap();
        let run = uuid::Uuid::now_v7().simple().to_string();
        bus.publish_message(event(format!("{run}-warm")))
            .await
            .unwrap();

        let ((), publish) = statements(&pool, async {
            bus.publish_message(event(format!("{run}-0")))
                .await
                .unwrap();
        })
        .await;
        assert_at_most("publish an event", &publish, 3);
        assert_eq!(publish.len(), 3, "the publish path is pinned exactly");

        for n in 1..16 {
            bus.publish_message(event(format!("{run}-{n}")))
                .await
                .unwrap();
        }
        let group = format!("count-{run}");
        let handlers = Arc::new(
            Handlers::new()
                .named(group)
                .on_event("count.shipped", |_: &Message| async { Ok(()) }),
        );
        let (result, consume) = statements(&pool, async {
            bus.subscribe(handlers, RunOptions::until_idle()).await
        })
        .await;
        result.unwrap();
        // A run starts by creating the tables and preparing the identity: 14
        // statements however much the log holds.
        let startup = consume
            .iter()
            .position(|sql| sql.starts_with("SELECT source_epoch FROM"))
            .unwrap();
        assert!(startup <= 14, "startup ran {startup} statements");
        let reads = &consume[startup..];
        // Three reads (16 rows, the 17th, then the empty read that ends the
        // run) of 3 or 4 statements, 15 + 0 epoch checks for buffered rows and
        // the lease release.
        assert_at_most("consume 17 events", reads, 4 * 3 + 15 + 1);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_bus_statements_are_pinned() {
        use crate::sqlx_repo::statement_count_tests::counted_sqlite_pool;
        let counting = COUNTING.lock().await;
        pin(counted_sqlite_pool(&counting).await).await;
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_bus_statements_are_pinned() {
        use crate::sqlx_repo::statement_count_tests::counted_postgres_pool;
        let counting = COUNTING.lock().await;
        let Some((_schema, pool)) =
            counted_postgres_pool(&counting, "DISTRIBUTED_TEST_POSTGRES_URL", "bus_statements")
                .await
        else {
            return;
        };
        pin(pool).await;
    }
}
