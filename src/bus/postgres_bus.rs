//! Postgres [`Bus`] + [`BusConsumer`] — a complete single-DB bus.
//!
//! Postgres covers **both** bus modes (see [[specs/transport-bus-facade]]):
//!
//! - **`send` / `listen` (point-to-point, competing):** a durable work-queue
//!   table (`bus_queue`) claimed with `FOR UPDATE SKIP LOCKED` under a lease, so
//!   one of N competing `listen`ers handles each command and the row is deleted
//!   on success (redelivered on nack, until a `dead_letter`/`park` drops it).
//! - **`publish` / `subscribe` (fan-out):** Postgres modelled as a log — an
//!   append-only `bus_log` table (monotonic `seq`, retained), its durable
//!   generation identity (`bus_log_identity`), and a per-consumer offset table
//!   (`bus_offset`: `consumer → (source_epoch, last_seq)`). `publish` appends; each
//!   `subscribe`r (keyed by its `group`) reads `seq > last_seq` for its event
//!   names in order and advances its own offset, so every group sees every event.
//!   Because the log, the offset, and projection writes share one Postgres, the
//!   offset advances in the same database as the effects — the cleanest path to
//!   transactional effectively-once of any transport (the offset is the inbox).
//!
//! The bus model itself (builders, sources, settlement, corruption handling) is
//! shared with the SQLite bus in `sql_bus_common`; this
//! module contributes only the Postgres dialect: `$n` placeholders, the `now()`
//! clock, `= ANY` array binding for name lists, and `gen_random_uuid()` claim
//! tokens.
//!
//! ## Why claim-lease, not sqlxmq (implementation note)
//!
//! Decision #8 of the locked spec names `sqlxmq` as the recommended work-queue
//! backend and keeps the claim-lease queue as the **no-extra-dependency
//! alternative**. At implementation time the claim-lease queue was chosen because
//! sqlxmq owns an always-on, push-based `JobRunner` loop, which does not compose
//! with the facade's uniform *drain-to-idle* [`run_source`] model that every other
//! `*Bus` (in-memory, NATS, RabbitMQ, Kafka) and their tests share — a claim-lease
//! [`MessageSource`] returns `Ok(None)` when the queue is empty and stops
//! cleanly. sqlxmq remains a viable future backend for its mature NOTIFY/backoff;
//! revisit if those are needed. See [[tasks/build-transport-bus-facade]].
//!
//! Requires the `postgres` feature. Integration-tested in `tests/postgres_transport`.
//!
//! [`Bus`]: super::Bus
//! [`BusConsumer`]: super::BusConsumer
//! [`MessageSource`]: super::MessageSource
//! [`run_source`]: super::run_source

use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::PgListener;
use sqlx::{PgConnection, PgPool, Row};
use tokio::sync::Mutex;

use super::sql_bus_common::{
    db_err as sql_db_err, message_from_row, metadata_json, validate_log_retry, ClaimedRow,
    ReceivedRow, SqlBus, SqlBusDialect, SqlLogReceived, SqlQueueReceived,
};
use super::{Message, TransportError};
use crate::projection_protocol::ProjectionEpoch;

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS bus_queue (
    seq          BIGSERIAL PRIMARY KEY,
    claim_token  TEXT,
    name         TEXT NOT NULL,
    message_id   TEXT,
    kind         TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/json',
    metadata     TEXT NOT NULL DEFAULT '[]',
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    attempts     INTEGER NOT NULL DEFAULT 0,
    CHECK (claim_token IS NULL OR claim_token <> ''),
    CHECK (name <> ''),
    CHECK (kind IN ('command', 'event')),
    CHECK (content_type <> ''),
    CHECK (attempts >= 0)
);
CREATE INDEX IF NOT EXISTS bus_queue_claim_idx
    ON bus_queue (name, available_at, locked_until, seq);
CREATE TABLE IF NOT EXISTS bus_log (
    seq          BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    message_id   TEXT,
    kind         TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/json',
    metadata     TEXT NOT NULL DEFAULT '[]',
    appended_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (name <> ''),
    CHECK (kind IN ('command', 'event')),
    CHECK (content_type <> '')
);
CREATE INDEX IF NOT EXISTS bus_log_name_seq_idx ON bus_log (name, seq);
CREATE TABLE IF NOT EXISTS bus_offset (
    consumer TEXT PRIMARY KEY,
    source_epoch TEXT NOT NULL,
    last_seq BIGINT NOT NULL DEFAULT 0,
    CHECK (consumer <> ''),
    CHECK (source_epoch <> ''),
    CHECK (last_seq >= 0)
);
CREATE TABLE IF NOT EXISTS bus_log_identity (
    singleton    SMALLINT PRIMARY KEY DEFAULT 1,
    source_epoch TEXT NOT NULL,
    generation   BIGINT NOT NULL DEFAULT 1,
    high_water   BIGINT NOT NULL DEFAULT 0,
    CHECK (singleton = 1),
    CHECK (source_epoch <> ''),
    CHECK (generation > 0),
    CHECK (high_water >= 0)
)";

fn db_err(context: &str, err: sqlx::Error) -> TransportError {
    sql_db_err(PostgresBusDialect::BACKEND, context, err)
}

/// Postgres [`Bus`](super::Bus) + [`BusConsumer`](super::BusConsumer). Cheap to
/// clone (the pool is an `Arc`).
pub type PostgresBus = SqlBus<PostgresBusDialect>;

/// A claimed Postgres `bus_queue` row: `ack` deletes it (done); `nack` makes it
/// available again (redelivery); `dead_letter`/`park` delete it (stop
/// redelivery). Settlement is fenced by the claim token.
pub type QueueReceived = SqlQueueReceived<PostgresBusDialect>;

/// A Postgres `bus_log` entry: `ack` advances this consumer's offset to its
/// `seq`; `nack` leaves the offset (redelivery); `dead_letter`/`park` advance
/// past it (skip, don't get stuck).
pub type LogReceived = SqlLogReceived<PostgresBusDialect>;

impl PostgresBus {
    /// Build a bus over an existing pool.
    ///
    /// For event subscriptions, `subscribe` uses the router's consumer identity as
    /// the durable Postgres log offset. Service consumers usually get that identity
    /// from [`Service::named`](crate::microsvc::Service::named). Direct consumers
    /// can set it with [`group`](Self::group). Commands are claimed from
    /// `bus_queue` by message name, so command replicas compete by listening to the
    /// same registered command names.
    pub fn new(pool: PgPool) -> Self {
        SqlBus::from_dialect(PostgresBusDialect {
            pool,
            listener: Arc::new(Mutex::new(None)),
        })
    }

    /// Build a bus with an explicit group for direct/low-level use.
    pub fn new_with_group(pool: PgPool, group: impl Into<String>) -> Self {
        Self::new(pool).group(group)
    }
}

/// The Postgres [`SqlBusDialect`].
#[derive(Clone)]
pub struct PostgresBusDialect {
    pool: PgPool,
    listener: Arc<Mutex<Option<PgListener>>>,
}

impl PostgresBusDialect {
    /// Lock and verify the durable identity paired with `bus_log`.
    ///
    /// Every framework append takes this singleton lock before allocating a
    /// sequence. That makes `high_water` an exact committed fence rather than a
    /// best-effort observation. A lower current maximum proves only that the
    /// currently observed log ends below a previously committed position. It
    /// fails closed; only [`SqlBus::reset_ordered_log`] may retire that cursor
    /// domain.
    async fn reconcile_log_identity(
        connection: &mut PgConnection,
        epoch_candidate: &ProjectionEpoch,
        expected_epoch: Option<&ProjectionEpoch>,
    ) -> Result<ProjectionEpoch, TransportError> {
        let inserted = sqlx::query(
            "INSERT INTO bus_log_identity \
                 (singleton, source_epoch, generation, high_water) \
             VALUES (1, $1, 1, 0) \
             ON CONFLICT (singleton) DO NOTHING",
        )
        .bind(epoch_candidate.as_str())
        .execute(&mut *connection)
        .await
        .map_err(|err| db_err("prepare log identity", err))?
        .rows_affected()
            == 1;

        let identity = sqlx::query(
            "SELECT source_epoch, generation, high_water \
             FROM bus_log_identity WHERE singleton = 1 FOR UPDATE",
        )
        .fetch_one(&mut *connection)
        .await
        .map_err(|err| db_err("lock log identity", err))?;
        let source_epoch: String = identity
            .try_get("source_epoch")
            .map_err(|err| db_err("decode log identity epoch", err))?;
        let high_water: i64 = identity
            .try_get("high_water")
            .map_err(|err| db_err("decode log identity high water", err))?;
        let log_max: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0)::BIGINT FROM bus_log")
            .fetch_one(&mut *connection)
            .await
            .map_err(|err| db_err("read log high water", err))?;

        if inserted && log_max > 0 && expected_epoch.is_none() {
            return Err(TransportError::permanent(format!(
                "postgres bus ordered-log identity is missing while the log \
                 still contains positions through {log_max}; refusing to assign \
                 a random epoch, explicitly adopt the retained log with \
                 with_source_epoch"
            )));
        }
        if inserted {
            // An identity recreated independently of its offsets cannot prove
            // which cursor generation those offsets belong to. Retire them in
            // the same transaction that installs the new identity.
            sqlx::query("DELETE FROM bus_offset")
                .execute(&mut *connection)
                .await
                .map_err(|err| db_err("clear unbound log offsets", err))?;
        }
        if log_max < high_water {
            return Err(TransportError::permanent(format!(
                "postgres bus observed ordered-log maximum {log_max} below durable \
                 high-water {high_water}; ordinary startup cannot rotate cursor \
                 identity, use reset_ordered_log with the expected epoch"
            )));
        } else if log_max > high_water {
            sqlx::query("UPDATE bus_log_identity SET high_water = $1 WHERE singleton = 1")
                .bind(log_max)
                .execute(&mut *connection)
                .await
                .map_err(|err| db_err("advance log high water", err))?;
        }
        if let Some(expected_epoch) = expected_epoch {
            if source_epoch != expected_epoch.as_str() {
                return Err(TransportError::permanent(format!(
                    "postgres bus configured source epoch {:?} does not match \
                     durable ordered-log epoch {:?}",
                    expected_epoch.as_str(),
                    source_epoch
                )));
            }
        }

        ProjectionEpoch::new(source_epoch).map_err(|error| {
            TransportError::permanent(format!("postgres bus corrupt log identity epoch: {error}"))
        })
    }

    async fn lock_expected_log_epoch(
        connection: &mut PgConnection,
        expected_epoch: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        let actual: String = sqlx::query_scalar(
            "SELECT source_epoch FROM bus_log_identity \
             WHERE singleton = 1 FOR SHARE",
        )
        .fetch_one(&mut *connection)
        .await
        .map_err(|err| db_err("lock log identity for read", err))?;
        let actual = ProjectionEpoch::new(actual).map_err(|error| {
            TransportError::permanent(format!("postgres bus corrupt log identity epoch: {error}"))
        })?;
        if &actual != expected_epoch {
            return Err(TransportError::permanent(format!(
                "postgres bus ordered-log epoch changed from {:?} to {:?}; \
                 the subscriber must restart",
                expected_epoch.as_str(),
                actual.as_str()
            )));
        }
        Ok(())
    }
}

impl SqlBusDialect for PostgresBusDialect {
    const BACKEND: &'static str = "postgres";
    const SCHEMA: &'static str = SCHEMA;

    async fn execute_ddl(&self, statement: &'static str) -> Result<(), TransportError> {
        sqlx::query(statement)
            .execute(&self.pool)
            .await
            .map_err(|err| db_err("ensure_tables", err))?;
        Ok(())
    }

    async fn ensure_ordered_log_schema(&self) -> Result<(), TransportError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|err| db_err("begin ordered-log schema upgrade", err))?;
        // Serialize legacy duplicate inspection, deletion, and index creation
        // with every writer, including writers that predate this framework.
        sqlx::query("LOCK TABLE bus_log IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("lock ordered log for schema upgrade", err))?;
        sqlx::query("ALTER TABLE bus_offset ADD COLUMN IF NOT EXISTS source_epoch TEXT")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("add offset epoch", err))?;
        sqlx::query("DELETE FROM bus_offset WHERE source_epoch IS NULL OR source_epoch = ''")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("clear legacy unbound offsets", err))?;
        sqlx::query("ALTER TABLE bus_offset ALTER COLUMN source_epoch SET NOT NULL")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("require offset epoch", err))?;

        let duplicate_rows = sqlx::query(
            "SELECT seq, name, message_id, kind, payload, content_type, metadata \
             FROM bus_log \
             WHERE message_id IN ( \
                 SELECT message_id FROM bus_log \
                 WHERE message_id IS NOT NULL \
                 GROUP BY message_id HAVING COUNT(*) > 1 \
             ) \
             ORDER BY message_id, seq",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|err| db_err("inspect legacy stable message IDs", err))?;
        let mut authoritative: Option<(String, Message)> = None;
        let mut redundant = Vec::new();
        for row in &duplicate_rows {
            let message_id: String = row
                .try_get("message_id")
                .map_err(|err| db_err("decode legacy stable message ID", err))?;
            let message = message_from_row(Self::BACKEND, row)?;
            match authoritative.as_ref() {
                Some((authoritative_id, authoritative_message))
                    if authoritative_id == &message_id =>
                {
                    validate_log_retry(Self::BACKEND, authoritative_message, &message)?;
                    redundant.push(
                        row.try_get::<i64, _>("seq")
                            .map_err(|err| db_err("decode redundant log position", err))?,
                    );
                }
                _ => authoritative = Some((message_id, message)),
            }
        }
        if !redundant.is_empty() {
            sqlx::query("DELETE FROM bus_log WHERE seq = ANY($1)")
                .bind(&redundant)
                .execute(&mut *transaction)
                .await
                .map_err(|err| db_err("deduplicate legacy stable message IDs", err))?;
        }
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS bus_log_message_id_unique_idx \
             ON bus_log (message_id) WHERE message_id IS NOT NULL",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|err| db_err("fence stable message IDs", err))?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit ordered-log schema upgrade", err))?;
        Ok(())
    }

    async fn insert_queue(&self, message: &Message) -> Result<(), TransportError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|err| db_err("begin enqueue", err))?;
        let metadata = metadata_json(message);
        sqlx::query(
            "INSERT INTO bus_queue (name, message_id, kind, payload, content_type, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&message.name)
        .bind(&message.id)
        .bind(message.kind.as_str())
        .bind(&message.payload)
        .bind(&message.content_type)
        .bind(metadata)
        .execute(&mut *transaction)
        .await
        .map_err(|err| db_err("enqueue", err))?;
        sqlx::query("SELECT pg_notify('distributed_bus_queue', '')")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("notify queue", err))?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit enqueue", err))?;
        Ok(())
    }

    async fn insert_log(
        &self,
        message: &Message,
        epoch_candidate: &ProjectionEpoch,
        expected_epoch: Option<&ProjectionEpoch>,
    ) -> Result<(), TransportError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|err| db_err("begin append", err))?;
        Self::reconcile_log_identity(&mut transaction, epoch_candidate, expected_epoch).await?;

        let metadata = metadata_json(message);
        let inserted_seq = if message.id().is_some() {
            sqlx::query_scalar::<_, i64>(
                "INSERT INTO bus_log \
                     (name, message_id, kind, payload, content_type, metadata) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT (message_id) WHERE message_id IS NOT NULL DO NOTHING \
                 RETURNING seq",
            )
            .bind(&message.name)
            .bind(&message.id)
            .bind(message.kind.as_str())
            .bind(&message.payload)
            .bind(&message.content_type)
            .bind(&metadata)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|err| db_err("append", err))?
        } else {
            Some(
                sqlx::query_scalar::<_, i64>(
                    "INSERT INTO bus_log \
                         (name, message_id, kind, payload, content_type, metadata) \
                     VALUES ($1, $2, $3, $4, $5, $6) \
                     RETURNING seq",
                )
                .bind(&message.name)
                .bind(&message.id)
                .bind(message.kind.as_str())
                .bind(&message.payload)
                .bind(&message.content_type)
                .bind(&metadata)
                .fetch_one(&mut *transaction)
                .await
                .map_err(|err| db_err("append", err))?,
            )
        };

        let seq = match inserted_seq {
            Some(seq) => seq,
            None => {
                let existing = sqlx::query(
                    "SELECT seq, name, message_id, kind, payload, content_type, metadata \
                     FROM bus_log WHERE message_id = $1",
                )
                .bind(message.id())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|err| db_err("read idempotent append", err))?
                .ok_or_else(|| {
                    TransportError::permanent(
                        "postgres bus stable message ID conflict had no existing log row",
                    )
                })?;
                let existing_message = message_from_row(Self::BACKEND, &existing)?;
                validate_log_retry(Self::BACKEND, &existing_message, message)?;
                existing
                    .try_get("seq")
                    .map_err(|err| db_err("decode idempotent append position", err))?
            }
        };

        sqlx::query(
            "UPDATE bus_log_identity SET high_water = GREATEST(high_water, $1) \
             WHERE singleton = 1",
        )
        .bind(seq)
        .execute(&mut *transaction)
        .await
        .map_err(|err| db_err("advance log high water", err))?;
        sqlx::query("SELECT pg_notify('distributed_bus_queue', '')")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("notify log append", err))?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit append", err))?;
        Ok(())
    }

    async fn prepare_log_epoch(
        &self,
        epoch_candidate: &ProjectionEpoch,
        expected_epoch: Option<&ProjectionEpoch>,
    ) -> Result<ProjectionEpoch, TransportError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|err| db_err("begin log identity", err))?;
        let epoch =
            Self::reconcile_log_identity(&mut transaction, epoch_candidate, expected_epoch).await?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit log identity", err))?;
        Ok(epoch)
    }

    async fn reset_log(
        &self,
        expected_epoch: &ProjectionEpoch,
        next_epoch: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|err| db_err("begin ordered-log reset", err))?;
        let actual: String = sqlx::query_scalar(
            "SELECT source_epoch FROM bus_log_identity \
             WHERE singleton = 1 FOR UPDATE",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(|err| db_err("lock ordered-log reset fence", err))?;
        if actual != expected_epoch.as_str() {
            return Err(TransportError::permanent(format!(
                "postgres bus ordered-log reset expected epoch {:?} but durable \
                 epoch is {:?}",
                expected_epoch.as_str(),
                actual
            )));
        }
        sqlx::query("TRUNCATE TABLE bus_log RESTART IDENTITY")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("clear ordered log", err))?;
        sqlx::query("DELETE FROM bus_offset")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("clear ordered-log offsets", err))?;
        sqlx::query(
            "UPDATE bus_log_identity \
             SET source_epoch = $1, generation = generation + 1, high_water = 0 \
             WHERE singleton = 1",
        )
        .bind(next_epoch.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|err| db_err("install next ordered-log epoch", err))?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit ordered-log reset", err))?;
        Ok(())
    }

    async fn claim(
        &self,
        names: &[String],
        lease_secs: f64,
        limit: i64,
    ) -> Result<Vec<ClaimedRow>, TransportError> {
        // `FOR UPDATE SKIP LOCKED` keeps the claim safe under competing
        // listeners — only one claims (and later settles) each row.
        // `gen_random_uuid()` is volatile: each claimed row gets its own token.
        let rows = sqlx::query(
            "UPDATE bus_queue SET locked_until = now() + ($1 * interval '1 second'), \
                    claim_token = gen_random_uuid()::text, \
                    attempts = attempts + 1 \
             WHERE seq IN ( \
                SELECT seq FROM bus_queue \
                WHERE (name = ANY($2) OR name IS NULL) AND available_at <= now() \
                      AND (locked_until IS NULL OR locked_until <= now()) \
                ORDER BY seq FOR UPDATE SKIP LOCKED LIMIT $3 \
             ) \
             RETURNING seq, claim_token, name, message_id, kind, payload, content_type, metadata",
        )
        .bind(lease_secs)
        .bind(names)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| db_err("claim", err))?;

        rows.into_iter()
            .map(|row| {
                let claim_token = row
                    .try_get("claim_token")
                    .map_err(|err| db_err("claim token", err))?;
                Ok(ClaimedRow {
                    row: ReceivedRow::from_row(Self::BACKEND, &row),
                    claim_token,
                })
            })
            .collect()
    }

    async fn has_claimable(&self, names: &[String]) -> Result<bool, TransportError> {
        let row = sqlx::query(
            "SELECT 1 FROM bus_queue \
             WHERE (name = ANY($1) OR name IS NULL) AND available_at <= now() \
                   AND (locked_until IS NULL OR locked_until <= now()) \
             LIMIT 1",
        )
        .bind(names)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| db_err("peek claimable", err))?;
        Ok(row.is_some())
    }

    async fn listen_wakeup(&self) -> Result<(), TransportError> {
        let mut listener = {
            let mut slot = self.listener.lock().await;
            match slot.take() {
                Some(existing) => existing,
                None => {
                    let mut created = PgListener::connect_with(&self.pool)
                        .await
                        .map_err(|err| db_err("connect notify listener", err))?;
                    created
                        .listen("distributed_bus_queue")
                        .await
                        .map_err(|err| db_err("listen distributed_bus_queue", err))?;
                    created
                }
            }
        };
        let received = tokio::time::timeout(Duration::from_millis(250), listener.recv()).await;
        *self.listener.lock().await = Some(listener);
        match received {
            Ok(Ok(_)) | Err(_) => Ok(()),
            Ok(Err(err)) => Err(db_err("recv bus notify", err)),
        }
    }

    async fn log_read(
        &self,
        names: &[String],
        consumer: &str,
        limit: i64,
        expected_epoch: &ProjectionEpoch,
    ) -> Result<Vec<ReceivedRow>, TransportError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|err| db_err("begin log read", err))?;
        Self::lock_expected_log_epoch(&mut transaction, expected_epoch).await?;
        let rows = sqlx::query(
            "SELECT seq, name, message_id, kind, payload, content_type, metadata FROM bus_log \
             WHERE (name = ANY($1) OR name IS NULL) \
                   AND seq > COALESCE(( \
                       SELECT last_seq FROM bus_offset \
                       WHERE consumer = $2 AND source_epoch = $3 \
                   ), 0) \
             ORDER BY seq LIMIT $4",
        )
        .bind(names)
        .bind(consumer)
        .bind(expected_epoch.as_str())
        .bind(limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|err| db_err("log read", err))?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit log read", err))?;

        Ok(rows
            .iter()
            .map(|row| ReceivedRow::from_row(Self::BACKEND, row))
            .collect())
    }

    async fn verify_log_epoch(
        &self,
        expected_epoch: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|err| db_err("begin log epoch check", err))?;
        Self::lock_expected_log_epoch(&mut transaction, expected_epoch).await?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit log epoch check", err))?;
        Ok(())
    }

    async fn delete_claimed(&self, seq: i64, claim_token: &str) -> Result<(), TransportError> {
        sqlx::query("DELETE FROM bus_queue WHERE seq = $1 AND claim_token = $2")
            .bind(seq)
            .bind(claim_token)
            .execute(&self.pool)
            .await
            .map_err(|err| db_err("delete", err))?;
        Ok(())
    }

    async fn release_claim(&self, seq: i64, claim_token: &str) -> Result<(), TransportError> {
        sqlx::query(
            "UPDATE bus_queue \
             SET locked_until = NULL, claim_token = NULL \
             WHERE seq = $1 AND claim_token = $2",
        )
        .bind(seq)
        .bind(claim_token)
        .execute(&self.pool)
        .await
        .map_err(|err| db_err("nack", err))?;
        Ok(())
    }

    async fn advance_offset(
        &self,
        consumer: &str,
        seq: i64,
        expected_epoch: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|err| db_err("begin advance offset", err))?;
        Self::lock_expected_log_epoch(&mut transaction, expected_epoch).await?;
        sqlx::query(
            "INSERT INTO bus_offset (consumer, source_epoch, last_seq) VALUES ($1, $2, $3) \
             ON CONFLICT (consumer) DO UPDATE SET \
                 source_epoch = EXCLUDED.source_epoch, \
                 last_seq = CASE \
                     WHEN bus_offset.source_epoch = EXCLUDED.source_epoch \
                     THEN GREATEST(bus_offset.last_seq, EXCLUDED.last_seq) \
                     ELSE EXCLUDED.last_seq \
                 END",
        )
        .bind(consumer)
        .bind(expected_epoch.as_str())
        .bind(seq)
        .execute(&mut *transaction)
        .await
        .map_err(|err| db_err("advance offset", err))?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit advance offset", err))?;
        Ok(())
    }
}
