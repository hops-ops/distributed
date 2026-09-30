//! SQLite [`Bus`] + [`BusConsumer`] — a local durable bus.
//!
//! SQLite covers both bus modes with the same app-facing shape as
//! [`PostgresBus`](super::PostgresBus), but with SQLite-native concurrency:
//!
//! - **`send` / `listen` (point-to-point, competing):** a durable work-queue
//!   table (`bus_queue`) claimed by one atomic `UPDATE ... RETURNING` under a
//!   lease. SQLite serializes writers, so one of N competing `listen`ers handles
//!   each command and the row is deleted on success.
//! - **`publish` / `subscribe` (fan-out):** an append-only `bus_log` table, its
//!   durable generation identity (`bus_log_identity`), and a per-consumer
//!   epoch-bound offset table (`bus_offset`). Each `group` reads independently
//!   past its own offset, so every group sees every event.
//!
//! The bus model itself (builders, sources, settlement, corruption handling) is
//! shared with the Postgres bus in `sql_bus_common`;
//! this module contributes only the SQLite dialect: `?` placeholders, the
//! `unixepoch('now','subsec')` clock, `IN`-list name binding, and
//! `randomblob(16)` claim tokens.
//!
//! This is a no-extra-process transport for local development, tests, demos, and
//! small single-node deployments. It is not a high-throughput broker replacement.
//!
//! Requires the `sqlite` feature. Integration-tested in `tests/sqlite_transport`.
//!
//! [`Bus`]: super::Bus
//! [`BusConsumer`]: super::BusConsumer

use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection, SqlitePool};

use super::sql_bus_common::{
    db_err as sql_db_err, message_from_row, metadata_json, validate_log_retry, ClaimedRow,
    ReceivedRow, SqlBus, SqlBusDialect, SqlLogReceived, SqlQueueReceived,
};
use super::{Message, TransportError};
use crate::projection_protocol::ProjectionEpoch;

const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS bus_queue (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_token  TEXT,
    name         TEXT NOT NULL,
    message_id   TEXT,
    kind         TEXT NOT NULL,
    payload      BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/json',
    metadata     TEXT NOT NULL DEFAULT '[]',
    available_at REAL NOT NULL DEFAULT (unixepoch('now','subsec')),
    locked_until REAL,
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
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    message_id   TEXT,
    kind         TEXT NOT NULL,
    payload      BLOB NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/json',
    metadata     TEXT NOT NULL DEFAULT '[]',
    appended_at  REAL NOT NULL DEFAULT (unixepoch('now','subsec')),
    CHECK (name <> ''),
    CHECK (kind IN ('command', 'event')),
    CHECK (content_type <> '')
);
CREATE INDEX IF NOT EXISTS bus_log_name_seq_idx ON bus_log (name, seq);
CREATE TABLE IF NOT EXISTS bus_offset (
    consumer TEXT PRIMARY KEY,
    source_epoch TEXT NOT NULL,
    last_seq INTEGER NOT NULL DEFAULT 0,
    CHECK (consumer <> ''),
    CHECK (source_epoch <> ''),
    CHECK (last_seq >= 0)
);
CREATE TABLE IF NOT EXISTS bus_log_identity (
    singleton    INTEGER PRIMARY KEY DEFAULT 1,
    source_epoch TEXT NOT NULL,
    generation   INTEGER NOT NULL DEFAULT 1,
    high_water   INTEGER NOT NULL DEFAULT 0,
    CHECK (singleton = 1),
    CHECK (source_epoch <> ''),
    CHECK (generation > 0),
    CHECK (high_water >= 0)
)";

fn db_err(context: &str, err: sqlx::Error) -> TransportError {
    sql_db_err(SqliteBusDialect::BACKEND, context, err)
}

/// Push `(name IN (…) OR name IS NULL)` with one bind per name.
fn push_name_filter(query: &mut QueryBuilder<Sqlite>, names: &[String]) {
    query.push("(name IN (");
    {
        let mut separated = query.separated(", ");
        for name in names {
            separated.push_bind(name.as_str());
        }
    }
    query.push(") OR name IS NULL)");
}

/// SQLite [`Bus`](super::Bus) + [`BusConsumer`](super::BusConsumer). Cheap to
/// clone (the pool is an `Arc`).
pub type SqliteBus = SqlBus<SqliteBusDialect>;

/// A claimed SQLite `bus_queue` row: `ack` deletes it (done); `nack` makes it
/// available again (redelivery); `dead_letter`/`park` delete it (stop
/// redelivery). Settlement is fenced by the claim token.
pub type SqliteQueueReceived = SqlQueueReceived<SqliteBusDialect>;

/// A SQLite `bus_log` entry: `ack` advances this consumer's offset to its
/// `seq`; `nack` leaves the offset (redelivery); `dead_letter`/`park` advance
/// past it (skip, don't get stuck).
pub type SqliteLogReceived = SqlLogReceived<SqliteBusDialect>;

impl SqliteBus {
    /// Build a bus over an existing pool.
    ///
    /// For event subscriptions, `subscribe` uses the router's consumer identity
    /// as the durable SQLite log offset. Service consumers usually get that
    /// identity from [`Service::named`](crate::microsvc::Service::named). Direct
    /// consumers can set it with [`group`](Self::group). Commands are claimed
    /// from `bus_queue` by message name, so command replicas compete by listening
    /// to the same registered command names.
    pub fn new(pool: SqlitePool) -> Self {
        SqlBus::from_dialect(SqliteBusDialect { pool })
    }

    /// Build a bus with an explicit group for direct/low-level use.
    pub fn new_with_group(pool: SqlitePool, group: impl Into<String>) -> Self {
        Self::new(pool).group(group)
    }
}

/// The SQLite [`SqlBusDialect`].
#[derive(Clone)]
pub struct SqliteBusDialect {
    pool: SqlitePool,
}

impl SqliteBusDialect {
    async fn insert(
        &self,
        sql: &'static str,
        context: &'static str,
        message: &Message,
    ) -> Result<(), TransportError> {
        let metadata = metadata_json(message);
        sqlx::query(sql)
            .bind(&message.name)
            .bind(&message.id)
            .bind(message.kind.as_str())
            .bind(&message.payload)
            .bind(&message.content_type)
            .bind(metadata)
            .execute(&self.pool)
            .await
            .map_err(|err| db_err(context, err))?;
        Ok(())
    }

    /// SQLite's first write serializes this transaction with every other
    /// framework append. The singleton row therefore keeps the persisted
    /// epoch/high-water pair exact. An observed maximum below the high-water
    /// fails closed; only [`SqlBus::reset_ordered_log`] may retire the cursor
    /// domain.
    async fn reconcile_log_identity(
        connection: &mut SqliteConnection,
        epoch_candidate: &ProjectionEpoch,
        expected_epoch: Option<&ProjectionEpoch>,
    ) -> Result<ProjectionEpoch, TransportError> {
        let inserted = sqlx::query(
            "INSERT INTO bus_log_identity \
                 (singleton, source_epoch, generation, high_water) \
             VALUES (1, ?, 1, 0) \
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
             FROM bus_log_identity WHERE singleton = 1",
        )
        .fetch_one(&mut *connection)
        .await
        .map_err(|err| db_err("read log identity", err))?;
        let source_epoch: String = identity
            .try_get("source_epoch")
            .map_err(|err| db_err("decode log identity epoch", err))?;
        let high_water: i64 = identity
            .try_get("high_water")
            .map_err(|err| db_err("decode log identity high water", err))?;
        let log_max: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM bus_log")
            .fetch_one(&mut *connection)
            .await
            .map_err(|err| db_err("read log high water", err))?;

        if inserted && log_max > 0 && expected_epoch.is_none() {
            return Err(TransportError::permanent(format!(
                "sqlite bus ordered-log identity is missing while the log still \
                 contains positions through {log_max}; refusing to assign a \
                 random epoch, explicitly adopt the retained log with \
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
                "sqlite bus observed ordered-log maximum {log_max} below durable \
                 high-water {high_water}; ordinary startup cannot rotate cursor \
                 identity, use reset_ordered_log with the expected epoch"
            )));
        } else if log_max > high_water {
            sqlx::query("UPDATE bus_log_identity SET high_water = ? WHERE singleton = 1")
                .bind(log_max)
                .execute(&mut *connection)
                .await
                .map_err(|err| db_err("advance log high water", err))?;
        }
        if let Some(expected_epoch) = expected_epoch {
            if source_epoch != expected_epoch.as_str() {
                return Err(TransportError::permanent(format!(
                    "sqlite bus configured source epoch {:?} does not match \
                     durable ordered-log epoch {:?}",
                    expected_epoch.as_str(),
                    source_epoch
                )));
            }
        }

        ProjectionEpoch::new(source_epoch).map_err(|error| {
            TransportError::permanent(format!("sqlite bus corrupt log identity epoch: {error}"))
        })
    }

    async fn validate_expected_log_epoch(
        connection: &mut SqliteConnection,
        expected_epoch: &ProjectionEpoch,
    ) -> Result<(), TransportError> {
        let actual: String =
            sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
                .fetch_one(&mut *connection)
                .await
                .map_err(|err| db_err("read log identity for delivery", err))?;
        let actual = ProjectionEpoch::new(actual).map_err(|error| {
            TransportError::permanent(format!("sqlite bus corrupt log identity epoch: {error}"))
        })?;
        if &actual != expected_epoch {
            return Err(TransportError::permanent(format!(
                "sqlite bus ordered-log epoch changed from {:?} to {:?}; \
                 the subscriber must restart",
                expected_epoch.as_str(),
                actual.as_str()
            )));
        }
        Ok(())
    }
}

impl SqlBusDialect for SqliteBusDialect {
    const BACKEND: &'static str = "sqlite";
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
        // Acquire SQLite's writer reservation before inspecting legacy rows so
        // duplicate validation, deletion, and unique-index creation are one
        // serialized migration.
        sqlx::query("UPDATE bus_log SET seq = seq WHERE 0")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("lock ordered log for schema upgrade", err))?;
        let offset_columns = sqlx::query("PRAGMA table_info(bus_offset)")
            .fetch_all(&mut *transaction)
            .await
            .map_err(|err| db_err("inspect offset schema", err))?;
        let has_source_epoch = offset_columns.iter().any(|column| {
            column
                .try_get::<String, _>("name")
                .map(|name| name == "source_epoch")
                .unwrap_or(false)
        });
        if !has_source_epoch {
            sqlx::query("ALTER TABLE bus_offset ADD COLUMN source_epoch TEXT")
                .execute(&mut *transaction)
                .await
                .map_err(|err| db_err("add offset epoch", err))?;
        }
        sqlx::query("DELETE FROM bus_offset WHERE source_epoch IS NULL OR source_epoch = ''")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("clear legacy unbound offsets", err))?;

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
        for seq in redundant {
            sqlx::query("DELETE FROM bus_log WHERE seq = ?")
                .bind(seq)
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
        self.insert(
            "INSERT INTO bus_queue (name, message_id, kind, payload, content_type, metadata) \
             VALUES (?, ?, ?, ?, ?, ?)",
            "enqueue",
            message,
        )
        .await
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
                 VALUES (?, ?, ?, ?, ?, ?) \
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
                     VALUES (?, ?, ?, ?, ?, ?) \
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
                     FROM bus_log WHERE message_id = ?",
                )
                .bind(message.id())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|err| db_err("read idempotent append", err))?
                .ok_or_else(|| {
                    TransportError::permanent(
                        "sqlite bus stable message ID conflict had no existing log row",
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
            "UPDATE bus_log_identity SET high_water = MAX(high_water, ?) \
             WHERE singleton = 1",
        )
        .bind(seq)
        .execute(&mut *transaction)
        .await
        .map_err(|err| db_err("advance log high water", err))?;
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
        // Take the writer reservation before reading the compare-and-swap
        // fence; no append or competing reset can commit between validation
        // and generation replacement.
        sqlx::query("UPDATE bus_log_identity SET high_water = high_water WHERE singleton = 1")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("lock ordered-log reset fence", err))?;
        let actual: String =
            sqlx::query_scalar("SELECT source_epoch FROM bus_log_identity WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(|err| db_err("read ordered-log reset fence", err))?;
        if actual != expected_epoch.as_str() {
            return Err(TransportError::permanent(format!(
                "sqlite bus ordered-log reset expected epoch {:?} but durable \
                 epoch is {:?}",
                expected_epoch.as_str(),
                actual
            )));
        }
        sqlx::query("DELETE FROM bus_log")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("clear ordered log", err))?;
        sqlx::query("DELETE FROM sqlite_sequence WHERE name = 'bus_log'")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("reset ordered-log sequence", err))?;
        sqlx::query("DELETE FROM bus_offset")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("clear ordered-log offsets", err))?;
        sqlx::query(
            "UPDATE bus_log_identity \
             SET source_epoch = ?, generation = generation + 1, high_water = 0 \
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
        // SQLite serializes writers, so the `UPDATE ... RETURNING` claim is
        // atomic under competing listeners — only one claims each row.
        // `randomblob` is non-deterministic: each claimed row gets its own token.
        let mut query = QueryBuilder::<Sqlite>::new(
            "UPDATE bus_queue \
             SET locked_until = unixepoch('now','subsec') + ",
        );
        query.push_bind(lease_secs);
        query.push(
            ", claim_token = lower(hex(randomblob(16))), \
                attempts = attempts + 1 \
             WHERE seq IN ( \
                SELECT seq FROM bus_queue \
                WHERE ",
        );
        push_name_filter(&mut query, names);
        query.push(
            " AND available_at <= unixepoch('now','subsec') \
              AND (locked_until IS NULL OR locked_until <= unixepoch('now','subsec')) \
              ORDER BY seq LIMIT ",
        );
        query.push_bind(limit);
        query.push(
            ") \
             RETURNING seq, claim_token, name, message_id, kind, payload, content_type, metadata",
        );

        let rows = query
            .build()
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
        let mut query = QueryBuilder::<Sqlite>::new("SELECT 1 FROM bus_queue WHERE ");
        push_name_filter(&mut query, names);
        query.push(
            " AND available_at <= unixepoch('now','subsec') \
             AND (locked_until IS NULL OR locked_until <= unixepoch('now','subsec')) \
             LIMIT 1",
        );
        let row = query
            .build()
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| db_err("peek claimable", err))?;
        Ok(row.is_some())
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
        Self::validate_expected_log_epoch(&mut transaction, expected_epoch).await?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT seq, name, message_id, kind, payload, content_type, metadata \
             FROM bus_log \
             WHERE ",
        );
        push_name_filter(&mut query, names);
        query.push(" AND seq > COALESCE((SELECT last_seq FROM bus_offset WHERE consumer = ");
        query.push_bind(consumer);
        query.push(" AND source_epoch = ");
        query.push_bind(expected_epoch.as_str());
        query.push("), 0) ORDER BY seq LIMIT ");
        query.push_bind(limit);

        let rows = query
            .build()
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
        Self::validate_expected_log_epoch(&mut transaction, expected_epoch).await?;
        transaction
            .commit()
            .await
            .map_err(|err| db_err("commit log epoch check", err))?;
        Ok(())
    }

    async fn delete_claimed(&self, seq: i64, claim_token: &str) -> Result<(), TransportError> {
        sqlx::query("DELETE FROM bus_queue WHERE seq = ? AND claim_token = ?")
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
             WHERE seq = ? AND claim_token = ?",
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
        // Take SQLite's writer reservation before checking the epoch so a
        // generation rotation cannot commit between validation and settlement.
        sqlx::query("UPDATE bus_log_identity SET high_water = high_water WHERE singleton = 1")
            .execute(&mut *transaction)
            .await
            .map_err(|err| db_err("lock log identity for settlement", err))?;
        Self::validate_expected_log_epoch(&mut transaction, expected_epoch).await?;
        sqlx::query(
            "INSERT INTO bus_offset (consumer, source_epoch, last_seq) VALUES (?, ?, ?) \
             ON CONFLICT (consumer) DO UPDATE SET \
                 source_epoch = excluded.source_epoch, \
                 last_seq = CASE \
                     WHEN bus_offset.source_epoch = excluded.source_epoch \
                     THEN MAX(bus_offset.last_seq, excluded.last_seq) \
                     ELSE excluded.last_seq \
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
