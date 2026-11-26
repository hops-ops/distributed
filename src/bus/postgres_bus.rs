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
//!   One consumer of a group reads at a time, under the group's lease in
//!   `bus_consumer_lease`.
//!   Because the log, the offset, and projection writes share one Postgres, the
//!   offset advances in the same database as the effects — the cleanest path to
//!   transactional effectively-once of any transport (the offset is the inbox).
//!
//! The bus model itself (builders, sources, settlement, corruption handling) is
//! shared with the SQLite bus in `sql_bus_common`; this
//! module contributes only the Postgres dialect: row locks (`FOR UPDATE`,
//! `FOR SHARE`, `SKIP LOCKED`), the `now()` clock, `= ANY` array binding for
//! name lists, `gen_random_uuid()` claim tokens and `TRUNCATE ... RESTART
//! IDENTITY`.
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
//! Requires the `postgres` feature. Integration-tested in `tests/transports/postgres_transport`.
//!
//! [`Bus`]: super::Bus
//! [`BusConsumer`]: super::BusConsumer
//! [`MessageSource`]: super::MessageSource
//! [`run_source`]: super::run_source

use sqlx::{Postgres, QueryBuilder};

use super::sql_bus_common::{SqlBus, SqlBusDialect, SqlLogReceived, SqlQueueReceived};

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
CREATE UNIQUE INDEX IF NOT EXISTS bus_log_message_id_unique_idx
    ON bus_log (message_id) WHERE message_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS bus_offset (
    consumer TEXT PRIMARY KEY,
    source_epoch TEXT NOT NULL,
    last_seq BIGINT NOT NULL DEFAULT 0,
    CHECK (consumer <> ''),
    CHECK (source_epoch <> ''),
    CHECK (last_seq >= 0)
);
CREATE TABLE IF NOT EXISTS bus_consumer_lease (
    consumer     TEXT PRIMARY KEY,
    holder       TEXT NOT NULL,
    locked_until TIMESTAMPTZ NOT NULL,
    CHECK (consumer <> ''),
    CHECK (holder <> '')
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

/// Postgres [`Bus`](super::Bus) + [`BusConsumer`](super::BusConsumer). Cheap to
/// clone (the pool is an `Arc`). Build it with [`SqlBus::new`] over a `PgPool`.
pub type PostgresBus = SqlBus<Postgres>;

/// A claimed Postgres `bus_queue` row: `ack` deletes it (done); `nack` makes it
/// available again after the requested delay. Settlement is fenced by the
/// claim token.
pub type QueueReceived = SqlQueueReceived<Postgres>;

/// A Postgres `bus_log` entry: `ack` records it as done for the group; `nack`
/// leaves the offset, so the entry is read again.
pub type LogReceived = SqlLogReceived;

impl SqlBusDialect for Postgres {
    const BACKEND: &'static str = "postgres";
    const SCHEMA: &'static str = SCHEMA;
    const BEGIN_WRITE: &'static str = "BEGIN";
    const LOCK_IDENTITY: &'static str = "SELECT source_epoch, high_water FROM bus_log_identity \
         WHERE singleton = 1 FOR UPDATE";
    const SHARE_IDENTITY: &'static str =
        "SELECT source_epoch FROM bus_log_identity WHERE singleton = 1 FOR SHARE";
    const SKIP_LOCKED: &'static str = " FOR UPDATE SKIP LOCKED";
    const GREATEST: &'static str = "GREATEST";
    const CLOCK: &'static str = "now()";
    // Volatile: each claimed row gets its own token.
    const NEW_CLAIM_TOKEN: &'static str = "gen_random_uuid()::text";
    const CLEAR_LOG: &'static [&'static str] = &["TRUNCATE TABLE bus_log RESTART IDENTITY"];
    const COUNT_SCHEMA_OBJECTS: &'static str = "SELECT CAST(count(*) AS BIGINT) AS present FROM \
         (SELECT relname::text AS name FROM pg_class \
          WHERE relnamespace = current_schema()::regnamespace) AS objects WHERE ";

    fn push_clock_plus(query: &mut QueryBuilder<Self>, seconds: f64) {
        query.push("now() + (");
        query.push_bind(seconds);
        query.push(" * interval '1 second')");
    }

    fn push_name_in(query: &mut QueryBuilder<Self>, names: &[String]) {
        query.push("name = ANY(");
        query.push_bind(names.to_vec());
        query.push(")");
    }
}
