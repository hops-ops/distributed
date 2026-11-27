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
//!   past its own offset, so every group sees every event. One consumer of a
//!   group reads at a time, under the group's lease in `bus_consumer_lease`.
//!
//! The bus model itself (builders, sources, settlement, corruption handling) is
//! shared with the Postgres bus in `sql_bus_common`;
//! this module contributes only the SQLite dialect: `BEGIN IMMEDIATE` in place
//! of row locks, the `unixepoch('now','subsec')` clock, `IN`-list name
//! binding, `randomblob(16)` claim tokens and the `sqlite_sequence` reset.
//!
//! This is a no-extra-process transport for local development, tests, demos, and
//! small single-node deployments. It is not a high-throughput broker replacement.
//!
//! Requires the `sqlite` feature. Integration-tested in `tests/transports/sqlite_transport`.
//!
//! [`Bus`]: super::Bus
//! [`BusConsumer`]: super::BusConsumer

use sqlx::{QueryBuilder, Sqlite};

use super::sql_bus_common::{SqlBus, SqlBusDialect, SqlLogReceived, SqlQueueReceived};

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
CREATE UNIQUE INDEX IF NOT EXISTS bus_log_message_id_unique_idx
    ON bus_log (message_id) WHERE message_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS bus_offset (
    consumer TEXT PRIMARY KEY,
    source_epoch TEXT NOT NULL,
    last_seq INTEGER NOT NULL DEFAULT 0,
    CHECK (consumer <> ''),
    CHECK (source_epoch <> ''),
    CHECK (last_seq >= 0)
);
CREATE TABLE IF NOT EXISTS bus_consumer_lease (
    consumer     TEXT PRIMARY KEY,
    holder       TEXT NOT NULL,
    locked_until REAL NOT NULL,
    CHECK (consumer <> ''),
    CHECK (holder <> '')
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

/// SQLite [`Bus`](super::Bus) + [`BusConsumer`](super::BusConsumer). Cheap to
/// clone (the pool is an `Arc`). Build it with `SqlBus::new` over a
/// `SqlitePool`.
pub type SqliteBus = SqlBus<Sqlite>;

/// A claimed SQLite `bus_queue` row: `ack` deletes it (done); `nack` makes it
/// available again after the requested delay. Settlement is fenced by the
/// claim token.
pub type SqliteQueueReceived = SqlQueueReceived<Sqlite>;

/// A SQLite `bus_log` entry: `ack` records it as done for the group; `nack`
/// leaves the offset, so the entry is read again.
pub type SqliteLogReceived = SqlLogReceived;

/// SQLite has one writer at a time: a transaction that will write opens with
/// `BEGIN IMMEDIATE`, which is the lock every row lock stands for.
impl SqlBusDialect for Sqlite {
    const BACKEND: &'static str = "sqlite";
    const SCHEMA: &'static str = SCHEMA;
    const BEGIN_WRITE: &'static str = "BEGIN IMMEDIATE";
    const LOCK_IDENTITY: &'static str =
        "SELECT source_epoch, high_water FROM bus_log_identity WHERE singleton = 1";
    const SHARE_IDENTITY: &'static str =
        "SELECT source_epoch FROM bus_log_identity WHERE singleton = 1";
    const SKIP_LOCKED: &'static str = "";
    const GREATEST: &'static str = "MAX";
    const CLOCK: &'static str = "unixepoch('now','subsec')";
    // Non-deterministic: each claimed row gets its own token.
    const NEW_CLAIM_TOKEN: &'static str = "lower(hex(randomblob(16)))";
    const CLEAR_LOG: &'static [&'static str] = &[
        "DELETE FROM bus_log",
        "DELETE FROM sqlite_sequence WHERE name = 'bus_log'",
    ];
    const COUNT_SCHEMA_OBJECTS: &'static str =
        "SELECT CAST(count(*) AS BIGINT) AS present FROM sqlite_master WHERE ";

    fn push_clock_plus(query: &mut QueryBuilder<Self>, seconds: f64) {
        query.push("unixepoch('now','subsec') + ");
        query.push_bind(seconds);
    }

    fn push_name_in(query: &mut QueryBuilder<Self>, names: &[String]) {
        query.push("name IN (");
        let mut separated = query.separated(", ");
        for name in names {
            separated.push_bind(name.clone());
        }
        query.push(")");
    }
}
