-- Every framework timestamp becomes bigint microseconds since the UNIX
-- epoch, the representation SQLite stores too. Statements bind and read
-- plain integers in both dialects, so no select list converts a column and
-- the database clock is one expression.
--
-- These are the tables an aggregate cell also holds; leases and schedules
-- follow in 0017. Each column is converted in place, rows preserved:
-- - timestamptz to its exact microsecond count (timestamptz's own
--   resolution, so nothing is lost);
-- - epoch-second double precision rounded to the microsecond, unless it is
--   already microseconds (a lock table created by the standalone lock DDL
--   of this release before these migrations ran).
-- A default is dropped before its column's type changes and set again after.

ALTER TABLE aggregate_events
  ALTER COLUMN recorded_at DROP DEFAULT,
  ALTER COLUMN recorded_at TYPE bigint
    USING (EXTRACT(EPOCH FROM recorded_at) * 1000000)::bigint,
  ALTER COLUMN recorded_at SET DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;

ALTER TABLE aggregate_snapshots
  ALTER COLUMN recorded_at DROP DEFAULT,
  ALTER COLUMN updated_at DROP DEFAULT,
  ALTER COLUMN recorded_at TYPE bigint
    USING (EXTRACT(EPOCH FROM recorded_at) * 1000000)::bigint,
  ALTER COLUMN updated_at TYPE bigint
    USING (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint,
  ALTER COLUMN recorded_at SET DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint,
  ALTER COLUMN updated_at SET DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;

ALTER TABLE outbox_messages
  ALTER COLUMN updated_at DROP DEFAULT,
  ALTER COLUMN created_at TYPE bigint
    USING (EXTRACT(EPOCH FROM created_at) * 1000000)::bigint,
  ALTER COLUMN next_available_at TYPE bigint
    USING (EXTRACT(EPOCH FROM next_available_at) * 1000000)::bigint,
  ALTER COLUMN claimed_until TYPE bigint
    USING (EXTRACT(EPOCH FROM claimed_until) * 1000000)::bigint,
  ALTER COLUMN failed_at TYPE bigint
    USING (EXTRACT(EPOCH FROM failed_at) * 1000000)::bigint,
  ALTER COLUMN updated_at TYPE bigint
    USING (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint,
  ALTER COLUMN updated_at SET DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;

ALTER TABLE consumer_inbox
  ALTER COLUMN processed_at DROP DEFAULT,
  ALTER COLUMN processed_at TYPE bigint
    USING (EXTRACT(EPOCH FROM processed_at) * 1000000)::bigint,
  ALTER COLUMN processed_at SET DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;

ALTER TABLE aggregate_locks
  ALTER COLUMN acquired_at TYPE bigint
    USING (CASE WHEN acquired_at < 1000000000000 THEN round(acquired_at * 1000000) ELSE acquired_at END)::bigint,
  ALTER COLUMN expires_at TYPE bigint
    USING (CASE WHEN expires_at < 1000000000000 THEN round(expires_at * 1000000) ELSE expires_at END)::bigint;

ALTER TABLE command_ledger
  ALTER COLUMN created_at DROP DEFAULT,
  ALTER COLUMN updated_at DROP DEFAULT,
  ALTER COLUMN lease_expires_at TYPE bigint
    USING (EXTRACT(EPOCH FROM lease_expires_at) * 1000000)::bigint,
  ALTER COLUMN created_at TYPE bigint
    USING (EXTRACT(EPOCH FROM created_at) * 1000000)::bigint,
  ALTER COLUMN updated_at TYPE bigint
    USING (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint,
  ALTER COLUMN completed_at TYPE bigint
    USING (EXTRACT(EPOCH FROM completed_at) * 1000000)::bigint,
  ALTER COLUMN retention_expires_at TYPE bigint
    USING (EXTRACT(EPOCH FROM retention_expires_at) * 1000000)::bigint,
  ALTER COLUMN compacted_at TYPE bigint
    USING (EXTRACT(EPOCH FROM compacted_at) * 1000000)::bigint,
  ALTER COLUMN created_at SET DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint,
  ALTER COLUMN updated_at SET DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;

-- Already integer epoch milliseconds: rename the column after its new unit.
ALTER TABLE cell_alarms RENAME COLUMN due_at_ms TO due_at;
UPDATE cell_alarms SET due_at = due_at * 1000;
