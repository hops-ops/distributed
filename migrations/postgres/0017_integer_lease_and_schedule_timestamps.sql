-- Leases and schedules move to bigint microseconds since the UNIX epoch,
-- like every other framework timestamp (0016). Aggregate cells hold neither
-- table, so this is a migration of its own.

-- Epoch seconds round to the microsecond; a time already in microseconds (a
-- table the standalone lease DDL of this release created before these
-- migrations ran) is kept. A released lease's `expires_at = 0` stays 0.
ALTER TABLE coordination_leases
  ALTER COLUMN acquired_at TYPE bigint
    USING (CASE WHEN acquired_at < 1000000000000 THEN round(acquired_at * 1000000) ELSE acquired_at END)::bigint,
  ALTER COLUMN expires_at TYPE bigint
    USING (CASE WHEN expires_at < 1000000000000 THEN round(expires_at * 1000000) ELSE expires_at END)::bigint;

-- Already integer epoch milliseconds: rename the columns after their new
-- unit. The armed-due index follows the rename.
ALTER TABLE aggregate_schedules RENAME COLUMN due_at_ms TO due_at;
ALTER TABLE aggregate_schedules RENAME COLUMN claimed_until_ms TO claimed_until;
UPDATE aggregate_schedules
SET due_at = due_at * 1000,
    claimed_until = claimed_until * 1000;
