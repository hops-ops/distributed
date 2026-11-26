-- Leases and schedules move to INTEGER microseconds since the UNIX epoch,
-- like every other framework timestamp (0016). Aggregate cells hold neither
-- table, so this is a migration of its own.

-- REAL keeps its affinity, so the lease table is rebuilt. Epoch seconds
-- round to the microsecond; a time already in microseconds (a table the
-- standalone lease DDL of this release created before these migrations ran)
-- is kept. A released lease's `expires_at = 0` stays 0.
CREATE TABLE coordination_leases_rebuilt (
  lease_key TEXT NOT NULL PRIMARY KEY,
  holder TEXT NOT NULL,
  fencing_token INTEGER NOT NULL,
  acquired_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  CHECK (lease_key <> ''),
  CHECK (holder <> ''),
  CHECK (fencing_token > 0)
);

INSERT INTO coordination_leases_rebuilt (lease_key, holder, fencing_token, acquired_at, expires_at)
SELECT
  lease_key, holder, fencing_token,
  CASE WHEN acquired_at < 1000000000000 THEN CAST(round(acquired_at * 1000000) AS INTEGER) ELSE CAST(acquired_at AS INTEGER) END,
  CASE WHEN expires_at < 1000000000000 THEN CAST(round(expires_at * 1000000) AS INTEGER) ELSE CAST(expires_at AS INTEGER) END
FROM coordination_leases;

DROP TABLE coordination_leases;
ALTER TABLE coordination_leases_rebuilt RENAME TO coordination_leases;

-- Already integer epoch milliseconds: rename the columns after their new
-- unit. The armed-due index follows the rename.
ALTER TABLE aggregate_schedules RENAME COLUMN due_at_ms TO due_at;
ALTER TABLE aggregate_schedules RENAME COLUMN claimed_until_ms TO claimed_until;
UPDATE aggregate_schedules
SET due_at = due_at * 1000,
    claimed_until = claimed_until * 1000;
