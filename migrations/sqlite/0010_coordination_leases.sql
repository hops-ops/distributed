-- Durable TTL leases with monotonic fencing tokens (see `distributed::lease`).
-- One row per lease key. `fencing_token` increments on every acquisition and
-- is never reset: release sets `expires_at = 0` and keeps the row, so a later
-- holder always receives a strictly greater token. `acquired_at`/`expires_at`
-- are epoch seconds from the database clock (unixepoch('now','subsec')).
CREATE TABLE IF NOT EXISTS coordination_leases (
  lease_key TEXT NOT NULL PRIMARY KEY,
  holder TEXT NOT NULL,
  fencing_token INTEGER NOT NULL,
  acquired_at REAL NOT NULL,
  expires_at REAL NOT NULL,
  CHECK (lease_key <> ''),
  CHECK (holder <> ''),
  CHECK (fencing_token > 0)
);
