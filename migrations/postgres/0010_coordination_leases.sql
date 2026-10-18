-- Durable TTL leases with monotonic fencing tokens (see `distributed::lease`).
-- One row per lease key. `fencing_token` increments on every acquisition and
-- is never reset: release sets `expires_at = 0` and keeps the row, so a later
-- holder always receives a strictly greater token. `acquired_at`/`expires_at`
-- are epoch seconds from the database clock (clock_timestamp()).
CREATE TABLE IF NOT EXISTS coordination_leases (
  lease_key text NOT NULL PRIMARY KEY,
  holder text NOT NULL,
  fencing_token bigint NOT NULL,
  acquired_at double precision NOT NULL,
  expires_at double precision NOT NULL,
  CHECK (lease_key <> ''),
  CHECK (holder <> ''),
  CHECK (fencing_token > 0)
);
