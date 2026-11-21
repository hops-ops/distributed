-- Schedule worker claims (see `distributed::schedule::worker`). A worker
-- claims a due armed row by setting `claimed_until_ms` (epoch ms, database
-- clock) and dispatches it; other workers skip the row until the claim
-- expires. Settling or re-arming clears the claim. Exactly-once effects per
-- (owner, name, generation) come from the command ledger, not the claim: a
-- crashed worker's claim simply lapses and the next worker replays.
ALTER TABLE aggregate_schedules ADD COLUMN claimed_until_ms BIGINT;
