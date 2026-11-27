-- Every framework timestamp becomes INTEGER microseconds since the UNIX
-- epoch, the representation PostgreSQL stores too. Timestamps compare and
-- order as plain integers, so no statement casts a column and the outbox
-- claim index can serve its predicate and order.
--
-- These are the tables an aggregate cell also holds; leases and schedules
-- follow in 0017. Columns typed TEXT or REAL keep that affinity, so each
-- table is rebuilt; its rows are converted, never dropped:
-- - `"secs.nanos"` text (written by the repository) keeps its first six
--   fractional digits;
-- - `YYYY-MM-DD HH:MM:SS` text (a `CURRENT_TIMESTAMP` default) is whole
--   seconds;
-- - REAL epoch seconds (`unixepoch('now','subsec')`, millisecond precision)
--   round to the microsecond;
-- - a lock time already in microseconds (a lock table the standalone lock
--   DDL of this release created before these migrations ran) is kept.
-- Text that is neither form fails the NOT NULL constraint and aborts the
-- migration instead of guessing.

CREATE TABLE aggregate_events_rebuilt (
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  event_name TEXT NOT NULL,
  event_version INTEGER NOT NULL DEFAULT 1,
  payload BLOB NOT NULL,
  payload_codec TEXT NOT NULL,
  payload_codec_version INTEGER NOT NULL,
  metadata TEXT NOT NULL DEFAULT '{}',
  recorded_at INTEGER NOT NULL DEFAULT (CAST(round(unixepoch('now', 'subsec') * 1000) AS INTEGER) * 1000),
  PRIMARY KEY (aggregate_type, aggregate_id, sequence),
  CHECK (aggregate_type <> ''),
  CHECK (aggregate_id <> ''),
  CHECK (sequence > 0),
  CHECK (event_version > 0),
  CHECK (payload_codec <> ''),
  CHECK (payload_codec_version > 0)
);

INSERT INTO aggregate_events_rebuilt (
  aggregate_type, aggregate_id, sequence, event_name, event_version, payload,
  payload_codec, payload_codec_version, metadata, recorded_at
)
SELECT
  aggregate_type, aggregate_id, sequence, event_name, event_version, payload,
  payload_codec, payload_codec_version, metadata,
  CASE
    WHEN instr(recorded_at, '-') > 0 THEN unixepoch(recorded_at) * 1000000
    WHEN instr(recorded_at, '.') = 0 THEN CAST(recorded_at AS INTEGER) * 1000000
    ELSE CAST(substr(recorded_at, 1, instr(recorded_at, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(recorded_at, instr(recorded_at, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END
FROM aggregate_events;

DROP TABLE aggregate_events;
ALTER TABLE aggregate_events_rebuilt RENAME TO aggregate_events;

CREATE TABLE aggregate_snapshots_rebuilt (
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  snapshot_version INTEGER NOT NULL,
  payload BLOB NOT NULL,
  payload_codec TEXT NOT NULL,
  payload_codec_version INTEGER NOT NULL,
  metadata TEXT NOT NULL DEFAULT '{}',
  recorded_at INTEGER NOT NULL DEFAULT (CAST(round(unixepoch('now', 'subsec') * 1000) AS INTEGER) * 1000),
  updated_at INTEGER NOT NULL DEFAULT (CAST(round(unixepoch('now', 'subsec') * 1000) AS INTEGER) * 1000),
  PRIMARY KEY (aggregate_type, aggregate_id),
  CHECK (aggregate_type <> ''),
  CHECK (aggregate_id <> ''),
  CHECK (version > 0),
  CHECK (snapshot_version > 0),
  CHECK (payload_codec <> ''),
  CHECK (payload_codec_version > 0)
);

INSERT INTO aggregate_snapshots_rebuilt (
  aggregate_type, aggregate_id, version, snapshot_version, payload,
  payload_codec, payload_codec_version, metadata, recorded_at, updated_at
)
SELECT
  aggregate_type, aggregate_id, version, snapshot_version, payload,
  payload_codec, payload_codec_version, metadata,
  CASE
    WHEN instr(recorded_at, '-') > 0 THEN unixepoch(recorded_at) * 1000000
    WHEN instr(recorded_at, '.') = 0 THEN CAST(recorded_at AS INTEGER) * 1000000
    ELSE CAST(substr(recorded_at, 1, instr(recorded_at, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(recorded_at, instr(recorded_at, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END,
  CASE
    WHEN instr(updated_at, '-') > 0 THEN unixepoch(updated_at) * 1000000
    WHEN instr(updated_at, '.') = 0 THEN CAST(updated_at AS INTEGER) * 1000000
    ELSE CAST(substr(updated_at, 1, instr(updated_at, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(updated_at, instr(updated_at, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END
FROM aggregate_snapshots;

DROP TABLE aggregate_snapshots;
ALTER TABLE aggregate_snapshots_rebuilt RENAME TO aggregate_snapshots;

CREATE TABLE outbox_messages_rebuilt (
  message_id TEXT NOT NULL PRIMARY KEY,
  event_type TEXT NOT NULL,
  payload BLOB NOT NULL,
  payload_codec TEXT NOT NULL,
  payload_codec_version INTEGER NOT NULL,
  destination TEXT,
  metadata TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  next_available_at INTEGER NOT NULL,
  claimed_by TEXT,
  claimed_until INTEGER,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  failed_at INTEGER,
  source_aggregate_type TEXT,
  source_aggregate_id TEXT,
  source_sequence INTEGER,
  correlation_id TEXT,
  causation_id TEXT,
  updated_at INTEGER NOT NULL DEFAULT (CAST(round(unixepoch('now', 'subsec') * 1000) AS INTEGER) * 1000),
  CHECK (message_id <> ''),
  CHECK (event_type <> ''),
  CHECK (payload_codec <> ''),
  CHECK (payload_codec_version > 0),
  CHECK (status IN ('pending', 'in_flight', 'failed')),
  CHECK (attempts >= 0)
);

INSERT INTO outbox_messages_rebuilt (
  message_id, event_type, payload, payload_codec, payload_codec_version,
  destination, metadata, status, created_at, next_available_at, claimed_by,
  claimed_until, attempts, last_error, failed_at, source_aggregate_type,
  source_aggregate_id, source_sequence, correlation_id, causation_id, updated_at
)
SELECT
  message_id, event_type, payload, payload_codec, payload_codec_version,
  destination, metadata, status,
  CASE
    WHEN instr(created_at, '-') > 0 THEN unixepoch(created_at) * 1000000
    WHEN instr(created_at, '.') = 0 THEN CAST(created_at AS INTEGER) * 1000000
    ELSE CAST(substr(created_at, 1, instr(created_at, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(created_at, instr(created_at, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END,
  CASE
    WHEN instr(next_available_at, '-') > 0 THEN unixepoch(next_available_at) * 1000000
    WHEN instr(next_available_at, '.') = 0 THEN CAST(next_available_at AS INTEGER) * 1000000
    ELSE CAST(substr(next_available_at, 1, instr(next_available_at, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(next_available_at, instr(next_available_at, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END,
  claimed_by,
  CASE
    WHEN claimed_until IS NULL THEN NULL
    WHEN instr(claimed_until, '-') > 0 THEN unixepoch(claimed_until) * 1000000
    WHEN instr(claimed_until, '.') = 0 THEN CAST(claimed_until AS INTEGER) * 1000000
    ELSE CAST(substr(claimed_until, 1, instr(claimed_until, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(claimed_until, instr(claimed_until, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END,
  attempts, last_error,
  CASE
    WHEN failed_at IS NULL THEN NULL
    WHEN instr(failed_at, '-') > 0 THEN unixepoch(failed_at) * 1000000
    WHEN instr(failed_at, '.') = 0 THEN CAST(failed_at AS INTEGER) * 1000000
    ELSE CAST(substr(failed_at, 1, instr(failed_at, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(failed_at, instr(failed_at, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END,
  source_aggregate_type, source_aggregate_id, source_sequence, correlation_id,
  causation_id,
  CASE
    WHEN instr(updated_at, '-') > 0 THEN unixepoch(updated_at) * 1000000
    WHEN instr(updated_at, '.') = 0 THEN CAST(updated_at AS INTEGER) * 1000000
    ELSE CAST(substr(updated_at, 1, instr(updated_at, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(updated_at, instr(updated_at, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END
FROM outbox_messages;

DROP TABLE outbox_messages;
ALTER TABLE outbox_messages_rebuilt RENAME TO outbox_messages;

-- The claim takes the oldest claimable rows of either status in
-- (created_at, message_id) order, as on PostgreSQL. The statement repeats
-- this index's WHERE term so SQLite can use it.
CREATE INDEX outbox_messages_claim_idx
  ON outbox_messages (created_at, message_id)
  WHERE status IN ('pending', 'in_flight');

CREATE INDEX outbox_messages_destination_idx
  ON outbox_messages (destination, status, created_at);

CREATE TABLE consumer_inbox_rebuilt (
  consumer TEXT NOT NULL,
  message_id TEXT NOT NULL,
  processed_at INTEGER NOT NULL DEFAULT (CAST(round(unixepoch('now', 'subsec') * 1000) AS INTEGER) * 1000),
  PRIMARY KEY (consumer, message_id),
  CHECK (consumer <> ''),
  CHECK (message_id <> '')
);

INSERT INTO consumer_inbox_rebuilt (consumer, message_id, processed_at)
SELECT
  consumer, message_id,
  CASE
    WHEN instr(processed_at, '-') > 0 THEN unixepoch(processed_at) * 1000000
    WHEN instr(processed_at, '.') = 0 THEN CAST(processed_at AS INTEGER) * 1000000
    ELSE CAST(substr(processed_at, 1, instr(processed_at, '.') - 1) AS INTEGER) * 1000000
      + CAST(substr(substr(processed_at, instr(processed_at, '.') + 1) || '000000', 1, 6) AS INTEGER)
  END
FROM consumer_inbox;

DROP TABLE consumer_inbox;
ALTER TABLE consumer_inbox_rebuilt RENAME TO consumer_inbox;

CREATE TABLE aggregate_locks_rebuilt (
  lock_key TEXT NOT NULL PRIMARY KEY,
  owner_token TEXT NOT NULL,
  acquired_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  CHECK (lock_key <> ''),
  CHECK (owner_token <> '')
);

INSERT INTO aggregate_locks_rebuilt (lock_key, owner_token, acquired_at, expires_at)
SELECT
  lock_key, owner_token,
  CASE WHEN acquired_at < 1000000000000 THEN CAST(round(acquired_at * 1000000) AS INTEGER) ELSE CAST(acquired_at AS INTEGER) END,
  CASE WHEN expires_at < 1000000000000 THEN CAST(round(expires_at * 1000000) AS INTEGER) ELSE CAST(expires_at AS INTEGER) END
FROM aggregate_locks;

DROP TABLE aggregate_locks;
ALTER TABLE aggregate_locks_rebuilt RENAME TO aggregate_locks;

CREATE INDEX aggregate_locks_expires_at_idx ON aggregate_locks (expires_at);

CREATE TABLE command_ledger_rebuilt (
  service_id TEXT NOT NULL,
  principal_partition TEXT NOT NULL,
  command_id TEXT NOT NULL,
  command_name TEXT NOT NULL,
  command_contract_hash BLOB NOT NULL,
  input_hash BLOB NOT NULL,
  state TEXT NOT NULL,
  causation_id TEXT NOT NULL,
  attempt_token TEXT,
  attempt_number INTEGER NOT NULL,
  lease_expires_at INTEGER,
  outcome TEXT,
  created_at INTEGER NOT NULL DEFAULT (CAST(round(unixepoch('now', 'subsec') * 1000) AS INTEGER) * 1000),
  updated_at INTEGER NOT NULL DEFAULT (CAST(round(unixepoch('now', 'subsec') * 1000) AS INTEGER) * 1000),
  completed_at INTEGER,
  retention_expires_at INTEGER NOT NULL,
  compacted_at INTEGER,
  external_binding TEXT,
  PRIMARY KEY (service_id, principal_partition, command_id),
  UNIQUE (service_id, causation_id),
  CHECK (service_id <> ''),
  CHECK (principal_partition <> ''),
  CHECK (command_id <> ''),
  CHECK (command_name <> ''),
  CHECK (typeof(command_contract_hash) = 'blob' AND length(command_contract_hash) = 32),
  CHECK (typeof(input_hash) = 'blob' AND length(input_hash) = 32),
  CHECK (causation_id <> ''),
  CHECK (attempt_number > 0),
  CHECK (outcome IS NULL OR json_valid(outcome)),
  CHECK (state IN (
    'in_progress',
    'retryable_unknown',
    'succeeded',
    'succeeded_pending_projection',
    'atomic',
    'rejected',
    'projection_failed',
    'expired'
  )),
  CHECK (
    (state = 'in_progress'
      AND attempt_token IS NOT NULL
      AND lease_expires_at IS NOT NULL
      AND outcome IS NULL
      AND completed_at IS NULL
      AND compacted_at IS NULL)
    OR
    (state = 'retryable_unknown'
      AND attempt_token IS NULL
      AND lease_expires_at IS NULL
      AND outcome IS NULL
      AND completed_at IS NULL
      AND compacted_at IS NULL)
    OR
    (state IN (
        'succeeded',
        'succeeded_pending_projection',
        'atomic',
        'rejected',
        'projection_failed'
      )
      AND attempt_token IS NULL
      AND lease_expires_at IS NULL
      AND outcome IS NOT NULL
      AND completed_at IS NOT NULL
      AND compacted_at IS NULL)
    OR
    (state = 'expired'
      AND attempt_token IS NULL
      AND lease_expires_at IS NULL
      AND outcome IS NULL
      AND compacted_at IS NOT NULL)
  )
);

INSERT INTO command_ledger_rebuilt (
  service_id, principal_partition, command_id, command_name,
  command_contract_hash, input_hash, state, causation_id, attempt_token,
  attempt_number, lease_expires_at, outcome, created_at, updated_at,
  completed_at, retention_expires_at, compacted_at, external_binding
)
SELECT
  service_id, principal_partition, command_id, command_name,
  command_contract_hash, input_hash, state, causation_id, attempt_token,
  attempt_number,
  CAST(round(lease_expires_at * 1000000) AS INTEGER),
  outcome,
  CAST(round(created_at * 1000000) AS INTEGER),
  CAST(round(updated_at * 1000000) AS INTEGER),
  CAST(round(completed_at * 1000000) AS INTEGER),
  CAST(round(retention_expires_at * 1000000) AS INTEGER),
  CAST(round(compacted_at * 1000000) AS INTEGER),
  external_binding
FROM command_ledger;

DROP TABLE command_ledger;
ALTER TABLE command_ledger_rebuilt RENAME TO command_ledger;

CREATE INDEX command_ledger_retention_idx
  ON command_ledger (retention_expires_at)
  WHERE state <> 'expired';

-- Already integer epoch milliseconds: rename the column after its new unit.
ALTER TABLE cell_alarms RENAME COLUMN due_at_ms TO due_at;
UPDATE cell_alarms SET due_at = due_at * 1000;
