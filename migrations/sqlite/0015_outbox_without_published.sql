-- A published outbox row is deleted, never kept: drop the `published`
-- status and the `published_at` column no statement writes. Rows an earlier
-- release kept as `published` were already delivered.
-- SQLite cannot alter a CHECK constraint, so the table is rebuilt with the
-- same columns (less `published_at`) and indexes.
DELETE FROM outbox_messages WHERE status = 'published';

CREATE TABLE outbox_messages_rebuilt (
  message_id TEXT NOT NULL PRIMARY KEY,
  event_type TEXT NOT NULL,
  payload BLOB NOT NULL,
  payload_codec TEXT NOT NULL,
  payload_codec_version INTEGER NOT NULL,
  destination TEXT,
  metadata TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  next_available_at TEXT NOT NULL,
  claimed_by TEXT,
  claimed_until TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  failed_at TEXT,
  source_aggregate_type TEXT,
  source_aggregate_id TEXT,
  source_sequence INTEGER,
  correlation_id TEXT,
  causation_id TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
  destination, metadata, status, created_at, next_available_at, claimed_by,
  claimed_until, attempts, last_error, failed_at, source_aggregate_type,
  source_aggregate_id, source_sequence, correlation_id, causation_id, updated_at
FROM outbox_messages;

DROP TABLE outbox_messages;
ALTER TABLE outbox_messages_rebuilt RENAME TO outbox_messages;

CREATE INDEX outbox_messages_claimable_idx
  ON outbox_messages (status, next_available_at, claimed_until, created_at);

CREATE INDEX outbox_messages_destination_idx
  ON outbox_messages (destination, status, created_at);
