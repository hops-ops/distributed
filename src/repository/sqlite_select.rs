//! SQLite select lists shared by SQLx and Durable Object SQL. Timestamps are
//! integer microseconds and JSON is text, so columns are read as stored.

pub(crate) const EVENT_SELECT: &str = "event_name, event_version, payload, payload_codec, payload_codec_version, metadata, sequence, recorded_at";
pub(crate) const SNAPSHOT_SELECT: &str = "aggregate_type, aggregate_id, version, snapshot_version, payload, payload_codec, payload_codec_version, metadata, recorded_at";
pub(crate) const OUTBOX_SELECT: &str = "message_id, event_type, payload, payload_codec, payload_codec_version, metadata, status, created_at, claimed_by, claimed_until, attempts, last_error, destination, source_aggregate_type, source_aggregate_id, source_sequence, correlation_id, causation_id, next_available_at";
pub(crate) const COMMAND_LEDGER_SELECT: &str = "command_name, command_contract_hash, input_hash, state, causation_id, attempt_token, attempt_number, external_binding, lease_expires_at, outcome, created_at, updated_at, completed_at, retention_expires_at, compacted_at";
