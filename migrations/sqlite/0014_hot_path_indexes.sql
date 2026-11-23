-- Indexes follow the statements that run, not the columns that exist.

-- Causation evidence is read by causation within a set of topologies, across
-- every partition (`read_projection_causation_evidence_in_executor`). The
-- old indexes put partition_hash between the two, and the observation one
-- only repeated the primary key's prefix.
CREATE INDEX IF NOT EXISTS projection_observations_topology_causation_idx
  ON projection_observations (topology_hash, causation_id);
CREATE INDEX IF NOT EXISTS projection_failures_topology_causation_idx
  ON projection_failures (topology_hash, causation_id);
DROP INDEX IF EXISTS projection_observations_causation_idx;
DROP INDEX IF EXISTS projection_failures_causation_idx;

-- No statement reads these: events are read by stream, outbox rows by id,
-- status or destination, receipts by message or source cursor, records by
-- key, and changes by position.
DROP INDEX IF EXISTS aggregate_events_event_version_idx;
DROP INDEX IF EXISTS aggregate_events_recorded_at_idx;
DROP INDEX IF EXISTS outbox_messages_source_idx;
DROP INDEX IF EXISTS projection_input_receipts_causation_idx;
DROP INDEX IF EXISTS projection_records_change_idx;
DROP INDEX IF EXISTS projection_changes_causation_idx;
DROP INDEX IF EXISTS projection_changes_record_idx;
