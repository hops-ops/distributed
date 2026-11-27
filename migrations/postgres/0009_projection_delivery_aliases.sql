-- Alternate broker delivery positions for an already immutable logical input.
-- Keep the canonical globally unique message binding and all historical rows.
CREATE TABLE projection_input_delivery_aliases (
  topology_hash bytea NOT NULL,
  partition_hash bytea NOT NULL,
  source_hash bytea NOT NULL,
  source_partition_hash bytea NOT NULL,
  source_epoch text NOT NULL,
  source_position bigint NOT NULL CHECK (source_position >= 0),
  message_id text NOT NULL,
  PRIMARY KEY (topology_hash, partition_hash, source_hash, source_partition_hash, source_epoch, source_position),
  FOREIGN KEY (topology_hash, message_id)
    REFERENCES projection_input_identities (topology_hash, message_id)
);

