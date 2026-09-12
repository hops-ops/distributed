-- Fences survive deletion, process restart and change-log compaction.
-- NULL denotes a delivery-ordered (not source-snapshot) projection.
ALTER TABLE projection_records ADD COLUMN source_snapshot TEXT;
