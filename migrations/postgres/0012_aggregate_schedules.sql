-- Aggregate schedules of a shared repository (see `distributed::schedule`).
-- One row per (owning stream, schedule name), written in the command's
-- transaction; `generation` increments on every arm and is never reset
-- (cancelling keeps the row), so the command id derived from
-- (name, generation) is unique per arm. `due_at_ms` is epoch milliseconds;
-- `armed` is 1 while the schedule is pending. Aggregate cells keep their
-- own single-owner `cell_alarms` table instead.
CREATE TABLE IF NOT EXISTS aggregate_schedules (
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  name TEXT NOT NULL,
  command TEXT NOT NULL,
  input TEXT NOT NULL,
  due_at_ms BIGINT NOT NULL,
  generation BIGINT NOT NULL,
  attempts BIGINT NOT NULL DEFAULT 0,
  armed SMALLINT NOT NULL,
  PRIMARY KEY (aggregate_type, aggregate_id, name),
  CHECK (name <> ''),
  CHECK (command <> ''),
  CHECK (generation > 0),
  CHECK (armed IN (0, 1))
);
CREATE INDEX IF NOT EXISTS aggregate_schedules_armed_due ON aggregate_schedules (armed, due_at_ms);
