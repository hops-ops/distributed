-- Re-armable aggregate-cell alarms (see `distributed::cell_host::CellAlarm`).
-- Only aggregate cells read or write this table; it exists here so the
-- inventory stays identical across dialects. One row per alarm name;
-- `generation` increments on every arm and is never reset. `due_at_ms` is
-- epoch milliseconds; `armed` is 1 while the alarm is scheduled.
CREATE TABLE IF NOT EXISTS cell_alarms (
  name TEXT NOT NULL PRIMARY KEY,
  command TEXT NOT NULL,
  input TEXT NOT NULL,
  due_at_ms BIGINT NOT NULL,
  generation BIGINT NOT NULL,
  attempts BIGINT NOT NULL DEFAULT 0,
  armed SMALLINT NOT NULL,
  CHECK (name <> ''),
  CHECK (command <> ''),
  CHECK (generation > 0),
  CHECK (armed IN (0, 1))
);
CREATE INDEX IF NOT EXISTS cell_alarms_armed_due ON cell_alarms (armed, due_at_ms);
