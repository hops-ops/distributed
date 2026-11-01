-- Re-armable aggregate-cell alarms (see `distributed::cell_host::CellAlarm`).
-- Only aggregate cells (Durable Object `storage.sql`, native `cell-sqlite`)
-- read or write this table. One row per alarm name; `generation` increments
-- on every arm and is never reset (disarming keeps the row), so the command id
-- derived from (name, generation) is unique per arm. `due_at_ms` is epoch
-- milliseconds; `armed` is 1 while the alarm is scheduled.
CREATE TABLE IF NOT EXISTS cell_alarms (
  name TEXT NOT NULL PRIMARY KEY,
  command TEXT NOT NULL,
  input TEXT NOT NULL,
  due_at_ms INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  armed INTEGER NOT NULL,
  CHECK (name <> ''),
  CHECK (command <> ''),
  CHECK (generation > 0),
  CHECK (armed IN (0, 1))
);
CREATE INDEX IF NOT EXISTS cell_alarms_armed_due ON cell_alarms (armed, due_at_ms);
