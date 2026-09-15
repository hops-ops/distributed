-- Bind externally dispatched command reservations to their logical route.
-- NULL retains the pre-external-dispatch meaning for local reservations and
-- old cell-only rows; non-NULL values are canonical versioned JSON.
ALTER TABLE command_ledger ADD COLUMN external_binding TEXT;
