-- A published outbox row is deleted, never kept: drop the `published`
-- status and the `published_at` column no statement writes. Rows an earlier
-- release kept as `published` were already delivered.
-- Dropping the column drops the state CHECK that names it; it is recreated
-- without `published`.
DELETE FROM outbox_messages WHERE status = 'published';

ALTER TABLE outbox_messages DROP COLUMN published_at;

ALTER TABLE outbox_messages DROP CONSTRAINT outbox_messages_status_check;
ALTER TABLE outbox_messages
  ADD CONSTRAINT outbox_messages_status_check
  CHECK (status IN ('pending', 'in_flight', 'failed'));

ALTER TABLE outbox_messages
  ADD CONSTRAINT outbox_messages_state_check
  CHECK (
    CASE status
      WHEN 'pending' THEN
        claimed_by IS NULL
        AND claimed_until IS NULL
        AND failed_at IS NULL
      WHEN 'in_flight' THEN
        claimed_by IS NOT NULL
        AND claimed_until IS NOT NULL
        AND failed_at IS NULL
      WHEN 'failed' THEN
        claimed_by IS NULL
        AND claimed_until IS NULL
        AND failed_at IS NOT NULL
      ELSE FALSE
    END
  );
