DROP INDEX IF EXISTS ix_outbox_pending_aggregate;
DROP INDEX IF EXISTS ix_outbox_pending;
CREATE INDEX ix_outbox_pending
  ON outbox_messages (next_attempt_at NULLS FIRST, occurred_at) WHERE published_at IS NULL;

DROP INDEX IF EXISTS uq_outbox_messages_seq;
ALTER TABLE outbox_messages DROP COLUMN seq;
