-- Atenção: eventos em quarentena voltam a ser pendentes comuns (e serão publicados) ao reverter.
ALTER TABLE wager_transactions
  DROP CONSTRAINT ck_wager_tx_processing_failures,
  DROP COLUMN processing_failures;

DROP TABLE inbox_attempts;

DROP INDEX ix_outbox_quarantined;
DROP INDEX ix_outbox_pending_aggregate;
CREATE INDEX ix_outbox_pending_aggregate ON outbox_messages (aggregate_id, seq) WHERE published_at IS NULL;
DROP INDEX ix_outbox_pending;
CREATE INDEX ix_outbox_pending ON outbox_messages (seq) WHERE published_at IS NULL;

ALTER TABLE outbox_messages
  DROP CONSTRAINT ck_outbox_quarantine_unpublished,
  DROP COLUMN last_error,
  DROP COLUMN quarantined_at;
