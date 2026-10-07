-- Tratamento de falhas permanentes: quarentena na outbox, tentativas persistentes no consumidor e falhas de
-- reprocessamento das pendências.

-- ---------------------------------------------------------------------------
-- Outbox: quarentena de evento venenoso.
-- Um evento que esgota as tentativas de publicação sai da sequência da wallet (quarantined_at) para não bloquear os
-- seguintes para sempre. Ele NÃO é apagado nem marcado como publicado: fica auditável, com o último erro, e pode ser
-- devolvido à fila (`bun run outbox:requeue`). Os índices de pendentes passam a ignorar os quarentenados.
-- ---------------------------------------------------------------------------
ALTER TABLE outbox_messages
  ADD COLUMN quarantined_at timestamptz,
  ADD COLUMN last_error     text;

ALTER TABLE outbox_messages
  ADD CONSTRAINT ck_outbox_quarantine_unpublished CHECK (quarantined_at IS NULL OR published_at IS NULL);

DROP INDEX ix_outbox_pending;
CREATE INDEX ix_outbox_pending ON outbox_messages (seq) WHERE published_at IS NULL AND quarantined_at IS NULL;
DROP INDEX ix_outbox_pending_aggregate;
CREATE INDEX ix_outbox_pending_aggregate
  ON outbox_messages (aggregate_id, seq) WHERE published_at IS NULL AND quarantined_at IS NULL;
CREATE INDEX ix_outbox_quarantined ON outbox_messages (seq) WHERE quarantined_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- inbox_attempts: tentativas REAIS de processamento de uma mensagem da fila.
-- O ApproximateReceiveCount do SQS também sobe quando a mensagem é devolvida sem ter sido processada (cauda de um
-- grupo atrás de um retry, shutdown); contar aqui, só quando o processamento de fato falha, é o que decide a DLQ.
-- Fica separada de inbox_messages, que continua sendo só deduplicação (uma linha = efeito confirmado).
-- ---------------------------------------------------------------------------
CREATE TABLE inbox_attempts (
  consumer_name   text        NOT NULL,
  message_id      text        NOT NULL,
  attempts        integer     NOT NULL,
  last_error      text        NOT NULL,
  first_failed_at timestamptz NOT NULL,
  last_failed_at  timestamptz NOT NULL,
  PRIMARY KEY (consumer_name, message_id),
  CONSTRAINT ck_inbox_attempts_positive CHECK (attempts >= 1)
);

-- ---------------------------------------------------------------------------
-- wager_transactions: falhas de reprocessamento de uma PENDING_REFERENCE.
-- Diferente de `attempts` (espera pela referência): conta erros não transitórios do próprio reprocessamento. Esgotado o
-- limite, a transação vira FAILED/INFRA_PERMANENT em vez de ser tentada a cada segundo para sempre.
-- ---------------------------------------------------------------------------
ALTER TABLE wager_transactions
  ADD COLUMN processing_failures integer NOT NULL DEFAULT 0,
  ADD CONSTRAINT ck_wager_tx_processing_failures CHECK (processing_failures >= 0);
