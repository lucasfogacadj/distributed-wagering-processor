-- Ordem de publicação por agregado.
--
-- `seq` (identity) dá a ordem de inserção. Os eventos de uma mesma wallet são inseridos dentro da
-- transação que detém o lock da wallet, então para um mesmo aggregate_id a ordem de `seq` é a ordem
-- de commit. O publisher usa isso para só liberar o evento mais antigo ainda não publicado de cada
-- agregado (head-of-line), preservando a ordem mesmo com vários publishers concorrentes.
ALTER TABLE outbox_messages ADD COLUMN seq bigint GENERATED ALWAYS AS IDENTITY;

CREATE UNIQUE INDEX uq_outbox_messages_seq ON outbox_messages (seq);

DROP INDEX ix_outbox_pending;
CREATE INDEX ix_outbox_pending ON outbox_messages (seq) WHERE published_at IS NULL;
CREATE INDEX ix_outbox_pending_aggregate ON outbox_messages (aggregate_id, seq) WHERE published_at IS NULL;
