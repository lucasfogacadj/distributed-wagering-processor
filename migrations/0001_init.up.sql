-- Schema inicial. As garantias de unicidade, imutabilidade e não-negatividade vivem AQUI,
-- no banco, e não apenas no código da aplicação.

-- ---------------------------------------------------------------------------
-- wallets: unidade de concorrência. Saldo materializado, nunca negativo.
-- ---------------------------------------------------------------------------
CREATE TABLE wallets (
  id          uuid          PRIMARY KEY,
  player_id   uuid          NOT NULL,
  currency    char(3)       NOT NULL,
  balance     numeric(20,2) NOT NULL,
  version     integer       NOT NULL,
  created_at  timestamptz   NOT NULL,
  updated_at  timestamptz   NOT NULL,
  CONSTRAINT ck_wallets_currency             CHECK (currency ~ '^[A-Z]{3}$'),
  CONSTRAINT ck_wallets_balance_non_negative CHECK (balance >= 0),
  CONSTRAINT ck_wallets_version_positive     CHECK (version >= 1),
  CONSTRAINT uq_wallets_player_currency      UNIQUE (player_id, currency),
  -- alvo da FK composta do ledger: garante que o lançamento tem a moeda da wallet
  CONSTRAINT uq_wallets_id_currency          UNIQUE (id, currency)
);

-- ---------------------------------------------------------------------------
-- wager_transactions: registro de cada operação recebida (idempotência persistente).
-- wallet_id e player_id NÃO têm FK de propósito: uma transação para wallet inexistente ou
-- com player divergente precisa ser persistida como REJECTED (auditável).
-- ---------------------------------------------------------------------------
CREATE TABLE wager_transactions (
  id                                uuid          PRIMARY KEY,
  provider_id                       text          NOT NULL,
  external_transaction_id           text          NOT NULL,
  idempotency_key                   text          NOT NULL,
  payload_hash                      text          NOT NULL,
  wallet_id                         uuid          NOT NULL,
  player_id                         uuid          NOT NULL,
  round_id                          text          NOT NULL,
  game_id                           text          NOT NULL,
  kind                              text          NOT NULL,
  amount                            numeric(20,2) NOT NULL,
  currency                          char(3)       NOT NULL,
  reference_external_transaction_id text,
  reference_transaction_id          uuid          REFERENCES wager_transactions (id),
  status                            text          NOT NULL,
  failure_code                      text,
  -- saldo observado após a decisão: devolvido em replays (regra 7.7)
  result_balance                    numeric(20,2),
  -- agendamento do worker de PENDING_REFERENCE
  attempts                          integer       NOT NULL DEFAULT 0,
  next_attempt_at                   timestamptz,
  processed_at                      timestamptz,
  created_at                        timestamptz   NOT NULL,
  CONSTRAINT uq_wager_tx_idempotency_key  UNIQUE (idempotency_key),
  CONSTRAINT uq_wager_tx_provider_external UNIQUE (provider_id, external_transaction_id),
  CONSTRAINT ck_wager_tx_kind   CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
  CONSTRAINT ck_wager_tx_status CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
  CONSTRAINT ck_wager_tx_amount CHECK (amount >= 0 AND (amount > 0 OR kind = 'LOSS')),
  CONSTRAINT ck_wager_tx_currency CHECK (currency ~ '^[A-Z]{3}$'),
  CONSTRAINT ck_wager_tx_attempts CHECK (attempts >= 0),
  CONSTRAINT ck_wager_tx_reference_required
    CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
  CONSTRAINT ck_wager_tx_failure_code_required
    CHECK (status NOT IN ('REJECTED','FAILED') OR failure_code IS NOT NULL),
  CONSTRAINT ck_wager_tx_processed_consistent
    CHECK (status <> 'PROCESSED' OR (processed_at IS NOT NULL AND failure_code IS NULL)),
  CONSTRAINT ck_wager_tx_processed_reversal_has_reference
    CHECK (NOT (status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')) OR reference_transaction_id IS NOT NULL)
);

-- "Uma referência não pode ser revertida duas vezes pelo mesmo tipo de operação."
CREATE UNIQUE INDEX uq_wager_tx_single_reversal
  ON wager_transactions (reference_transaction_id, kind)
  WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');

-- worker de referências pendentes e resolução de dependentes
CREATE INDEX ix_wager_tx_pending_reference
  ON wager_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX ix_wager_tx_waiting_for_reference
  ON wager_transactions (provider_id, reference_external_transaction_id) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX ix_wager_tx_wallet ON wager_transactions (wallet_id, created_at);

CREATE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'wager_transactions não podem ser excluídas (trilha de auditoria)'
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;

  IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN
    RAISE EXCEPTION 'transação % está em estado terminal (%) e não pode ser alterada', OLD.id, OLD.status
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;

  IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN
    RAISE EXCEPTION 'transição inválida PENDING_REFERENCE -> PENDING na transação %', OLD.id
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;

  IF ROW(NEW.id, NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key, NEW.payload_hash,
         NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id, NEW.kind, NEW.amount, NEW.currency,
         NEW.reference_external_transaction_id, NEW.created_at)
     IS DISTINCT FROM
     ROW(OLD.id, OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key, OLD.payload_hash,
         OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id, OLD.kind, OLD.amount, OLD.currency,
         OLD.reference_external_transaction_id, OLD.created_at)
  THEN
    RAISE EXCEPTION 'campos de identidade da transação % são imutáveis', OLD.id
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;

  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_wager_transactions_guard
  BEFORE UPDATE OR DELETE ON wager_transactions
  FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();

-- ---------------------------------------------------------------------------
-- ledger_entries: append-only. seq é a ordem estável usada pelo cursor de paginação
-- (lançamentos de uma mesma wallet são serializados pelo lock da wallet).
-- ---------------------------------------------------------------------------
CREATE TABLE ledger_entries (
  seq             bigint        GENERATED ALWAYS AS IDENTITY,
  id              uuid          PRIMARY KEY,
  wallet_id       uuid          NOT NULL,
  transaction_id  uuid          NOT NULL,
  direction       text          NOT NULL,
  amount          numeric(20,2) NOT NULL,
  currency        char(3)       NOT NULL,
  balance_before  numeric(20,2) NOT NULL,
  balance_after   numeric(20,2) NOT NULL,
  created_at      timestamptz   NOT NULL,
  CONSTRAINT uq_ledger_entries_seq         UNIQUE (seq),
  -- no máximo um lançamento por transação (por wallet, já que cada transação tem uma wallet)
  CONSTRAINT uq_ledger_entries_transaction UNIQUE (transaction_id),
  CONSTRAINT fk_ledger_entries_wallet      FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
  CONSTRAINT fk_ledger_entries_transaction FOREIGN KEY (transaction_id) REFERENCES wager_transactions (id),
  CONSTRAINT ck_ledger_entries_direction   CHECK (direction IN ('DEBIT','CREDIT')),
  CONSTRAINT ck_ledger_entries_amount_positive CHECK (amount > 0),
  CONSTRAINT ck_ledger_entries_balances_non_negative CHECK (balance_before >= 0 AND balance_after >= 0),
  CONSTRAINT ck_ledger_entries_arithmetic CHECK (
    (direction = 'DEBIT'  AND balance_after = balance_before - amount) OR
    (direction = 'CREDIT' AND balance_after = balance_before + amount)
  )
);

CREATE INDEX ix_ledger_entries_wallet_seq ON ledger_entries (wallet_id, seq);

CREATE FUNCTION ledger_entries_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ledger_entries é append-only: % não é permitido', TG_OP
    USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE TRIGGER trg_ledger_entries_no_update_delete
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION ledger_entries_append_only();

CREATE TRIGGER trg_ledger_entries_no_truncate
  BEFORE TRUNCATE ON ledger_entries
  FOR EACH STATEMENT EXECUTE FUNCTION ledger_entries_append_only();

-- Continuidade da cadeia: cada lançamento parte exatamente do saldo em que o anterior terminou.
CREATE FUNCTION ledger_entries_check_chain() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  previous numeric(20,2);
BEGIN
  SELECT balance_after INTO previous
    FROM ledger_entries
   WHERE wallet_id = NEW.wallet_id
   ORDER BY seq DESC
   LIMIT 1;

  IF NEW.balance_before IS DISTINCT FROM COALESCE(previous, 0) THEN
    RAISE EXCEPTION 'cadeia do ledger quebrada na wallet %: balance_before % mas o lançamento anterior terminou em %',
      NEW.wallet_id, NEW.balance_before, COALESCE(previous, 0)
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_ledger_entries_chain
  BEFORE INSERT ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION ledger_entries_check_chain();

-- Toda alteração de saldo tem lançamento (e vice-versa): ao COMMIT, o saldo da wallet precisa
-- ser exatamente o balance_after do último lançamento (0 se não houver nenhum).
-- Com a cadeia acima, isso implica wallet.balance = soma do ledger. Custo O(log n) por commit.
CREATE FUNCTION wallets_assert_ledger_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  last_after numeric(20,2);
BEGIN
  SELECT balance_after INTO last_after
    FROM ledger_entries
   WHERE wallet_id = NEW.id
   ORDER BY seq DESC
   LIMIT 1;

  IF NEW.balance IS DISTINCT FROM COALESCE(last_after, 0) THEN
    RAISE EXCEPTION 'saldo da wallet % (%) diverge do ledger (%)', NEW.id, NEW.balance, COALESCE(last_after, 0)
      USING ERRCODE = 'integrity_constraint_violation';
  END IF;
  RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_wallets_ledger_consistency
  AFTER INSERT OR UPDATE ON wallets
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION wallets_assert_ledger_consistency();

-- ---------------------------------------------------------------------------
-- inbox_messages: deduplicação persistente de mensagens SQS.
-- ---------------------------------------------------------------------------
CREATE TABLE inbox_messages (
  consumer_name text        NOT NULL,
  message_id    text        NOT NULL,
  payload_hash  text        NOT NULL,
  received_at  timestamptz NOT NULL,
  processed_at  timestamptz,
  PRIMARY KEY (consumer_name, message_id)
);

-- ---------------------------------------------------------------------------
-- outbox_messages: eventos gravados na mesma transação SQL da alteração financeira.
-- locked_until/locked_by: lease para publishers concorrentes (um publisher que morre
-- deixa o lease expirar e outro assume).
-- ---------------------------------------------------------------------------
CREATE TABLE outbox_messages (
  id              uuid        PRIMARY KEY,
  aggregate_id    uuid        NOT NULL,
  event_type      text        NOT NULL,
  payload         jsonb       NOT NULL,
  occurred_at     timestamptz NOT NULL,
  attempts        integer     NOT NULL DEFAULT 0,
  next_attempt_at timestamptz,
  published_at    timestamptz,
  locked_until    timestamptz,
  locked_by       text,
  CONSTRAINT ck_outbox_attempts CHECK (attempts >= 0)
);

CREATE INDEX ix_outbox_pending
  ON outbox_messages (next_attempt_at NULLS FIRST, occurred_at) WHERE published_at IS NULL;
