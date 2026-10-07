DROP INDEX uq_wager_tx_single_reversal;

CREATE UNIQUE INDEX uq_wager_tx_single_reversal
  ON wager_transactions (reference_transaction_id, kind)
  WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');
