-- Uma referência é revertida no máximo UMA vez, por qualquer tipo de reversão.
--
-- A regra 4 do enunciado ("uma referência não pode ser revertida duas vezes pelo mesmo tipo de operação")
-- sozinha permitiria um REFUND e um ROLLBACK sobre a mesma BET, e os dois creditam o valor: o dinheiro seria
-- creditado duas vezes, o que fere o invariante global "não duplicar créditos". Adotamos a leitura
-- conservadora; o ROLLBACK de um REFUND continua válido porque a referência dele é o próprio REFUND.
--
-- Atenção: falha se já existirem, na mesma referência, duas reversões PROCESSED de tipos diferentes —
-- exatamente o estado que a regra proíbe. Nesse caso é preciso resolver os dados antes de migrar.
DROP INDEX uq_wager_tx_single_reversal;

CREATE UNIQUE INDEX uq_wager_tx_single_reversal
  ON wager_transactions (reference_transaction_id)
  WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');
