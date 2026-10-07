DROP TABLE IF EXISTS outbox_messages;
DROP TABLE IF EXISTS inbox_messages;

DROP TRIGGER IF EXISTS trg_wallets_ledger_consistency ON wallets;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;

DROP FUNCTION IF EXISTS wallets_assert_ledger_consistency();
DROP FUNCTION IF EXISTS ledger_entries_check_chain();
DROP FUNCTION IF EXISTS ledger_entries_append_only();
DROP FUNCTION IF EXISTS wager_transactions_guard();
