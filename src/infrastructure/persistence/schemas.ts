import { EntitySchema } from "@mikro-orm/postgresql";

/**
 * Modelos de persistência. São deliberadamente separados das classes de domínio: o domínio não conhece
 * ORM nem decorators; os mappers convertem nos dois sentidos (e usam `rehydrate`).
 * Colunas `numeric` são mapeadas como string: representação exata, sem passar por number.
 */

export interface WalletRow {
  id: string;
  playerId: string;
  currency: string;
  balance: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export const WalletSchema = new EntitySchema<WalletRow>({
  name: "WalletRow",
  tableName: "wallets",
  properties: {
    id: { type: "uuid", primary: true },
    playerId: { type: "uuid", fieldName: "player_id" },
    currency: { type: "string", columnType: "char(3)" },
    balance: { type: "string", columnType: "numeric(20,2)" },
    version: { type: "integer" },
    createdAt: { type: "Date", fieldName: "created_at", columnType: "timestamptz" },
    updatedAt: { type: "Date", fieldName: "updated_at", columnType: "timestamptz" },
  },
});

export interface WagerTransactionRow {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  amount: string;
  currency: string;
  referenceExternalTransactionId: string | null;
  referenceTransactionId: string | null;
  status: string;
  failureCode: string | null;
  resultBalance: string | null;
  attempts: number;
  nextAttemptAt: Date | null;
  processedAt: Date | null;
  createdAt: Date;
}

export const WagerTransactionSchema = new EntitySchema<WagerTransactionRow>({
  name: "WagerTransactionRow",
  tableName: "wager_transactions",
  properties: {
    id: { type: "uuid", primary: true },
    providerId: { type: "string", fieldName: "provider_id" },
    externalTransactionId: { type: "string", fieldName: "external_transaction_id" },
    idempotencyKey: { type: "string", fieldName: "idempotency_key" },
    payloadHash: { type: "string", fieldName: "payload_hash" },
    walletId: { type: "uuid", fieldName: "wallet_id" },
    playerId: { type: "uuid", fieldName: "player_id" },
    roundId: { type: "string", fieldName: "round_id" },
    gameId: { type: "string", fieldName: "game_id" },
    kind: { type: "string" },
    amount: { type: "string", columnType: "numeric(20,2)" },
    currency: { type: "string", columnType: "char(3)" },
    referenceExternalTransactionId: { type: "string", fieldName: "reference_external_transaction_id", nullable: true },
    referenceTransactionId: { type: "uuid", fieldName: "reference_transaction_id", nullable: true },
    status: { type: "string" },
    failureCode: { type: "string", fieldName: "failure_code", nullable: true },
    resultBalance: { type: "string", fieldName: "result_balance", columnType: "numeric(20,2)", nullable: true },
    attempts: { type: "integer" },
    nextAttemptAt: { type: "Date", fieldName: "next_attempt_at", columnType: "timestamptz", nullable: true },
    processedAt: { type: "Date", fieldName: "processed_at", columnType: "timestamptz", nullable: true },
    createdAt: { type: "Date", fieldName: "created_at", columnType: "timestamptz" },
  },
});

/** `seq` (identity) existe só no banco: ordena o cursor do ledger e nunca é escrito pela aplicação. */
export interface LedgerEntryRow {
  id: string;
  walletId: string;
  transactionId: string;
  direction: string;
  amount: string;
  currency: string;
  balanceBefore: string;
  balanceAfter: string;
  createdAt: Date;
}

export const LedgerEntrySchema = new EntitySchema<LedgerEntryRow>({
  name: "LedgerEntryRow",
  tableName: "ledger_entries",
  properties: {
    id: { type: "uuid", primary: true },
    walletId: { type: "uuid", fieldName: "wallet_id" },
    transactionId: { type: "uuid", fieldName: "transaction_id" },
    direction: { type: "string" },
    amount: { type: "string", columnType: "numeric(20,2)" },
    currency: { type: "string", columnType: "char(3)" },
    balanceBefore: { type: "string", fieldName: "balance_before", columnType: "numeric(20,2)" },
    balanceAfter: { type: "string", fieldName: "balance_after", columnType: "numeric(20,2)" },
    createdAt: { type: "Date", fieldName: "created_at", columnType: "timestamptz" },
  },
});

export interface OutboxRow {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Record<string, unknown>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt: Date | null;
  publishedAt: Date | null;
  lockedUntil: Date | null;
  lockedBy: string | null;
}

export const OutboxSchema = new EntitySchema<OutboxRow>({
  name: "OutboxRow",
  tableName: "outbox_messages",
  properties: {
    id: { type: "uuid", primary: true },
    aggregateId: { type: "uuid", fieldName: "aggregate_id" },
    eventType: { type: "string", fieldName: "event_type" },
    payload: { type: "json", columnType: "jsonb" },
    occurredAt: { type: "Date", fieldName: "occurred_at", columnType: "timestamptz" },
    attempts: { type: "integer" },
    nextAttemptAt: { type: "Date", fieldName: "next_attempt_at", columnType: "timestamptz", nullable: true },
    publishedAt: { type: "Date", fieldName: "published_at", columnType: "timestamptz", nullable: true },
    lockedUntil: { type: "Date", fieldName: "locked_until", columnType: "timestamptz", nullable: true },
    lockedBy: { type: "string", fieldName: "locked_by", nullable: true },
  },
});

export const ALL_SCHEMAS = [WalletSchema, WagerTransactionSchema, LedgerEntrySchema, OutboxSchema];
