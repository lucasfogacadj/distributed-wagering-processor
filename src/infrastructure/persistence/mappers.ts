import { Money } from "../../domain/money";
import type { OutboxMessage } from "../../domain/outbox-message";
import type { FailureCode } from "../../domain/failure-code";
import {
  WagerTransaction,
  type WagerTransactionKind,
  type WagerTransactionStatus,
} from "../../domain/wager-transaction";
import { Wallet } from "../../domain/wallet";
import { WalletLedgerEntry, type LedgerDirection } from "../../domain/wallet-ledger-entry";
import type { StoredTransaction } from "../../application/ports";
import type { LedgerEntryRow, OutboxRow, WagerTransactionRow, WalletRow } from "./schemas";

export function walletToRow(w: Wallet): WalletRow {
  return {
    id: w.id,
    playerId: w.playerId,
    currency: w.currency,
    balance: w.balance.toJSON().amount,
    version: w.version,
    createdAt: w.createdAt,
    updatedAt: w.updatedAt,
  };
}

export function walletFromRow(r: WalletRow): Wallet {
  return Wallet.rehydrate({
    id: r.id,
    playerId: r.playerId,
    currency: r.currency,
    balance: Money.from({ amount: r.balance, currency: r.currency }),
    version: r.version,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  });
}

export function transactionFromRow(r: WagerTransactionRow): StoredTransaction {
  const transaction = WagerTransaction.rehydrate({
    id: r.id,
    providerId: r.providerId,
    externalTransactionId: r.externalTransactionId,
    idempotencyKey: r.idempotencyKey,
    payloadHash: r.payloadHash,
    walletId: r.walletId,
    playerId: r.playerId,
    roundId: r.roundId,
    gameId: r.gameId,
    kind: r.kind as WagerTransactionKind,
    money: Money.from({ amount: r.amount, currency: r.currency }),
    ...(r.referenceExternalTransactionId !== null ? { referenceExternalTransactionId: r.referenceExternalTransactionId } : {}),
    createdAt: r.createdAt,
    status: r.status as WagerTransactionStatus,
    ...(r.referenceTransactionId !== null ? { referenceTransactionId: r.referenceTransactionId } : {}),
    ...(r.failureCode !== null ? { failureCode: r.failureCode as FailureCode } : {}),
    ...(r.processedAt !== null ? { processedAt: r.processedAt } : {}),
  });

  return {
    transaction,
    ...(r.resultBalance !== null ? { resultBalance: Money.from({ amount: r.resultBalance, currency: r.currency }) } : {}),
    attempts: r.attempts,
    ...(r.nextAttemptAt !== null ? { nextAttemptAt: r.nextAttemptAt } : {}),
  };
}

export function ledgerEntryToRow(e: WalletLedgerEntry): LedgerEntryRow {
  return {
    id: e.id,
    walletId: e.walletId,
    transactionId: e.transactionId,
    direction: e.direction,
    amount: e.money.toJSON().amount,
    currency: e.money.currency,
    balanceBefore: e.balanceBefore.toJSON().amount,
    balanceAfter: e.balanceAfter.toJSON().amount,
    createdAt: e.createdAt,
  };
}

export function ledgerEntryFromRow(r: LedgerEntryRow): WalletLedgerEntry {
  return WalletLedgerEntry.rehydrate({
    id: r.id,
    walletId: r.walletId,
    transactionId: r.transactionId,
    direction: r.direction as LedgerDirection,
    money: Money.from({ amount: r.amount, currency: r.currency }),
    balanceBefore: Money.from({ amount: r.balanceBefore, currency: r.currency }),
    balanceAfter: Money.from({ amount: r.balanceAfter, currency: r.currency }),
    createdAt: r.createdAt,
  });
}

export function outboxToRow(m: OutboxMessage): OutboxRow {
  return {
    id: m.id,
    aggregateId: m.aggregateId,
    eventType: m.eventType,
    payload: { ...m.payload },
    occurredAt: m.occurredAt,
    attempts: m.attempts,
    nextAttemptAt: m.nextAttemptAt ?? null,
    publishedAt: m.publishedAt ?? null,
    lockedUntil: null,
    lockedBy: null,
  };
}
