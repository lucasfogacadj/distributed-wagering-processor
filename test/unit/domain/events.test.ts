import { describe, expect, it } from "bun:test";
import { Money } from "../../../src/domain/money";
import { Wallet } from "../../../src/domain/wallet";
import { FailureCode } from "../../../src/domain/failure-code";
import { WagerTransaction, WagerTransactionKind } from "../../../src/domain/wager-transaction";
import { IntegrationEvent } from "../../../src/domain/integration-event";
import { LedgerDirection } from "../../../src/domain/wallet-ledger-entry";
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from "../../../src/domain/events";

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });
const at = new Date("2026-07-29T15:00:00.000Z");
const ctx = { eventId: "ev1", correlationId: "corr-1", causationId: "msg-1", occurredAt: at };

const tx = (kind = WagerTransactionKind.Bet, extra: Record<string, unknown> = {}) =>
  WagerTransaction.create({
    id: "t1",
    providerId: "provider-a",
    externalTransactionId: "ext-1",
    idempotencyKey: "provider-a:ext-1",
    payloadHash: "h",
    walletId: "w1",
    playerId: "p1",
    roundId: "r1",
    gameId: "g1",
    kind,
    money: brl("25.00"),
    createdAt: at,
    ...extra,
  } as never);

describe("IntegrationEvent", () => {
  it("subclasses carregam eventType e version no tipo", () => {
    const e = WagerTransactionProcessed.from(tx(), ctx);
    expect(e).toBeInstanceOf(IntegrationEvent);
    expect(e.eventType).toBe("WagerTransactionProcessed");
    expect(e.version).toBe(1);
    expect(e.aggregateId).toBe("w1");
  });

  it("toJSON serializa o envelope com ISO-8601 e Money como MoneyProps", () => {
    const json = WagerTransactionProcessed.from(tx(), ctx).toJSON();
    expect(json).toEqual({
      eventId: "ev1",
      eventType: "WagerTransactionProcessed",
      aggregateId: "w1",
      correlationId: "corr-1",
      causationId: "msg-1",
      occurredAt: "2026-07-29T15:00:00.000Z",
      version: 1,
      data: {
        transactionId: "t1",
        providerId: "provider-a",
        externalTransactionId: "ext-1",
        walletId: "w1",
        playerId: "p1",
        roundId: "r1",
        kind: "BET",
        money: { amount: "25.00", currency: "BRL" },
      },
    });
    expect(JSON.parse(JSON.stringify(json))).toEqual(json);
  });

  it("Rejected inclui o failureCode", () => {
    const t = tx();
    t.reject(FailureCode.InsufficientFunds);
    const e = WagerTransactionRejected.from(t, ctx);
    expect(e.eventType).toBe("WagerTransactionRejected");
    expect(e.data.failureCode).toBe(FailureCode.InsufficientFunds);
  });

  it("PendingReference inclui a referência externa", () => {
    const t = tx(WagerTransactionKind.Refund, { referenceExternalTransactionId: "ext-0" });
    const e = WagerTransactionPendingReference.from(t, ctx);
    expect(e.eventType).toBe("WagerTransactionPendingReference");
    expect(e.data.referenceExternalTransactionId).toBe("ext-0");
  });

  it("WalletBalanceChanged descreve a movimentação e a versão da wallet", () => {
    const wallet = Wallet.open({ id: "w1", playerId: "p1", initialBalance: brl("100.00"), now: at });
    const entry = wallet.debit({ entryId: "e1", transactionId: "t1", money: brl("25.00"), at });
    const e = WalletBalanceChanged.from(wallet, entry, ctx);
    expect(e.eventType).toBe("WalletBalanceChanged");
    expect(e.data).toEqual({
      walletId: "w1",
      transactionId: "t1",
      direction: LedgerDirection.Debit,
      money: { amount: "25.00", currency: "BRL" },
      balanceBefore: { amount: "100.00", currency: "BRL" },
      balanceAfter: { amount: "75.00", currency: "BRL" },
      walletVersion: 2,
    });
  });

  it("causationId é opcional", () => {
    const e = WagerTransactionProcessed.from(tx(), { eventId: "e", correlationId: "c", occurredAt: at });
    expect(e.toJSON().causationId).toBeUndefined();
  });
});
