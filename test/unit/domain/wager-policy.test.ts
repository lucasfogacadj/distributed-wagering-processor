import { describe, expect, it } from "bun:test";
import { Money } from "../../../src/domain/money";
import { FailureCode } from "../../../src/domain/failure-code";
import {
  WagerTransaction,
  WagerTransactionKind as K,
  WagerTransactionStatus as S,
} from "../../../src/domain/wager-transaction";
import { WagerPolicy } from "../../../src/domain/wager-policy";
import { BalanceLimitExceededError, CurrencyMismatchError, InsufficientFundsError } from "../../../src/domain/errors";

const brl = (amount: string, currency = "BRL") => Money.from({ amount, currency });
const at = new Date("2026-07-29T15:00:00.000Z");

const base = {
  providerId: "provider-a",
  playerId: "p1",
  walletId: "w1",
  roundId: "r1",
  gameId: "g1",
  payloadHash: "h",
  createdAt: at,
};

const reference = (over: Record<string, unknown> = {}) =>
  WagerTransaction.rehydrate({
    ...base,
    id: "ref",
    externalTransactionId: "ext-bet",
    idempotencyKey: "provider-a:ext-bet",
    kind: K.Bet,
    money: brl("25.00"),
    status: S.Processed,
    processedAt: at,
    ...over,
  } as never);

const reversal = (kind: K.Refund | K.Rollback = K.Refund, over: Record<string, unknown> = {}) =>
  WagerTransaction.create({
    ...base,
    id: "rev",
    externalTransactionId: "ext-rev",
    idempotencyKey: "provider-a:ext-rev",
    kind,
    money: brl("25.00"),
    referenceExternalTransactionId: "ext-bet",
    ...over,
  } as never);

describe("WagerPolicy.evaluateReference", () => {
  it("referência ausente → aguardar", () => {
    expect(WagerPolicy.evaluateReference(reversal(), undefined, false)).toEqual({ outcome: "wait" });
  });

  it("REFUND de BET processada, mesmo contexto e valor → aplicar", () => {
    expect(WagerPolicy.evaluateReference(reversal(), reference(), false)).toEqual({ outcome: "apply" });
  });

  it.each([K.Bet, K.Win, K.Refund])("ROLLBACK pode referenciar %s", (kind) => {
    const ref = reference({ kind, ...(kind === K.Refund ? { referenceExternalTransactionId: "x" } : {}) });
    expect(WagerPolicy.evaluateReference(reversal(K.Rollback), ref, false)).toEqual({ outcome: "apply" });
  });

  it.each([
    ["providerId", { providerId: "provider-b" }],
    ["playerId", { playerId: "p2" }],
    ["walletId", { walletId: "w2" }],
    ["roundId", { roundId: "r2" }],
    ["moeda", { money: brl("25.00", "USD") }],
  ])("%s divergente → REFERENCE_MISMATCH", (_n, over) => {
    expect(WagerPolicy.evaluateReference(reversal(), reference(over), false)).toEqual({
      outcome: "reject",
      code: FailureCode.ReferenceMismatch,
    });
  });

  it.each([
    [K.Refund, K.Win],
    [K.Refund, K.Loss],
    [K.Refund, K.Refund],
    [K.Rollback, K.Loss],
    [K.Rollback, K.Rollback],
  ])("%s não pode referenciar %s → INVALID_REFERENCE_KIND", (revKind, refKind) => {
    const ref = reference({
      kind: refKind,
      ...(refKind === K.Refund || refKind === K.Rollback ? { referenceExternalTransactionId: "x" } : {}),
    });
    expect(WagerPolicy.evaluateReference(reversal(revKind as K.Refund | K.Rollback), ref, false)).toEqual({
      outcome: "reject",
      code: FailureCode.InvalidReferenceKind,
    });
  });

  it("referência ainda não aplicada → aguardar", () => {
    const pending = reference({ status: S.Pending, processedAt: undefined });
    expect(WagerPolicy.evaluateReference(reversal(), pending, false)).toEqual({ outcome: "wait" });
    const waiting = reference({ status: S.PendingReference, processedAt: undefined });
    expect(WagerPolicy.evaluateReference(reversal(), waiting, false)).toEqual({ outcome: "wait" });
  });

  it.each([S.Rejected, S.Failed])("referência %s → REFERENCE_NOT_PROCESSED", (status) => {
    const ref = reference({ status, processedAt: undefined, failureCode: FailureCode.InsufficientFunds });
    expect(WagerPolicy.evaluateReference(reversal(), ref, false)).toEqual({
      outcome: "reject",
      code: FailureCode.ReferenceNotProcessed,
    });
  });

  it("valor diferente → AMOUNT_MISMATCH", () => {
    const rev = reversal(K.Refund, { money: brl("24.99") });
    expect(WagerPolicy.evaluateReference(rev, reference(), false)).toEqual({
      outcome: "reject",
      code: FailureCode.AmountMismatch,
    });
  });

  it("já revertida (por qualquer tipo de reversão) → ALREADY_REVERSED", () => {
    expect(WagerPolicy.evaluateReference(reversal(), reference(), true)).toEqual({
      outcome: "reject",
      code: FailureCode.AlreadyReversed,
    });
  });

  it("transação que não exige referência → aplicar", () => {
    const bet = WagerTransaction.create({
      ...base,
      id: "b",
      externalTransactionId: "e",
      idempotencyKey: "k",
      kind: K.Bet,
      money: brl("1.00"),
    } as never);
    expect(WagerPolicy.evaluateReference(bet, undefined, false)).toEqual({ outcome: "apply" });
  });

  describe("WIN com referência", () => {
    const win = (over: Record<string, unknown> = {}) =>
      WagerTransaction.create({
        ...base,
        id: "win",
        externalTransactionId: "ext-win",
        idempotencyKey: "provider-a:ext-win",
        kind: K.Win,
        money: brl("90.00"),
        referenceExternalTransactionId: "ext-bet",
        ...over,
      } as never);

    it("sem referência informada → aplicar", () => {
      expect(WagerPolicy.evaluateReference(win({ referenceExternalTransactionId: undefined }), undefined, false)).toEqual({
        outcome: "apply",
      });
    });

    it("BET ausente → aguardar", () => {
      expect(WagerPolicy.evaluateReference(win(), undefined, false)).toEqual({ outcome: "wait" });
    });

    it("BET processada com valor diferente → aplicar (prêmio é livre)", () => {
      expect(WagerPolicy.evaluateReference(win(), reference(), false)).toEqual({ outcome: "apply" });
    });

    it("BET rejeitada → REFERENCE_NOT_PROCESSED", () => {
      expect(WagerPolicy.evaluateReference(win(), reference({ status: S.Rejected }), false)).toEqual({
        outcome: "reject",
        code: FailureCode.ReferenceNotProcessed,
      });
    });

    it("BET já revertida → ALREADY_REVERSED", () => {
      expect(WagerPolicy.evaluateReference(win(), reference(), true)).toEqual({
        outcome: "reject",
        code: FailureCode.AlreadyReversed,
      });
    });

    it("referência que não é BET → INVALID_REFERENCE_KIND", () => {
      expect(WagerPolicy.evaluateReference(win(), reference({ kind: K.Win }), false)).toEqual({
        outcome: "reject",
        code: FailureCode.InvalidReferenceKind,
      });
    });

    it("outra rodada → REFERENCE_MISMATCH", () => {
      expect(WagerPolicy.evaluateReference(win(), reference({ roundId: "r2" }), false)).toEqual({
        outcome: "reject",
        code: FailureCode.ReferenceMismatch,
      });
    });
  });
});

describe("WagerPolicy.failureCodeFor", () => {
  const bet = () =>
    WagerTransaction.create({
      ...base,
      id: "b",
      externalTransactionId: "e",
      idempotencyKey: "k",
      kind: K.Bet,
      money: brl("1.00"),
    } as never);

  it("saldo insuficiente em BET → INSUFFICIENT_FUNDS", () => {
    expect(WagerPolicy.failureCodeFor(bet(), new InsufficientFundsError("x"))).toBe(FailureCode.InsufficientFunds);
  });

  it("saldo insuficiente em reversão → REVERSAL_WOULD_OVERDRAW", () => {
    expect(WagerPolicy.failureCodeFor(reversal(K.Rollback), new InsufficientFundsError("x"))).toBe(
      FailureCode.ReversalWouldOverdraw,
    );
  });

  it("conflito de moeda → CURRENCY_MISMATCH", () => {
    expect(WagerPolicy.failureCodeFor(bet(), new CurrencyMismatchError("x"))).toBe(FailureCode.CurrencyMismatch);
  });

  it("crédito acima do limite → BALANCE_LIMIT_EXCEEDED", () => {
    expect(WagerPolicy.failureCodeFor(reversal(K.Refund), new BalanceLimitExceededError("x"))).toBe(
      FailureCode.BalanceLimitExceeded,
    );
  });

  it("erro desconhecido → undefined (não é regra de negócio)", () => {
    expect(WagerPolicy.failureCodeFor(bet(), new Error("boom"))).toBeUndefined();
  });
});
