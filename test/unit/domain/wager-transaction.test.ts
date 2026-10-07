import { describe, expect, it } from "bun:test";
import { Money } from "../../../src/domain/money";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../../src/domain/wager-transaction";
import { FailureCode } from "../../../src/domain/failure-code";
import { LedgerDirection } from "../../../src/domain/wallet-ledger-entry";
import { InvalidTransactionStateError, InvalidTransactionError } from "../../../src/domain/errors";

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });
const at = new Date("2026-07-29T15:00:00.000Z");

const props = (over: Record<string, unknown> = {}) => ({
  id: "t1",
  providerId: "provider-a",
  externalTransactionId: "ext-1",
  idempotencyKey: "provider-a:ext-1",
  payloadHash: "hash-1",
  walletId: "w1",
  playerId: "p1",
  roundId: "r1",
  gameId: "g1",
  kind: WagerTransactionKind.Bet,
  money: brl("25.00"),
  createdAt: at,
  ...over,
});

const make = (over: Record<string, unknown> = {}) => WagerTransaction.create(props(over) as never);

describe("WagerTransaction", () => {
  describe("create", () => {
    it("nasce PENDING", () => {
      expect(make().status).toBe(WagerTransactionStatus.Pending);
    });

    it("rejeita OPENING vindo de fora", () => {
      expect(() => make({ kind: WagerTransactionKind.Opening })).toThrow(InvalidTransactionError);
    });

    it.each([WagerTransactionKind.Refund, WagerTransactionKind.Rollback])(
      "%s exige referenceExternalTransactionId",
      (kind) => {
        expect(() => make({ kind })).toThrow(InvalidTransactionError);
        const t = make({ kind, referenceExternalTransactionId: "ext-0" });
        expect(t.requiresReference()).toBe(true);
        expect(t.referenceExternalTransactionId).toBe("ext-0");
      },
    );

    it("BET não aceita referência", () => {
      expect(() => make({ referenceExternalTransactionId: "x" })).toThrow(InvalidTransactionError);
    });

    it("WIN pode referenciar uma BET opcionalmente", () => {
      expect(make({ kind: WagerTransactionKind.Win }).requiresReference()).toBe(false);
      expect(
        make({ kind: WagerTransactionKind.Win, referenceExternalTransactionId: "ext-0" }).referenceExternalTransactionId,
      ).toBe("ext-0");
    });

    it("rejeita valor zero ou negativo, exceto LOSS com zero", () => {
      expect(() => make({ money: brl("0.00") })).toThrow(InvalidTransactionError);
      expect(() => make({ money: brl("1.00").negate() })).toThrow(InvalidTransactionError);
      expect(make({ kind: WagerTransactionKind.Loss, money: brl("0.00") }).affectsBalance()).toBe(false);
    });
  });

  describe("consultas", () => {
    it("affectsBalance é false só para LOSS", () => {
      expect(make({ kind: WagerTransactionKind.Loss }).affectsBalance()).toBe(false);
      expect(make({ kind: WagerTransactionKind.Bet }).affectsBalance()).toBe(true);
      expect(make({ kind: WagerTransactionKind.Win }).affectsBalance()).toBe(true);
    });

    it("matchesPayload compara o hash", () => {
      const t = make();
      expect(t.matchesPayload("hash-1")).toBe(true);
      expect(t.matchesPayload("outro")).toBe(false);
    });
  });

  describe("transições", () => {
    it("PENDING → PROCESSED", () => {
      const t = make();
      t.markProcessed(undefined, at);
      expect(t.status).toBe(WagerTransactionStatus.Processed);
      expect(t.processedAt).toEqual(at);
      expect(t.isTerminal()).toBe(true);
    });

    it("markProcessed guarda a referência interna", () => {
      const t = make({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: "ext-0" });
      t.markProcessed("t0", at);
      expect(t.referenceTransactionId).toBe("t0");
    });

    it("PENDING → PENDING_REFERENCE → PROCESSED", () => {
      const t = make({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: "ext-0" });
      t.markPendingReference();
      expect(t.status).toBe(WagerTransactionStatus.PendingReference);
      expect(t.isTerminal()).toBe(false);
      t.markProcessed("t0", at);
      expect(t.status).toBe(WagerTransactionStatus.Processed);
    });

    it("PENDING_REFERENCE → REJECTED com failureCode", () => {
      const t = make({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: "ext-0" });
      t.markPendingReference();
      t.reject(FailureCode.ReferenceNotFound);
      expect(t.status).toBe(WagerTransactionStatus.Rejected);
      expect(t.failureCode).toBe(FailureCode.ReferenceNotFound);
    });

    it("PENDING → FAILED", () => {
      const t = make();
      t.fail(FailureCode.InfraPermanent);
      expect(t.status).toBe(WagerTransactionStatus.Failed);
      expect(t.isTerminal()).toBe(true);
    });

    it.each([
      ["markProcessed", (t: WagerTransaction) => t.markProcessed(undefined, at)],
      ["markPendingReference", (t: WagerTransaction) => t.markPendingReference()],
      ["reject", (t: WagerTransaction) => t.reject(FailureCode.InsufficientFunds)],
      ["fail", (t: WagerTransaction) => t.fail(FailureCode.InfraPermanent)],
    ])("estado terminal rejeita %s", (_name, act) => {
      const t = make();
      t.markProcessed(undefined, at);
      expect(() => act(t)).toThrow(InvalidTransactionStateError);
    });
  });

  describe("createOpening", () => {
    const opening = (money = brl("1000.00")) =>
      WagerTransaction.createOpening({ id: "o1", walletId: "w1", playerId: "p1", money, at });

    it("nasce PROCESSED, interna e com chaves determinísticas por wallet", () => {
      const t = opening();
      expect(t.kind).toBe(WagerTransactionKind.Opening);
      expect(t.status).toBe(WagerTransactionStatus.Processed);
      expect(t.processedAt).toEqual(at);
      expect(t.providerId).toBe("internal");
      expect(t.idempotencyKey).toBe("opening:w1");
      expect(t.externalTransactionId).toBe("opening:w1");
      expect(t.affectsBalance()).toBe(true);
    });

    it("credita o saldo de abertura", () => {
      expect(opening().ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    });

    it("rejeita valor zero ou negativo", () => {
      expect(() => opening(brl("0.00"))).toThrow(InvalidTransactionError);
      expect(() => opening(brl("1.00").negate())).toThrow(InvalidTransactionError);
    });
  });

  describe("ledgerDirectionFor", () => {
    const ref = (kind: WagerTransactionKind) =>
      make({
        id: "ref",
        kind,
        ...(kind === WagerTransactionKind.Refund ? { referenceExternalTransactionId: "ext-bet" } : {}),
      });

    it("BET debita, WIN e REFUND creditam", () => {
      expect(make().ledgerDirectionFor()).toBe(LedgerDirection.Debit);
      expect(make({ kind: WagerTransactionKind.Win }).ledgerDirectionFor()).toBe(LedgerDirection.Credit);
      expect(
        make({ kind: WagerTransactionKind.Refund, referenceExternalTransactionId: "x" }).ledgerDirectionFor(),
      ).toBe(LedgerDirection.Credit);
    });

    it("ROLLBACK é o inverso da referência", () => {
      const rb = make({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: "x" });
      expect(rb.ledgerDirectionFor(ref(WagerTransactionKind.Bet))).toBe(LedgerDirection.Credit);
      expect(rb.ledgerDirectionFor(ref(WagerTransactionKind.Win))).toBe(LedgerDirection.Debit);
      expect(rb.ledgerDirectionFor(ref(WagerTransactionKind.Refund))).toBe(LedgerDirection.Debit);
    });

    it("ROLLBACK sem referência é erro", () => {
      const rb = make({ kind: WagerTransactionKind.Rollback, referenceExternalTransactionId: "x" });
      expect(() => rb.ledgerDirectionFor()).toThrow(InvalidTransactionError);
    });

    it("LOSS não tem direção", () => {
      expect(() => make({ kind: WagerTransactionKind.Loss }).ledgerDirectionFor()).toThrow(InvalidTransactionError);
    });
  });
});
