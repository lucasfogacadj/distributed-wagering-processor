import { describe, expect, it } from "bun:test";
import { Money } from "../../../src/domain/money";
import { Wallet } from "../../../src/domain/wallet";
import { LedgerDirection } from "../../../src/domain/wallet-ledger-entry";
import { BalanceLimitExceededError, CurrencyMismatchError, InsufficientFundsError, InvalidMoneyError } from "../../../src/domain/errors";

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });
const t0 = new Date("2026-07-29T15:00:00.000Z");
const t1 = new Date("2026-07-29T15:00:01.000Z");

const open = (initial = "100.00") => Wallet.open({ id: "w1", playerId: "p1", initialBalance: brl(initial), now: t0 });
const move = (money: Money) => ({ entryId: "e1", transactionId: "t1", money, at: t1 });

describe("Wallet", () => {
  describe("open", () => {
    it("começa com version 1 e o saldo inicial", () => {
      const w = open("1000.00");
      expect(w.version).toBe(1);
      expect(w.balance.toJSON().amount).toBe("1000.00");
      expect(w.currency).toBe("BRL");
    });

    it("aceita saldo inicial zero", () => {
      expect(open("0.00").balance.isZero()).toBe(true);
    });

    it("rejeita saldo inicial negativo", () => {
      expect(() =>
        Wallet.open({ id: "w", playerId: "p", initialBalance: brl("1.00").negate(), now: t0 }),
      ).toThrow(InvalidMoneyError);
    });
  });

  describe("debit", () => {
    it("reduz o saldo, incrementa version e devolve lançamento balanceado", () => {
      const w = open("100.00");
      const entry = w.debit(move(brl("80.00")));
      expect(w.balance.toJSON().amount).toBe("20.00");
      expect(w.version).toBe(2);
      expect(w.updatedAt).toEqual(t1);
      expect(entry.direction).toBe(LedgerDirection.Debit);
      expect(entry.balanceBefore.toJSON().amount).toBe("100.00");
      expect(entry.balanceAfter.toJSON().amount).toBe("20.00");
      expect(entry.walletId).toBe("w1");
      expect(entry.isBalanced()).toBe(true);
    });

    it("permite zerar o saldo", () => {
      const w = open("80.00");
      w.debit(move(brl("80.00")));
      expect(w.balance.isZero()).toBe(true);
    });

    it("rejeita saldo insuficiente sem alterar estado", () => {
      const w = open("50.00");
      expect(() => w.debit(move(brl("50.01")))).toThrow(InsufficientFundsError);
      expect(w.balance.toJSON().amount).toBe("50.00");
      expect(w.version).toBe(1);
    });

    it("rejeita moeda diferente", () => {
      const w = open();
      expect(() => w.debit(move(Money.from({ amount: "1.00", currency: "USD" })))).toThrow(CurrencyMismatchError);
    });

    it("rejeita valor não positivo", () => {
      expect(() => open().debit(move(brl("0.00")))).toThrow(InvalidMoneyError);
    });
  });

  describe("credit", () => {
    it("aumenta o saldo, incrementa version e devolve lançamento CREDIT", () => {
      const w = open("10.00");
      const entry = w.credit(move(brl("5.50")));
      expect(w.balance.toJSON().amount).toBe("15.50");
      expect(w.version).toBe(2);
      expect(entry.direction).toBe(LedgerDirection.Credit);
      expect(entry.isBalanced()).toBe(true);
    });

    it("rejeita moeda diferente", () => {
      expect(() => open().credit(move(Money.from({ amount: "1.00", currency: "USD" })))).toThrow(
        CurrencyMismatchError,
      );
    });

    it("rejeita crédito que levaria o saldo acima do limite, sem alterar estado", () => {
      const w = open("999999999999999999.00");
      w.credit(move(brl("0.99")));
      expect(() => w.credit(move(brl("0.01")))).toThrow(BalanceLimitExceededError);
      expect(w.balance.toJSON().amount).toBe("999999999999999999.99");
      expect(w.version).toBe(2);
    });
  });

  it("rehydrate reconstrói o estado sem alterá-lo", () => {
    const w = Wallet.rehydrate({
      id: "w9",
      playerId: "p9",
      currency: "BRL",
      balance: brl("7.00"),
      version: 5,
      createdAt: t0,
      updatedAt: t1,
    });
    expect(w.version).toBe(5);
    expect(w.balance.toJSON().amount).toBe("7.00");
  });
});
