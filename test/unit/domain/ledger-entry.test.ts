import { describe, expect, it } from "bun:test";
import { Money } from "../../../src/domain/money";
import { LedgerDirection, WalletLedgerEntry } from "../../../src/domain/wallet-ledger-entry";
import { InvalidLedgerEntryError } from "../../../src/domain/errors";

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });
const at = new Date("2026-07-29T15:00:00.000Z");

const base = {
  id: "e1",
  walletId: "w1",
  transactionId: "t1",
  createdAt: at,
};

describe("WalletLedgerEntry", () => {
  it("cria débito balanceado", () => {
    const e = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Debit,
      money: brl("25.00"),
      balanceBefore: brl("100.00"),
      balanceAfter: brl("75.00"),
    });
    expect(e.isBalanced()).toBe(true);
    expect(e.direction).toBe(LedgerDirection.Debit);
  });

  it("cria crédito balanceado", () => {
    const e = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Credit,
      money: brl("25.00"),
      balanceBefore: brl("0.00"),
      balanceAfter: brl("25.00"),
    });
    expect(e.isBalanced()).toBe(true);
  });

  it("rejeita aritmética inconsistente", () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        money: brl("25.00"),
        balanceBefore: brl("100.00"),
        balanceAfter: brl("80.00"),
      }),
    ).toThrow(InvalidLedgerEntryError);
  });

  it("rejeita valor zero", () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Credit,
        money: brl("0.00"),
        balanceBefore: brl("10.00"),
        balanceAfter: brl("10.00"),
      }),
    ).toThrow(InvalidLedgerEntryError);
  });

  it("rejeita saldo posterior negativo", () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        money: brl("10.00"),
        balanceBefore: brl("5.00"),
        balanceAfter: brl("5.00").subtract(brl("10.00")),
      }),
    ).toThrow(InvalidLedgerEntryError);
  });

  it("é estruturalmente imutável: não expõe métodos de transição", () => {
    const e = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Credit,
      money: brl("1.00"),
      balanceBefore: brl("0.00"),
      balanceAfter: brl("1.00"),
    });
    const methods = Object.getOwnPropertyNames(Object.getPrototypeOf(e)).filter((n) => n !== "constructor");
    expect(methods).toEqual(["isBalanced"]);
  });

  it("rehydrate não revalida", () => {
    const e = WalletLedgerEntry.rehydrate({
      ...base,
      direction: LedgerDirection.Debit,
      money: brl("25.00"),
      balanceBefore: brl("100.00"),
      balanceAfter: brl("80.00"),
    });
    expect(e.isBalanced()).toBe(false);
  });
});
