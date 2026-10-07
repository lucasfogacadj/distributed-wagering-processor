import { describe, expect, it } from "bun:test";
import { Money } from "../../../src/domain/money";
import { InvalidMoneyError, CurrencyMismatchError } from "../../../src/domain/errors";

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });

describe("Money", () => {
  describe("criação", () => {
    it("aceita decimal com exatamente 2 casas", () => {
      expect(brl("25.00").toString()).toBe("25.00 BRL");
    });

    it("serializa como string decimal com escala 2", () => {
      expect(brl("1000.50").toJSON()).toEqual({ amount: "1000.50", currency: "BRL" });
    });

    it("zero() devolve 0.00", () => {
      expect(Money.zero("BRL").toJSON()).toEqual({ amount: "0.00", currency: "BRL" });
    });

    it.each(["", " ", "NaN", "Infinity", "-Infinity", "1e3", "1E3", "25", "25.0", "25.001", "-1.00", "+1.00", "1,00", ".50", "abc"])(
      "rejeita entrada inválida %p",
      (amount) => {
        expect(() => brl(amount)).toThrow(InvalidMoneyError);
      },
    );

    it("rejeita número JS no lugar de string", () => {
      expect(() => Money.from({ amount: 25 as unknown as string, currency: "BRL" })).toThrow(InvalidMoneyError);
    });

    it("aceita o maior valor suportado e rejeita a partir de 10^18 (limite de numeric(20,2))", () => {
      expect(brl("999999999999999999.99").toJSON().amount).toBe("999999999999999999.99");
      expect(() => brl("1000000000000000000.00")).toThrow(InvalidMoneyError);
      expect(() => brl(`${"9".repeat(40)}.00`)).toThrow(InvalidMoneyError);
    });

    it("zeros à esquerda não contam para o limite", () => {
      expect(brl("0000000000000000000001.00").toJSON().amount).toBe("1.00");
    });

    it("isWithinLimit vale também para resultados de aritmética", () => {
      const max = brl("999999999999999999.99");
      expect(max.isWithinLimit()).toBe(true);
      expect(max.add(brl("0.01")).isWithinLimit()).toBe(false);
      expect(max.add(brl("0.01")).negate().isWithinLimit()).toBe(false);
    });

    it.each(["", "br", "BRLX", "brl", "12A"])("rejeita moeda inválida %p", (currency) => {
      expect(() => Money.from({ amount: "1.00", currency })).toThrow(InvalidMoneyError);
    });
  });

  describe("aritmética exata", () => {
    it("0.10 + 0.20 === 0.30 (sem erro de ponto flutuante)", () => {
      expect(brl("0.10").add(brl("0.20")).toJSON().amount).toBe("0.30");
    });

    it("subtract devolve nova instância e não muta a original", () => {
      const a = brl("100.00");
      const b = a.subtract(brl("80.00"));
      expect(b.toJSON().amount).toBe("20.00");
      expect(a.toJSON().amount).toBe("100.00");
    });

    it("suporta valores acima de Number.MAX_SAFE_INTEGER", () => {
      const big = brl("90071992547409.93");
      expect(big.add(brl("0.01")).toJSON().amount).toBe("90071992547409.94");
    });

    it("negate inverte o sinal", () => {
      expect(brl("5.00").negate().isNegative()).toBe(true);
      expect(brl("5.00").negate().toJSON().amount).toBe("-5.00");
    });
  });

  describe("consultas", () => {
    it("isZero / isPositive / isNegative", () => {
      expect(brl("0.00").isZero()).toBe(true);
      expect(brl("0.01").isPositive()).toBe(true);
      expect(brl("1.00").subtract(brl("2.00")).isNegative()).toBe(true);
    });

    it("isLessThan e equals", () => {
      expect(brl("1.00").isLessThan(brl("1.01"))).toBe(true);
      expect(brl("1.00").isLessThan(brl("1.00"))).toBe(false);
      expect(brl("1.00").equals(brl("1.00"))).toBe(true);
    });
  });

  describe("moedas diferentes", () => {
    const usd = Money.from({ amount: "1.00", currency: "USD" });

    it("add lança CurrencyMismatchError", () => {
      expect(() => brl("1.00").add(usd)).toThrow(CurrencyMismatchError);
    });

    it("subtract lança CurrencyMismatchError", () => {
      expect(() => brl("1.00").subtract(usd)).toThrow(CurrencyMismatchError);
    });

    it("isLessThan lança CurrencyMismatchError", () => {
      expect(() => brl("1.00").isLessThan(usd)).toThrow(CurrencyMismatchError);
    });

    it("equals entre moedas diferentes é false (sem lançar)", () => {
      expect(brl("1.00").equals(usd)).toBe(false);
    });
  });
});
