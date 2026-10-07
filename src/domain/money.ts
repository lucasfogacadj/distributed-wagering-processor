import { CurrencyMismatchError, InvalidMoneyError } from "./errors";

export interface MoneyProps {
  /** String decimal com escala fixa de 2 casas, ex.: "25.00". */
  amount: string;
  /** ISO-4217, ex.: "BRL". */
  currency: string;
}

const AMOUNT_PATTERN = /^\d+\.\d{2}$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;
/**
 * Limite do sistema: valores absolutos abaixo de 10^18 (com 2 casas, cabem em numeric(20,2)).
 * Acima disso a entrada é inválida e um crédito que ultrapassaria o limite é rejeitado pela Wallet.
 */
const MAX_MINOR_UNITS_EXCLUSIVE = 10n ** 20n;

/**
 * Valor monetário imutável. Internamente guarda centavos como bigint:
 * aritmética exata, sem number/float em nenhum ponto.
 */
export class Money {
  private constructor(
    private readonly minorUnits: bigint,
    public readonly currency: string,
  ) {}

  /** Entrada externa: só aceita decimal não negativo com exatamente 2 casas. */
  static from(props: MoneyProps): Money {
    const { amount, currency } = props;
    if (typeof amount !== "string" || !AMOUNT_PATTERN.test(amount)) {
      throw new InvalidMoneyError(`Valor monetário inválido: ${JSON.stringify(amount)}`);
    }
    Money.assertCurrency(currency);
    const minorUnits = BigInt(amount.replace(".", ""));
    if (minorUnits >= MAX_MINOR_UNITS_EXCLUSIVE) {
      throw new InvalidMoneyError(`Valor monetário acima do limite suportado: ${JSON.stringify(amount)}`);
    }
    return new Money(minorUnits, currency);
  }

  static zero(currency: string): Money {
    Money.assertCurrency(currency);
    return new Money(0n, currency);
  }

  private static assertCurrency(currency: unknown): asserts currency is string {
    if (typeof currency !== "string" || !CURRENCY_PATTERN.test(currency)) {
      throw new InvalidMoneyError(`Moeda inválida: ${JSON.stringify(currency)}`);
    }
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits + other.minorUnits, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits - other.minorUnits, this.currency);
  }

  negate(): Money {
    return new Money(-this.minorUnits, this.currency);
  }

  isZero(): boolean {
    return this.minorUnits === 0n;
  }

  isPositive(): boolean {
    return this.minorUnits > 0n;
  }

  isNegative(): boolean {
    return this.minorUnits < 0n;
  }

  /** Dentro do limite suportado pelo sistema (|valor| < 10^18). */
  isWithinLimit(): boolean {
    const abs = this.minorUnits < 0n ? -this.minorUnits : this.minorUnits;
    return abs < MAX_MINOR_UNITS_EXCLUSIVE;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits < other.minorUnits;
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.minorUnits === other.minorUnits;
  }

  toJSON(): MoneyProps {
    const negative = this.minorUnits < 0n;
    const digits = (negative ? -this.minorUnits : this.minorUnits).toString().padStart(3, "0");
    const amount = `${negative ? "-" : ""}${digits.slice(0, -2)}.${digits.slice(-2)}`;
    return { amount, currency: this.currency };
  }

  toString(): string {
    const { amount, currency } = this.toJSON();
    return `${amount} ${currency}`;
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(`Moedas diferentes: ${this.currency} e ${other.currency}`);
    }
  }
}
