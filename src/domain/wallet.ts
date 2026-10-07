import { BalanceLimitExceededError, CurrencyMismatchError, InsufficientFundsError, InvalidMoneyError } from "./errors";
import { Money } from "./money";
import { LedgerDirection, WalletLedgerEntry } from "./wallet-ledger-entry";

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovement {
  entryId: string;
  transactionId: string;
  money: Money;
  at: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: { id: string; playerId: string; initialBalance: Money; now?: Date }): Wallet {
    if (props.initialBalance.isNegative()) {
      throw new InvalidMoneyError("Saldo inicial não pode ser negativo");
    }
    const now = props.now ?? new Date();
    return new Wallet(props.id, props.playerId, props.initialBalance.currency, props.initialBalance, 1, now, now);
  }

  /** Reconstrução a partir da persistência — não revalida transições. */
  static rehydrate(s: WalletState): Wallet {
    return new Wallet(s.id, s.playerId, s.currency, s.balance, s.version, s.createdAt, s.updatedAt);
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  /** Aplica o débito e devolve o lançamento correspondente (saldo e ledger nascem juntos). */
  debit(m: WalletMovement): WalletLedgerEntry {
    this.assertMovable(m.money);
    if (this._balance.isLessThan(m.money)) {
      throw new InsufficientFundsError(`Saldo ${this._balance} insuficiente para ${m.money}`);
    }
    return this.apply(LedgerDirection.Debit, this._balance.subtract(m.money), m);
  }

  credit(m: WalletMovement): WalletLedgerEntry {
    this.assertMovable(m.money);
    const balanceAfter = this._balance.add(m.money);
    if (!balanceAfter.isWithinLimit()) {
      throw new BalanceLimitExceededError(`Crédito de ${m.money} levaria o saldo ${this._balance} acima do limite`);
    }
    return this.apply(LedgerDirection.Credit, balanceAfter, m);
  }

  private apply(direction: LedgerDirection, balanceAfter: Money, m: WalletMovement): WalletLedgerEntry {
    const entry = WalletLedgerEntry.create({
      id: m.entryId,
      walletId: this.id,
      transactionId: m.transactionId,
      direction,
      money: m.money,
      balanceBefore: this._balance,
      balanceAfter,
      createdAt: m.at,
    });
    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = m.at;
    return entry;
  }

  private assertMovable(money: Money): void {
    this.assertSameCurrency(money);
    if (!money.isPositive()) {
      throw new InvalidMoneyError("Movimentação exige valor positivo");
    }
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(`Wallet ${this.currency} não aceita ${money.currency}`);
    }
  }
}
