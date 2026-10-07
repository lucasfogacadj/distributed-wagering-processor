import { Money, type MoneyProps } from "../domain/money";
import { OutboxMessage } from "../domain/outbox-message";
import { WalletBalanceChanged } from "../domain/events";
import { WagerTransaction } from "../domain/wager-transaction";
import { Wallet } from "../domain/wallet";
import { LedgerDirection, WalletLedgerEntry } from "../domain/wallet-ledger-entry";
import { WalletAlreadyExistsError } from "./errors";
import type { Clock, IdGenerator, UnitOfWork } from "./ports";

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: MoneyProps;
  correlationId: string;
}

export interface CreateWalletResult {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

export class CreateWallet {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
  ) {}

  async execute(cmd: CreateWalletCommand): Promise<CreateWalletResult> {
    const initialBalance = Money.from(cmd.initialBalance);
    const now = this.clock.now();
    const wallet = Wallet.open({ id: this.ids.next(), playerId: cmd.playerId, initialBalance, now });
    const opening = initialBalance.isPositive()
      ? WagerTransaction.createOpening({ id: this.ids.next(), walletId: wallet.id, playerId: cmd.playerId, money: initialBalance, at: now })
      : undefined;
    const entryId = opening ? this.ids.next() : undefined;

    return this.uow.run(async (repos) => {
      if (await repos.wallets.findByPlayerAndCurrency(cmd.playerId, initialBalance.currency)) {
        throw new WalletAlreadyExistsError(`Já existe wallet ${initialBalance.currency} para o player ${cmd.playerId}`);
      }

      await repos.wallets.add(wallet);

      // O saldo inicial vira OPENING + lançamento CREDIT na MESMA transação SQL.
      if (opening && entryId) {
        await repos.transactions.insertIfAbsent(opening, { resultBalance: initialBalance });
        const entry = WalletLedgerEntry.create({
          id: entryId,
          walletId: wallet.id,
          transactionId: opening.id,
          direction: LedgerDirection.Credit,
          money: initialBalance,
          balanceBefore: Money.zero(initialBalance.currency),
          balanceAfter: initialBalance,
          createdAt: now,
        });
        await repos.ledger.add(entry);

        const event = WalletBalanceChanged.from(wallet, entry, {
          eventId: this.ids.next(),
          correlationId: cmd.correlationId,
          occurredAt: now,
        });
        await repos.outbox.add(OutboxMessage.enqueue(event));
      }

      return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
    });
  }
}
