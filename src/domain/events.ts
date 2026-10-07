import { InvalidTransactionStateError } from "./errors";
import { FailureCode } from "./failure-code";
import { EventContext, IntegrationEvent } from "./integration-event";
import { MoneyProps } from "./money";
import { WagerTransaction, WagerTransactionStatus } from "./wager-transaction";
import { Wallet } from "./wallet";
import { LedgerDirection, WalletLedgerEntry } from "./wallet-ledger-entry";

/** `data` carrega MoneyProps (string decimal), nunca a instância de Money. */
export interface WagerTransactionEventData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  kind: string;
  money: MoneyProps;
}

function transactionData(tx: WagerTransaction): WagerTransactionEventData {
  return {
    transactionId: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    kind: tx.kind,
    money: tx.money.toJSON(),
  };
}

/** O aggregateId é a wallet: a unidade de concorrência e de ordenação. */
export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed({ ...ctx, aggregateId: tx.walletId, data: transactionData(tx) });
  }
}

export interface WagerTransactionRejectedData extends WagerTransactionEventData {
  failureCode: FailureCode;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    if (!tx.failureCode) {
      throw new InvalidTransactionStateError(`Transação ${tx.id} não possui failureCode`);
    }
    return new WagerTransactionRejected({
      ...ctx,
      aggregateId: tx.walletId,
      data: { ...transactionData(tx), failureCode: tx.failureCode },
    });
  }
}

export interface WagerTransactionFailedData extends WagerTransactionEventData {
  failureCode: FailureCode;
}

/**
 * Falha permanente de infraestrutura (estado terminal FAILED): diferente de uma rejeição de negócio, não diz nada sobre
 * o payload — o processamento falhou de forma não transitória e a transação foi encerrada de forma auditável.
 */
export class WagerTransactionFailed extends IntegrationEvent<WagerTransactionFailedData> {
  readonly eventType = "WagerTransactionFailed";
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionFailed {
    if (tx.status !== WagerTransactionStatus.Failed || !tx.failureCode) {
      throw new InvalidTransactionStateError(`Transação ${tx.id} não está FAILED com failureCode`);
    }
    return new WagerTransactionFailed({
      ...ctx,
      aggregateId: tx.walletId,
      data: { ...transactionData(tx), failureCode: tx.failureCode },
    });
  }
}

export interface WagerTransactionPendingReferenceData extends WagerTransactionEventData {
  referenceExternalTransactionId: string | undefined;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference({
      ...ctx,
      aggregateId: tx.walletId,
      data: { ...transactionData(tx), referenceExternalTransactionId: tx.referenceExternalTransactionId },
    });
  }
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

/** Publicado somente quando o saldo muda. */
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({
      ...ctx,
      aggregateId: wallet.id,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}
