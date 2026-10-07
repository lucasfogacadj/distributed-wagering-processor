import { InvalidTransactionError } from "../domain/errors";
import {
  WagerTransactionFailed,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from "../domain/events";
import { FailureCode } from "../domain/failure-code";
import type { EventContext, IntegrationEvent } from "../domain/integration-event";
import type { Money, MoneyProps } from "../domain/money";
import { OutboxMessage } from "../domain/outbox-message";
import { WagerPolicy } from "../domain/wager-policy";
import type { WagerTransaction, WagerTransactionStatus } from "../domain/wager-transaction";
import type { Wallet } from "../domain/wallet";
import { LedgerDirection, type WalletLedgerEntry } from "../domain/wallet-ledger-entry";
import { NO_METRICS } from "./observability";
import type { Clock, IdGenerator, MetricsPort, Repositories } from "./ports";

export interface ProcessWagerResult {
  transactionId: string;
  status: WagerTransactionStatus;
  failureCode?: FailureCode;
  /** Ausente apenas quando a wallet não existe. */
  balance?: MoneyProps;
  idempotentReplay: boolean;
}

export interface SettleContext {
  correlationId: string;
  causationId?: string | undefined;
}

export type Settlement =
  | { type: "settled"; result: ProcessWagerResult }
  /** `referenceFound`: a referência existe mas ainda não tem decisão final (ex.: ela própria aguarda referência). */
  | { type: "waiting"; balance: Money; referenceFound: boolean };

/**
 * Decide e aplica uma transação já registrada, dentro da transação SQL do chamador.
 * Compartilhado pela entrada (HTTP/SQS) e pelo reprocessamento de PENDING_REFERENCE.
 *
 * Pré-condição: o chamador já tomou o lock da wallet (`findByIdForUpdate`) — a wallet é a unidade de
 * concorrência. A ordem de locks do sistema é sempre wallet → linha da transação.
 */
export class WagerSettler {
  constructor(
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly metrics: MetricsPort = NO_METRICS,
  ) {}

  async settle(repos: Repositories, wallet: Wallet | undefined, tx: WagerTransaction, ctx: SettleContext): Promise<Settlement> {
    if (!wallet) {
      // Sem linha para travar: o lock sobre o id mantém em ordem os eventos de rejeições concorrentes desta wallet.
      await repos.wallets.lockAbsent(tx.walletId);
      return this.settled(await this.reject(repos, tx, FailureCode.WalletNotFound, undefined, ctx));
    }
    if (wallet.playerId !== tx.playerId) {
      return this.settled(await this.reject(repos, tx, FailureCode.WalletPlayerMismatch, wallet.balance, ctx));
    }
    if (wallet.currency !== tx.money.currency) {
      return this.settled(await this.reject(repos, tx, FailureCode.CurrencyMismatch, wallet.balance, ctx));
    }

    let reference: WagerTransaction | undefined;
    if (tx.hasReferenceToResolve()) {
      const referenceExternalId = tx.referenceExternalTransactionId;
      if (!referenceExternalId) throw new InvalidTransactionError(`${tx.kind} sem referenceExternalTransactionId`);

      reference = (await repos.transactions.findByProviderExternal(tx.providerId, referenceExternalId))?.transaction;
      const alreadyReversed = reference ? await repos.transactions.hasProcessedReversal(reference.id) : false;

      const decision = WagerPolicy.evaluateReference(tx, reference, alreadyReversed);
      if (decision.outcome === "wait") {
        return { type: "waiting", balance: wallet.balance, referenceFound: reference !== undefined };
      }
      if (decision.outcome === "reject") {
        return this.settled(await this.reject(repos, tx, decision.code, wallet.balance, ctx));
      }
    }

    return this.settled(await this.apply(repos, wallet, tx, reference, ctx));
  }

  /**
   * Marca REJECTED, persiste o snapshot e enfileira WagerTransactionRejected. Não altera saldo nem ledger.
   * Quem aguardava esta transação como referência é liberado na hora (será rejeitado como REFERENCE_NOT_PROCESSED).
   */
  async reject(
    repos: Repositories,
    tx: WagerTransaction,
    code: FailureCode,
    balance: Money | undefined,
    ctx: SettleContext,
  ): Promise<ProcessWagerResult> {
    tx.reject(code);
    await repos.transactions.save(tx, { resultBalance: balance, nextAttemptAt: null });
    await this.publish(repos, ctx, (e) => WagerTransactionRejected.from(tx, e));
    await repos.transactions.releaseDependents(tx.providerId, tx.externalTransactionId, this.clock.now());
    this.metrics.transactionDecided(tx.status, tx.kind);

    return {
      transactionId: tx.id,
      status: tx.status,
      failureCode: code,
      ...(balance ? { balance: balance.toJSON() } : {}),
      idempotentReplay: false,
    };
  }

  /**
   * Encerra a transação como FAILED (falha permanente de infraestrutura): terminal e auditável, sem saldo nem ledger.
   * Publica WagerTransactionFailed e libera quem a aguardava como referência (será rejeitado como REFERENCE_NOT_PROCESSED).
   */
  async fail(
    repos: Repositories,
    tx: WagerTransaction,
    code: FailureCode,
    balance: Money | undefined,
    ctx: SettleContext,
  ): Promise<ProcessWagerResult> {
    tx.fail(code);
    await repos.transactions.save(tx, { resultBalance: balance, nextAttemptAt: null });
    await this.publish(repos, ctx, (e) => WagerTransactionFailed.from(tx, e));
    await repos.transactions.releaseDependents(tx.providerId, tx.externalTransactionId, this.clock.now());
    this.metrics.transactionDecided(tx.status, tx.kind);

    return {
      transactionId: tx.id,
      status: tx.status,
      failureCode: code,
      ...(balance ? { balance: balance.toJSON() } : {}),
      idempotentReplay: false,
    };
  }

  private async apply(
    repos: Repositories,
    wallet: Wallet,
    tx: WagerTransaction,
    reference: WagerTransaction | undefined,
    ctx: SettleContext,
  ): Promise<ProcessWagerResult> {
    let entry: WalletLedgerEntry | undefined;
    const now = this.clock.now();

    if (tx.affectsBalance()) {
      const movement = { entryId: this.ids.next(), transactionId: tx.id, money: tx.money, at: now };
      try {
        entry =
          tx.ledgerDirectionFor(reference) === LedgerDirection.Debit ? wallet.debit(movement) : wallet.credit(movement);
      } catch (error) {
        const code = WagerPolicy.failureCodeFor(tx, error);
        if (!code) throw error;
        return this.reject(repos, tx, code, wallet.balance, ctx);
      }
    }

    tx.markProcessed(reference?.id, now);

    if (entry) {
      await repos.wallets.save(wallet);
      await repos.ledger.add(entry);
    }
    await repos.transactions.save(tx, { resultBalance: wallet.balance, nextAttemptAt: null });

    await this.publish(repos, ctx, (e) => WagerTransactionProcessed.from(tx, e));
    if (entry) {
      const changed = entry;
      await this.publish(repos, ctx, (e) => WalletBalanceChanged.from(wallet, changed, e));
    }

    // A chegada desta transação libera quem a aguardava como referência.
    await repos.transactions.releaseDependents(tx.providerId, tx.externalTransactionId, now);
    this.metrics.transactionDecided(tx.status, tx.kind);

    return { transactionId: tx.id, status: tx.status, balance: wallet.balance.toJSON(), idempotentReplay: false };
  }

  private async publish(
    repos: Repositories,
    ctx: SettleContext,
    build: (eventContext: EventContext) => IntegrationEvent<unknown>,
  ): Promise<void> {
    const eventContext: EventContext = {
      eventId: this.ids.next(),
      correlationId: ctx.correlationId,
      ...(ctx.causationId !== undefined ? { causationId: ctx.causationId } : {}),
      occurredAt: this.clock.now(),
    };
    await repos.outbox.add(OutboxMessage.enqueue(build(eventContext)));
  }

  private settled(result: ProcessWagerResult): Settlement {
    return { type: "settled", result };
  }
}
