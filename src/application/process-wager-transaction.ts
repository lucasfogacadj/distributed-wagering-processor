import { WagerTransactionPendingReference } from "../domain/events";
import type { FailureCode } from "../domain/failure-code";
import { InboxMessage } from "../domain/inbox-message";
import { Money, type MoneyProps } from "../domain/money";
import { OutboxMessage } from "../domain/outbox-message";
import { pendingReferenceDelayMs } from "../domain/pending-reference-schedule";
import { WagerTransaction, type WagerTransactionKind } from "../domain/wager-transaction";
import { lockConflictKind } from "./error-classification";
import { IdempotencyConflictError } from "./errors";
import { NO_OBSERVABILITY } from "./observability";
import { payloadHash } from "./payload-hash";
import type { Clock, IdGenerator, Observability, Repositories, StoredTransaction, UnitOfWork } from "./ports";
import { WagerSettler, type ProcessWagerResult, type SettleContext } from "./wager-settler";

export type { ProcessWagerResult } from "./wager-settler";

export interface ProcessWagerCommand {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  correlationId: string;
  /** Ex.: messageId da mensagem SQS que originou o comando. */
  causationId?: string;
  /**
   * Entrada por fila: o registro na inbox persistente entra na MESMA transação SQL do efeito financeiro,
   * então "mensagem registrada" e "efeito aplicado" são a mesma coisa, sem janela entre os dois.
   */
  inbox?: { messageId: string; consumerName: string };
}

/**
 * Único caso de uso de entrada de transações — usado tanto pela API HTTP quanto pelo consumidor SQS.
 *
 * Fluxo (uma transação SQL):
 *  1. INSERT … ON CONFLICT DO NOTHING da transação (idempotência persistente, decidida pelo banco);
 *  2. se já existia: replay (mesmo payload) ou conflito (payload diferente);
 *  3. lock da wallet (SELECT … FOR UPDATE) e decisão/aplicação;
 *  4. saldo, ledger, snapshot do resultado e outbox são confirmados juntos.
 */
export class ProcessWagerTransaction {
  private readonly settler: WagerSettler;

  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly observability: Observability = NO_OBSERVABILITY,
  ) {
    this.settler = new WagerSettler(clock, ids, observability.metrics);
  }

  async execute(cmd: ProcessWagerCommand): Promise<ProcessWagerResult> {
    const { metrics, logger, context } = this.observability;
    const source = cmd.inbox ? "sqs" : "http";
    const started = performance.now();
    const elapsedSeconds = () => (performance.now() - started) / 1000;

    context.add({ walletId: cmd.walletId, providerId: cmd.providerId });

    try {
      const result = await this.run(cmd);

      context.add({ transactionId: result.transactionId });
      if (result.idempotentReplay) metrics.duplicateDetected(source);
      metrics.processingLatency(elapsedSeconds(), source, result.status);
      logger.info("wager_transaction_decided", {
        status: result.status,
        ...(result.failureCode ? { failureCode: result.failureCode } : {}),
        idempotentReplay: result.idempotentReplay,
        source,
      });
      return result;
    } catch (error) {
      const conflict = lockConflictKind(error);
      if (conflict) metrics.lockConflict(conflict);
      metrics.processingLatency(elapsedSeconds(), source, "error");
      throw error;
    }
  }

  /**
   * Encerra como FAILED uma transação cujo processamento falhou de forma PERMANENTE (o consumidor chama isto antes de
   * enviar a mensagem à DLQ). Roda numa transação SQL própria — a do processamento foi desfeita — e grava juntos a
   * inbox (se veio da fila), a transação FAILED e o evento WagerTransactionFailed, sem tocar saldo nem ledger.
   * Se a transação já existe (decidida por outro caminho nesse meio-tempo), devolve o resultado dela como replay.
   */
  async recordFailure(cmd: ProcessWagerCommand, code: FailureCode): Promise<ProcessWagerResult> {
    const { hash, candidate, ctx } = this.prepare(cmd);
    const result = await this.uow.run(async (repos) => {
      if (cmd.inbox && !(await repos.inbox.insertIfAbsent(this.receivedInbox(cmd.inbox, hash)))) {
        return this.replayFromInbox(repos, cmd.inbox, cmd.idempotencyKey, hash);
      }
      if (!(await repos.transactions.insertIfAbsent(candidate))) {
        return this.replay(repos, cmd.idempotencyKey, hash);
      }
      // Mesmo lock do caminho normal: mantém a ordem de `seq` dos eventos desta wallet igual à ordem de commit.
      const wallet = await repos.wallets.findByIdForUpdate(candidate.walletId);
      if (!wallet) await repos.wallets.lockAbsent(candidate.walletId);
      return this.settler.fail(repos, candidate, code, wallet?.balance, ctx);
    });

    this.observability.context.add({ transactionId: result.transactionId });
    this.observability.logger.error("wager_transaction_failed", {
      status: result.status,
      failureCode: result.failureCode ?? code,
      idempotentReplay: result.idempotentReplay,
    });
    return result;
  }

  /** Valida o comando e monta a transação candidata (ainda não persistida) e o hash do payload. */
  private prepare(cmd: ProcessWagerCommand): { hash: string; candidate: WagerTransaction; ctx: SettleContext } {
    const money = Money.from(cmd.money);
    const hash = payloadHash({
      providerId: cmd.providerId,
      externalTransactionId: cmd.externalTransactionId,
      playerId: cmd.playerId,
      walletId: cmd.walletId,
      roundId: cmd.roundId,
      gameId: cmd.gameId,
      kind: cmd.kind,
      money: money.toJSON(),
      referenceExternalTransactionId: cmd.referenceExternalTransactionId,
    });

    const candidate = WagerTransaction.create({
      id: this.ids.next(),
      providerId: cmd.providerId,
      externalTransactionId: cmd.externalTransactionId,
      idempotencyKey: cmd.idempotencyKey,
      payloadHash: hash,
      walletId: cmd.walletId,
      playerId: cmd.playerId,
      roundId: cmd.roundId,
      gameId: cmd.gameId,
      kind: cmd.kind,
      money,
      ...(cmd.referenceExternalTransactionId !== undefined
        ? { referenceExternalTransactionId: cmd.referenceExternalTransactionId }
        : {}),
      createdAt: this.clock.now(),
    });
    return { hash, candidate, ctx: { correlationId: cmd.correlationId, causationId: cmd.causationId } };
  }

  /** Nasce processada porque só é confirmada junto com o efeito: se a transação desfaz, a linha some. */
  private receivedInbox(inbox: { messageId: string; consumerName: string }, hash: string): InboxMessage {
    const now = this.clock.now();
    const received = InboxMessage.receive({
      messageId: inbox.messageId,
      consumerName: inbox.consumerName,
      payloadHash: hash,
      receivedAt: now,
    });
    received.markProcessed(now);
    return received;
  }

  private async run(cmd: ProcessWagerCommand): Promise<ProcessWagerResult> {
    const { metrics } = this.observability;
    const { hash, candidate, ctx } = this.prepare(cmd);

    return this.uow.run(async (repos) => {
      if (cmd.inbox && !(await repos.inbox.insertIfAbsent(this.receivedInbox(cmd.inbox, hash)))) {
        return this.replayFromInbox(repos, cmd.inbox, cmd.idempotencyKey, hash);
      }

      if (!(await repos.transactions.insertIfAbsent(candidate))) {
        return this.replay(repos, cmd.idempotencyKey, hash);
      }

      // Tempo esperando o lock da wallet: é o sinal de contenção de uma wallet quente.
      const lockStarted = performance.now();
      const wallet = await repos.wallets.findByIdForUpdate(candidate.walletId);
      metrics.lockWait((performance.now() - lockStarted) / 1000);

      const settlement = await this.settler.settle(repos, wallet, candidate, ctx);
      if (settlement.type === "settled") return settlement.result;

      // Referência ausente: persiste como PENDING_REFERENCE para o worker reprocessar com backoff.
      const now = this.clock.now();
      candidate.markPendingReference();
      await repos.transactions.save(candidate, {
        resultBalance: settlement.balance,
        attempts: 0,
        nextAttemptAt: new Date(now.getTime() + pendingReferenceDelayMs(0)),
      });
      await repos.outbox.add(
        OutboxMessage.enqueue(
          WagerTransactionPendingReference.from(candidate, {
            eventId: this.ids.next(),
            correlationId: ctx.correlationId,
            ...(ctx.causationId !== undefined ? { causationId: ctx.causationId } : {}),
            occurredAt: now,
          }),
        ),
      );
      metrics.transactionDecided(candidate.status, candidate.kind);

      return {
        transactionId: candidate.id,
        status: candidate.status,
        balance: settlement.balance.toJSON(),
        idempotentReplay: false,
      };
    });
  }

  /** Redelivery: a mensagem já foi processada. Mesmo payload → resultado original; outro payload → conflito. */
  private async replayFromInbox(
    repos: Repositories,
    inbox: { messageId: string; consumerName: string },
    idempotencyKey: string,
    hash: string,
  ): Promise<ProcessWagerResult> {
    const existing = await repos.inbox.find(inbox.consumerName, inbox.messageId);
    if (existing && existing.payloadHash !== hash) {
      throw new IdempotencyConflictError(`O messageId ${inbox.messageId} já foi usado com um payload diferente`);
    }
    return this.replay(repos, idempotencyKey, hash);
  }

  /** A transação já existe: mesmo payload → resultado original; payload diferente → conflito. */
  private async replay(repos: Repositories, idempotencyKey: string, hash: string): Promise<ProcessWagerResult> {
    const existing = await repos.transactions.findByIdempotencyKey(idempotencyKey);
    if (!existing) {
      throw new IdempotencyConflictError(
        "O par (providerId, externalTransactionId) já foi usado com outra idempotency key",
      );
    }
    if (!existing.transaction.matchesPayload(hash)) {
      throw new IdempotencyConflictError(`A idempotency key ${idempotencyKey} já foi usada com um payload diferente`);
    }
    return resultOf(existing);
  }
}

function resultOf(stored: StoredTransaction): ProcessWagerResult {
  const { transaction, resultBalance } = stored;
  return {
    transactionId: transaction.id,
    status: transaction.status,
    ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
    ...(resultBalance ? { balance: resultBalance.toJSON() } : {}),
    idempotentReplay: true,
  };
}
