import { FailureCode } from "../domain/failure-code";
import { PENDING_REFERENCE_MAX_ATTEMPTS, pendingReferenceDelayMs } from "../domain/pending-reference-schedule";
import { WagerTransactionStatus } from "../domain/wager-transaction";
import { NO_OBSERVABILITY } from "./observability";
import type { Clock, IdGenerator, Observability, UnitOfWork } from "./ports";
import { WagerSettler, type ProcessWagerResult } from "./wager-settler";

/**
 * Reprocessa uma transação PENDING_REFERENCE (chamado pelo worker agendado).
 * Idempotente e seguro com várias instâncias: o estado é conferido de novo depois dos locks.
 * Devolve undefined quando não há nada a fazer (inexistente ou já decidida por outra instância).
 */
export class ReprocessPendingReference {
  private readonly settler: WagerSettler;

  constructor(
    private readonly uow: UnitOfWork,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly observability: Observability = NO_OBSERVABILITY,
  ) {
    this.settler = new WagerSettler(clock, ids, observability.metrics);
  }

  async execute(transactionId: string, correlationId: string = this.ids.next()): Promise<ProcessWagerResult | undefined> {
    const { metrics, context } = this.observability;
    return this.uow.run(async (repos) => {
      const peek = await repos.transactions.findById(transactionId);
      if (!peek || peek.transaction.status !== WagerTransactionStatus.PendingReference) return undefined;

      context.add({
        transactionId,
        walletId: peek.transaction.walletId,
        providerId: peek.transaction.providerId,
      });

      // Ordem de locks: wallet → linha da transação (a mesma do caminho de entrada; evita deadlock).
      const lockStarted = performance.now();
      const wallet = await repos.wallets.findByIdForUpdate(peek.transaction.walletId);
      metrics.lockWait((performance.now() - lockStarted) / 1000);
      const locked = await repos.transactions.findByIdForUpdate(transactionId);
      if (!locked || locked.transaction.status !== WagerTransactionStatus.PendingReference) return undefined;

      const tx = locked.transaction;
      const settlement = await this.settler.settle(repos, wallet, tx, { correlationId });
      if (settlement.type === "settled") return settlement.result;

      const attempts = locked.attempts + 1;
      if (attempts >= PENDING_REFERENCE_MAX_ATTEMPTS) {
        // A referência que existe mas nunca foi decidida é outra situação operacional que a que nunca chegou.
        const code = settlement.referenceFound ? FailureCode.ReferenceUnresolved : FailureCode.ReferenceNotFound;
        return this.settler.reject(repos, tx, code, settlement.balance, { correlationId });
      }

      metrics.retry("pending_reference");
      await repos.transactions.save(tx, {
        resultBalance: settlement.balance,
        attempts,
        nextAttemptAt: new Date(this.clock.now().getTime() + pendingReferenceDelayMs(attempts)),
      });
      return {
        transactionId: tx.id,
        status: tx.status,
        balance: settlement.balance.toJSON(),
        idempotentReplay: false,
      };
    });
  }

  /**
   * Encerra como FAILED uma PENDING_REFERENCE cujo reprocessamento falhou de forma permanente repetidas vezes (quem
   * decide é o worker). Mesma ordem de locks do reprocessamento; não faz nada se outra instância já a decidiu.
   */
  async fail(transactionId: string, code: FailureCode, correlationId: string = this.ids.next()): Promise<ProcessWagerResult | undefined> {
    return this.uow.run(async (repos) => {
      const peek = await repos.transactions.findById(transactionId);
      if (!peek || peek.transaction.status !== WagerTransactionStatus.PendingReference) return undefined;

      const wallet = await repos.wallets.findByIdForUpdate(peek.transaction.walletId);
      if (!wallet) await repos.wallets.lockAbsent(peek.transaction.walletId);
      const locked = await repos.transactions.findByIdForUpdate(transactionId);
      if (!locked || locked.transaction.status !== WagerTransactionStatus.PendingReference) return undefined;

      return this.settler.fail(repos, locked.transaction, code, wallet?.balance ?? locked.resultBalance, { correlationId });
    });
  }
}
