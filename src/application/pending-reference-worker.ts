import { FailureCode } from "../domain/failure-code";
import { WagerTransactionStatus } from "../domain/wager-transaction";
import { errorLogFields } from "./describe-error";
import { classifyError } from "./error-classification";
import type { Clock, LoggerPort, UnitOfWork } from "./ports";
import type { ProcessWagerResult } from "./wager-settler";

export interface PendingReferenceWorkerOptions {
  uow: UnitOfWork;
  reprocess: {
    execute(transactionId: string): Promise<ProcessWagerResult | undefined>;
    fail(transactionId: string, code: FailureCode): Promise<ProcessWagerResult | undefined>;
  };
  clock: Clock;
  logger: LoggerPort;
  batchSize?: number;
  /** Falhas NÃO transitórias no reprocessamento de uma transação antes de ela virar FAILED (padrão 5). */
  maxProcessingFailures?: number;
}

const DEFAULT_BATCH_SIZE = 20;
const DEFAULT_MAX_PROCESSING_FAILURES = 5;
/** Atraso até a nova tentativa de uma transação cujo reprocessamento FALHOU (não confundir com o backoff da referência). */
const FAILURE_RETRY_DELAY_MS = 1_000;

/**
 * Worker agendado das transações PENDING_REFERENCE. Lista as vencidas (o backoff exponencial está em
 * `next_attempt_at`) e delega cada uma ao `ReprocessPendingReference`, que confere o estado de novo sob
 * lock — por isso várias instâncias podem rodar este worker ao mesmo tempo sem duplicar efeitos.
 */
export class PendingReferenceWorker {
  private readonly batchSize: number;
  private readonly maxProcessingFailures: number;

  constructor(private readonly options: PendingReferenceWorkerOptions) {
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.maxProcessingFailures = options.maxProcessingFailures ?? DEFAULT_MAX_PROCESSING_FAILURES;
  }

  /** Devolve quantas transações vencidas foram reprocessadas sem erro. Falhas isoladas não param o lote. */
  async tick(): Promise<number> {
    const { uow, reprocess, clock, logger } = this.options;
    const due = await uow.run((repos) => repos.transactions.listDuePendingReferences(clock.now(), this.batchSize));

    const results = await Promise.allSettled(due.map((id) => reprocess.execute(id)));
    let succeeded = 0;
    const failed: { transactionId: string; error: unknown }[] = [];
    results.forEach((result, index) => {
      if (result.status === "fulfilled") {
        succeeded += 1;
        return;
      }
      const transactionId = due[index]!;
      failed.push({ transactionId, error: result.reason });
      logger.error("pending_reference_reprocess_failed", { transactionId, ...errorLogFields(result.reason) });
    });
    await Promise.all(failed.map(({ transactionId, error }) => this.afterFailure(transactionId, error)));
    return succeeded;
  }

  /**
   * A transação da tentativa que falhou foi desfeita, então `next_attempt_at` continuaria vencido e a linha ficaria
   * no topo de `listDuePendingReferences` para sempre: com `batchSize` linhas assim, nenhuma outra pendência andaria
   * (starvation). Aqui o próximo reprocessamento é adiado, numa transação própria, por um atraso CURTO e fixo
   * (`FAILURE_RETRY_DELAY_MS`): basta para a linha deixar o topo (a fila é ordenada por `next_attempt_at`, então as
   * pendências vencidas há mais tempo passam na frente) sem punir uma falha transitória com o backoff da referência,
   * que no nível mais alto chega a minutos. `attempts` NÃO muda: uma falha de infraestrutura não pode gastar a espera
   * pela referência.
   *
   * Falha transitória (lock timeout, conexão) só adia. Falha NÃO transitória (bug, dado que o banco recusa) também
   * conta em `processing_failures`; esgotado `maxProcessingFailures`, a transação vira FAILED/INFRA_PERMANENT (terminal
   * e auditável) em vez de ser tentada a cada segundo para sempre. A contagem fica no banco: vale entre instâncias.
   */
  private async afterFailure(transactionId: string, error: unknown): Promise<void> {
    const { uow, clock, logger, reprocess } = this.options;
    const nextAttemptAt = new Date(clock.now().getTime() + FAILURE_RETRY_DELAY_MS);
    try {
      if (classifyError(error) === "transient") {
        await uow.run(async (repos) => {
          const locked = await repos.transactions.findByIdForUpdate(transactionId);
          // Outra instância pode ter decidido a transação nesse meio-tempo: aí não há o que adiar.
          if (!locked || locked.transaction.status !== WagerTransactionStatus.PendingReference) return;
          await repos.transactions.save(locked.transaction, { resultBalance: locked.resultBalance, nextAttemptAt });
        });
        return;
      }

      const failures = await uow.run((repos) => repos.transactions.recordProcessingFailure(transactionId, nextAttemptAt));
      if (failures === undefined || failures < this.maxProcessingFailures) return;

      const result = await reprocess.fail(transactionId, FailureCode.InfraPermanent);
      if (result) {
        logger.error("pending_reference_failed_permanently", { transactionId, processingFailures: failures, ...errorLogFields(error) });
      }
    } catch (deferError) {
      logger.error("pending_reference_defer_failed", { transactionId, ...errorLogFields(deferError) });
    }
  }
}
