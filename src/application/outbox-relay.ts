import type { OutboxMessage } from "../domain/outbox-message";
import type { Clock, EventPublisher, LoggerPort, MetricsPort, UnitOfWork } from "./ports";

export interface OutboxRelayOptions {
  uow: UnitOfWork;
  publisher: EventPublisher;
  clock: Clock;
  logger: LoggerPort;
  /** Identifica o dono do lease; precisa ser único por instância/processo. */
  instanceId: string;
  /** Máximo de eventos reivindicados por rodada (somando todas as wallets). */
  batchSize?: number;
  /** Máximo de eventos CONSECUTIVOS de uma mesma wallet por rodada. */
  runSize?: number;
  leaseMs?: number;
  /**
   * Tentativas de publicação antes de o evento ir para a quarentena (padrão 10: com o backoff de 1 s a 5 min, ~13 min
   * de falhas seguidas). Sem isto um evento venenoso bloquearia a sequência da wallet para sempre.
   */
  maxAttempts?: number;
  metrics?: MetricsPort;
}

const DEFAULT_BATCH_SIZE = 200;
const DEFAULT_RUN_SIZE = 20;
const DEFAULT_LEASE_MS = 30_000;
const DEFAULT_MAX_ATTEMPTS = 10;
const MAX_DRAIN_ITERATIONS = 10_000;
/** Limite de entradas de um SendMessageBatch. */
const BROKER_BATCH_SIZE = 10;

/** O que sobrou de uma sequência de uma wallet depois de tentar publicá-la. */
interface RunOutcome {
  published: OutboxMessage[];
  /** Presente se a sequência não foi publicada por inteiro: a cabeça do que restou e o resto atrás dela. */
  failure?: { head: OutboxMessage; rest: OutboxMessage[]; error: unknown };
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size));
  return chunks;
}

/**
 * Publica os eventos da outbox no broker. Funciona com vários publishers concorrentes:
 *  - a unidade de trabalho é a SEQUÊNCIA de eventos de uma wallet: quem a reivindica (com `SKIP LOCKED` + lease)
 *    leva os próximos eventos dela de uma vez e os publica em lotes ORDENADOS; dois publishers nunca dividem uma
 *    wallet, então a ordem por wallet se mantém sem que cada evento precise de uma ida ao banco;
 *  - uma rodada custa duas transações no banco (reivindicar e marcar), qualquer que seja o nº de eventos;
 *  - se um publisher morre, o lease expira e outro assume (nada se perde);
 *  - se morre DEPOIS de publicar e ANTES de marcar, os eventos são republicados: a garantia é at-least-once e o
 *    consumidor deduplica pelo eventId (idêntico nas duas publicações).
 */
export class OutboxRelay {
  private readonly batchSize: number;
  private readonly runSize: number;
  private readonly leaseMs: number;
  private readonly maxAttempts: number;

  constructor(private readonly options: OutboxRelayOptions) {
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.runSize = options.runSize ?? DEFAULT_RUN_SIZE;
    this.leaseMs = options.leaseMs ?? DEFAULT_LEASE_MS;
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    if (!Number.isInteger(this.maxAttempts) || this.maxAttempts < 1) throw new Error("maxAttempts deve ser um inteiro >= 1");
  }

  /** Uma rodada: reivindica sequências, publica e marca. Devolve quantos eventos foram reivindicados. */
  async tick(): Promise<number> {
    const { uow, clock, logger, instanceId, metrics } = this.options;

    const claimed = await uow.run((repos) =>
      repos.outbox.claimBatch({ limit: this.batchSize, perAggregate: this.runSize, leaseMs: this.leaseMs, owner: instanceId }),
    );
    if (claimed.length === 0) return 0;

    // Wallets diferentes em paralelo; dentro de uma wallet, os lotes seguem em série para manter a ordem.
    const runs = new Map<string, OutboxMessage[]>();
    for (const message of claimed) runs.set(message.aggregateId, [...(runs.get(message.aggregateId) ?? []), message]);
    const outcomes = await Promise.all([...runs.values()].map((run) => this.publishRun(run)));

    const published = outcomes.flatMap((o) => o.published);
    const failures = outcomes.flatMap((o) => (o.failure ? [o.failure] : []));

    // Marcação consolidada: UMA transação para o que foi publicado e para o que precisa de retry/destravar.
    await uow.run(async (repos) => {
      const marked = await repos.outbox.markPublished(
        published.map((m) => m.id),
        instanceId,
        clock.now(),
      );
      if (marked < published.length) {
        // O lease expirou durante a publicação e outra instância assumiu: haverá republicação (segura).
        logger.warn("outbox_lease_lost", { published: published.length, stillOurs: marked });
      }
      for (const { head, rest } of failures) {
        if (head.isQuarantined()) await repos.outbox.quarantine(head, instanceId);
        else await repos.outbox.releaseForRetry(head, instanceId);
        // Com a cabeça em backoff a cauda continua atrás dela; com a cabeça em quarentena a cauda segue na próxima rodada.
        await repos.outbox.unlock(
          rest.map((m) => m.id),
          instanceId,
        );
      }
    });

    metrics?.outboxPublished(published.length);
    for (const { head } of failures) {
      const fields = { eventId: head.id, eventType: head.eventType, aggregateId: head.aggregateId, attempts: head.attempts, error: head.lastError };
      if (head.isQuarantined()) {
        // Ação manual necessária: o evento saiu da sequência da wallet (quebra de ordem deliberada) e espera um requeue.
        metrics?.outboxQuarantined();
        logger.error("outbox_event_quarantined", fields);
      } else {
        metrics?.retry("outbox_relay");
        logger.warn("outbox_publish_failed", fields);
      }
    }

    return claimed.length;
  }

  /** Publica a sequência de uma wallet em lotes sucessivos; para no primeiro lote que não sai por inteiro. */
  private async publishRun(run: OutboxMessage[]): Promise<RunOutcome> {
    const published: OutboxMessage[] = [];

    for (const batch of chunk(run, BROKER_BATCH_SIZE)) {
      let accepted: number;
      try {
        accepted = await this.options.publisher.publishBatch(batch);
      } catch (error) {
        return this.failed(run, published, error);
      }
      published.push(...batch.slice(0, accepted));
      if (accepted < batch.length) {
        return this.failed(run, published, new Error(`lote publicado parcialmente (${accepted}/${batch.length})`));
      }
    }
    return { published };
  }

  /**
   * A cabeça do que NÃO saiu ganha backoff; os eventos atrás dela só são destravados (a ordem os mantém na fila).
   * Se a cabeça esgotou as tentativas, vai para a quarentena: deixa de bloquear a wallet, e a cauda segue.
   */
  private failed(run: OutboxMessage[], published: OutboxMessage[], error: unknown): RunOutcome {
    const [head, ...rest] = run.slice(published.length);
    if (!head) return { published };
    const now = this.options.clock.now();
    const reason = error instanceof Error ? error.message : String(error);
    head.scheduleRetry(now, reason);
    if (head.hasExhaustedAttempts(this.maxAttempts)) head.quarantine(now, reason);
    return { published, failure: { head, rest, error } };
  }

  /** Roda até não haver mais nada elegível (eventos em backoff ou com lease alheio não contam). */
  async drain(): Promise<number> {
    let total = 0;
    for (let i = 0; i < MAX_DRAIN_ITERATIONS; i++) {
      const claimed = await this.tick();
      if (claimed === 0) break;
      total += claimed;
    }
    return total;
  }
}
