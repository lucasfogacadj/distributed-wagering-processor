import { errorLogFields } from "../../application/describe-error";
import type { LoggerPort } from "../../application/ports";
import type { OutboxRelay } from "../../application/outbox-relay";
import type { PendingReferenceWorker } from "../../application/pending-reference-worker";
import { PollingLoop } from "../../application/polling-loop";
import type { SqsWagerConsumer } from "../../interfaces/messaging/sqs-wager-consumer";

export interface BackgroundWorkersOptions {
  logger: LoggerPort;
  /** Cada peça é opcional: dá para ligar só o que a instância deve fazer. */
  consumer?: SqsWagerConsumer | undefined;
  relay?: OutboxRelay | undefined;
  pendingReference?: PendingReferenceWorker | undefined;
}

/** Agrupa consumidor SQS, relay da outbox e worker de referências pendentes sob um único start/stop. */
export class BackgroundWorkers {
  private readonly loops: PollingLoop[] = [];

  constructor(private readonly options: BackgroundWorkersOptions) {
    const { relay, pendingReference, logger } = options;

    if (relay) {
      this.loops.push(
        new PollingLoop({
          name: "outbox-relay",
          work: async () => (await relay.tick()) > 0,
          idleDelayMs: 200,
          errorDelayMs: 2_000,
          onError: (error) => logger.error("outbox_relay_failed", errorLogFields(error)),
        }),
      );
    }

    if (pendingReference) {
      this.loops.push(
        new PollingLoop({
          name: "pending-reference-worker",
          work: async () => (await pendingReference.tick()) > 0,
          idleDelayMs: 1_000,
          errorDelayMs: 2_000,
          onError: (error) => logger.error("pending_reference_worker_failed", errorLogFields(error)),
        }),
      );
    }
  }

  get names(): string[] {
    return [...(this.options.consumer ? ["sqs-wager-consumer"] : []), ...this.loops.map((l) => l.name)];
  }

  start(): void {
    this.options.consumer?.start();
    for (const loop of this.loops) loop.start();
    this.options.logger.info("background_workers_started", { workers: this.names });
  }

  /** O consumidor para primeiro (drena o que está em andamento); os demais laços terminam a iteração atual. */
  async stop(): Promise<void> {
    await Promise.all([this.options.consumer?.stop(), ...this.loops.map((loop) => loop.stop())]);
    this.options.logger.info("background_workers_stopped", { workers: this.names });
  }
}
