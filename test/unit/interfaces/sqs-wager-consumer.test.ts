import { describe, expect, it } from "bun:test";
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  ReceiveMessageCommand,
  type Message,
} from "@aws-sdk/client-sqs";
import type { ProcessWagerCommand, ProcessWagerResult } from "../../../src/application/process-wager-transaction";
import type { LogContextPort, LoggerPort } from "../../../src/application/ports";
import { WagerTransactionStatus } from "../../../src/domain/wager-transaction";
import { SqsWagerConsumer, type SqsSender } from "../../../src/interfaces/messaging/sqs-wager-consumer";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitUntil(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const started = Date.now();
  while (!condition()) {
    if (Date.now() - started > timeoutMs) throw new Error("condição não atingida a tempo");
    await sleep(5);
  }
}

class RecordingLogger implements LoggerPort {
  readonly records: { level: string; message: string; fields: Record<string, unknown> | undefined }[] = [];
  info(message: string, fields?: Record<string, unknown>) {
    this.records.push({ level: "info", message, fields });
  }
  warn(message: string, fields?: Record<string, unknown>) {
    this.records.push({ level: "warn", message, fields });
  }
  error(message: string, fields?: Record<string, unknown>) {
    this.records.push({ level: "error", message, fields });
  }
  has(level: string, message: string): boolean {
    return this.records.some((r) => r.level === level && r.message === message);
  }
}

function body(messageId: string, walletId: string): string {
  return JSON.stringify({
    messageId,
    type: "WagerTransactionRequested",
    occurredAt: "2026-07-29T15:00:00.000Z",
    data: {
      providerId: "provider-a",
      externalTransactionId: `ext-${messageId}`,
      idempotencyKey: `provider-a:ext-${messageId}`,
      playerId: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
      walletId,
      roundId: "round-1",
      gameId: "fortune-chimp",
      kind: "BET",
      money: { amount: "25.00", currency: "BRL" },
    },
  });
}

function message(id: string, walletId: string): Message {
  return {
    MessageId: `sqs-${id}`,
    ReceiptHandle: `rh-${id}`,
    Body: body(id, walletId),
    Attributes: { MessageGroupId: walletId, ApproximateReceiveCount: "1" },
  };
}

/** Cliente SQS falso: entrega o lote uma única vez e registra deletes e mudanças de visibilidade. */
class FakeSqs {
  readonly deleted: string[] = [];
  readonly released: string[] = [];
  private delivered = false;

  constructor(private readonly batch: Message[]) {}

  readonly client: SqsSender = {
    send: (async (command: unknown) => {
      if (command instanceof ReceiveMessageCommand) {
        if (this.delivered) {
          await sleep(10);
          return { Messages: [] };
        }
        this.delivered = true;
        return { Messages: this.batch };
      }
      if (command instanceof DeleteMessageBatchCommand) {
        for (const entry of command.input.Entries ?? []) this.deleted.push(entry.ReceiptHandle!);
        return { Successful: [], Failed: [] };
      }
      if (command instanceof ChangeMessageVisibilityCommand) {
        if (command.input.VisibilityTimeout === 0) this.released.push(command.input.ReceiptHandle!);
        return {};
      }
      return {};
    }) as SqsSender["send"],
  };
}

const processed: ProcessWagerResult = {
  transactionId: "t1",
  status: WagerTransactionStatus.Processed,
  idempotentReplay: false,
};

const WALLET_A = "0192f291-27dd-7d3f-8071-5f8685deef37";
const WALLET_B = "0192f291-27dd-7d3f-8071-5f8685deef38";

function consumer(sqs: FakeSqs, logger: LoggerPort, over: Partial<ConstructorParameters<typeof SqsWagerConsumer>[0]> = {}) {
  return new SqsWagerConsumer({
    client: sqs.client,
    queueUrl: "q",
    dlqUrl: "dlq",
    process: { execute: async () => processed },
    logger,
    waitTimeSeconds: 0,
    ...over,
  });
}

/** Escopo de log cuja PRIMEIRA execução lança de forma síncrona: falha inesperada, fora do tratamento normal. */
function throwingOnFirstRun(): LogContextPort {
  let calls = 0;
  return {
    run: (_fields, fn) => {
      calls += 1;
      if (calls === 1) throw new Error("falha inesperada no escopo de log");
      return fn();
    },
    add: () => {},
    snapshot: () => ({}),
  };
}

describe("SqsWagerConsumer (unidade)", () => {
  it("um grupo que falha de forma inesperada não descarta os acks dos outros grupos do lote", async () => {
    const a = message("a", WALLET_A);
    const b = message("b", WALLET_B);
    const sqs = new FakeSqs([a, b]);
    const logger = new RecordingLogger();

    const c = consumer(sqs, logger, { context: throwingOnFirstRun() });
    c.start();
    await waitUntil(() => sqs.deleted.length > 0);
    await c.stop();

    expect(sqs.deleted).toEqual([b.ReceiptHandle!]); // o ack do grupo saudável saiu; o do grupo com falha não
    expect(logger.has("error", "sqs_group_failed")).toBe(true);
  });

  it("falha inesperada numa mensagem devolve o resto do MESMO grupo (para não furar a ordem)", async () => {
    const first = message("1", WALLET_A);
    const second = message("2", WALLET_A);
    const third = message("3", WALLET_A);
    const sqs = new FakeSqs([first, second, third]);
    const logger = new RecordingLogger();

    const c = consumer(sqs, logger, { context: throwingOnFirstRun() });
    c.start();
    await waitUntil(() => sqs.released.length >= 2);
    await c.stop();

    expect([...sqs.released].sort()).toEqual([second.ReceiptHandle!, third.ReceiptHandle!].sort());
    expect(sqs.deleted).toEqual([]);
  });

  it("estouro do drain: o stop() ainda espera o handler em andamento (até stopGraceMs) antes de devolver o controle", async () => {
    const sqs = new FakeSqs([message("slow", WALLET_A)]);
    const logger = new RecordingLogger();
    let finished = false;
    let started = false;
    const c = consumer(sqs, logger, {
      drainTimeoutMs: 30,
      stopGraceMs: 3_000,
      process: {
        execute: async (_cmd: ProcessWagerCommand) => {
          started = true;
          await sleep(300);
          finished = true;
          return processed;
        },
      },
    });
    c.start();
    await waitUntil(() => started);
    await c.stop();

    expect(finished).toBe(true); // sem o segundo teto, o stop() voltaria aos ~30ms com o handler ainda vivo
    expect(logger.has("warn", "sqs_consumer_drain_timeout")).toBe(true);
    expect(logger.has("error", "sqs_consumer_handlers_still_running")).toBe(false);
  });

  it("handler que passa do segundo teto fica registrado (sqs_consumer_handlers_still_running)", async () => {
    const sqs = new FakeSqs([message("stuck", WALLET_A)]);
    const logger = new RecordingLogger();
    let started = false;
    let release!: () => void;
    const hold = new Promise<void>((resolve) => (release = resolve));
    const c = consumer(sqs, logger, {
      drainTimeoutMs: 20,
      stopGraceMs: 40,
      process: {
        execute: async () => {
          started = true;
          await hold;
          return processed;
        },
      },
    });
    c.start();
    await waitUntil(() => started);
    await c.stop();

    expect(logger.has("error", "sqs_consumer_handlers_still_running")).toBe(true);
    release(); // não deixa o handler pendurado depois do teste
    await sleep(20);
  });
});
