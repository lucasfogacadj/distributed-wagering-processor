import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { SendMessageCommand } from "@aws-sdk/client-sqs";
import { OutboxRelay } from "../../src/application/outbox-relay";
import type { EventPublisher, Observability } from "../../src/application/ports";
import type { ProcessWagerCommand } from "../../src/application/process-wager-transaction";
import { AsyncLocalLogContext } from "../../src/infrastructure/observability/log-context";
import { SqsWagerConsumer, type WagerConsumerOptions } from "../../src/interfaces/messaging/sqs-wager-consumer";
import { createHarness, type Harness } from "../support/harness";
import { CapturingLogger } from "../support/http-harness";
import { RecordingMetrics } from "../support/recording-metrics";
import { approximateMessages, createTestQueues, drainQueue, waitFor, type TestQueues } from "../support/sqs";

const brl = (amount: string) => ({ amount, currency: "BRL" });

// O harness e os componentes sob teste compartilham o mesmo contexto/logger/métricas: assim o que o caso de
// uso acrescenta ao contexto (walletId, providerId, transactionId) aparece nos logs do consumidor.
const context = new AsyncLocalLogContext();
const logger = new CapturingLogger(context);
const metrics = new RecordingMetrics();

let h: Harness;
let q: TestQueues;
let consumers: SqsWagerConsumer[] = [];

beforeAll(async () => {
  h = await createHarness({ observability: { metrics, logger, context } });
});
afterAll(async () => {
  await h.close();
});
beforeEach(async () => {
  q = await createTestQueues({ visibilityTimeoutSeconds: 30, maxReceiveCount: 10 });
  logger.records.length = 0;
  metrics.calls.length = 0;
});
afterEach(async () => {
  for (const c of consumers) await c.stop();
  consumers = [];
  await q.cleanup();
});

async function newWallet() {
  const playerId = randomUUID();
  const w = await h.createWallet.execute({ playerId, initialBalance: brl("100.00"), correlationId: "c" });
  return { id: w.id, playerId };
}

function message(w: { id: string; playerId: string }) {
  const ext = randomUUID();
  return {
    messageId: `msg-${randomUUID()}`,
    type: "WagerTransactionRequested",
    occurredAt: new Date().toISOString(),
    data: {
      providerId: "provider-a",
      externalTransactionId: ext,
      idempotencyKey: `provider-a:${ext}`,
      playerId: w.playerId,
      walletId: w.id,
      roundId: "r1",
      gameId: "g1",
      kind: "BET",
      money: brl("25.00"),
    },
  };
}

const send = (body: unknown, groupId = "grupo") =>
  q.client.send(
    new SendMessageCommand({
      QueueUrl: q.urls.requests,
      MessageBody: typeof body === "string" ? body : JSON.stringify(body),
      MessageGroupId: groupId,
      MessageDeduplicationId: randomUUID(),
    }),
  );

function start(over: Partial<WagerConsumerOptions> = {}): SqsWagerConsumer {
  const consumer = new SqsWagerConsumer({
    client: q.client,
    queueUrl: q.urls.requests,
    dlqUrl: q.urls.requestsDlq,
    process: h.process,
    logger,
    metrics,
    context,
    waitTimeSeconds: 1,
    retryBaseDelaySeconds: 1,
    maxReceives: 3,
    ...over,
  });
  consumers.push(consumer);
  consumer.start();
  return consumer;
}

const queueDrained = () => waitFor(async () => (await approximateMessages(q.client, q.urls.requests)) === 0, { timeoutMs: 20_000, what: "fila vazia" });

describe("observabilidade do consumidor", () => {
  it("cada mensagem processada é logada com messageId, correlationId, transactionId, walletId e providerId", async () => {
    const w = await newWallet();
    const m = message(w);
    await send(m, w.id);
    start();
    await waitFor(async () => logger.records.some((l) => l.message === "sqs_message_processed"), { what: "log do consumidor" });

    const record = logger.records.find((l) => l.message === "sqs_message_processed")!;
    expect(record.fields).toMatchObject({
      messageId: m.messageId,
      correlationId: m.messageId,
      walletId: w.id,
      providerId: "provider-a",
      transactionId: expect.any(String),
      status: "PROCESSED",
    });
    expect(JSON.stringify(logger.records)).not.toContain('"amount"');
    await queueDrained();
  });

  it("falhas transitórias contam retries", async () => {
    const w = await newWallet();
    let calls = 0;
    const flaky = {
      execute: async (cmd: ProcessWagerCommand) => {
        calls += 1;
        if (calls <= 2) throw Object.assign(new Error("deadlock detected"), { code: "40P01" });
        return h.process.execute(cmd);
      },
    };
    await send(message(w), w.id);
    start({ process: flaky, maxReceives: 5 });

    await waitFor(async () => calls >= 3, { timeoutMs: 20_000, what: "três tentativas" });
    await queueDrained();
    expect(metrics.callsOf("retry")).toEqual([["sqs_consumer"], ["sqs_consumer"]]);
    expect(metrics.callsOf("deadLettered")).toEqual([]);
  }, 30_000);

  it("mensagem inválida conta DLQ com o motivo", async () => {
    await send("{ não é json");
    start();
    await drainQueue(q.client, q.urls.requestsDlq, 1, 15_000);
    expect(metrics.callsOf("deadLettered")).toEqual([["invalid_message"]]);
  });

  it("tentativas esgotadas contam DLQ por retries_exhausted", async () => {
    const w = await newWallet();
    const alwaysDown = {
      execute: async () => {
        throw Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" });
      },
    };
    await send(message(w), w.id);
    start({ process: alwaysDown, maxReceives: 2 });

    await drainQueue(q.client, q.urls.requestsDlq, 1, 25_000);
    expect(metrics.callsOf("deadLettered")).toEqual([["retries_exhausted"]]);
    expect(metrics.callsOf("retry")).toEqual([["sqs_consumer"]]);
  }, 40_000);
});

describe("observabilidade do relay da outbox", () => {
  it("falha ao publicar conta retry e loga com o eventId, sem o payload", async () => {
    // Só o evento desta wallet deve estar pendente: o resto da outbox é de outros testes.
    await h.pool.query("UPDATE outbox_messages SET published_at = now() WHERE published_at IS NULL");
    await newWallet();
    logger.records.length = 0;
    metrics.calls.length = 0;
    const failing: EventPublisher = { publishBatch: async () => Promise.reject(new Error("SQS indisponível")) };
    const observability: Pick<Observability, "metrics"> = { metrics };
    const relay = new OutboxRelay({ uow: h.uow, publisher: failing, clock: h.clock, logger, instanceId: "obs", ...observability });

    await relay.tick();

    expect(metrics.callsOf("retry")).toEqual([["outbox_relay"]]);
    const record = logger.records.find((l) => l.message === "outbox_publish_failed")!;
    expect(record.fields).toMatchObject({ eventId: expect.any(String), eventType: "WalletBalanceChanged", attempts: 1 });
    expect(JSON.stringify(record)).not.toContain('"payload"');
  });
});
