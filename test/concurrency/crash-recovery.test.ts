import { describe, expect, it } from "bun:test";
import { createClusterEnv, type ClusterEnv } from "../support/cluster";
import { assertLedgerMatchesBalances } from "../support/harness";
import { approximateMessages, drainQueue, waitFor } from "../support/sqs";
import { SqsProxy, type StallMode } from "../support/sqs-proxy";
import {
  balanceOf,
  brl,
  count,
  createWalletVia,
  debitsOf,
  sendMessages,
  submitVia,
  wagerBody,
  wagerMessage,
  type TestWallet,
} from "../support/wager-fixtures";

/** Cada cenário tem banco, filas e processos próprios: nenhum herda estado do anterior. */
async function withEnv(fn: (env: ClusterEnv) => Promise<void>): Promise<void> {
  const env = await createClusterEnv();
  try {
    await fn(env);
  } finally {
    await env.close();
  }
}

const NO_WORKERS = { consumer: false, outboxRelay: false, pendingReference: false } as const;

describe("worker morto depois do commit e antes do ack", () => {
  it("SIGKILL com o DeleteMessage retido: outra instância recebe de novo e a inbox impede o segundo efeito", async () => {
    await withEnv(async (env) => {
      const proxy = SqsProxy.start();
      proxy.stall("DeleteMessageBatch", "before");
      try {
        const seed = await env.startOne({ id: "seed", ...NO_WORKERS });
        const w = await createWalletVia(seed, "100.00");

        // Só a vítima consome, e o ack dela fica preso no proxy.
        const victim = await env.startOne({ id: "victim", sqsEndpoint: proxy.url, outboxRelay: false, pendingReference: false });
        const message = wagerMessage(wagerBody(w));
        await sendMessages(env.queues, [message]);
        await proxy.waitForStall("DeleteMessageBatch");

        // O efeito já foi confirmado no banco; o ack é que não aconteceu.
        expect(await debitsOf(env.pool, w.id)).toBe(1);
        expect(await balanceOf(env.pool, w.id)).toBe("75.00");
        expect(await count(env.pool, "inbox_messages WHERE message_id = $1", [message.messageId])).toBe(1);
        expect(await approximateMessages(env.queues.client, env.queues.urls.requests)).toBe(1); // continua na fila

        await victim.kill(); // SIGKILL: sem shutdown, sem chance de limpeza
        proxy.clearStalls();

        const rescuer = await env.startOne({ id: "rescuer", outboxRelay: false, pendingReference: false });
        await waitFor(async () => (await approximateMessages(env.queues.client, env.queues.urls.requests)) === 0, {
          timeoutMs: 30_000,
          what: "mensagem redelivered e apagada",
        });

        expect(await debitsOf(env.pool, w.id)).toBe(1); // nenhum débito duplicado
        expect(await balanceOf(env.pool, w.id)).toBe("75.00");
        expect(await count(env.pool, "inbox_messages WHERE message_id = $1", [message.messageId])).toBe(1);
        expect(
          rescuer.records().some((r) => r["message"] === "sqs_message_processed" && r["messageId"] === message.messageId && r["idempotentReplay"] === true),
        ).toBe(true); // a redelivery foi reconhecida como duplicata
        expect(await approximateMessages(env.queues.client, env.queues.urls.requestsDlq)).toBe(0);
        await assertLedgerMatchesBalances(env.pool);
      } finally {
        proxy.stop();
      }
    });
  }, 120_000);
});

/** Cria wallets e apostas por uma instância sem workers, deixando todos os eventos pendentes na outbox. */
async function seedPendingEvents(env: ClusterEnv, wallets = 4, betsPerWallet = 2): Promise<{ w: TestWallet[]; total: number }> {
  const seed = await env.startOne({ id: "seed", ...NO_WORKERS });
  const created = await Promise.all(Array.from({ length: wallets }, () => createWalletVia(seed, "100.00")));
  for (const w of created) for (let i = 0; i < betsPerWallet; i++) await submitVia(seed, wagerBody(w, { money: brl("1.00") }));
  const total = await count(env.pool, "outbox_messages");
  expect(total).toBe(wallets * (1 + betsPerWallet * 2));
  expect(await count(env.pool, "outbox_messages WHERE published_at IS NOT NULL")).toBe(0);
  return { w: created, total };
}

const eventIds = (received: { body: string }[]) => received.map((m) => JSON.parse(m.body).eventId as string);

describe("publisher da outbox morto no meio do trabalho", () => {
  async function scenario(mode: StallMode): Promise<{ total: number; sent: number; received: { body: string }[] }> {
    let result!: { total: number; sent: number; received: { body: string }[] };
    await withEnv(async (env) => {
      const proxy = SqsProxy.start();
      proxy.stall("SendMessageBatch", mode); // o relay publica em lotes
      try {
        const { total } = await seedPendingEvents(env);

        const doomed = await env.startOne({
          id: "doomed",
          sqsEndpoint: proxy.url,
          consumer: false,
          pendingReference: false,
          outboxLeaseMs: 1_500,
          outboxBatchSize: 5,
        });
        await proxy.waitForStall("SendMessageBatch");

        // O publisher reivindicou eventos (lease) mas morreu antes de marcar qualquer um como publicado.
        expect(await count(env.pool, "outbox_messages WHERE locked_by = 'doomed'")).toBeGreaterThan(0);
        expect(await count(env.pool, "outbox_messages WHERE published_at IS NOT NULL")).toBe(0);
        const sent = proxy.entries("SendMessageBatch"); // mensagens que chegaram ao SQS (não chamadas)
        await doomed.kill();
        proxy.clearStalls();

        // Outra instância assume depois que o lease expira.
        await env.startOne({ id: "successor", consumer: false, pendingReference: false, outboxLeaseMs: 1_500 });
        await waitFor(async () => (await count(env.pool, "outbox_messages WHERE published_at IS NULL")) === 0, {
          timeoutMs: 45_000,
          what: "outbox totalmente publicada pelo sucessor",
        });

        const received = await drainQueue(env.queues.client, env.queues.urls.events, total, 30_000);
        result = { total, sent, received };
      } finally {
        proxy.stop();
      }
    });
    return result;
  }

  it("morto ANTES de publicar: nada se perde e nada é duplicado — o sucessor publica tudo", async () => {
    const { total, sent, received } = await scenario("before");

    expect(sent).toBe(0); // a vítima não chegou a publicar nada
    expect(received).toHaveLength(total);
    expect(new Set(eventIds(received)).size).toBe(total);
  }, 150_000);

  it("morto DEPOIS de publicar e ANTES de marcar: o evento é republicado com o mesmo eventId e o mesmo corpo", async () => {
    const { total, sent, received } = await scenario("after");

    expect(sent).toBeGreaterThan(0); // a vítima publicou de verdade antes de morrer
    // Nenhum evento se perdeu; se a fila entregar a republicação, o corpo é idêntico (consumidor deduplica por eventId).
    expect(new Set(eventIds(received)).size).toBe(total);
    const bodiesById = new Map<string, Set<string>>();
    for (const m of received) {
      const id = JSON.parse(m.body).eventId as string;
      bodiesById.set(id, (bodiesById.get(id) ?? new Set()).add(m.body));
    }
    for (const bodies of bodiesById.values()) expect(bodies.size).toBe(1);
  }, 150_000);
});
