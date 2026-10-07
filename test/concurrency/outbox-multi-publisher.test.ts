import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createClusterEnv, type ClusterEnv } from "../support/cluster";
import { drainQueue, waitFor } from "../support/sqs";
import { SqsProxy } from "../support/sqs-proxy";
import { brl, count, createWalletVia, submitVia, wagerBody } from "../support/wager-fixtures";

let env: ClusterEnv;
let proxy: SqsProxy;

beforeAll(async () => {
  env = await createClusterEnv();
  proxy = SqsProxy.start(); // só conta: nenhum SendMessage é retido
}, 60_000);
afterAll(async () => {
  proxy.stop();
  await env.close();
});

describe("vários publishers (processos) sobre a mesma outbox", () => {
  it("3 processos publicando ao mesmo tempo enviam cada evento exatamente uma vez", async () => {
    // Dados criados por uma instância sem workers, para a outbox começar cheia e sem ninguém publicando.
    const seed = await env.startOne({ id: "seed", consumer: false, outboxRelay: false, pendingReference: false });
    const wallets = await Promise.all(Array.from({ length: 20 }, () => createWalletVia(seed, "100.00")));
    for (const w of wallets) for (let i = 0; i < 5; i++) await submitVia(seed, wagerBody(w, { money: brl("1.00") }));

    const total = await count(env.pool, "outbox_messages");
    expect(total).toBe(20 * (1 + 5 * 2));
    expect(proxy.entries("SendMessageBatch")).toBe(0);

    // Os 3 publishers sobem juntos e disputam a mesma outbox (lotes pequenos para forçar a disputa).
    const relays = await Promise.all(
      Array.from({ length: 3 }, (_, i) =>
        env.startOne({ id: `relay-${i + 1}`, sqsEndpoint: proxy.url, consumer: false, pendingReference: false, outboxBatchSize: 10 }),
      ),
    );
    expect(relays.every((r) => r.alive)).toBe(true);

    await waitFor(async () => (await count(env.pool, "outbox_messages WHERE published_at IS NULL")) === 0, {
      timeoutMs: 60_000,
      what: "outbox publicada",
    });

    // Cada evento saiu uma única vez: a soma das mensagens enviadas pelos 3 processos (em lotes) é exatamente o total.
    expect(proxy.entries("SendMessageBatch")).toBe(total);

    const received = await drainQueue(env.queues.client, env.queues.urls.events, total, 30_000);
    const ids = received.map((m) => JSON.parse(m.body).eventId as string);
    expect(ids).toHaveLength(total);
    expect(new Set(ids).size).toBe(total);

    // Nenhum evento precisou de retry: não houve falha de publicação.
    expect(await count(env.pool, "outbox_messages WHERE attempts > 0")).toBe(0);
  }, 150_000);

  it("a ordem por wallet é preservada mesmo com 3 publishers: walletVersion crescente na fila", async () => {
    const seed = await env.startOne({ id: "seed-order", consumer: false, outboxRelay: false, pendingReference: false });
    const w = await createWalletVia(seed, "500.00");
    for (let i = 0; i < 8; i++) await submitVia(seed, wagerBody(w, { money: brl("1.00") }));
    const total = await count(env.pool, "outbox_messages WHERE aggregate_id = $1", [w.id]);

    await waitFor(async () => (await count(env.pool, "outbox_messages WHERE published_at IS NULL")) === 0, {
      timeoutMs: 60_000,
      what: "outbox publicada pelos relays já em execução",
    });

    const received = await drainQueue(env.queues.client, env.queues.urls.events, total, 30_000);
    const versions = received
      .map((m) => JSON.parse(m.body))
      .filter((e) => e.aggregateId === w.id && e.eventType === "WalletBalanceChanged")
      .map((e) => e.data.walletVersion as number);
    expect(versions).toEqual([...versions].sort((a, b) => a - b));
    expect(versions).toHaveLength(9);
  }, 120_000);
});
