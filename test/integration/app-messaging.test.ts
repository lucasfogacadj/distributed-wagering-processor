import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { DeleteQueueCommand, SendMessageCommand } from "@aws-sdk/client-sqs";
import { createHttpHarness, type HttpHarness } from "../support/http-harness";
import { createTestQueues, drainQueue, waitFor, type TestQueues } from "../support/sqs";

const brl = (amount: string) => ({ amount, currency: "BRL" });

let q: TestQueues;
let h: HttpHarness;

beforeAll(async () => {
  // A aplicação cria as filas com maxReceiveCount = receiveHardLimit (9 no harness) + 1; um CreateQueue com
  // atributos diferentes de uma fila existente é recusado pelo SQS, então as filas de teste usam o mesmo valor.
  q = await createTestQueues({ visibilityTimeoutSeconds: 30, maxReceiveCount: 10 });
  h = await createHttpHarness({ messaging: { queues: q.names } });
});
afterAll(async () => {
  await h.close(); // o shutdown precisa terminar sem ficar preso nos workers
  await q.cleanup();
});

async function newWallet(balance = "100.00") {
  const playerId = randomUUID();
  const r = await h.post("/wallets", { playerId, initialBalance: brl(balance) });
  return { id: r.body.id as string, playerId };
}

function request(w: { id: string; playerId: string }, data: Record<string, unknown> = {}) {
  const ext = (data["externalTransactionId"] as string | undefined) ?? randomUUID();
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
      roundId: "round-1",
      gameId: "fortune-chimp",
      kind: "BET",
      money: brl("25.00"),
      ...data,
    },
  };
}

const publish = (w: { id: string }, body: unknown) =>
  q.client.send(
    new SendMessageCommand({
      QueueUrl: q.urls.requests,
      MessageBody: JSON.stringify(body),
      MessageGroupId: w.id,
      MessageDeduplicationId: randomUUID(),
    }),
  );

const statusOf = async (ext: string) => (await h.get(`/providers/provider-a/wagering/transactions/${ext}`)).body?.status as string | undefined;

describe("aplicação com mensageria ligada", () => {
  it("fluxo completo: SQS → consumidor → banco → outbox → relay → fila de eventos", async () => {
    const w = await newWallet("100.00");
    const m = request(w);
    await publish(w, m);

    await waitFor(async () => (await statusOf(m.data.externalTransactionId as string)) === "PROCESSED", { what: "transação processada via SQS" });
    expect((await h.get(`/wallets/${w.id}`)).body.balance).toEqual(brl("75.00"));

    // Abertura + WagerTransactionProcessed + WalletBalanceChanged, publicados pelo relay.
    const events = (await drainQueue(q.client, q.urls.events, 3, 15_000)).map((e) => JSON.parse(e.body)).filter((e) => e.aggregateId === w.id);
    expect(events.map((e) => e.eventType)).toEqual(["WalletBalanceChanged", "WagerTransactionProcessed", "WalletBalanceChanged"]);
    expect(events[2].data.balanceAfter).toEqual(brl("75.00"));
    expect(new Set(events.map((e) => e.eventId)).size).toBe(3);
  }, 40_000);

  it("REFUND que chega antes da BET é resolvido pelo worker de referências pendentes", async () => {
    const w = await newWallet("100.00");
    const bet = request(w);
    const refund = request(w, { kind: "REFUND", referenceExternalTransactionId: bet.data.externalTransactionId });

    await publish(w, refund); // fora de ordem
    await waitFor(async () => (await statusOf(refund.data.externalTransactionId as string)) === "PENDING_REFERENCE", { what: "REFUND pendente" });

    await publish(w, bet);
    await waitFor(async () => (await statusOf(refund.data.externalTransactionId as string)) === "PROCESSED", { timeoutMs: 20_000, what: "REFUND resolvido" });

    expect(await statusOf(bet.data.externalTransactionId as string)).toBe("PROCESSED");
    expect((await h.get(`/wallets/${w.id}`)).body.balance).toEqual(brl("100.00"));
    expect((await h.post(`/wallets/${w.id}/reconciliation`)).body.consistent).toBe(true);
  }, 40_000);

  it("a API HTTP e a fila compartilham a idempotência: o mesmo envio pelos dois canais debita uma vez", async () => {
    const w = await newWallet("100.00");
    const m = request(w);
    await publish(w, m);
    await waitFor(async () => (await statusOf(m.data.externalTransactionId as string)) === "PROCESSED", { what: "processada via SQS" });

    const viaHttp = await h.post("/wagering/transactions", m.data, { "Idempotency-Key": m.data.idempotencyKey as string });
    expect(viaHttp.status).toBe(200);
    expect(viaHttp.body.idempotentReplay).toBe(true);
    expect((await h.get(`/wallets/${w.id}`)).body.balance).toEqual(brl("75.00"));
  }, 30_000);

  it("readiness verifica o SQS e cai quando a fila some", async () => {
    const ready = await h.get("/health/ready");
    expect(ready.status).toBe(200);
    expect(ready.body.checks).toEqual({ postgres: "up", sqs: "up" });

    await q.client.send(new DeleteQueueCommand({ QueueUrl: q.urls.requests }));
    await waitFor(async () => (await h.get("/health/ready")).status === 503, { timeoutMs: 15_000, what: "readiness 503" });
    const down = await h.get("/health/ready");
    expect(down.body).toEqual({ status: "unavailable", checks: { postgres: "up", sqs: "down" } });
    expect((await h.get("/health/live")).status).toBe(200);
  }, 30_000);
});
