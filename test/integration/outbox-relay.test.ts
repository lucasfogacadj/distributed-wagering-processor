import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { OutboxRelay } from "../../src/application/outbox-relay";
import type { EventPublisher, Repositories, UnitOfWork } from "../../src/application/ports";
import type { OutboxMessage } from "../../src/domain/outbox-message";
import { WagerTransactionKind } from "../../src/domain/wager-transaction";
import { SqsEventPublisher } from "../../src/infrastructure/messaging/sqs-event-publisher";
import { CapturingLogger } from "../support/http-harness";
import { RecordingMetrics } from "../support/recording-metrics";
import { createHarness, type Harness } from "../support/harness";
import { createTestQueues, drainQueue, type TestQueues } from "../support/sqs";

let h: Harness;
let q: TestQueues;
let sqsPublisher: SqsEventPublisher;

beforeAll(async () => {
  h = await createHarness();
  q = await createTestQueues();
  sqsPublisher = new SqsEventPublisher(q.client, q.urls.events);
});
afterAll(async () => {
  await q.cleanup();
  await h.close();
});

/**
 * Publisher que observa (e, se pedido, falha) o comportamento do relay. Segue o contrato da porta: publica em ordem e
 * devolve quantos do PREFIXO do lote foram publicados; se o primeiro já falha, lança.
 */
class SpyPublisher implements EventPublisher {
  readonly published: string[] = [];
  /** Tamanho de cada chamada de lote, na ordem em que ocorreram. */
  readonly batches: number[] = [];
  failFor = new Set<string>();

  constructor(private readonly inner: EventPublisher = sqsPublisher) {}

  async publishBatch(messages: OutboxMessage[]): Promise<number> {
    const failing = messages.findIndex((m) => this.failFor.has(m.id));
    const prefix = failing === -1 ? messages : messages.slice(0, failing);
    if (prefix.length === 0) throw new Error("SQS indisponível");

    await this.inner.publishBatch(prefix);
    this.batches.push(prefix.length);
    this.published.push(...prefix.map((m) => m.id));
    return prefix.length;
  }
}

/** UnitOfWork que conta quantas transações o relay abre. */
class CountingUow implements UnitOfWork {
  runs = 0;

  constructor(private readonly inner: UnitOfWork) {}

  run<T>(work: (repos: Repositories) => Promise<T>): Promise<T> {
    this.runs += 1;
    return this.inner.run(work);
  }
}

function relay(
  instanceId: string,
  publisher: EventPublisher,
  over: {
    batchSize?: number;
    runSize?: number;
    leaseMs?: number;
    uow?: UnitOfWork;
    maxAttempts?: number;
    logger?: CapturingLogger;
    metrics?: RecordingMetrics;
  } = {},
) {
  return new OutboxRelay({
    uow: over.uow ?? h.uow,
    publisher,
    clock: h.clock,
    logger: over.logger ?? new CapturingLogger(),
    instanceId,
    batchSize: over.batchSize ?? 50,
    runSize: over.runSize ?? 20,
    leaseMs: over.leaseMs ?? 30_000,
    ...(over.maxAttempts !== undefined ? { maxAttempts: over.maxAttempts } : {}),
    ...(over.metrics ? { metrics: over.metrics } : {}),
  });
}

async function newWallet(balance = "1000.00") {
  const playerId = randomUUID();
  const w = await h.createWallet.execute({ playerId, initialBalance: { amount: balance, currency: "BRL" }, correlationId: "c" });
  return { id: w.id, playerId };
}

async function bet(w: { id: string; playerId: string }, amount = "1.00") {
  const externalTransactionId = randomUUID();
  await h.process.execute({
    idempotencyKey: `provider-a:${externalTransactionId}`,
    providerId: "provider-a",
    externalTransactionId,
    playerId: w.playerId,
    walletId: w.id,
    roundId: "r1",
    gameId: "g1",
    kind: WagerTransactionKind.Bet,
    money: { amount, currency: "BRL" },
    correlationId: "c",
  });
}

const pending = async () =>
  (await h.pool.query<{ n: number }>("SELECT count(*)::int AS n FROM outbox_messages WHERE published_at IS NULL")).rows[0]!.n;

const eventIdsOf = (messages: { body: string }[]) => messages.map((m) => JSON.parse(m.body).eventId as string);

async function purgeOutbox(): Promise<void> {
  // Cada teste começa de uma outbox limpa para contar exatamente os eventos que ele gerou.
  await h.pool.query("UPDATE outbox_messages SET published_at = now(), quarantined_at = NULL WHERE published_at IS NULL");
  await drainQueue(q.client, q.urls.events, 10_000, 1_500);
}

describe("OutboxRelay", () => {
  it("publica os eventos pendentes, na ordem da wallet, e os marca como publicados", async () => {
    await purgeOutbox();
    const w = await newWallet("100.00");
    await bet(w, "25.00");

    const spy = new SpyPublisher();
    await relay("a", spy).drain();

    expect(await pending()).toBe(0);
    const received = await drainQueue(q.client, q.urls.events, 3);
    expect(received.map((m) => JSON.parse(m.body).eventType)).toEqual([
      "WalletBalanceChanged", // abertura
      "WagerTransactionProcessed",
      "WalletBalanceChanged",
    ]);
    expect(received.every((m) => JSON.parse(m.body).aggregateId === w.id)).toBe(true);
    expect(JSON.parse(received[2]!.body).data.balanceAfter).toEqual({ amount: "75.00", currency: "BRL" });
  });

  it("três relays concorrentes publicam cada evento exatamente uma vez, sem perder nenhum", async () => {
    await purgeOutbox();
    const wallets = await Promise.all(Array.from({ length: 15 }, () => newWallet("1000.00")));
    for (let round = 0; round < 4; round++) await Promise.all(wallets.map((w) => bet(w)));
    const expectedEvents = 15 * (1 + 4 * 2);
    expect(await pending()).toBe(expectedEvents);

    const spy = new SpyPublisher();
    await Promise.all([relay("a", spy, { batchSize: 10 }).drain(), relay("b", spy, { batchSize: 10 }).drain(), relay("c", spy, { batchSize: 10 }).drain()]);

    expect(await pending()).toBe(0);
    expect(spy.published).toHaveLength(expectedEvents);
    expect(new Set(spy.published).size).toBe(expectedEvents);

    const received = await drainQueue(q.client, q.urls.events, expectedEvents, 20_000);
    expect(new Set(eventIdsOf(received)).size).toBe(expectedEvents);
  });

  it("mantém a ordem por wallet mesmo com publishers concorrentes", async () => {
    await purgeOutbox();
    const w = await newWallet("1000.00");
    for (let i = 0; i < 6; i++) await bet(w, `${i + 1}.00`);

    const spy = new SpyPublisher();
    await Promise.all([relay("a", spy).drain(), relay("b", spy).drain()]);

    const received = await drainQueue(q.client, q.urls.events, 13);
    const balanceEvents = received.map((m) => JSON.parse(m.body)).filter((e) => e.eventType === "WalletBalanceChanged");
    expect(balanceEvents.map((e) => e.data.walletVersion)).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it("publisher morto ANTES de publicar: o lease expira e outra instância assume", async () => {
    await purgeOutbox();
    const w = await newWallet("100.00");
    await bet(w);
    const total = await pending();

    // A "morre" logo depois de reivindicar os eventos (lease curto).
    const claimed = await h.uow.run((r) => r.outbox.claimBatch({ limit: 50, perAggregate: 20, leaseMs: 400, owner: "morto" }));
    expect(claimed.length).toBeGreaterThan(0);

    const spy = new SpyPublisher();
    expect(await relay("b", spy).tick()).toBe(0); // lease ativo: B não toca
    expect(spy.published).toHaveLength(0);

    await new Promise((r) => setTimeout(r, 600));
    await relay("b", spy).drain();
    expect(spy.published).toHaveLength(total);
    expect(await pending()).toBe(0);
  });

  it("publisher morto DEPOIS de publicar e ANTES de marcar: a republicação é segura (mesmo eventId, mesmo corpo)", async () => {
    await purgeOutbox();
    const w = await newWallet("100.00");
    const claimed = await h.uow.run((r) => r.outbox.claimBatch({ limit: 50, perAggregate: 20, leaseMs: 400, owner: "morto" }));
    expect(claimed).toHaveLength(1);
    await sqsPublisher.publishBatch([claimed[0]!]); // publicou… e morreu antes de marcar published_at

    await new Promise((r) => setTimeout(r, 600));
    await relay("b", new SpyPublisher()).drain();

    const received = await drainQueue(q.client, q.urls.events, 2, 2_500);
    const copies = received.filter((m) => JSON.parse(m.body).eventId === claimed[0]!.id);
    expect(copies.length).toBeGreaterThanOrEqual(1); // a fila FIFO pode descartar a duplicata; ambos os casos são seguros
    expect(new Set(copies.map((m) => m.body)).size).toBe(1); // se chegar duas vezes, o corpo é idêntico
    expect(JSON.parse(copies[0]!.body).aggregateId).toBe(w.id);
    expect(await pending()).toBe(0);
  }, 20_000);

  it("falha ao publicar agenda retry com backoff e libera o lease", async () => {
    await purgeOutbox();
    await newWallet("100.00");
    const [first] = (await h.pool.query<{ id: string }>("SELECT id FROM outbox_messages WHERE published_at IS NULL")).rows;

    const spy = new SpyPublisher();
    spy.failFor.add(first!.id);
    await relay("a", spy).tick();

    const row = (
      await h.pool.query(
        "SELECT attempts, next_attempt_at > now() AS in_future, locked_by, published_at FROM outbox_messages WHERE id = $1",
        [first!.id],
      )
    ).rows[0];
    expect(row).toEqual({ attempts: 1, in_future: true, locked_by: null, published_at: null });

    expect(await relay("a", spy).tick()).toBe(0); // ainda em backoff

    spy.failFor.clear();
    await new Promise((r) => setTimeout(r, 1_200));
    await relay("a", spy).drain();
    expect(await pending()).toBe(0);
  });

  it("um evento em retry bloqueia os seguintes da MESMA wallet (a ordem não é quebrada)", async () => {
    await purgeOutbox();
    const w = await newWallet("100.00");
    await bet(w);
    const ids = (
      await h.pool.query<{ id: string }>("SELECT id FROM outbox_messages WHERE published_at IS NULL ORDER BY seq")
    ).rows.map((r) => r.id);

    const spy = new SpyPublisher();
    spy.failFor.add(ids[0]!);
    await relay("a", spy).drain();
    expect(spy.published).toHaveLength(0); // o primeiro falhou; os demais esperam por ele

    spy.failFor.clear();
    await new Promise((r) => setTimeout(r, 1_200));
    await relay("a", spy).drain();
    expect(spy.published).toEqual(ids); // publicados na ordem original
  });

  it("evento venenoso: esgotadas as tentativas vai para a quarentena e a wallet volta a andar; o requeue o publica", async () => {
    await purgeOutbox();
    const w = await newWallet("100.00");
    await bet(w);
    const ids = (
      await h.pool.query<{ id: string }>("SELECT id FROM outbox_messages WHERE published_at IS NULL ORDER BY seq")
    ).rows.map((r) => r.id);
    expect(ids).toHaveLength(3);

    const spy = new SpyPublisher();
    spy.failFor.add(ids[0]!);
    const logger = new CapturingLogger();
    const metrics = new RecordingMetrics();
    const poisoned = () => relay("a", spy, { maxAttempts: 2, logger, metrics });

    await poisoned().drain(); // 1ª falha: backoff, a cauda espera
    expect(spy.published).toHaveLength(0);
    await new Promise((r) => setTimeout(r, 1_200));
    await poisoned().drain(); // 2ª falha: quarentena; a cauda sai na mesma drenagem, em ordem
    expect(spy.published).toEqual(ids.slice(1));

    const row = (
      await h.pool.query(
        "SELECT attempts, quarantined_at IS NOT NULL AS quarantined, published_at, next_attempt_at, locked_by, last_error FROM outbox_messages WHERE id = $1",
        [ids[0]],
      )
    ).rows[0];
    expect(row).toEqual({ attempts: 2, quarantined: true, published_at: null, next_attempt_at: null, locked_by: null, last_error: "SQS indisponível" });
    expect(await h.uow.run((r) => r.outbox.countQuarantined())).toBe(1);
    expect(await h.uow.run((r) => r.outbox.oldestPendingAt())).toBeUndefined(); // o lag não conta o quarentenado
    expect(metrics.callsOf("outboxQuarantined")).toHaveLength(1);
    expect(logger.records.some((l) => l.level === "error" && l.message === "outbox_event_quarantined" && l.fields["eventId"] === ids[0])).toBe(true);

    // Nenhum relay o pega de novo sozinho.
    expect(await relay("b", spy).drain()).toBe(0);

    // Ação manual: corrigido o problema, o requeue devolve o evento, que é publicado (depois dos que já saíram).
    spy.failFor.clear();
    expect(await h.uow.run((r) => r.outbox.requeueQuarantined([ids[0]!]))).toBe(1);
    await relay("a", spy).drain();
    expect(spy.published).toEqual([...ids.slice(1), ids[0]!]);
    expect(await pending()).toBe(0);
    expect(await h.uow.run((r) => r.outbox.countQuarantined())).toBe(0);
  });

  it("maxAttempts inválido é rejeitado", () => {
    expect(() => relay("a", new SpyPublisher(), { maxAttempts: 0 })).toThrow("maxAttempts");
  });

  it("oldestPendingAt reflete o evento pendente mais antigo", async () => {
    await purgeOutbox();
    expect(await h.uow.run((r) => r.outbox.oldestPendingAt())).toBeUndefined();
    await newWallet("10.00");
    const oldest = await h.uow.run((r) => r.outbox.oldestPendingAt());
    expect(oldest).toBeInstanceOf(Date);
    expect(Date.now() - oldest!.getTime()).toBeLessThan(10_000);
  });
});

/** Quantidade de eventos de uma wallet com `bets` apostas: 1 (abertura) + 2 por aposta. */
const eventsFor = (bets: number) => 1 + 2 * bets;

async function pendingIdsOf(walletId: string): Promise<string[]> {
  return (
    await h.pool.query<{ id: string }>("SELECT id FROM outbox_messages WHERE aggregate_id = $1 AND published_at IS NULL ORDER BY seq", [walletId])
  ).rows.map((r) => r.id);
}

describe("OutboxRelay — publicação em lotes ordenados", () => {
  it("uma wallet sai em poucas chamadas de lote, em ordem, com apenas 2 transações no banco", async () => {
    await purgeOutbox();
    const w = await newWallet("1000.00");
    for (let i = 0; i < 14; i++) await bet(w);
    expect(await pending()).toBe(eventsFor(14)); // 29 eventos

    const spy = new SpyPublisher();
    const counting = new CountingUow(h.uow);
    const claimed = await relay("a", spy, { batchSize: 100, runSize: 30, uow: counting }).tick();

    expect(claimed).toBe(29);
    expect(spy.batches).toEqual([10, 10, 9]); // 3 chamadas ao broker, e não 29
    expect(counting.runs).toBe(2); // 1 transação para reivindicar + 1 para marcar, qualquer que seja o nº de eventos
    expect(await pending()).toBe(0);

    const versions = (await drainQueue(q.client, q.urls.events, 29))
      .map((m) => JSON.parse(m.body))
      .filter((e) => e.eventType === "WalletBalanceChanged")
      .map((e) => e.data.walletVersion as number);
    expect(versions).toEqual(Array.from({ length: 15 }, (_, i) => i + 1)); // a ordem por wallet foi preservada
  });

  it("runSize limita quantos eventos consecutivos de uma wallet saem por rodada", async () => {
    await purgeOutbox();
    const w = await newWallet("1000.00");
    for (let i = 0; i < 14; i++) await bet(w);

    const spy = new SpyPublisher();
    const r = relay("a", spy, { batchSize: 100, runSize: 10 });
    expect([await r.tick(), await r.tick(), await r.tick(), await r.tick()]).toEqual([10, 10, 9, 0]);
    expect(spy.published).toHaveLength(29);
    expect(await pending()).toBe(0);
  });

  it("o backlog de uma wallet quente não impede que as outras sejam atendidas na mesma rodada", async () => {
    await purgeOutbox();
    const hot = await newWallet("1000000.00");
    for (let i = 0; i < 40; i++) await bet(hot); // 81 eventos, TODOS mais antigos que os das outras
    const others = await Promise.all(Array.from({ length: 20 }, () => newWallet("10.00")));
    expect(others).toHaveLength(20);

    const spy = new SpyPublisher();
    await relay("a", spy, { batchSize: 100, runSize: 20 }).tick();

    const othersPending = (
      await h.pool.query<{ n: number }>("SELECT count(*)::int AS n FROM outbox_messages WHERE published_at IS NULL AND aggregate_id <> $1", [hot.id])
    ).rows[0]!.n;
    expect(othersPending).toBe(0); // as 20 wallets pequenas foram todas atendidas
    expect((await pendingIdsOf(hot.id)).length).toBe(eventsFor(40) - 20); // a quente avançou só até o runSize
  });

  it("falha parcial no meio do lote: o prefixo é marcado, o resto espera na ordem e nada se perde", async () => {
    await purgeOutbox();
    const w = await newWallet("100.00");
    for (let i = 0; i < 3; i++) await bet(w); // 7 eventos
    const ids = await pendingIdsOf(w.id);
    expect(ids).toHaveLength(7);

    const spy = new SpyPublisher();
    spy.failFor.add(ids[3]!);
    await relay("a", spy).tick();

    expect(spy.published).toEqual(ids.slice(0, 3)); // só o prefixo saiu
    expect(await pendingIdsOf(w.id)).toEqual(ids.slice(3));
    const rows = (
      await h.pool.query(
        "SELECT id, attempts, next_attempt_at > now() AS backoff, locked_by FROM outbox_messages WHERE published_at IS NULL AND aggregate_id = $1 ORDER BY seq",
        [w.id],
      )
    ).rows;
    expect(rows[0]).toMatchObject({ id: ids[3], attempts: 1, backoff: true, locked_by: null }); // a cabeça tem backoff
    for (const tail of rows.slice(1)) expect(tail).toMatchObject({ attempts: 0, backoff: null, locked_by: null }); // o resto só destravou

    spy.failFor.clear();
    await new Promise((r) => setTimeout(r, 1_200));
    await relay("a", spy).drain();
    expect(spy.published).toEqual(ids); // tudo publicado, em ordem, uma única vez
  });

  it("3 relays sobre UMA wallet quente: cada evento sai uma vez, em ordem", async () => {
    await purgeOutbox();
    const w = await newWallet("1000000.00");
    for (let i = 0; i < 60; i++) await bet(w);
    const total = eventsFor(60);

    const spy = new SpyPublisher();
    await Promise.all(["a", "b", "c"].map((id) => relay(id, spy, { batchSize: 100, runSize: 20 }).drain()));

    expect(await pending()).toBe(0);
    expect(spy.published).toHaveLength(total);
    expect(new Set(spy.published).size).toBe(total); // nenhum evento publicado em dobro

    const versions = (await drainQueue(q.client, q.urls.events, total, 20_000))
      .map((m) => JSON.parse(m.body))
      .filter((e) => e.eventType === "WalletBalanceChanged")
      .map((e) => e.data.walletVersion as number);
    expect(versions).toEqual(Array.from({ length: 61 }, (_, i) => i + 1));
  }, 60_000);
});

describe("OutboxRepository.claimBatch — a reivindicação em sequência", () => {
  const claim = (owner: string, over: { limit?: number; perAggregate?: number; leaseMs?: number } = {}) =>
    h.uow.run((r) => r.outbox.claimBatch({ limit: over.limit ?? 1_000, perAggregate: over.perAggregate ?? 50, leaseMs: over.leaseMs ?? 30_000, owner }));

  it("reivindicações concorrentes nunca repartem uma wallet: cada uma sai inteira, em sequência, com um único dono", async () => {
    await purgeOutbox();
    const wallets = await Promise.all(Array.from({ length: 30 }, () => newWallet("100.00")));
    for (let round = 0; round < 3; round++) await Promise.all(wallets.map((w) => bet(w)));
    const total = 30 * eventsFor(3);
    expect(await pending()).toBe(total);

    const owners = ["o1", "o2", "o3", "o4", "o5", "o6"];
    const claims = await Promise.all(owners.map((owner) => claim(owner)));
    const all = claims.flat();

    expect(all).toHaveLength(total); // tudo foi reivindicado…
    expect(new Set(all.map((m) => m.id)).size).toBe(total); // …exatamente uma vez

    const ownerOf = new Map<string, string>();
    claims.forEach((batch, i) => {
      for (const m of batch) {
        const previous = ownerOf.get(m.aggregateId);
        expect(previous === undefined || previous === owners[i]).toBe(true); // uma wallet nunca é dividida entre donos
        ownerOf.set(m.aggregateId, owners[i]!);
      }
    });

    for (const w of wallets) {
      const claimedIds = all.filter((m) => m.aggregateId === w.id).map((m) => m.id);
      const inOrder = (await h.pool.query<{ id: string }>("SELECT id FROM outbox_messages WHERE aggregate_id = $1 ORDER BY seq", [w.id])).rows.map((r) => r.id);
      expect(claimedIds).toEqual(inOrder); // a sequência completa, na ordem do seq
    }
  });

  it("não reivindica a cauda de uma wallet cuja cabeça está com lease de outro", async () => {
    await purgeOutbox();
    const w = await newWallet("100.00");
    for (let i = 0; i < 3; i++) await bet(w);

    const first = await claim("dono-1", { perAggregate: 2 }); // pega só a cabeça e o 2º
    expect(first).toHaveLength(2);
    expect(await claim("dono-2")).toEqual([]); // a cabeça está leased: o restante da wallet fica bloqueado
  });

  it("a cabeça em backoff bloqueia a sequência inteira da wallet, mas não as outras", async () => {
    await purgeOutbox();
    const blocked = await newWallet("100.00");
    await bet(blocked);
    const free = await newWallet("100.00");
    const [head] = await pendingIdsOf(blocked.id);
    await h.pool.query("UPDATE outbox_messages SET next_attempt_at = now() + interval '1 hour' WHERE id = $1", [head]);

    const claimed = await claim("a");
    expect(claimed.filter((m) => m.aggregateId === blocked.id)).toEqual([]);
    expect(claimed.filter((m) => m.aggregateId === free.id)).toHaveLength(1);
  });

  it("respeita o limite total de eventos", async () => {
    await purgeOutbox();
    const wallets = await Promise.all(Array.from({ length: 10 }, () => newWallet("100.00")));
    for (const w of wallets) await bet(w);
    expect(await claim("a", { limit: 12 })).toHaveLength(12);
  });

  it("markPublished em lote só afeta as linhas cujo lease é do dono", async () => {
    await purgeOutbox();
    const w = await newWallet("100.00");
    for (let i = 0; i < 3; i++) await bet(w);
    const claimed = await claim("A");
    const ids = claimed.map((m) => m.id);

    expect(await h.uow.run((r) => r.outbox.markPublished(ids, "B", new Date()))).toBe(0); // outro dono: nada
    expect(await pending()).toBe(ids.length);
    expect(await h.uow.run((r) => r.outbox.markPublished(ids, "A", new Date()))).toBe(ids.length);
    expect(await h.uow.run((r) => r.outbox.markPublished(ids, "A", new Date()))).toBe(0); // idempotente
    expect(await pending()).toBe(0);
  });
});
