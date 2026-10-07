import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { DeleteMessageBatchCommand, GetQueueAttributesCommand, SendMessageCommand, type SQSClient } from "@aws-sdk/client-sqs";
import type { ProcessWagerCommand } from "../../src/application/process-wager-transaction";
import { FailureCode } from "../../src/domain/failure-code";
import { WagerTransactionStatus as S } from "../../src/domain/wager-transaction";
import { SqsWagerConsumer, type SqsSender, type WagerConsumerOptions } from "../../src/interfaces/messaging/sqs-wager-consumer";
import { assertLedgerMatchesBalances, createHarness, type Harness } from "../support/harness";
import { CapturingLogger } from "../support/http-harness";
import { approximateMessages, createTestQueues, drainQueue, waitFor, type TestQueues } from "../support/sqs";

const brl = (amount: string) => ({ amount, currency: "BRL" });
const CONSUMER = "wager-transactions-consumer";

let h: Harness;
let q: TestQueues;
let active: SqsWagerConsumer[] = [];

beforeAll(async () => {
  h = await createHarness();
});
afterAll(async () => {
  await h.close();
});
beforeEach(async () => {
  q = await createTestQueues({ visibilityTimeoutSeconds: 30, maxReceiveCount: 10 });
});
afterEach(async () => {
  for (const c of active) await c.stop();
  active = [];
  await q.cleanup();
});

interface W {
  id: string;
  playerId: string;
}

async function newWallet(balance = "100.00"): Promise<W> {
  const playerId = randomUUID();
  const w = await h.createWallet.execute({ playerId, initialBalance: brl(balance), correlationId: "c" });
  return { id: w.id, playerId };
}

function message(w: W, data: Record<string, unknown> = {}, top: Record<string, unknown> = {}) {
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
      roundId: "round-1",
      gameId: "fortune-chimp",
      kind: "BET",
      money: brl("25.00"),
      ...data,
    },
    ...top,
  };
}

async function send(body: unknown, options: { groupId?: string; dedupId?: string; queue?: TestQueues } = {}) {
  const queue = options.queue ?? q;
  await queue.client.send(
    new SendMessageCommand({
      QueueUrl: queue.urls.requests,
      MessageBody: typeof body === "string" ? body : JSON.stringify(body),
      MessageGroupId: options.groupId ?? "grupo",
      MessageDeduplicationId: options.dedupId ?? randomUUID(),
    }),
  );
}

/** O contador de tentativas de produção: inbox_attempts, numa transação própria. */
const dbAttempts = () => ({
  record: (consumerName: string, messageId: string, error: string) =>
    h.uow.run((repos) => repos.inbox.recordFailedAttempt(consumerName, messageId, error, new Date())),
});

function startConsumer(over: Partial<WagerConsumerOptions> = {}, queue: TestQueues = q): SqsWagerConsumer {
  const consumer = new SqsWagerConsumer({
    client: queue.client,
    queueUrl: queue.urls.requests,
    dlqUrl: queue.urls.requestsDlq,
    process: h.process,
    attempts: dbAttempts(),
    logger: new CapturingLogger(),
    consumerName: CONSUMER,
    waitTimeSeconds: 1,
    retryBaseDelaySeconds: 1,
    maxReceives: 3,
    ...over,
  });
  active.push(consumer);
  consumer.start();
  return consumer;
}

const txOf = async (ext: string) =>
  (
    await h.pool.query<{ status: string; failure_code: string | null }>(
      "SELECT status, failure_code FROM wager_transactions WHERE provider_id = 'provider-a' AND external_transaction_id = $1",
      [ext],
    )
  ).rows[0];

const balanceOf = async (id: string) =>
  (await h.pool.query<{ b: string }>("SELECT balance::text AS b FROM wallets WHERE id = $1", [id])).rows[0]!.b;

const debits = async (id: string) =>
  (await h.pool.query<{ n: number }>("SELECT count(*)::int AS n FROM ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'", [id])).rows[0]!.n;

const queueDrained = () => waitFor(async () => (await approximateMessages(q.client, q.urls.requests)) === 0, { timeoutMs: 15_000, what: "fila de entrada vazia" });

describe("SqsWagerConsumer", () => {
  it("processa a mensagem com o mesmo use case, registra a inbox e só então faz ack", async () => {
    const w = await newWallet();
    const m = message(w);
    await send(m, { groupId: w.id });
    startConsumer();

    await waitFor(async () => (await txOf(m.data.externalTransactionId as string))?.status === S.Processed, { what: "transação processada" });
    expect(await balanceOf(w.id)).toBe("75.00");
    const inbox = await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1 AND consumer_name = $2", [m.messageId, CONSUMER]);
    expect(inbox.rows[0].n).toBe(1);
    await queueDrained();
    await assertLedgerMatchesBalances(h.pool);
  });

  it("redelivery: a mesma mensagem entregue duas vezes debita uma só vez", async () => {
    const w = await newWallet();
    const m = message(w);
    await send(m, { groupId: w.id, dedupId: "entrega-1" });
    await send(m, { groupId: w.id, dedupId: "entrega-2" }); // o broker entrega duas vezes
    startConsumer();

    await queueDrained();
    expect(await debits(w.id)).toBe(1);
    expect(await balanceOf(w.id)).toBe("75.00");
    const inbox = await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1", [m.messageId]);
    expect(inbox.rows[0].n).toBe(1);
  });

  it("rejeição de negócio é terminal: ack, sem retry e sem DLQ", async () => {
    const w = await newWallet("10.00");
    const m = message(w, { money: brl("80.00") });
    await send(m, { groupId: w.id });
    startConsumer();

    await waitFor(async () => (await txOf(m.data.externalTransactionId as string))?.status === S.Rejected, { what: "transação rejeitada" });
    expect((await txOf(m.data.externalTransactionId as string))?.failure_code).toBe("INSUFFICIENT_FUNDS");
    await queueDrained();
    expect(await approximateMessages(q.client, q.urls.requestsDlq)).toBe(0);
    expect(await balanceOf(w.id)).toBe("10.00");
  });

  it("messageId reutilizado com payload diferente é conflito de negócio: ack, sem DLQ, sem segundo efeito", async () => {
    const w = await newWallet();
    const original = message(w);
    const conflicting = { ...message(w, { money: brl("26.00") }), messageId: original.messageId };
    await send(original, { groupId: w.id });
    startConsumer();
    await waitFor(async () => (await debits(w.id)) === 1, { what: "primeiro débito" });

    await send(conflicting, { groupId: w.id });
    await queueDrained();
    expect(await debits(w.id)).toBe(1);
    expect(await approximateMessages(q.client, q.urls.requestsDlq)).toBe(0);
  });

  it.each([
    ["JSON malformado", "{ não é json", "invalid_message"],
    ["campo obrigatório ausente", (() => { const m: any = message({ id: randomUUID(), playerId: randomUUID() }); delete m.data.roundId; return m; })(), "invalid_message"],
    ["kind OPENING (interno)", message({ id: randomUUID(), playerId: randomUUID() }, { kind: "OPENING" }), "invalid_message"],
    ["valor monetário inválido", message({ id: randomUUID(), playerId: randomUUID() }, { money: brl("10") }), "invalid_payload"],
  ])("erro permanente vai direto para a DLQ: %s", async (_name, body, reason) => {
    await send(body);
    startConsumer();

    const [dead] = await drainQueue(q.client, q.urls.requestsDlq, 1, 15_000);
    expect(dead).toBeDefined();
    expect(dead!.attributes["reason"]).toBe(reason);
    expect(dead!.attributes["originalMessageId"]).toBeTruthy();
    await queueDrained();
  });

  it("falha transitória: retry com backoff e depois processa — um único efeito", async () => {
    const w = await newWallet();
    const m = message(w);
    let calls = 0;
    const flaky = {
      execute: async (cmd: ProcessWagerCommand) => {
        calls += 1;
        if (calls <= 2) throw Object.assign(new Error("deadlock detected"), { code: "40P01" });
        return h.process.execute(cmd);
      },
    };
    await send(m, { groupId: w.id });
    startConsumer({ process: flaky, maxReceives: 5 });

    await waitFor(async () => (await txOf(m.data.externalTransactionId as string))?.status === S.Processed, { timeoutMs: 20_000, what: "processada após retries" });
    expect(calls).toBe(3);
    expect(await debits(w.id)).toBe(1);
    await queueDrained();
    expect(await approximateMessages(q.client, q.urls.requestsDlq)).toBe(0);
  }, 30_000);

  it("esgotado o limite de tentativas a mensagem vai para a DLQ com o motivo", async () => {
    const w = await newWallet();
    const m = message(w);
    let calls = 0;
    const alwaysDown = {
      execute: async () => {
        calls += 1;
        throw Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" });
      },
    };
    await send(m, { groupId: w.id });
    startConsumer({ process: alwaysDown, maxReceives: 3 });

    const [dead] = await drainQueue(q.client, q.urls.requestsDlq, 1, 25_000);
    expect(dead).toBeDefined();
    expect(dead!.attributes["reason"]).toBe("retries_exhausted");
    expect(JSON.parse(dead!.body).messageId).toBe(m.messageId);
    expect(calls).toBe(3);
    expect(dead!.attributes["attempts"]).toBe("3");
    expect(await debits(w.id)).toBe(0);
    // Esgotamento por falha TRANSITÓRIA não grava nada: a mensagem na DLQ pode ser reenviada quando a infra voltar.
    expect(await txOf(m.data.externalTransactionId as string)).toBeUndefined();
    await queueDrained();
  }, 40_000);

  it("a cauda de um grupo devolvida atrás de um retry NÃO gasta tentativas (o contador é do banco, não do SQS)", async () => {
    const w = await newWallet();
    const head = message(w);
    const tail = message(w);
    const calls = new Map<string, number>();
    // A cabeça falha 2 vezes; a cauda é devolvida junto em cada retry (o ApproximateReceiveCount dela sobe) e, quando
    // finalmente é processada, falha UMA vez. Com o contador do SQS ela já estaria no limite (3) e iria para a DLQ.
    const process = {
      execute: async (cmd: ProcessWagerCommand) => {
        const n = (calls.get(cmd.externalTransactionId) ?? 0) + 1;
        calls.set(cmd.externalTransactionId, n);
        const failures = cmd.externalTransactionId === head.data.externalTransactionId ? 2 : 1;
        if (n <= failures) throw Object.assign(new Error("deadlock detected"), { code: "40P01" });
        return h.process.execute(cmd);
      },
    };
    await send(head, { groupId: w.id });
    await send(tail, { groupId: w.id });
    const logger = new CapturingLogger();
    startConsumer({ process, maxReceives: 3, logger });

    await waitFor(async () => (await debits(w.id)) === 2, { timeoutMs: 30_000, what: "cabeça e cauda processadas" });
    expect(calls.get(tail.data.externalTransactionId as string)).toBe(2);
    expect(await approximateMessages(q.client, q.urls.requestsDlq)).toBe(0);

    const attempts = await h.pool.query<{ message_id: string; attempts: number }>(
      "SELECT message_id, attempts FROM inbox_attempts WHERE message_id = ANY($1) ORDER BY attempts DESC",
      [[head.messageId, tail.messageId]],
    );
    expect(attempts.rows).toEqual([
      { message_id: head.messageId, attempts: 2 },
      { message_id: tail.messageId, attempts: 1 },
    ]);
    await queueDrained();
    await assertLedgerMatchesBalances(h.pool);
  }, 60_000);

  it("falha PERMANENTE (não transitória) esgotada: a transação vira FAILED/INFRA_PERMANENT, com evento, e a mensagem vai à DLQ", async () => {
    const w = await newWallet();
    const m = message(w);
    let calls = 0;
    const broken = {
      execute: async () => {
        calls += 1;
        throw new Error("bug: estado impossível");
      },
      recordFailure: h.process.recordFailure.bind(h.process),
    };
    await send(m, { groupId: w.id });
    const brokenConsumer = startConsumer({ process: broken, maxReceives: 2 });

    const [dead] = await drainQueue(q.client, q.urls.requestsDlq, 1, 25_000);
    expect(dead!.attributes["reason"]).toBe("retries_exhausted");
    expect(dead!.attributes["attempts"]).toBe("2");
    expect(calls).toBe(2);

    const ext = m.data.externalTransactionId as string;
    expect(await txOf(ext)).toEqual({ status: S.Failed, failure_code: FailureCode.InfraPermanent });
    expect(await debits(w.id)).toBe(0);
    expect(await balanceOf(w.id)).toBe("100.00");
    const events = await h.pool.query(
      "SELECT payload->'data'->>'failureCode' AS code FROM outbox_messages WHERE event_type = 'WagerTransactionFailed' AND payload->'data'->>'externalTransactionId' = $1",
      [ext],
    );
    expect(events.rows).toEqual([{ code: FailureCode.InfraPermanent }]);
    await queueDrained();

    // FAILED é terminal: a mesma mensagem reenviada (ex.: redrive da DLQ) a um consumidor saudável devolve o resultado
    // original (replay pela inbox), sem efeito e sem voltar à DLQ.
    await brokenConsumer.stop();
    startConsumer();
    await send(m, { groupId: w.id });
    await queueDrained();
    expect(await txOf(ext)).toEqual({ status: S.Failed, failure_code: FailureCode.InfraPermanent });
    const replay = await h.process.execute({ ...(m.data as unknown as ProcessWagerCommand), correlationId: "c" });
    expect(replay).toMatchObject({ status: S.Failed, failureCode: FailureCode.InfraPermanent, idempotentReplay: true });
    expect(await approximateMessages(q.client, q.urls.requestsDlq)).toBe(0);
  }, 60_000);

  it("se o contador do banco está fora (PostgreSQL caiu), cai para o ApproximateReceiveCount com o teto mais alto", async () => {
    const w = await newWallet();
    const m = message(w);
    let calls = 0;
    const dbDown = {
      execute: async () => {
        calls += 1;
        throw Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" });
      },
    };
    const unreachable = { record: async () => Promise.reject(Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" })) };
    await send(m, { groupId: w.id });
    startConsumer({ process: dbDown, attempts: unreachable, maxReceives: 2, receiveHardLimit: 4 });

    const [dead] = await drainQueue(q.client, q.urls.requestsDlq, 1, 30_000);
    expect(dead!.attributes["reason"]).toBe("retries_exhausted_unrecorded");
    expect(calls).toBe(4); // não parou em maxReceives (2): uma queda do banco não manda a mensagem cedo para a DLQ
    await queueDrained();
  }, 45_000);

  it("worker morto depois do commit e antes do ack: a redelivery é deduplicada pela inbox", async () => {
    const short = await createTestQueues({ visibilityTimeoutSeconds: 2, maxReceiveCount: 10 });
    try {
      const w = await newWallet();
      const m = message(w);
      await send(m, { groupId: w.id, queue: short });

      // O primeiro ack falha: o efeito já foi confirmado no banco, mas a mensagem continua na fila.
      let failAcks = 1;
      const flakyAck: SqsSender = {
        send: ((command: unknown) => {
          if (command instanceof DeleteMessageBatchCommand && failAcks-- > 0) return Promise.reject(new Error("processo morreu antes do ack"));
          return (short.client as SQSClient).send(command as never);
        }) as SqsSender["send"],
      };
      startConsumer({ client: flakyAck }, short);

      await waitFor(async () => (await debits(w.id)) === 1, { what: "efeito confirmado" });
      await waitFor(async () => (await approximateMessages(short.client, short.urls.requests)) === 0, { timeoutMs: 20_000, what: "mensagem apagada após a redelivery" });

      expect(await debits(w.id)).toBe(1);
      expect(await balanceOf(w.id)).toBe("75.00");
      const inbox = await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1", [m.messageId]);
      expect(inbox.rows[0].n).toBe(1);
    } finally {
      await short.cleanup();
    }
  }, 40_000);

  it("shutdown gracioso: conclui a mensagem em andamento e devolve a visibilidade das demais", async () => {
    const w = await newWallet("1000.00");
    const messages = Array.from({ length: 5 }, () => message(w));
    for (const m of messages) await send(m, { groupId: w.id });

    const slow = {
      execute: async (cmd: ProcessWagerCommand) => {
        await new Promise((r) => setTimeout(r, 500));
        return h.process.execute(cmd);
      },
    };
    const first = startConsumer({ process: slow });
    await waitFor(async () => (await debits(w.id)) >= 1, { what: "primeira mensagem processada" });
    await first.stop(); // SIGTERM

    const processedByFirst = await debits(w.id);
    expect(processedByFirst).toBeGreaterThanOrEqual(1);
    expect(processedByFirst).toBeLessThan(5);
    // Nada ficou escondido pelo visibility timeout (30s): o que sobrou está imediatamente disponível.
    const remaining = await approximateMessages(q.client, q.urls.requests);
    expect(remaining).toBe(5 - processedByFirst);

    startConsumer(); // outra instância assume
    await waitFor(async () => (await debits(w.id)) === 5, { timeoutMs: 15_000, what: "restante processado pela segunda instância" });
    expect(await balanceOf(w.id)).toBe("875.00");
    await queueDrained();
    await assertLedgerMatchesBalances(h.pool);
  }, 40_000);

  it("carga mista: 30 wallets e uma wallet quente disputada, tudo via SQS, com duas instâncias", async () => {
    const wallets = await Promise.all(Array.from({ length: 30 }, () => newWallet("100.00")));
    const hot = await newWallet("100.00");
    for (const w of wallets) await send(message(w), { groupId: w.id });
    for (let i = 0; i < 20; i++) await send(message(hot, { money: brl("10.00") }), { groupId: hot.id });

    startConsumer();
    startConsumer();
    await queueDrained();
    await waitFor(async () => (await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n >= 50, { what: "50 mensagens na inbox" });

    expect(await balanceOf(hot.id)).toBe("0.00");
    expect(await debits(hot.id)).toBe(10);
    for (const w of wallets) expect(await balanceOf(w.id)).toBe("75.00");
    expect(await approximateMessages(q.client, q.urls.requestsDlq)).toBe(0);
    await assertLedgerMatchesBalances(h.pool);
  }, 60_000);

  it("concorrência: vários laços numa instância processam em paralelo, uma vez cada e sem quebrar a ordem por wallet", async () => {
    const wallets = await Promise.all(Array.from({ length: 24 }, () => newWallet("100.00")));
    for (let round = 0; round < 6; round++) for (const w of wallets) await send(message(w, { money: brl("10.00") }), { groupId: w.id });

    let running = 0;
    let peak = 0;
    const spy = {
      execute: async (command: ProcessWagerCommand) => {
        running++;
        peak = Math.max(peak, running);
        try {
          return await h.process.execute(command);
        } finally {
          running--;
        }
      },
    };
    startConsumer({ concurrency: 4, process: spy });
    await queueDrained();
    await waitFor(async () => (await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n >= 144, { what: "144 mensagens na inbox", timeoutMs: 30_000 });

    expect(peak).toBeGreaterThan(1);
    for (const w of wallets) {
      expect(await balanceOf(w.id)).toBe("40.00");
      expect(await debits(w.id)).toBe(6);
    }
    expect(await approximateMessages(q.client, q.urls.requestsDlq)).toBe(0);
    await assertLedgerMatchesBalances(h.pool);
  }, 60_000);

  it("concorrência inválida é rejeitada", () => {
    expect(() => new SqsWagerConsumer({ client: q.client, queueUrl: q.urls.requests, dlqUrl: q.urls.requestsDlq, process: h.process, logger: new CapturingLogger(), concurrency: 0 })).toThrow();
  });

  it("prefetch: processa tudo uma vez e mantém o saldo exato com recebimento antecipado e vários laços", async () => {
    const wallets = await Promise.all(Array.from({ length: 24 }, () => newWallet("100.00")));
    for (let round = 0; round < 6; round++) for (const w of wallets) await send(message(w, { money: brl("10.00") }), { groupId: w.id });

    startConsumer({ concurrency: 2, prefetch: true });
    await queueDrained();
    await waitFor(async () => (await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n >= 144, { what: "144 mensagens na inbox", timeoutMs: 30_000 });

    for (const w of wallets) {
      expect(await balanceOf(w.id)).toBe("40.00");
      expect(await debits(w.id)).toBe(6);
    }
    expect(await approximateMessages(q.client, q.urls.requestsDlq)).toBe(0);
    await assertLedgerMatchesBalances(h.pool);
  }, 60_000);

  it("prefetch + shutdown: o que foi recebido por antecipação e não será processado volta à fila na hora", async () => {
    const wallets = await Promise.all(Array.from({ length: 40 }, () => newWallet("100.00")));
    for (const w of wallets) await send(message(w), { groupId: w.id });

    const base = (await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n as number;
    const slow = {
      execute: async (cmd: ProcessWagerCommand) => {
        await new Promise((r) => setTimeout(r, 400));
        return h.process.execute(cmd);
      },
    };
    const first = startConsumer({ process: slow, prefetch: true });
    await waitFor(async () => (await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n - base >= 1, { what: "primeiro lote processado" });
    await first.stop();

    const inboxAfterStop = ((await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n as number) - base;
    const { Attributes = {} } = await q.client.send(
      new GetQueueAttributesCommand({ QueueUrl: q.urls.requests, AttributeNames: ["ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessages"] }),
    );
    // Nada ficou escondido (visibility 30s) por causa do prefetch: tudo que sobrou está disponível agora.
    expect(Number(Attributes["ApproximateNumberOfMessagesNotVisible"])).toBe(0);
    expect(Number(Attributes["ApproximateNumberOfMessages"])).toBe(40 - inboxAfterStop);

    startConsumer({ prefetch: true });
    await waitFor(async () => (await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n - base === 40, { timeoutMs: 20_000, what: "as 40 mensagens processadas" });
    for (const w of wallets) expect(await debits(w.id)).toBe(1);
    await queueDrained();
    await assertLedgerMatchesBalances(h.pool);
  }, 60_000);

  it("prefetch + shutdown com timeout de drenagem: o prefetch também volta à fila na hora", async () => {
    const wallets = await Promise.all(Array.from({ length: 40 }, () => newWallet("100.00")));
    for (const w of wallets) await send(message(w), { groupId: w.id });

    const base = (await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n as number;
    let started = 0;
    const slow = {
      execute: async (cmd: ProcessWagerCommand) => {
        started += 1;
        await new Promise((r) => setTimeout(r, 1_500));
        return h.process.execute(cmd);
      },
    };
    const first = startConsumer({ process: slow, prefetch: true, drainTimeoutMs: 100 });
    await waitFor(async () => started >= 1, { what: "primeiro lote em andamento" });
    // Dá tempo de o prefetch do próximo lote voltar do SQS antes do shutdown.
    await new Promise((r) => setTimeout(r, 500));
    await first.stop();

    const { Attributes = {} } = await q.client.send(
      new GetQueueAttributesCommand({ QueueUrl: q.urls.requests, AttributeNames: ["ApproximateNumberOfMessagesNotVisible"] }),
    );
    // As em andamento e as do prefetch tiveram a visibilidade devolvida: nada fica escondido pelos 30s do timeout.
    expect(Number(Attributes["ApproximateNumberOfMessagesNotVisible"])).toBe(0);

    startConsumer({ prefetch: true });
    await waitFor(async () => (await h.pool.query("SELECT count(*)::int AS n FROM inbox_messages")).rows[0].n - base === 40, { timeoutMs: 20_000, what: "as 40 mensagens processadas" });
    for (const w of wallets) expect(await debits(w.id)).toBe(1);
    await queueDrained();
    await assertLedgerMatchesBalances(h.pool);
  }, 60_000);
});
