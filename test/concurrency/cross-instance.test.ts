import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createClusterEnv, type AppInstance, type ClusterEnv } from "../support/cluster";
import { assertLedgerMatchesBalances } from "../support/harness";
import { approximateMessages, waitFor } from "../support/sqs";
import {
  balanceOf,
  brl,
  count,
  createWalletVia,
  debitsOf,
  keyFor,
  sendMessages,
  submitVia,
  wagerBody,
  wagerMessage,
} from "../support/wager-fixtures";

let env: ClusterEnv;
let instances: AppInstance[];

beforeAll(async () => {
  env = await createClusterEnv();
  instances = await env.start(3); // três processos de verdade, cada um com seu pool e seus workers
}, 120_000);
afterAll(async () => {
  await env.close();
});

/** Distribui a carga entre as instâncias em rodízio. */
const via = (i: number): AppInstance => instances[i % instances.length]!;

describe("três instâncias simultâneas sobre o mesmo PostgreSQL", () => {
  it("há de fato 3 processos distintos, todos prontos", async () => {
    expect(new Set(instances.map((i) => i.port)).size).toBe(3);
    for (const instance of instances) {
      expect(instance.alive).toBe(true);
      expect((await instance.request("GET", "/health/ready")).body).toEqual({ status: "ok", checks: { postgres: "up", sqs: "up" } });
    }
  });

  it("a mesma aposta enviada 50 vezes em paralelo, entre as 3 instâncias, gera um único débito", async () => {
    const w = await createWalletVia(via(0), "100.00");
    const body = wagerBody(w);
    const key = keyFor(body);

    const responses = await Promise.all(Array.from({ length: 50 }, (_, i) => submitVia(via(i), body, key)));

    expect(responses.every((r) => r.status === 200)).toBe(true);
    expect(responses.filter((r) => !r.body.idempotentReplay)).toHaveLength(1);
    expect(new Set(responses.map((r) => r.body.transactionId)).size).toBe(1);
    expect(await debitsOf(env.pool, w.id)).toBe(1);
    expect(await balanceOf(env.pool, w.id)).toBe("75.00");
    await assertLedgerMatchesBalances(env.pool);
  });

  it("cenário obrigatório ×20: duas apostas de 80.00 sobre 100.00 em instâncias diferentes", async () => {
    const wallets = await Promise.all(Array.from({ length: 20 }, (_, i) => createWalletVia(via(i), "100.00")));

    const results = await Promise.all(
      wallets.map(async (w, i) => {
        const [a, b] = await Promise.all([
          submitVia(via(i), wagerBody(w, { money: brl("80.00") })),
          submitVia(via(i + 1), wagerBody(w, { money: brl("80.00") })),
        ]);
        return { w, statuses: [a.status, b.status].sort(), codes: [a.body.failureCode, b.body.failureCode].filter(Boolean) };
      }),
    );

    for (const { w, statuses, codes } of results) {
      expect(statuses).toEqual([200, 422]);
      expect(codes).toEqual(["INSUFFICIENT_FUNDS"]);
      expect(await balanceOf(env.pool, w.id)).toBe("20.00");
      expect(await debitsOf(env.pool, w.id)).toBe(1);
    }
    await assertLedgerMatchesBalances(env.pool);
  });

  it("wallet quente: 150 apostas de 10.00 sobre 1000.00 espalhadas nas 3 instâncias → exatamente 100 aplicadas", async () => {
    const w = await createWalletVia(via(0), "1000.00");
    const responses = await Promise.all(Array.from({ length: 150 }, (_, i) => submitVia(via(i), wagerBody(w, { money: brl("10.00") }))));

    expect(responses.filter((r) => r.status === 200)).toHaveLength(100);
    const rejected = responses.filter((r) => r.status === 422);
    expect(rejected).toHaveLength(50);
    expect(new Set(rejected.map((r) => r.body.failureCode))).toEqual(new Set(["INSUFFICIENT_FUNDS"]));
    expect(await balanceOf(env.pool, w.id)).toBe("0.00");
    expect(await debitsOf(env.pool, w.id)).toBe(100);
    await assertLedgerMatchesBalances(env.pool);
  }, 60_000);

  it("wallets distintas processadas em paralelo, sem interferência", async () => {
    const wallets = await Promise.all(Array.from({ length: 30 }, (_, i) => createWalletVia(via(i), "100.00")));
    const responses = await Promise.all(wallets.map((w, i) => submitVia(via(i + 1), wagerBody(w))));

    for (const r of responses) expect(r).toMatchObject({ status: 200, body: { status: "PROCESSED", balance: brl("75.00") } });
    await assertLedgerMatchesBalances(env.pool);
  });

  it("HTTP e SQS disputando a mesma wallet quente ao mesmo tempo: 60 + 60 apostas de 10.00 sobre 1000.00", async () => {
    const w = await createWalletVia(via(0), "1000.00");
    const bets = Array.from({ length: 120 }, () => wagerBody(w, { money: brl("10.00") }));
    const viaHttp = bets.slice(0, 60);
    const viaSqs = bets.slice(60);

    await Promise.all([
      Promise.all(viaHttp.map((body, i) => submitVia(via(i), body))),
      sendMessages(env.queues, viaSqs.map(wagerMessage)),
    ]);

    await waitFor(async () => (await count(env.pool, "wager_transactions WHERE wallet_id = $1 AND kind = 'BET' AND status IN ('PROCESSED','REJECTED')", [w.id])) === 120, {
      timeoutMs: 60_000,
      what: "120 apostas decididas",
    });
    expect(await count(env.pool, "wager_transactions WHERE wallet_id = $1 AND status = 'PROCESSED' AND kind = 'BET'", [w.id])).toBe(100);
    expect(await balanceOf(env.pool, w.id)).toBe("0.00");
    expect(await debitsOf(env.pool, w.id)).toBe(100);
    await waitFor(async () => (await approximateMessages(env.queues.client, env.queues.urls.requests)) === 0, { what: "fila de entrada vazia" });
    expect(await approximateMessages(env.queues.client, env.queues.urls.requestsDlq)).toBe(0);
    await assertLedgerMatchesBalances(env.pool);
  }, 90_000);

  it("REFUND entregue por uma instância ANTES da BET entregue por outra é resolvido por qualquer uma delas", async () => {
    const w = await createWalletVia(via(0), "100.00");
    const bet = wagerBody(w);
    const refund = wagerBody(w, { kind: "REFUND", referenceExternalTransactionId: bet.externalTransactionId });

    const parked = await submitVia(via(0), refund);
    expect(parked.status).toBe(202);
    expect((await submitVia(via(1), bet)).status).toBe(200);

    await waitFor(async () => (await via(2).request("GET", `/wagering/transactions/${parked.body.transactionId}`)).body.status === "PROCESSED", {
      timeoutMs: 30_000,
      what: "REFUND resolvido pelo worker",
    });
    expect(await balanceOf(env.pool, w.id)).toBe("100.00");
    expect((await via(2).request("POST", `/wallets/${w.id}/reconciliation`)).body.consistent).toBe(true);
    await assertLedgerMatchesBalances(env.pool);
  }, 60_000);

  it("a carga foi distribuída: todas as 3 instâncias processaram transações", async () => {
    const decidedBy = instances.map((i) => i.records().filter((r) => r["message"] === "wager_transaction_decided").length);
    expect(decidedBy.every((n) => n > 0)).toBe(true);
  });
});
