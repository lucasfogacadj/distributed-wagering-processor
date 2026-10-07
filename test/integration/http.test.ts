import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { TOKENS } from "../../src/interfaces/http/tokens";
import { createHttpHarness, type HttpHarness } from "../support/http-harness";

const brl = (amount: string) => ({ amount, currency: "BRL" });

let h: HttpHarness;
beforeAll(async () => {
  h = await createHttpHarness();
});
afterAll(async () => {
  await h.close();
});

interface W {
  id: string;
  playerId: string;
}

async function newWallet(balance = "1000.00"): Promise<W> {
  const playerId = randomUUID();
  const r = await h.post("/wallets", { playerId, initialBalance: brl(balance) });
  expect(r.status).toBe(201);
  return { id: r.body.id, playerId };
}

function body(w: W, over: Record<string, unknown> = {}) {
  return {
    providerId: "provider-a",
    externalTransactionId: randomUUID(),
    playerId: w.playerId,
    walletId: w.id,
    roundId: "round-987",
    gameId: "fortune-chimp",
    kind: "BET",
    money: brl("25.00"),
    ...over,
  };
}

const submit = (payload: Record<string, unknown>, key: string | null = `provider-a:${payload["externalTransactionId"]}`) =>
  h.post("/wagering/transactions", payload, key === null ? {} : { "Idempotency-Key": key });

describe("POST /wallets", () => {
  it("201 com o contrato do enunciado", async () => {
    const playerId = randomUUID();
    const r = await h.post("/wallets", { playerId, initialBalance: brl("1000.00") });
    expect(r.status).toBe(201);
    expect(r.body).toEqual({ id: expect.any(String), playerId, balance: brl("1000.00"), version: 1 });
  });

  it("409 WALLET_ALREADY_EXISTS para o mesmo player + moeda", async () => {
    const playerId = randomUUID();
    await h.post("/wallets", { playerId, initialBalance: brl("1.00") });
    const r = await h.post("/wallets", { playerId, initialBalance: brl("1.00") });
    expect(r.status).toBe(409);
    expect(r.body.error.code).toBe("WALLET_ALREADY_EXISTS");
  });

  it.each([
    ["playerId ausente", { initialBalance: brl("1.00") }],
    ["playerId não é uuid", { playerId: "abc", initialBalance: brl("1.00") }],
    ["initialBalance ausente", { playerId: randomUUID() }],
    ["amount sem 2 casas", { playerId: randomUUID(), initialBalance: brl("10") }],
    ["amount negativo", { playerId: randomUUID(), initialBalance: brl("-1.00") }],
    ["amount numérico", { playerId: randomUUID(), initialBalance: { amount: 10, currency: "BRL" } }],
    ["moeda inválida", { playerId: randomUUID(), initialBalance: { amount: "1.00", currency: "br" } }],
    ["amount acima do limite", { playerId: randomUUID(), initialBalance: brl("1000000000000000000.00") }],
  ])("400 INVALID_REQUEST ou INVALID_MONEY: %s", async (_name, payload) => {
    const r = await h.post("/wallets", payload);
    expect(r.status).toBe(400);
    expect(["INVALID_REQUEST", "INVALID_MONEY"]).toContain(r.body.error.code);
  });

  it("400 para JSON malformado, no mesmo formato de erro", async () => {
    const r = await h.post("/wallets", "{ não é json");
    expect(r.status).toBe(400);
    expect(r.body.error).toEqual({ code: expect.any(String), message: expect.any(String) });
  });
});

describe("GET /wallets/:walletId", () => {
  it("200 com saldo e version", async () => {
    const w = await newWallet("50.00");
    const r = await h.get(`/wallets/${w.id}`);
    expect(r.status).toBe(200);
    expect(r.body).toEqual({ id: w.id, playerId: w.playerId, balance: brl("50.00"), version: 1 });
  });

  it("404 para wallet inexistente e 400 para id malformado", async () => {
    const missing = await h.get(`/wallets/${randomUUID()}`);
    expect(missing.status).toBe(404);
    expect(missing.body.error.code).toBe("WALLET_NOT_FOUND");
    expect((await h.get("/wallets/xyz")).status).toBe(400);
  });
});

describe("POST /wagering/transactions", () => {
  it("200 PROCESSED com saldo e idempotentReplay=false", async () => {
    const w = await newWallet("1000.00");
    const r = await submit(body(w));
    expect(r.status).toBe(200);
    expect(r.body).toEqual({
      transactionId: expect.any(String),
      status: "PROCESSED",
      balance: brl("975.00"),
      idempotentReplay: false,
    });
  });

  it("replay: mesma resposta com idempotentReplay=true", async () => {
    const w = await newWallet("1000.00");
    const payload = body(w);
    const first = await submit(payload);
    const replay = await submit(payload);
    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });
  });

  it("400 sem o header Idempotency-Key", async () => {
    const w = await newWallet();
    const r = await submit(body(w), null);
    expect(r.status).toBe(400);
    expect(r.body.error.code).toBe("INVALID_REQUEST");
  });

  it("409 IDEMPOTENCY_CONFLICT: mesma key, payload diferente", async () => {
    const w = await newWallet();
    const payload = body(w);
    await submit(payload);
    const r = await submit({ ...payload, money: brl("26.00") });
    expect(r.status).toBe(409);
    expect(r.body.error.code).toBe("IDEMPOTENCY_CONFLICT");
  });

  it("422 REJECTED por regra de negócio, com failureCode estável", async () => {
    const w = await newWallet("10.00");
    const r = await submit(body(w, { money: brl("80.00") }));
    expect(r.status).toBe(422);
    expect(r.body).toMatchObject({ status: "REJECTED", failureCode: "INSUFFICIENT_FUNDS", balance: brl("10.00") });
  });

  it("a rejeição também é 422 no replay", async () => {
    const w = await newWallet("10.00");
    const payload = body(w, { money: brl("80.00") });
    await submit(payload);
    const replay = await submit(payload);
    expect(replay.status).toBe(422);
    expect(replay.body.idempotentReplay).toBe(true);
  });

  it("202 PENDING_REFERENCE quando a referência ainda não chegou", async () => {
    const w = await newWallet("100.00");
    const r = await submit(body(w, { kind: "REFUND", referenceExternalTransactionId: "ainda-nao-chegou" }));
    expect(r.status).toBe(202);
    expect(r.body).toMatchObject({ status: "PENDING_REFERENCE", balance: brl("100.00") });
  });

  it.each([
    ["kind OPENING (interno)", { kind: "OPENING" }],
    ["kind desconhecido", { kind: "DEPOSIT" }],
    ["REFUND sem referência", { kind: "REFUND" }],
    ["BET com referência", { referenceExternalTransactionId: "x" }],
    ["walletId não é uuid", { walletId: "abc" }],
    ["providerId vazio", { providerId: "" }],
    ["money ausente", { money: undefined }],
    ["amount com 3 casas", { money: brl("1.001") }],
    ["amount em notação científica", { money: brl("1e3") }],
    ["valor zero em BET", { money: brl("0.00") }],
    ["amount acima do limite", { money: brl("1000000000000000000.00") }],
  ])("400 payload inválido: %s", async (_name, over) => {
    const w = await newWallet();
    const r = await submit(body(w, over));
    expect(r.status).toBe(400);
    expect(r.body.error.code).toMatch(/^INVALID_/);
  });

  it("campos extras são ignorados e não entram no hash", async () => {
    const w = await newWallet();
    const payload = body(w);
    const first = await submit({ ...payload, extra: "a" });
    const replay = await submit({ ...payload, extra: "b" });
    expect(replay.status).toBe(200);
    expect(replay.body.idempotentReplay).toBe(true);
    expect(replay.body.transactionId).toBe(first.body.transactionId);
  });

  it("50 requisições HTTP idênticas em paralelo geram um único débito", async () => {
    const w = await newWallet("100.00");
    const payload = body(w);
    const responses = await Promise.all(Array.from({ length: 50 }, () => submit(payload)));

    expect(responses.every((r) => r.status === 200)).toBe(true);
    expect(responses.filter((r) => !r.body.idempotentReplay)).toHaveLength(1);
    expect(new Set(responses.map((r) => r.body.transactionId)).size).toBe(1);
    expect((await h.get(`/wallets/${w.id}`)).body.balance).toEqual(brl("75.00"));
  });

  it("duas apostas de 80.00 sobre 100.00: um 200 e um 422", async () => {
    const w = await newWallet("100.00");
    const [a, b] = await Promise.all([submit(body(w, { money: brl("80.00") })), submit(body(w, { money: brl("80.00") }))]);
    expect([a.status, b.status].sort()).toEqual([200, 422]);
    expect((await h.get(`/wallets/${w.id}`)).body.balance).toEqual(brl("20.00"));
  });
});

describe("consultas de transação", () => {
  it("GET /wagering/transactions/:id", async () => {
    const w = await newWallet();
    const payload = body(w);
    const created = await submit(payload);
    const r = await h.get(`/wagering/transactions/${created.body.transactionId}`);
    expect(r.status).toBe(200);
    expect(r.body).toMatchObject({
      transactionId: created.body.transactionId,
      providerId: "provider-a",
      externalTransactionId: payload.externalTransactionId,
      walletId: w.id,
      kind: "BET",
      status: "PROCESSED",
      money: brl("25.00"),
      balance: brl("975.00"),
    });
  });

  it("GET /providers/:providerId/wagering/transactions/:externalTransactionId", async () => {
    const w = await newWallet();
    const payload = body(w);
    const created = await submit(payload);
    const r = await h.get(`/providers/provider-a/wagering/transactions/${payload.externalTransactionId}`);
    expect(r.status).toBe(200);
    expect(r.body.transactionId).toBe(created.body.transactionId);
  });

  it("404 TRANSACTION_NOT_FOUND nas duas rotas, 400 para id malformado", async () => {
    const byId = await h.get(`/wagering/transactions/${randomUUID()}`);
    expect(byId.status).toBe(404);
    expect(byId.body.error.code).toBe("TRANSACTION_NOT_FOUND");
    expect((await h.get("/providers/provider-a/wagering/transactions/nao-existe")).status).toBe(404);
    expect((await h.get("/wagering/transactions/xyz")).status).toBe(400);
  });
});

describe("GET /wallets/:walletId/ledger", () => {
  async function walletWithBets(bets: number): Promise<W> {
    const w = await newWallet("1000.00");
    for (let i = 0; i < bets; i++) await submit(body(w, { money: brl(`${i + 1}.00`) }));
    return w;
  }

  it("pagina com cursor opaco, do mais novo para o mais antigo, sem repetir nem perder lançamentos", async () => {
    const w = await walletWithBets(5); // 1 abertura + 5 débitos = 6 lançamentos
    const seen: string[] = [];
    let cursor: string | null = null;
    let pages = 0;

    do {
      const r = await h.get(`/wallets/${w.id}/ledger?limit=2${cursor ? `&cursor=${cursor}` : ""}`);
      expect(r.status).toBe(200);
      expect(r.body.items.length).toBeLessThanOrEqual(2);
      seen.push(...r.body.items.map((i: any) => i.id));
      cursor = r.body.nextCursor;
      pages += 1;
    } while (cursor && pages < 10);

    expect(pages).toBe(3);
    expect(seen).toHaveLength(6);
    expect(new Set(seen).size).toBe(6);
  });

  it("o primeiro item é o lançamento mais recente, com o formato esperado", async () => {
    const w = await walletWithBets(2);
    const r = await h.get(`/wallets/${w.id}/ledger?limit=1`);
    expect(r.body.items[0]).toEqual({
      id: expect.any(String),
      transactionId: expect.any(String),
      direction: "DEBIT",
      money: brl("2.00"),
      balanceBefore: brl("999.00"),
      balanceAfter: brl("997.00"),
      createdAt: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T/),
    });
    expect(r.body.nextCursor).toEqual(expect.any(String));
  });

  it("o cursor continua estável quando novos lançamentos chegam entre as páginas", async () => {
    const w = await walletWithBets(3);
    const first = await h.get(`/wallets/${w.id}/ledger?limit=2`);
    await submit(body(w, { money: brl("9.00") }));
    const second = await h.get(`/wallets/${w.id}/ledger?limit=2&cursor=${first.body.nextCursor}`);
    const ids = new Set([...first.body.items, ...second.body.items].map((i: any) => i.id));
    expect(ids.size).toBe(4);
  });

  it("última página não traz nextCursor", async () => {
    const w = await walletWithBets(1);
    const r = await h.get(`/wallets/${w.id}/ledger`);
    expect(r.body.items).toHaveLength(2);
    expect(r.body.nextCursor).toBeNull();
  });

  it.each(["limit=0", "limit=201", "limit=abc", "cursor=lixo"])("400 INVALID_QUERY para %s", async (query) => {
    const w = await newWallet();
    const r = await h.get(`/wallets/${w.id}/ledger?${query}`);
    expect(r.status).toBe(400);
    expect(r.body.error.code).toBe("INVALID_QUERY");
  });

  it("404 para wallet inexistente", async () => {
    expect((await h.get(`/wallets/${randomUUID()}/ledger`)).status).toBe(404);
  });
});

describe("POST /wallets/:walletId/reconciliation", () => {
  it("consistente: difference zero e checkedEntries", async () => {
    const w = await newWallet("100.00");
    await submit(body(w, { money: brl("30.00") }));
    const r = await h.post(`/wallets/${w.id}/reconciliation`);
    expect(r.status).toBe(200);
    expect(r.body).toEqual({
      walletId: w.id,
      storedBalance: brl("70.00"),
      calculatedBalance: brl("70.00"),
      difference: brl("0.00"),
      consistent: true,
      checkedEntries: 2,
    });
    expect(h.metrics.divergences).toBe(0);
  });

  it("divergência é sinalizada, logada e contabilizada — nunca corrigida em silêncio", async () => {
    const w = await newWallet("100.00");
    // Só alguém com acesso privilegiado ao banco consegue divergir; simulamos desligando o trigger.
    await h.pool.query("ALTER TABLE wallets DISABLE TRIGGER trg_wallets_ledger_consistency");
    try {
      await h.pool.query("UPDATE wallets SET balance = 130.00 WHERE id = $1", [w.id]);
    } finally {
      await h.pool.query("ALTER TABLE wallets ENABLE TRIGGER trg_wallets_ledger_consistency");
    }

    const r = await h.post(`/wallets/${w.id}/reconciliation`);
    expect(r.status).toBe(200);
    expect(r.body).toMatchObject({
      storedBalance: brl("130.00"),
      calculatedBalance: brl("100.00"),
      difference: brl("30.00"),
      consistent: false,
      checkedEntries: 1,
    });
    expect(h.metrics.divergences).toBe(1);
    expect(h.logger.records.some((l) => l.level === "warn" && l.message === "reconciliation_divergence" && l.fields["walletId"] === w.id)).toBe(true);

    const still = await h.get(`/wallets/${w.id}`);
    expect(still.body.balance).toEqual(brl("130.00")); // não foi corrigido
  });

  it("404 para wallet inexistente", async () => {
    expect((await h.post(`/wallets/${randomUUID()}/reconciliation`)).status).toBe(404);
  });
});

describe("health", () => {
  it("live e ready respondem 200 sem autenticação", async () => {
    const live = await h.get("/health/live");
    expect(live.status).toBe(200);
    expect(live.body).toEqual({ status: "ok" });
    const ready = await h.get("/health/ready");
    expect(ready.status).toBe(200);
    expect(ready.body).toMatchObject({ status: "ok", checks: { postgres: "up" } });
  });
});

describe("observabilidade", () => {
  it("GET /metrics expõe as métricas obrigatórias no formato Prometheus, sem autenticação", async () => {
    const w = await newWallet("100.00");
    const payload = body(w);
    await submit(payload);
    await submit(payload); // replay → duplicata
    await submit(body(w, { money: brl("999.00") })); // rejeitada
    await h.get(`/wallets/${w.id}`);

    const response = await fetch(`${h.baseUrl}/metrics`);
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/plain");
    const text = await response.text();

    expect(text).toMatch(/wager_transactions_total\{status="PROCESSED",kind="BET"\} \d+/);
    expect(text).toMatch(/wager_transactions_total\{status="REJECTED",kind="BET"\} \d+/);
    expect(text).toMatch(/wager_duplicates_total\{source="http"\} \d+/);
    expect(text).toMatch(/wallet_lock_wait_seconds_count \d+/);
    expect(text).toMatch(/wager_processing_duration_seconds_count\{source="http",outcome="PROCESSED"\} \d+/);
    expect(text).toMatch(/outbox_lag_seconds [\d.]+/);
    expect(text).toContain('http_request_duration_seconds_count{method="POST",route="/wagering/transactions",status="200"}');
    expect(text).toContain('http_request_duration_seconds_count{method="GET",route="/wallets/:walletId",status="200"}');
  });

  it("outbox lag reflete o evento pendente mais antigo", async () => {
    await newWallet("10.00"); // gera evento na outbox (sem relay ligado neste harness)
    await new Promise((r) => setTimeout(r, 1_100));
    const text = await (await fetch(`${h.baseUrl}/metrics`)).text();
    const lag = Number(/outbox_lag_seconds ([\d.]+)/.exec(text)?.[1]);
    expect(lag).toBeGreaterThan(1);
  });

  it("a rota é o padrão (/wallets/:walletId), nunca o id: a cardinalidade das métricas é limitada", async () => {
    for (let i = 0; i < 3; i++) await h.get(`/wallets/${randomUUID()}`);
    const text = await (await fetch(`${h.baseUrl}/metrics`)).text();
    expect(text).not.toMatch(/route="\/wallets\/[0-9a-f]{8}-/);
  });

  it("a requisição é logada com correlationId, walletId, providerId e transactionId — sem valores monetários", async () => {
    const w = await newWallet("100.00");
    const payload = body(w, { money: brl("4321.09") });
    const r = await submit(payload, "key-obs-1");
    expect(r.status).toBe(422); // saldo insuficiente: o valor do payload não pode aparecer em lugar nenhum
    const ok = await h.post("/wagering/transactions", body(w), { "Idempotency-Key": "key-obs-2", "x-correlation-id": "corr-obs-12345" });

    const record = h.logger.records.find((l) => l.message === "wager_transaction_decided" && l.fields["transactionId"] === ok.body.transactionId);
    expect(record?.fields).toMatchObject({
      correlationId: "corr-obs-12345",
      walletId: w.id,
      providerId: "provider-a",
      transactionId: ok.body.transactionId,
      status: "PROCESSED",
    });
    const everything = JSON.stringify(h.logger.records);
    expect(everything).not.toContain("4321.09");
    expect(everything).not.toContain('"amount"');
  });
});

describe("contrato de erros", () => {
  it("rota inexistente: 404 no formato padrão", async () => {
    const r = await h.get("/nao-existe");
    expect(r.status).toBe(404);
    expect(r.body.error).toEqual({ code: expect.any(String), message: expect.any(String) });
  });

  it("devolve e propaga o x-correlation-id", async () => {
    const r = await h.get("/health/live", { "x-correlation-id": "corr-abc-123" });
    expect(r.headers.get("x-correlation-id")).toBe("corr-abc-123");
    const generated = await h.get("/health/live");
    expect(generated.headers.get("x-correlation-id")).toMatch(/^[\w-]{8,}$/);
  });
});

describe("falhas de infraestrutura", () => {
  it("falha transitória vira 503 + Retry-After, distinta de 4xx e de 500", async () => {
    const transient = await createHttpHarness({
      overrides: [
        {
          token: TOKENS.PROCESS,
          value: { execute: async () => Promise.reject(Object.assign(new Error("deadlock detected"), { code: "40P01" })) },
        },
      ],
    });
    try {
      const w = { id: randomUUID(), playerId: randomUUID() };
      const r = await transient.post("/wagering/transactions", body(w), { "Idempotency-Key": "k-1" });
      expect(r.status).toBe(503);
      expect(r.headers.get("retry-after")).toBe("1");
      expect(r.body.error.code).toBe("SERVICE_UNAVAILABLE");
    } finally {
      await transient.close();
    }
  });

  it("erro desconhecido vira 500 sem vazar detalhes internos", async () => {
    const broken = await createHttpHarness({
      overrides: [{ token: TOKENS.PROCESS, value: { execute: async () => Promise.reject(new Error("senha=segredo")) } }],
    });
    try {
      const w = { id: randomUUID(), playerId: randomUUID() };
      const r = await broken.post("/wagering/transactions", body(w), { "Idempotency-Key": "k-1" });
      expect(r.status).toBe(500);
      expect(r.body.error.code).toBe("INTERNAL_ERROR");
      expect(JSON.stringify(r.body)).not.toContain("segredo");
      expect(broken.logger.records.some((l) => l.level === "error")).toBe(true);
    } finally {
      await broken.close();
    }
  });

  it("readiness 503 quando uma dependência cai, listando qual", async () => {
    const down = await createHttpHarness({
      overrides: [
        {
          token: TOKENS.READINESS_CHECKS,
          value: [
            { name: "postgres", check: async () => undefined },
            { name: "sqs", check: async () => Promise.reject(new Error("unreachable")) },
          ],
        },
      ],
    });
    try {
      const r = await down.get("/health/ready");
      expect(r.status).toBe(503);
      expect(r.body).toEqual({ status: "unavailable", checks: { postgres: "up", sqs: "down" } });
      expect((await down.get("/health/live")).status).toBe(200);
    } finally {
      await down.close();
    }
  });
});
