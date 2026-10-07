import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import type { ProviderIdentity, ProviderIdentityPort } from "../../src/application/ports";
import { NoopProviderIdentity } from "../../src/infrastructure/auth/noop-provider-identity";
import { TOKENS } from "../../src/interfaces/http/tokens";
import { createHttpHarness, type HttpHarness } from "../support/http-harness";

const brl = (amount: string) => ({ amount, currency: "BRL" });

/**
 * Adaptador de teste: chave de API → identidade. Exercita o guard e as regras de autorização sem IdP; o adaptador
 * OIDC real (Keycloak) é testado em oidc-keycloak.test.ts.
 */
class ApiKeyIdentity implements ProviderIdentityPort {
  calls = 0;

  constructor(private readonly keys: Record<string, ProviderIdentity>) {}

  async authenticate(headers: Record<string, string | undefined>): Promise<ProviderIdentity | undefined> {
    this.calls += 1;
    const key = headers["x-api-key"];
    return key ? this.keys[key] : undefined;
  }
}

describe("AUTH_MODE=none (padrão em desenvolvimento): sem autenticação", () => {
  it("NoopProviderIdentity trata qualquer chamador como operador (sem restrição)", async () => {
    expect(await new NoopProviderIdentity().authenticate({})).toEqual({ operator: true });
    expect(await new NoopProviderIdentity().authenticate({ authorization: "Bearer qualquer" })).toEqual({ operator: true });
  });
});

describe("com um ProviderIdentityPort real plugado", () => {
  let h: HttpHarness;
  const identity = new ApiKeyIdentity({
    "key-a": { providerId: "provider-a", subject: "key:a" },
    "key-b": { providerId: "provider-b", subject: "key:b" },
    "key-op": { operator: true, subject: "key:op" },
    "key-none": { subject: "key:none" }, // autentica, mas não tem direito algum
  });

  beforeAll(async () => {
    h = await createHttpHarness({ overrides: [{ token: TOKENS.PROVIDER_IDENTITY, value: identity }] });
  });
  afterAll(async () => {
    await h.close();
  });

  const asA = { "x-api-key": "key-a" };
  const asOperator = { "x-api-key": "key-op" };
  const asNobody = { "x-api-key": "key-none" };

  async function newWallet(headers: Record<string, string> = asOperator) {
    const playerId = randomUUID();
    const r = await h.post("/wallets", { playerId, initialBalance: brl("100.00") }, headers);
    expect(r.status).toBe(201);
    return { id: r.body.id as string, playerId };
  }

  const bet = (w: { id: string; playerId: string }, providerId = "provider-a") => ({
    providerId,
    externalTransactionId: randomUUID(),
    playerId: w.playerId,
    walletId: w.id,
    roundId: "r1",
    gameId: "g1",
    kind: "BET",
    money: brl("25.00"),
  });

  it("sem credencial → 401 UNAUTHORIZED, no formato de erro padrão, em todos os endpoints de negócio", async () => {
    const w = await newWallet();
    const responses = await Promise.all([
      h.post("/wallets", { playerId: randomUUID(), initialBalance: brl("1.00") }),
      h.get(`/wallets/${w.id}`),
      h.get(`/wallets/${w.id}/ledger`),
      h.post(`/wallets/${w.id}/reconciliation`),
      h.post("/wagering/transactions", bet(w), { "Idempotency-Key": "k" }),
      h.get(`/wagering/transactions/${randomUUID()}`),
      h.get("/providers/provider-a/wagering/transactions/x"),
    ]);
    for (const r of responses) {
      expect(r.status).toBe(401);
      expect(r.body.error).toEqual({ code: "UNAUTHORIZED", message: expect.any(String) });
    }
  });

  it("credencial inválida → 401", async () => {
    expect((await h.get("/wallets/" + randomUUID(), { "x-api-key": "errada" })).status).toBe(401);
  });

  it("os endpoints de health e /metrics continuam abertos, sem sequer consultar o IdP", async () => {
    const before = identity.calls;
    expect((await h.get("/health/live")).status).toBe(200);
    expect((await h.get("/health/ready")).status).toBe(200);
    expect((await fetch(`${h.baseUrl}/metrics`)).status).toBe(200);
    expect(identity.calls).toBe(before);
  });

  it("credencial válida → o provedor opera normalmente", async () => {
    const w = await newWallet();
    const payload = bet(w);
    const r = await h.post("/wagering/transactions", payload, { ...asA, "Idempotency-Key": `provider-a:${payload.externalTransactionId}` });
    expect(r.status).toBe(200);
    expect((await h.get(`/wallets/${w.id}`, asA)).status).toBe(200);
    expect((await h.get(`/providers/provider-a/wagering/transactions/${payload.externalTransactionId}`, asA)).status).toBe(200);
  });

  it("um provedor autenticado não age em nome de outro → 403 FORBIDDEN", async () => {
    const w = await newWallet();
    const asProviderB = bet(w, "provider-b");
    const submit = await h.post("/wagering/transactions", asProviderB, { ...asA, "Idempotency-Key": "k-forbidden" });
    expect(submit.status).toBe(403);
    expect(submit.body.error.code).toBe("FORBIDDEN");

    const read = await h.get("/providers/provider-b/wagering/transactions/qualquer", asA);
    expect(read.status).toBe(403);
    expect(read.body.error.code).toBe("FORBIDDEN");

    // nada foi gravado pela tentativa negada
    const { rows } = await h.pool.query("SELECT count(*)::int AS n FROM wager_transactions WHERE provider_id = 'provider-b'");
    expect(rows[0].n).toBe(0);
  });

  it("o provedor B opera como B, e a mesma wallet não é de B por isso: a regra de negócio continua valendo", async () => {
    const w = await newWallet();
    const payload = bet(w, "provider-b");
    const r = await h.post("/wagering/transactions", payload, { "x-api-key": "key-b", "Idempotency-Key": "provider-b:1" });
    expect(r.status).toBe(200); // a autorização é por provedor; a wallet é validada pelo domínio (player/moeda)
  });

  it("operações de backoffice (criar wallet, ledger, reconciliação) são só do operador: provedor → 403", async () => {
    const w = await newWallet();
    const asProvider = await Promise.all([
      h.post("/wallets", { playerId: randomUUID(), initialBalance: brl("1.00") }, asA),
      h.get(`/wallets/${w.id}/ledger`, asA),
      h.post(`/wallets/${w.id}/reconciliation`, undefined, asA),
    ]);
    for (const r of asProvider) {
      expect(r.status).toBe(403);
      expect(r.body.error.code).toBe("FORBIDDEN");
    }

    expect((await h.get(`/wallets/${w.id}/ledger`, asOperator)).status).toBe(200);
    expect((await h.post(`/wallets/${w.id}/reconciliation`, undefined, asOperator)).status).toBe(200);
    // o saldo continua legível pelo provedor, que precisa dele para operar
    expect((await h.get(`/wallets/${w.id}`, asA)).status).toBe(200);
  });

  it("o operador pode agir por qualquer provedor (ex.: reprocessar manualmente uma transação)", async () => {
    const w = await newWallet();
    const payload = bet(w, "provider-b");
    const r = await h.post("/wagering/transactions", payload, { ...asOperator, "Idempotency-Key": `provider-b:${payload.externalTransactionId}` });
    expect(r.status).toBe(200);
    expect((await h.get(`/providers/provider-b/wagering/transactions/${payload.externalTransactionId}`, asOperator)).status).toBe(200);
  });

  it("um token válido sem providerId nem papel de operador autentica, mas não autoriza nada → 403 (a ausência de claim nunca dá acesso)", async () => {
    const w = await newWallet();
    const payload = bet(w);
    const responses = await Promise.all([
      h.post("/wallets", { playerId: randomUUID(), initialBalance: brl("1.00") }, asNobody),
      h.get(`/wallets/${w.id}`, asNobody),
      h.get(`/wallets/${w.id}/ledger`, asNobody),
      h.post("/wagering/transactions", payload, { ...asNobody, "Idempotency-Key": "k-nobody" }),
      h.get("/providers/provider-a/wagering/transactions/x", asNobody),
    ]);
    for (const r of responses) expect(r.status).toBe(403);
  });

  it("a resposta 401 carrega o correlation id", async () => {
    const r = await h.get("/wallets/" + randomUUID(), { "x-correlation-id": "corr-auth-12345" });
    expect(r.status).toBe(401);
    expect(r.headers.get("x-correlation-id")).toBe("corr-auth-12345");
  });
});

describe("IdP indisponível", () => {
  it("falha ao consultar o IdP é 503 (transitório), nunca 401 nem 500", async () => {
    const down: ProviderIdentityPort = { authenticate: async () => Promise.reject(new Error("idp inacessível")) };
    const h = await createHttpHarness({ overrides: [{ token: TOKENS.PROVIDER_IDENTITY, value: down }] });
    try {
      const r = await h.get("/wallets/" + randomUUID(), { "x-api-key": "qualquer" });
      expect(r.status).toBe(503);
      expect(r.body.error.code).toBe("SERVICE_UNAVAILABLE");
      expect((await h.get("/health/live")).status).toBe(200);
    } finally {
      await h.close();
    }
  });

  it("a causa da falha do IdP fica registrada (um bug no adaptador não pode parecer só uma queda)", async () => {
    const buggy: ProviderIdentityPort = {
      authenticate: async () => {
        throw new TypeError("headers.authorization é undefined");
      },
    };
    const h = await createHttpHarness({ overrides: [{ token: TOKENS.PROVIDER_IDENTITY, value: buggy }] });
    try {
      const r = await h.get("/wallets/" + randomUUID(), { "x-api-key": "qualquer" });
      expect(r.status).toBe(503);

      const record = h.logger.records.find((l) => l.level === "warn" && l.message === "identity_provider_failed");
      expect(record).toBeDefined();
      expect(record!.fields["error"]).toContain("TypeError: headers.authorization é undefined");
      expect(typeof record!.fields["stack"]).toBe("string");
    } finally {
      await h.close();
    }
  });
});
