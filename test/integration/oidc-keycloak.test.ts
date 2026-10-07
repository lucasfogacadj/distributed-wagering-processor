import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { OidcProviderIdentity } from "../../src/infrastructure/auth/oidc-provider-identity";
import { TOKENS } from "../../src/interfaces/http/tokens";
import { createHttpHarness, type HttpHarness } from "../support/http-harness";

/**
 * Autenticação ponta a ponta com o Keycloak do docker-compose (`bun run infra:up`): tokens reais emitidos por
 * client_credentials, validados pelo OidcProviderIdentity via JWKS. Realm e clients em keycloak/realm-wagering.json.
 */
const KEYCLOAK = process.env["TEST_KEYCLOAK_URL"] ?? "http://localhost:8180";
const ISSUER = `${KEYCLOAK}/realms/wagering`;
const brl = (amount: string) => ({ amount, currency: "BRL" });

async function tokenFor(clientId: string, secret = `${clientId}-secret`): Promise<string> {
  const response = await fetch(`${ISSUER}/protocol/openid-connect/token`, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "client_credentials", client_id: clientId, client_secret: secret }),
  });
  if (!response.ok) throw new Error(`Keycloak recusou ${clientId}: ${response.status} ${await response.text()}`);
  return ((await response.json()) as { access_token: string }).access_token;
}

let h: HttpHarness;
const auth: Record<string, Record<string, string>> = {};

beforeAll(async () => {
  for (const client of ["provider-a", "provider-b", "backoffice", "no-rights"]) {
    auth[client] = { authorization: `Bearer ${await tokenFor(client)}` };
  }
  const identity = new OidcProviderIdentity({
    issuer: ISSUER,
    audience: "wagering-api",
    jwksUrl: `${ISSUER}/protocol/openid-connect/certs`,
    providerClaim: "provider_id",
    operatorRole: "wagering-operator",
  });
  h = await createHttpHarness({ overrides: [{ token: TOKENS.PROVIDER_IDENTITY, value: identity }] });
});
afterAll(async () => {
  await h.close();
});

async function newWallet() {
  const playerId = randomUUID();
  const r = await h.post("/wallets", { playerId, initialBalance: brl("100.00") }, auth["backoffice"]);
  expect(r.status).toBe(201);
  return { id: r.body.id as string, playerId };
}

const bet = (w: { id: string; playerId: string }, providerId: string) => ({
  providerId,
  externalTransactionId: randomUUID(),
  playerId: w.playerId,
  walletId: w.id,
  roundId: "r1",
  gameId: "g1",
  kind: "BET",
  money: brl("25.00"),
});

describe("OIDC com Keycloak real", () => {
  it("o backoffice (papel wagering-operator) cria wallet, lê o ledger e reconcilia", async () => {
    const w = await newWallet();
    expect((await h.get(`/wallets/${w.id}/ledger`, auth["backoffice"])).status).toBe(200);
    expect((await h.post(`/wallets/${w.id}/reconciliation`, undefined, auth["backoffice"])).status).toBe(200);
  });

  it("o provedor A aposta como A e lê o saldo; o resultado é o mesmo de sempre", async () => {
    const w = await newWallet();
    const payload = bet(w, "provider-a");
    const r = await h.post("/wagering/transactions", payload, { ...auth["provider-a"], "Idempotency-Key": `provider-a:${payload.externalTransactionId}` });
    expect(r.status).toBe(200);
    expect(r.body.balance).toEqual(brl("75.00"));
    expect((await h.get(`/wallets/${w.id}`, auth["provider-a"])).body.balance).toEqual(brl("75.00"));
  });

  it("o provedor A não age como B, nem faz operações de backoffice → 403", async () => {
    const w = await newWallet();
    const asB = bet(w, "provider-b");
    expect((await h.post("/wagering/transactions", asB, { ...auth["provider-a"], "Idempotency-Key": "k-a-as-b" })).status).toBe(403);
    expect((await h.post("/wallets", { playerId: randomUUID(), initialBalance: brl("1.00") }, auth["provider-a"])).status).toBe(403);
    expect((await h.get(`/wallets/${w.id}/ledger`, auth["provider-a"])).status).toBe(403);
  });

  it("um client autenticado sem provider_id nem papel não faz nada → 403", async () => {
    const w = await newWallet();
    expect((await h.get(`/wallets/${w.id}`, auth["no-rights"])).status).toBe(403);
    const payload = bet(w, "provider-a");
    expect((await h.post("/wagering/transactions", payload, { ...auth["no-rights"], "Idempotency-Key": "k-none" })).status).toBe(403);
  });

  it("sem token, token adulterado ou secret errado → 401 / sem token", async () => {
    const w = await newWallet();
    expect((await h.get(`/wallets/${w.id}`)).status).toBe(401);
    const tampered = auth["provider-a"]!["authorization"]!.slice(0, -6) + "AAAAAA";
    expect((await h.get(`/wallets/${w.id}`, { authorization: tampered })).status).toBe(401);
    await expect(tokenFor("provider-a", "secret-errado")).rejects.toThrow("401");
  });

  it("health continua aberto", async () => {
    expect((await h.get("/health/live")).status).toBe(200);
    expect((await h.get("/health/ready")).status).toBe(200);
  });
});
