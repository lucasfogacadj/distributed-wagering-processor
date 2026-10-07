import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { exportJWK, generateKeyPair, SignJWT, type JWK } from "jose";
import { OidcProviderIdentity } from "../../../src/infrastructure/auth/oidc-provider-identity";

const ISSUER = "http://idp.test/realms/wagering";
const AUDIENCE = "wagering-api";

let server: ReturnType<typeof Bun.serve>;
let privateKey: CryptoKey;
let foreignKey: CryptoKey;
let identity: OidcProviderIdentity;

beforeAll(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  foreignKey = (await generateKeyPair("RS256")).privateKey;
  const jwk: JWK = { ...(await exportJWK(pair.publicKey)), kid: "k1", alg: "RS256", use: "sig" };

  // JWKS local: o adaptador busca as chaves por HTTP, como faria no Keycloak.
  server = Bun.serve({ port: 0, fetch: () => Response.json({ keys: [jwk] }) });
  identity = oidc(`http://127.0.0.1:${server.port}/certs`);
});
afterAll(() => {
  server.stop(true);
});

function oidc(jwksUrl: string): OidcProviderIdentity {
  return new OidcProviderIdentity({
    issuer: ISSUER,
    audience: AUDIENCE,
    jwksUrl,
    providerClaim: "provider_id",
    operatorRole: "wagering-operator",
    jwksTimeoutMs: 1_000,
  });
}

interface TokenOptions {
  claims?: Record<string, unknown>;
  issuer?: string;
  audience?: string;
  expiresIn?: string | number;
  key?: CryptoKey;
  kid?: string;
}

function token(o: TokenOptions = {}): Promise<string> {
  return new SignJWT({ provider_id: "provider-a", ...o.claims })
    .setProtectedHeader({ alg: "RS256", kid: o.kid ?? "k1" })
    .setIssuer(o.issuer ?? ISSUER)
    .setAudience(o.audience ?? AUDIENCE)
    .setSubject("sa-provider-a")
    .setIssuedAt()
    .setExpirationTime(o.expiresIn ?? "5m")
    .sign(o.key ?? privateKey);
}

const bearer = async (o: TokenOptions = {}) => ({ authorization: `Bearer ${await token(o)}` });

describe("OidcProviderIdentity", () => {
  it("token válido de provedor → providerId e subject (sem papel de operador)", async () => {
    expect(await identity.authenticate(await bearer())).toEqual({ providerId: "provider-a", subject: "sa-provider-a" });
  });

  it("papel de realm wagering-operator → operador", async () => {
    const headers = await bearer({ claims: { provider_id: undefined, realm_access: { roles: ["offline_access", "wagering-operator"] } } });
    expect(await identity.authenticate(headers)).toEqual({ operator: true, subject: "sa-provider-a" });
  });

  it("token válido sem providerId nem papel → identidade sem direito algum (o guard/controllers dão 403)", async () => {
    const headers = await bearer({ claims: { provider_id: undefined, realm_access: { roles: ["offline_access"] } } });
    expect(await identity.authenticate(headers)).toEqual({ subject: "sa-provider-a" });
  });

  it.each([
    ["expirado", { expiresIn: Math.floor(Date.now() / 1000) - 60 }],
    ["outro issuer", { issuer: "http://evil.test/realms/wagering" }],
    ["outra audiência", { audience: "outra-api" }],
  ] as [string, TokenOptions][])("token %s → undefined (401)", async (_name, options) => {
    expect(await identity.authenticate(await bearer(options))).toBeUndefined();
  });

  it("assinado por uma chave que não está no JWKS → undefined (401)", async () => {
    expect(await identity.authenticate(await bearer({ key: foreignKey }))).toBeUndefined();
    expect(await identity.authenticate(await bearer({ key: foreignKey, kid: "desconhecido" }))).toBeUndefined();
  });

  it("HS256 forjado com um segredo qualquer → undefined (só algoritmos assimétricos)", async () => {
    const forged = await new SignJWT({ provider_id: "provider-a", realm_access: { roles: ["wagering-operator"] } })
      .setProtectedHeader({ alg: "HS256", kid: "k1" })
      .setIssuer(ISSUER)
      .setAudience(AUDIENCE)
      .setExpirationTime("5m")
      .sign(new TextEncoder().encode("segredo-qualquer-com-32-bytes-ou-mais!!"));
    expect(await identity.authenticate({ authorization: `Bearer ${forged}` })).toBeUndefined();
  });

  it("sem header, outro esquema ou token malformado → undefined (401)", async () => {
    expect(await identity.authenticate({})).toBeUndefined();
    expect(await identity.authenticate({ authorization: `Basic ${btoa("a:b")}` })).toBeUndefined();
    expect(await identity.authenticate({ authorization: "Bearer nao-e-um-jwt" })).toBeUndefined();
    const valid = await token();
    expect(await identity.authenticate({ authorization: `Bearer ${valid.slice(0, -4)}AAAA` })).toBeUndefined();
  });

  it("JWKS inacessível → lança (a API responde 503, transitório), nunca 401", async () => {
    const down = oidc("http://127.0.0.1:1/certs");
    await expect(down.authenticate(await bearer())).rejects.toBeDefined();
  });
});
