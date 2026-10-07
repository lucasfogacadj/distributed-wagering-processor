import { createRemoteJWKSet, jwtVerify, type JWTPayload } from "jose";
import type { ProviderIdentity, ProviderIdentityPort } from "../../application/ports";

export interface OidcOptions {
  issuer: string;
  audience: string;
  jwksUrl: string;
  /** Claim com o providerId (posto no token por um mapper do client no IdP). */
  providerClaim: string;
  /** Papel de realm (`realm_access.roles`) que identifica o operador/backoffice. */
  operatorRole: string;
  /** Teto para buscar o JWKS; estourar é falha do IdP (503), não do chamador. */
  jwksTimeoutMs?: number;
}

/**
 * Códigos do `jose` que significam "token inválido" (401). Qualquer outro erro — JWKS inacessível, timeout, resposta
 * malformada do IdP — é falha de infraestrutura: o adaptador lança e a API responde 503, para o provedor reenviar.
 */
const INVALID_TOKEN_CODES = new Set([
  "ERR_JWT_EXPIRED",
  "ERR_JWT_CLAIM_VALIDATION_FAILED",
  "ERR_JWT_INVALID",
  "ERR_JWS_INVALID",
  "ERR_JWS_SIGNATURE_VERIFICATION_FAILED",
  "ERR_JWKS_NO_MATCHING_KEY",
  "ERR_JWKS_MULTIPLE_MATCHING_KEYS",
  "ERR_JOSE_ALG_NOT_ALLOWED",
  "ERR_JOSE_NOT_SUPPORTED",
]);

/** Só algoritmos assimétricos: um token HS256 assinado com um segredo qualquer nunca é aceito. */
const ALLOWED_ALGORITHMS = ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"];

/**
 * Autenticação por um Identity Provider externo (OIDC) — Keycloak no docker-compose, mas qualquer IdP que emita JWT
 * assinado e publique JWKS serve. Fluxo máquina a máquina: cada provedor de jogos é um client `client_credentials`; o
 * backoffice é um client com o papel de operador. A API não guarda usuários nem senhas.
 *
 * Valida assinatura (JWKS, cacheado pelo `jose` e renovado quando aparece um `kid` novo), `iss`, `aud`, `exp`/`nbf`.
 */
export class OidcProviderIdentity implements ProviderIdentityPort {
  private readonly jwks: ReturnType<typeof createRemoteJWKSet>;

  constructor(private readonly options: OidcOptions) {
    this.jwks = createRemoteJWKSet(new URL(options.jwksUrl), {
      timeoutDuration: options.jwksTimeoutMs ?? 3_000,
      cooldownDuration: 30_000,
      cacheMaxAge: 10 * 60_000,
    });
  }

  async authenticate(headers: Record<string, string | undefined>): Promise<ProviderIdentity | undefined> {
    const token = bearerToken(headers["authorization"]);
    if (!token) return undefined;

    let payload: JWTPayload;
    try {
      ({ payload } = await jwtVerify(token, this.jwks, {
        issuer: this.options.issuer,
        audience: this.options.audience,
        algorithms: ALLOWED_ALGORITHMS,
        clockTolerance: 5,
      }));
    } catch (error) {
      if (isInvalidToken(error)) return undefined;
      throw error;
    }

    const providerId = payload[this.options.providerClaim];
    const operator = realmRoles(payload).includes(this.options.operatorRole);
    return {
      ...(typeof providerId === "string" && providerId.length > 0 ? { providerId } : {}),
      ...(operator ? { operator: true } : {}),
      ...(typeof payload.sub === "string" ? { subject: payload.sub } : {}),
    };
  }
}

function bearerToken(header: string | undefined): string | undefined {
  const match = /^Bearer\s+([A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+\.[A-Za-z0-9\-_]+)\s*$/i.exec(header ?? "");
  return match?.[1];
}

function realmRoles(payload: JWTPayload): string[] {
  const roles = (payload["realm_access"] as { roles?: unknown } | undefined)?.roles;
  return Array.isArray(roles) ? roles.filter((r): r is string => typeof r === "string") : [];
}

function isInvalidToken(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" && INVALID_TOKEN_CODES.has(code);
}
