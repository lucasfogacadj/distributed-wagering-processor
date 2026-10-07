import type { ProviderIdentity, ProviderIdentityPort } from "../../application/ports";

/**
 * Adaptador para `AUTH_MODE=none` (desenvolvimento e testes): não autentica e trata todo chamador como OPERADOR, ou
 * seja, sem restrição. Em produção use `AUTH_MODE=oidc` (`OidcProviderIdentity`, Keycloak no docker-compose).
 */
export class NoopProviderIdentity implements ProviderIdentityPort {
  async authenticate(_headers: Record<string, string | undefined> = {}): Promise<ProviderIdentity> {
    return { operator: true };
  }
}
