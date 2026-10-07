import {
  ForbiddenException,
  Inject,
  Injectable,
  ServiceUnavailableException,
  UnauthorizedException,
  createParamDecorator,
  type CanActivate,
  type ExecutionContext,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import type { Request } from "express";
import { errorLogFields } from "../../application/describe-error";
import type { LoggerPort, ProviderIdentity, ProviderIdentityPort } from "../../application/ports";
import { IS_PUBLIC_KEY } from "./public.decorator";
import { TOKENS } from "./tokens";

type AuthenticatedRequest = Request & { identity?: ProviderIdentity };

function headersOf(request: Request): Record<string, string | undefined> {
  return Object.fromEntries(
    Object.entries(request.headers).map(([name, value]) => [name.toLowerCase(), Array.isArray(value) ? value.join(", ") : value]),
  );
}

/**
 * Guard global. Delega a autenticação ao `ProviderIdentityPort` (IdP externo), exceto nas rotas `@Public()`.
 *   credencial ausente/inválida → 401 · IdP inacessível → 503 (transitório) · sucesso → `request.identity`.
 */
@Injectable()
export class AuthGuard implements CanActivate {
  constructor(
    @Inject(TOKENS.PROVIDER_IDENTITY) private readonly identityPort: ProviderIdentityPort,
    @Inject(Reflector) private readonly reflector: Reflector,
    @Inject(TOKENS.LOGGER) private readonly logger: LoggerPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    if (this.reflector.getAllAndOverride<boolean | undefined>(IS_PUBLIC_KEY, [context.getHandler(), context.getClass()])) {
      return true;
    }

    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    let identity: ProviderIdentity | undefined;
    try {
      identity = await this.identityPort.authenticate(headersOf(request));
    } catch (error) {
      // 503 (transitório) para o chamador, mas a causa fica registrada: sem isto um bug no adaptador de identidade
      // (ex.: TypeError) seria indistinguível de uma queda do IdP.
      this.logger.warn("identity_provider_failed", { ...errorLogFields(error) });
      throw new ServiceUnavailableException("Provedor de identidade indisponível", { cause: error });
    }
    if (!identity) throw new UnauthorizedException("Credenciais ausentes ou inválidas");

    request.identity = identity;
    return true;
  }
}

/** A identidade autenticada da requisição (vazia — sem direito algum — se o guard não a definiu). */
export const CurrentIdentity = createParamDecorator(
  (_data: unknown, context: ExecutionContext): ProviderIdentity =>
    context.switchToHttp().getRequest<AuthenticatedRequest>().identity ?? {},
);

/** Um provedor só age em nome do PRÓPRIO providerId; o operador age por qualquer um. */
export function assertMayActAs(identity: ProviderIdentity, providerId: string): void {
  if (identity.operator === true) return;
  if (identity.providerId === undefined || identity.providerId !== providerId) {
    throw new ForbiddenException("O chamador autenticado não pode agir em nome deste provedor");
  }
}

/** Operações de backoffice (criar wallet, ledger, reconciliação): só o operador. */
export function assertOperator(identity: ProviderIdentity): void {
  if (identity.operator !== true) {
    throw new ForbiddenException("Operação restrita ao operador (backoffice)");
  }
}

/** Leitura de saldo: o operador ou qualquer provedor autenticado (o provedor precisa do saldo para operar). */
export function assertMayReadWallet(identity: ProviderIdentity): void {
  if (identity.operator !== true && identity.providerId === undefined) {
    throw new ForbiddenException("O chamador autenticado não pode consultar wallets");
  }
}
