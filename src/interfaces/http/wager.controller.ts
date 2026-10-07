import { Body, Controller, Get, Headers, Inject, Param, Post, Res } from "@nestjs/common";
import type { Response } from "express";
import type { ProcessWagerResult, ProcessWagerTransaction } from "../../application/process-wager-transaction";
import type { WalletQueries } from "../../application/queries";
import type { ProviderIdentity } from "../../application/ports";
import { WagerTransactionStatus } from "../../domain/wager-transaction";
import { assertMayActAs, CurrentIdentity } from "./auth.guard";
import { parse, parseIdempotencyKey, submitTransactionSchema, uuidSchema } from "./dto";
import { TOKENS } from "./tokens";

/**
 * Resultado de negócio → HTTP (o mesmo mapa vale para o primeiro envio e para replays):
 *   PROCESSED → 200 · PENDING_REFERENCE → 202 (aceita, resolução pendente) · REJECTED → 422 (+ failureCode)
 *   FAILED    → 500 com o resultado no corpo (`status: "FAILED"`, `failureCode: "INFRA_PERMANENT"`): a transação foi
 *               encerrada por falha permanente de infraestrutura (hoje só pela fila). É TERMINAL: reenviar a mesma key
 *               devolve o mesmo resultado; o corpo com `status` distingue do 500 genérico (`{error}`), que é reenviável.
 */
export function statusFor(result: ProcessWagerResult): number {
  switch (result.status) {
    case WagerTransactionStatus.Processed:
      return 200;
    case WagerTransactionStatus.PendingReference:
      return 202;
    case WagerTransactionStatus.Rejected:
      return 422;
    case WagerTransactionStatus.Failed:
      return 500;
    // Estado que um resultado de `execute` nunca devolve (PENDING só existe dentro da transação SQL). Listado (sem
    // `default`) para o compilador acusar um status novo que ficou sem mapeamento HTTP.
    case WagerTransactionStatus.Pending:
      return 500;
  }
}

@Controller()
export class WagerController {
  constructor(
    @Inject(TOKENS.PROCESS) private readonly process: ProcessWagerTransaction,
    @Inject(TOKENS.QUERIES) private readonly queries: WalletQueries,
  ) {}

  @Post("wagering/transactions")
  async submit(
    @Headers("idempotency-key") idempotencyKeyHeader: unknown,
    @Headers("x-correlation-id") correlationId: string,
    @Body() body: unknown,
    @CurrentIdentity() identity: ProviderIdentity,
    @Res() res: Response,
  ): Promise<void> {
    const idempotencyKey = parseIdempotencyKey(idempotencyKeyHeader);
    const input = parse(submitTransactionSchema, body);
    assertMayActAs(identity, input.providerId);
    const result = await this.process.execute({ ...input, idempotencyKey, correlationId });
    res.status(statusFor(result)).json(result);
  }

  @Get("wagering/transactions/:transactionId")
  async getById(@Param("transactionId") transactionId: string, @CurrentIdentity() identity: ProviderIdentity) {
    const view = await this.queries.getTransaction(parse(uuidSchema, transactionId));
    assertMayActAs(identity, view.providerId);
    return view;
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  getByProvider(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
    @CurrentIdentity() identity: ProviderIdentity,
  ) {
    assertMayActAs(identity, providerId);
    return this.queries.getTransactionByProvider(providerId, externalTransactionId);
  }
}
