import { randomUUID } from "node:crypto";
import { Inject, Injectable, type NestMiddleware } from "@nestjs/common";
import type { NextFunction, Request, Response } from "express";
import type { LogContextPort, MetricsPort } from "../../application/ports";
import { TOKENS } from "./tokens";

export const CORRELATION_HEADER = "x-correlation-id";

/**
 * Por requisição: aceita o x-correlation-id do chamador (se for razoável) ou gera um, devolve-o na resposta,
 * abre o escopo de log (todo log da requisição leva o correlationId) e mede a latência por ROTA.
 */
@Injectable()
export class CorrelationMiddleware implements NestMiddleware {
  constructor(
    @Inject(TOKENS.LOG_CONTEXT) private readonly context: LogContextPort,
    @Inject(TOKENS.METRICS) private readonly metrics: MetricsPort,
  ) {}

  use(req: Request, res: Response, next: NextFunction): void {
    const incoming = req.headers[CORRELATION_HEADER];
    const id = typeof incoming === "string" && /^[\w.:-]{8,128}$/.test(incoming) ? incoming : randomUUID();
    req.headers[CORRELATION_HEADER] = id;
    res.setHeader(CORRELATION_HEADER, id);

    const started = performance.now();
    res.on("finish", () => {
      // O padrão da rota (/wallets/:walletId), nunca a URL com ids: mantém a cardinalidade limitada.
      const route = typeof req.route?.path === "string" ? req.route.path : "unmatched";
      this.metrics.httpRequest(req.method, route, res.statusCode, (performance.now() - started) / 1000);
    });

    this.context.run({ correlationId: id }, () => next());
  }
}
