import { Controller, Get, Inject, Res } from "@nestjs/common";
import type { Response } from "express";
import type { PrometheusMetrics } from "../../infrastructure/observability/metrics";
import { Public } from "./public.decorator";
import { TOKENS } from "./tokens";

/** Formato texto do Prometheus. Sem autenticação, como o health: restrinja por rede/ingress em produção. */
@Public()
@Controller("metrics")
export class MetricsController {
  constructor(@Inject(TOKENS.METRICS) private readonly metrics: PrometheusMetrics) {}

  @Get()
  async scrape(@Res() res: Response): Promise<void> {
    res.setHeader("Content-Type", this.metrics.contentType);
    res.send(await this.metrics.render());
  }
}
