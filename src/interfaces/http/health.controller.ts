import { Controller, Get, Inject, Res } from "@nestjs/common";
import type { Response } from "express";
import { Public } from "./public.decorator";
import { TOKENS } from "./tokens";

export interface ReadinessCheck {
  name: string;
  check(): Promise<void>;
}

const CHECK_TIMEOUT_MS = 2_000;

async function isUp(check: ReadinessCheck): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      check.check(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timeout após ${CHECK_TIMEOUT_MS}ms`)), CHECK_TIMEOUT_MS);
      }),
    ]);
    return true;
  } catch {
    return false;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Sem autenticação, por definição do desafio. */
@Public()
@Controller("health")
export class HealthController {
  constructor(@Inject(TOKENS.READINESS_CHECKS) private readonly checks: ReadinessCheck[]) {}

  /** Processo vivo: não consulta dependências. */
  @Get("live")
  live(): { status: "ok" } {
    return { status: "ok" };
  }

  /** Pronto para tráfego: PostgreSQL e SQS alcançáveis. */
  @Get("ready")
  async ready(@Res() res: Response): Promise<void> {
    const results = await Promise.all(this.checks.map(async (c) => [c.name, await isUp(c)] as const));
    const healthy = results.every(([, up]) => up);
    res.status(healthy ? 200 : 503).json({
      status: healthy ? "ok" : "unavailable",
      checks: Object.fromEntries(results.map(([name, up]) => [name, up ? "up" : "down"])),
    });
  }
}
