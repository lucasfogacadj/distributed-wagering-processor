import type { INestApplication } from "@nestjs/common";

export function log(level: "info" | "error", message: string, fields: Record<string, unknown> = {}): void {
  const line = `${JSON.stringify({ level, message, ...fields })}\n`;
  if (level === "error") process.stderr.write(line);
  else process.stdout.write(line);
}

/**
 * Shutdown gracioso. Todos os gatilhos levam ao mesmo `app.close()` — que roda os hooks do Nest: parar de aceitar
 * conexões, terminar as requisições e mensagens em andamento, devolver a visibilidade das que não começaram, fechar o
 * ORM — e saem com código 0:
 *  - Linux/container: SIGTERM (`docker stop`, Kubernetes) e SIGINT (Ctrl+C);
 *  - Windows: não existe SIGTERM entre processos (`kill` vira TerminateProcess, sem chance de limpeza). Valem Ctrl+C
 *    (SIGINT), Ctrl+Break (SIGBREAK) e uma mensagem IPC `{ type: "shutdown" }` de um processo pai (supervisor, testes).
 *
 * Este módulo é o PRIMEIRO import do `main.ts` (só tem import de tipo), para os gatilhos existirem antes de carregar o
 * resto: no container o processo é o PID 1, e o kernel IGNORA um SIGTERM para o PID 1 sem handler — um `docker stop`
 * durante o boot (imports, migrations, criação do Nest) seria ignorado e o container só morreria no SIGKILL, 30 s
 * depois, possivelmente no meio de uma mensagem. Durante o boot nada está em andamento:
 *  - antes de a aplicação existir → sai na hora;
 *  - com a aplicação criada mas ainda subindo (workers iniciando no `listen`) → fecha assim que o boot termina.
 * Não usamos `enableShutdownHooks` do Nest: ele só existe depois do create, re-sinaliza o processo ao fim (saída 143,
 * sem log de conclusão) e não cobre SIGBREAK nem IPC.
 */
class Shutdown {
  private app: INestApplication | undefined;
  private ready = false;
  private trigger: string | undefined;

  install(): void {
    const signals: NodeJS.Signals[] = ["SIGTERM", "SIGINT", ...(process.platform === "win32" ? (["SIGBREAK"] as const) : [])];
    for (const signal of signals) process.on(signal, () => this.request(signal));
    if (typeof process.send === "function") {
      process.on("message", (message: unknown) => {
        if ((message as { type?: unknown } | null)?.type === "shutdown") this.request("ipc");
      });
    }
  }

  get requested(): boolean {
    return this.trigger !== undefined;
  }

  /** A aplicação foi criada (o `listen` ainda vai iniciar os workers). */
  created(app: INestApplication): void {
    this.app = app;
  }

  /** Boot concluído: se o shutdown foi pedido no meio dele, executa agora. */
  async booted(): Promise<void> {
    this.ready = true;
    if (this.trigger) await this.close(this.trigger);
  }

  private request(trigger: string): void {
    if (this.trigger) return;
    this.trigger = trigger;
    log("info", "shutdown_requested", { trigger, phase: this.ready ? "running" : "booting" });
    if (!this.app) {
      log("info", "shutdown_complete", { trigger, phase: "booting" });
      process.exit(0);
    }
    if (this.ready) void this.close(trigger);
  }

  private async close(trigger: string): Promise<void> {
    try {
      await this.app!.close();
      log("info", "shutdown_complete", { trigger });
      process.exit(0);
    } catch (error) {
      log("error", "shutdown_failed", { trigger, error: String(error) });
      process.exit(1);
    }
  }
}

export const shutdown = new Shutdown();
shutdown.install();
