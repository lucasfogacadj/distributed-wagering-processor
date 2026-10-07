import { createServer, type AddressInfo } from "node:net";
import { fileURLToPath } from "node:url";
import type { Subprocess } from "bun";
import type { QueueNames } from "../../src/infrastructure/messaging/sqs";
import { createTestDatabase, type TestDatabase } from "./db";
import { createTestQueues, LOCALSTACK_ENDPOINT, type TestQueues } from "./sqs";

const PROJECT_ROOT = fileURLToPath(new URL("../../", import.meta.url));

/** Atributos das filas dos testes de cluster: iguais aos que cada instância pede (receiveHardLimit 15 + 1). */
export const CLUSTER_QUEUE_OPTIONS = { visibilityTimeoutSeconds: 3, maxReceiveCount: 16 } as const;

export interface InstanceOptions {
  id?: string;
  /** Endpoint do SQS que esta instância usa (um proxy, para injetar falhas deterministas). */
  sqsEndpoint?: string;
  consumer?: boolean;
  outboxRelay?: boolean;
  pendingReference?: boolean;
  outboxLeaseMs?: number;
  outboxBatchSize?: number;
  extraEnv?: Record<string, string>;
  /** false: devolve a instância logo depois do spawn, sem esperar o /health/ready (para testar o boot). */
  waitReady?: boolean;
}

export interface ApiResponse {
  status: number;
  body: any;
  headers: Headers;
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

/** Um processo `bun run src/main.ts` de verdade: a aplicação completa, com seu próprio pool e seus workers. */
export class AppInstance {
  readonly logs: string[] = [];

  private constructor(
    readonly id: string,
    readonly port: number,
    private readonly proc: Subprocess<"ignore", "pipe", "pipe">,
  ) {}

  static async start(
    env: { databaseUrl: string; queues: QueueNames },
    options: InstanceOptions = {},
  ): Promise<AppInstance> {
    const id = options.id ?? `instance-${Math.random().toString(36).slice(2, 8)}`;
    const port = await freePort();

    const proc = Bun.spawn([process.execPath, "run", "src/main.ts"], {
      cwd: PROJECT_ROOT,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      // Canal IPC: no Windows é por ele que o shutdown gracioso é pedido (não há SIGTERM entre processos).
      ipc() {},
      serialization: "json",
      env: {
        ...(process.env as Record<string, string>),
        DATABASE_URL: env.databaseUrl,
        PORT: String(port),
        INSTANCE_ID: id,
        RUN_MIGRATIONS: "false", // o banco do teste já foi migrado
        POOL_MAX: "10",
        LOCK_TIMEOUT_MS: "15000",
        SQS_ENDPOINT: options.sqsEndpoint ?? LOCALSTACK_ENDPOINT,
        AWS_REGION: "us-east-1",
        QUEUE_REQUESTS: env.queues.requests,
        QUEUE_REQUESTS_DLQ: env.queues.requestsDlq,
        QUEUE_EVENTS: env.queues.events,
        SQS_MAX_RECEIVES: "5",
        SQS_VISIBILITY_TIMEOUT: String(CLUSTER_QUEUE_OPTIONS.visibilityTimeoutSeconds),
        SQS_WAIT_SECONDS: "1",
        CONSUMER_ENABLED: String(options.consumer ?? true),
        OUTBOX_RELAY_ENABLED: String(options.outboxRelay ?? true),
        PENDING_REFERENCE_WORKER_ENABLED: String(options.pendingReference ?? true),
        SHUTDOWN_DRAIN_MS: "3000",
        OUTBOX_LEASE_MS: String(options.outboxLeaseMs ?? 30_000),
        OUTBOX_BATCH_SIZE: String(options.outboxBatchSize ?? 50),
        ...options.extraEnv,
      },
    });

    const instance = new AppInstance(id, port, proc);
    instance.pump(proc.stdout);
    instance.pump(proc.stderr);
    if (options.waitReady !== false) await instance.waitReady();
    return instance;
  }

  get baseUrl(): string {
    return `http://127.0.0.1:${this.port}`;
  }

  get alive(): boolean {
    return this.proc.exitCode === null && !this.proc.killed;
  }

  /** Logs JSON da aplicação já parseados (linhas que não são JSON são ignoradas). */
  records(): Record<string, any>[] {
    return this.logs.flatMap((line) => {
      try {
        return [JSON.parse(line) as Record<string, any>];
      } catch {
        return [];
      }
    });
  }

  async request(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<ApiResponse> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { ...(body !== undefined ? { "content-type": "application/json" } : {}), ...headers },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    const text = await response.text();
    return { status: response.status, body: text ? JSON.parse(text) : undefined, headers: response.headers };
  }

  /**
   * Shutdown GRACIOSO, pelo gatilho nativo da plataforma: SIGTERM no Linux/macOS (o que `docker stop` e o Kubernetes
   * mandam); no Windows, onde `kill("SIGTERM")` vira TerminateProcess, a mensagem IPC `{ type: "shutdown" }`.
   * Devolve o exit code.
   */
  async shutdown(): Promise<number> {
    if (this.alive) {
      if (process.platform === "win32") this.proc.send({ type: "shutdown" });
      else this.proc.kill("SIGTERM");
    }
    return this.proc.exited;
  }

  /** SIGKILL: o processo some sem nenhuma chance de limpeza (como uma queda de nó ou OOM kill). */
  async kill(signal: "SIGKILL" | "SIGTERM" = "SIGKILL"): Promise<void> {
    if (this.alive) this.proc.kill(signal);
    await this.proc.exited;
  }

  private pump(stream: ReadableStream<Uint8Array>): void {
    void (async () => {
      const decoder = new TextDecoder();
      let buffer = "";
      for await (const chunk of stream) {
        buffer += decoder.decode(chunk, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        this.logs.push(...lines.filter((line) => line.length > 0));
      }
    })().catch(() => undefined);
  }

  private async waitReady(timeoutMs = 60_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (!this.alive) {
        throw new Error(`Instância ${this.id} encerrou ao subir:\n${this.logs.slice(-15).join("\n")}`);
      }
      try {
        if ((await fetch(`${this.baseUrl}/health/ready`)).status === 200) return;
      } catch {
        // ainda subindo
      }
      await new Promise((resolve) => setTimeout(resolve, 150));
    }
    await this.kill();
    throw new Error(`Instância ${this.id} não ficou pronta em ${timeoutMs}ms:\n${this.logs.slice(-15).join("\n")}`);
  }
}

export interface ClusterEnv {
  db: TestDatabase;
  pool: TestDatabase["pool"];
  queues: TestQueues;
  /** Sobe `count` instâncias em paralelo. */
  start(count: number, options?: InstanceOptions): Promise<AppInstance[]>;
  startOne(options?: InstanceOptions): Promise<AppInstance>;
  close(): Promise<void>;
}

/** Banco e filas isolados + instâncias reais da aplicação compartilhando os dois. */
export async function createClusterEnv(): Promise<ClusterEnv> {
  const db = await createTestDatabase();
  const queues = await createTestQueues(CLUSTER_QUEUE_OPTIONS);
  const started: AppInstance[] = [];

  const startOne = async (options: InstanceOptions = {}): Promise<AppInstance> => {
    const instance = await AppInstance.start({ databaseUrl: db.url, queues: queues.names }, options);
    started.push(instance);
    return instance;
  };

  return {
    db,
    pool: db.pool,
    queues,
    startOne,
    start: (count, options = {}) =>
      Promise.all(Array.from({ length: count }, (_, i) => startOne({ ...options, id: `${options.id ?? "app"}-${i + 1}` }))),
    async close() {
      await Promise.all(started.map((instance) => instance.kill().catch(() => undefined)));
      await queues.cleanup();
      await db.drop();
    },
  };
}
