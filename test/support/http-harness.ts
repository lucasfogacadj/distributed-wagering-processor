import "reflect-metadata";
import type { AddressInfo } from "node:net";
import { Test } from "@nestjs/testing";
import { NO_CONTEXT } from "../../src/application/observability";
import type { LogContextPort, LoggerPort } from "../../src/application/ports";
import type { WorkersConfig } from "../../src/config";
import { AsyncLocalLogContext } from "../../src/infrastructure/observability/log-context";
import type { QueueNames } from "../../src/infrastructure/messaging/sqs";
import { AppModule } from "../../src/interfaces/http/app.module";
import { TOKENS } from "../../src/interfaces/http/tokens";
import { createTestDatabase } from "./db";
import { RecordingMetrics } from "./recording-metrics";
import { LOCALSTACK_ENDPOINT } from "./sqs";

export interface LogRecord {
  level: "info" | "warn" | "error";
  message: string;
  fields: Record<string, unknown>;
}

/** Captura os logs já com o contexto corrente (correlationId etc.) mesclado, como o logger real faz. */
export class CapturingLogger implements LoggerPort {
  readonly records: LogRecord[] = [];

  constructor(private readonly context: LogContextPort = NO_CONTEXT) {}

  info(message: string, fields: Record<string, unknown> = {}): void {
    this.push("info", message, fields);
  }

  warn(message: string, fields: Record<string, unknown> = {}): void {
    this.push("warn", message, fields);
  }

  error(message: string, fields: Record<string, unknown> = {}): void {
    this.push("error", message, fields);
  }

  private push(level: LogRecord["level"], message: string, fields: Record<string, unknown>): void {
    this.records.push({ level, message, fields: { ...this.context.snapshot(), ...fields } });
  }
}

export interface ApiResponse {
  status: number;
  body: any;
  headers: Headers;
}

export interface HttpHarnessOptions {
  overrides?: { token: symbol; value: unknown }[];
  /** Liga consumidor, relay e worker de pendências contra o LocalStack, nas filas informadas. */
  messaging?: { queues: QueueNames; workers?: Partial<WorkersConfig> };
}

export async function createHttpHarness(options: HttpHarnessOptions = {}) {
  const db = await createTestDatabase();
  const context = new AsyncLocalLogContext();
  const logger = new CapturingLogger(context);
  const metrics = new RecordingMetrics();

  let builder = Test.createTestingModule({
    imports: [
      AppModule.register({
        databaseUrl: db.url,
        poolMax: 30,
        ...(options.messaging
          ? {
              messaging: {
                instanceId: "test-instance",
                sqs: {
                  enabled: true,
                  endpoint: LOCALSTACK_ENDPOINT,
                  region: "us-east-1",
                  queues: options.messaging.queues,
                  maxReceives: 3,
                  receiveHardLimit: 9,
                  visibilityTimeoutSeconds: 30,
                  waitTimeSeconds: 1,
                },
                workers: {
                  consumer: true,
                  outboxRelay: true,
                  pendingReference: true,
                  drainTimeoutMs: 5_000,
                  outboxLeaseMs: 30_000,
                  outboxBatchSize: 200,
                  outboxRunSize: 20,
                  outboxMaxAttempts: 10,
                  consumerConcurrency: 1,
                  consumerPrefetch: false,
                  ...options.messaging.workers,
                },
              },
            }
          : {}),
      }),
    ],
  })
    .overrideProvider(TOKENS.LOG_CONTEXT)
    .useValue(context)
    .overrideProvider(TOKENS.LOGGER)
    .useValue(logger)
    .overrideProvider(TOKENS.METRICS)
    .useValue(metrics);
  for (const o of options.overrides ?? []) builder = builder.overrideProvider(o.token).useValue(o.value);

  const moduleRef = await builder.compile();
  const app = moduleRef.createNestApplication();
  await app.listen(0);
  const baseUrl = `http://127.0.0.1:${(app.getHttpServer().address() as AddressInfo).port}`;

  async function request(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<ApiResponse> {
    const response = await fetch(`${baseUrl}${path}`, {
      method,
      headers: { ...(body !== undefined ? { "content-type": "application/json" } : {}), ...headers },
      ...(body !== undefined ? { body: typeof body === "string" ? body : JSON.stringify(body) } : {}),
    });
    const text = await response.text();
    return { status: response.status, body: text ? JSON.parse(text) : undefined, headers: response.headers };
  }

  return {
    pool: db.pool,
    baseUrl,
    logger,
    metrics,
    get: (path: string, headers?: Record<string, string>) => request("GET", path, undefined, headers),
    post: (path: string, body?: unknown, headers?: Record<string, string>) => request("POST", path, body, headers),
    async close(): Promise<void> {
      await app.close();
      await db.drop();
    },
  };
}

export type HttpHarness = Awaited<ReturnType<typeof createHttpHarness>>;
