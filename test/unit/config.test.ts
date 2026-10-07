import { describe, expect, it } from "bun:test";
import { loadConfig } from "../../src/config";

describe("loadConfig", () => {
  it("usa os padrões do ambiente local (docker compose)", () => {
    const config = loadConfig({});
    expect(config).toMatchObject({
      databaseUrl: "postgres://wager:wager@localhost:5433/wager",
      port: 3000,
      poolMax: 20,
      runMigrations: true,
      sqs: {
        enabled: true,
        endpoint: "http://localhost:4566",
        region: "us-east-1",
        queues: { requests: "wager-transactions.fifo", requestsDlq: "wager-transactions-dlq.fifo", events: "wallet-events.fifo" },
        maxReceives: 5,
        visibilityTimeoutSeconds: 30,
        waitTimeSeconds: 5,
      },
      workers: { consumer: true, outboxRelay: true, pendingReference: true, drainTimeoutMs: 20_000 },
    });
    expect(config.instanceId).toMatch(/.+-\d+$/);
    expect(config.lockTimeoutMs).toBe(10_000);
  });

  it("lease, lote e sequência da outbox têm padrão e são configuráveis", () => {
    expect(loadConfig({}).workers).toMatchObject({ consumerConcurrency: 4, consumerPrefetch: false, outboxLeaseMs: 30_000, outboxBatchSize: 200, outboxRunSize: 20 });
    expect(loadConfig({ OUTBOX_LEASE_MS: "2000", OUTBOX_BATCH_SIZE: "5", OUTBOX_RUN_SIZE: "3" }).workers).toMatchObject({
      outboxLeaseMs: 2000,
      outboxBatchSize: 5,
      outboxRunSize: 3,
    });
    expect(() => loadConfig({ OUTBOX_LEASE_MS: "100" })).toThrow(/OUTBOX_LEASE_MS/);
    expect(() => loadConfig({ OUTBOX_BATCH_SIZE: "0" })).toThrow(/OUTBOX_BATCH_SIZE/);
    expect(() => loadConfig({ OUTBOX_RUN_SIZE: "0" })).toThrow(/OUTBOX_RUN_SIZE/);
  });

  it("LOCK_TIMEOUT_MS configura o lock_timeout do PostgreSQL", () => {
    expect(loadConfig({ LOCK_TIMEOUT_MS: "2500" }).lockTimeoutMs).toBe(2500);
    expect(() => loadConfig({ LOCK_TIMEOUT_MS: "10" })).toThrow(/LOCK_TIMEOUT_MS/);
  });

  it("lê as variáveis de ambiente", () => {
    expect(
      loadConfig({
        DATABASE_URL: "postgres://u:p@db:5432/x",
        PORT: "8081",
        POOL_MAX: "5",
        RUN_MIGRATIONS: "false",
        INSTANCE_ID: "app-2",
        SQS_ENDPOINT: "http://ministack:4566",
        AWS_REGION: "sa-east-1",
        QUEUE_REQUESTS: "req.fifo",
        QUEUE_REQUESTS_DLQ: "req-dlq.fifo",
        QUEUE_EVENTS: "ev.fifo",
        SQS_MAX_RECEIVES: "7",
        SQS_VISIBILITY_TIMEOUT: "60",
        SQS_WAIT_SECONDS: "10",
        CONSUMER_ENABLED: "false",
        OUTBOX_RELAY_ENABLED: "false",
        PENDING_REFERENCE_WORKER_ENABLED: "false",
        SHUTDOWN_DRAIN_MS: "5000",
      }),
    ).toMatchObject({
      databaseUrl: "postgres://u:p@db:5432/x",
      port: 8081,
      poolMax: 5,
      runMigrations: false,
      instanceId: "app-2",
      sqs: {
        enabled: true,
        endpoint: "http://ministack:4566",
        region: "sa-east-1",
        queues: { requests: "req.fifo", requestsDlq: "req-dlq.fifo", events: "ev.fifo" },
        maxReceives: 7,
        visibilityTimeoutSeconds: 60,
        waitTimeSeconds: 10,
      },
      workers: { consumer: false, outboxRelay: false, pendingReference: false, drainTimeoutMs: 5000 },
    });
  });

  it("SQS_ENDPOINT vazio significa AWS real (sem endpoint)", () => {
    expect(loadConfig({ SQS_ENDPOINT: "" }).sqs.endpoint).toBeUndefined();
  });

  it("SQS_ENABLED=false desliga a mensageria", () => {
    expect(loadConfig({ SQS_ENABLED: "false" }).sqs.enabled).toBe(false);
  });

  it.each(["0", "70000", "abc", "-1", "30.5"])("rejeita PORT inválida %p", (PORT) => {
    expect(() => loadConfig({ PORT })).toThrow(/PORT/);
  });

  it.each(["0", "abc", "-3"])("rejeita POOL_MAX inválido %p", (POOL_MAX) => {
    expect(() => loadConfig({ POOL_MAX })).toThrow(/POOL_MAX/);
  });

  it.each(["yes", "1x"])("rejeita RUN_MIGRATIONS inválido %p", (RUN_MIGRATIONS) => {
    expect(() => loadConfig({ RUN_MIGRATIONS })).toThrow(/RUN_MIGRATIONS/);
  });

  it.each([
    ["SQS_MAX_RECEIVES", "0"],
    ["SQS_WAIT_SECONDS", "21"],
    ["SQS_VISIBILITY_TIMEOUT", "0"],
    ["SHUTDOWN_DRAIN_MS", "999999"],
  ])("rejeita %s fora da faixa (%p)", (name, value) => {
    expect(() => loadConfig({ [name]: value })).toThrow(new RegExp(name));
  });

  it.each(["sem-sufixo", "tem espaço.fifo", ".fifo"])("rejeita nome de fila FIFO inválido %p", (name) => {
    expect(() => loadConfig({ QUEUE_REQUESTS: name })).toThrow(/QUEUE_REQUESTS/);
  });
});
