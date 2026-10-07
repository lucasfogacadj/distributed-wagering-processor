import { hostname } from "node:os";
import type { QueueNames } from "./infrastructure/messaging/sqs";

export interface SqsRuntimeConfig {
  enabled: boolean;
  /** Ausente = AWS real. Local: LocalStack/MiniStack. */
  endpoint?: string | undefined;
  region: string;
  queues: QueueNames;
  /** Tentativas de processamento que FALHARAM (contadas no banco) antes de a mensagem ir para a DLQ. */
  maxReceives: number;
  /**
   * Teto de recebimentos do SQS, usado só quando a tentativa não pôde ser contada no banco (ex.: PostgreSQL fora).
   * O `maxReceiveCount` da redrive policy da fila é este valor + 1 (rede de segurança no broker).
   */
  receiveHardLimit: number;
  visibilityTimeoutSeconds: number;
  waitTimeSeconds: number;
}

export interface WorkersConfig {
  consumer: boolean;
  consumerConcurrency: number;
  consumerPrefetch: boolean;
  outboxRelay: boolean;
  pendingReference: boolean;
  drainTimeoutMs: number;
  /** Quanto tempo um publisher é dono dos eventos que reivindicou; se morrer, outro assume depois disso. */
  outboxLeaseMs: number;
  /** Eventos reivindicados por rodada (somando todas as wallets). */
  outboxBatchSize: number;
  /** Eventos CONSECUTIVOS de uma mesma wallet por rodada (a unidade de publicação é a sequência da wallet). */
  outboxRunSize: number;
  /** Tentativas de publicação de um evento antes da quarentena. */
  outboxMaxAttempts: number;
}

/**
 * Autenticação da API HTTP.
 *  - `none`: sem autenticação (desenvolvimento e testes); todo chamador é tratado como operador.
 *  - `oidc`: valida o JWT (client_credentials) emitido por um IdP externo (Keycloak no docker-compose).
 */
export type AuthConfig =
  | { mode: "none" }
  | {
      mode: "oidc";
      /** `iss` esperado no token, ex.: http://localhost:8180/realms/wagering. */
      issuer: string;
      /** `aud` esperado no token. */
      audience: string;
      /** Onde buscar as chaves públicas (JWKS). Pode diferir do issuer quando a rede interna usa outro host. */
      jwksUrl: string;
      /** Claim com o providerId do chamador (mapper do client no IdP). */
      providerClaim: string;
      /** Papel de realm que dá acesso de operador (backoffice): criar wallet, ledger, reconciliação. */
      operatorRole: string;
    };

/** O que a aplicação precisa para ligar a mensageria (consumidor, relay da outbox, worker de pendências). */
export interface MessagingConfig {
  sqs: SqsRuntimeConfig;
  workers: WorkersConfig;
  /** Dono do lease da outbox; único por instância/processo. */
  instanceId: string;
}

export interface RuntimeConfig {
  databaseUrl: string;
  port: number;
  poolMax: number;
  /** Aplica as migrations pendentes ao subir (seguro com várias instâncias: advisory lock). */
  runMigrations: boolean;
  /** `lock_timeout` do PostgreSQL nas conexões da aplicação. */
  lockTimeoutMs: number;
  instanceId: string;
  sqs: SqsRuntimeConfig;
  workers: WorkersConfig;
  auth: AuthConfig;
}

type Env = Record<string, string | undefined>;

function integer(env: Env, name: string, fallback: number, min: number, max: number): number {
  const raw = env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!/^\d+$/.test(raw) || value < min || value > max) {
    throw new Error(`${name} inválido (${JSON.stringify(raw)}): esperado inteiro entre ${min} e ${max}`);
  }
  return value;
}

function boolean(env: Env, name: string, fallback: boolean): boolean {
  const raw = env[name];
  if (raw === undefined) return fallback;
  if (raw === "true") return true;
  if (raw === "false") return false;
  throw new Error(`${name} inválido (${JSON.stringify(raw)}): esperado "true" ou "false"`);
}

function fifoQueueName(env: Env, name: string, fallback: string): string {
  const raw = env[name] ?? fallback;
  if (!/^[A-Za-z0-9_-]{1,75}\.fifo$/.test(raw)) {
    throw new Error(`${name} inválido (${JSON.stringify(raw)}): esperado nome de fila FIFO terminado em ".fifo"`);
  }
  return raw;
}

function url(env: Env, name: string, fallback?: string): string {
  const raw = env[name] ?? fallback;
  if (raw === undefined || !/^https?:\/\/[^\s]+$/.test(raw)) {
    throw new Error(`${name} inválido (${JSON.stringify(raw)}): esperado URL http(s)`);
  }
  return raw.replace(/\/+$/, "");
}

function loadAuth(env: Env): AuthConfig {
  const mode = env["AUTH_MODE"] ?? "none";
  if (mode === "none") return { mode };
  if (mode !== "oidc") throw new Error(`AUTH_MODE inválido (${JSON.stringify(mode)}): esperado "none" ou "oidc"`);

  const issuer = url(env, "OIDC_ISSUER");
  return {
    mode,
    issuer,
    audience: env["OIDC_AUDIENCE"] ?? "wagering-api",
    jwksUrl: url(env, "OIDC_JWKS_URL", `${issuer}/protocol/openid-connect/certs`),
    providerClaim: env["OIDC_PROVIDER_CLAIM"] ?? "provider_id",
    operatorRole: env["OIDC_OPERATOR_ROLE"] ?? "wagering-operator",
  };
}

export function loadConfig(env: Env = process.env): RuntimeConfig {
  const rawEndpoint = env["SQS_ENDPOINT"];
  const maxReceives = integer(env, "SQS_MAX_RECEIVES", 5, 1, 100);
  const receiveHardLimit = integer(env, "SQS_RECEIVE_HARD_LIMIT", maxReceives * 3, 1, 999);
  if (receiveHardLimit < maxReceives) {
    throw new Error(`SQS_RECEIVE_HARD_LIMIT (${receiveHardLimit}) deve ser >= SQS_MAX_RECEIVES (${maxReceives})`);
  }

  return {
    databaseUrl: env["DATABASE_URL"] ?? "postgres://wager:wager@localhost:5433/wager",
    port: integer(env, "PORT", 3000, 1, 65535),
    poolMax: integer(env, "POOL_MAX", 20, 1, 500),
    runMigrations: boolean(env, "RUN_MIGRATIONS", true),
    lockTimeoutMs: integer(env, "LOCK_TIMEOUT_MS", 10_000, 100, 600_000),
    instanceId: env["INSTANCE_ID"] ?? `${hostname()}-${process.pid}`,
    sqs: {
      enabled: boolean(env, "SQS_ENABLED", true),
      endpoint: rawEndpoint === undefined ? "http://localhost:4566" : rawEndpoint === "" ? undefined : rawEndpoint,
      region: env["AWS_REGION"] ?? "us-east-1",
      queues: {
        requests: fifoQueueName(env, "QUEUE_REQUESTS", "wager-transactions.fifo"),
        requestsDlq: fifoQueueName(env, "QUEUE_REQUESTS_DLQ", "wager-transactions-dlq.fifo"),
        events: fifoQueueName(env, "QUEUE_EVENTS", "wallet-events.fifo"),
      },
      maxReceives,
      receiveHardLimit,
      visibilityTimeoutSeconds: integer(env, "SQS_VISIBILITY_TIMEOUT", 30, 1, 43_200),
      waitTimeSeconds: integer(env, "SQS_WAIT_SECONDS", 5, 0, 20),
    },
    workers: {
      consumer: boolean(env, "CONSUMER_ENABLED", true),
      consumerConcurrency: integer(env, "CONSUMER_CONCURRENCY", 4, 1, 64),
      consumerPrefetch: boolean(env, "CONSUMER_PREFETCH", false),
      outboxRelay: boolean(env, "OUTBOX_RELAY_ENABLED", true),
      pendingReference: boolean(env, "PENDING_REFERENCE_WORKER_ENABLED", true),
      drainTimeoutMs: integer(env, "SHUTDOWN_DRAIN_MS", 20_000, 0, 120_000),
      outboxLeaseMs: integer(env, "OUTBOX_LEASE_MS", 30_000, 500, 600_000),
      outboxBatchSize: integer(env, "OUTBOX_BATCH_SIZE", 200, 1, 2_000),
      outboxRunSize: integer(env, "OUTBOX_RUN_SIZE", 20, 1, 200),
      outboxMaxAttempts: integer(env, "OUTBOX_MAX_ATTEMPTS", 10, 1, 1_000),
    },
    auth: loadAuth(env),
  };
}
