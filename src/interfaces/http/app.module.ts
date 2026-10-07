import { GetQueueAttributesCommand, type SQSClient } from "@aws-sdk/client-sqs";
import {
  Inject,
  Injectable,
  Module,
  type BeforeApplicationShutdown,
  type DynamicModule,
  type MiddlewareConsumer,
  type NestModule,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
  type Provider,
} from "@nestjs/common";
import { APP_FILTER, APP_GUARD } from "@nestjs/core";
import type { AuthConfig, MessagingConfig } from "../../config";
import { CreateWallet } from "../../application/create-wallet";
import { OutboxRelay } from "../../application/outbox-relay";
import { PendingReferenceWorker } from "../../application/pending-reference-worker";
import type {
  Clock,
  IdGenerator,
  LogContextPort,
  LoggerPort,
  MetricsPort,
  Observability,
  UnitOfWork,
} from "../../application/ports";
import { ProcessWagerTransaction } from "../../application/process-wager-transaction";
import { WalletQueries } from "../../application/queries";
import { ReprocessPendingReference } from "../../application/reprocess-pending-reference";
import { NoopProviderIdentity } from "../../infrastructure/auth/noop-provider-identity";
import { OidcProviderIdentity } from "../../infrastructure/auth/oidc-provider-identity";
import { BackgroundWorkers } from "../../infrastructure/messaging/background-workers";
import { createSqsClient, ensureQueues, type QueueUrls } from "../../infrastructure/messaging/sqs";
import { SqsEventPublisher } from "../../infrastructure/messaging/sqs-event-publisher";
import { JsonLogger } from "../../infrastructure/observability/json-logger";
import { AsyncLocalLogContext } from "../../infrastructure/observability/log-context";
import { PrometheusMetrics } from "../../infrastructure/observability/metrics";
import { createOrm, type AppOrm } from "../../infrastructure/persistence/orm";
import { MikroOrmUnitOfWork } from "../../infrastructure/persistence/unit-of-work";
import { SystemClock, UuidV7Generator } from "../../infrastructure/system";
import { SqsWagerConsumer } from "../messaging/sqs-wager-consumer";
import { ApiExceptionFilter } from "./api-exception.filter";
import { AuthGuard } from "./auth.guard";
import { CorrelationMiddleware } from "./correlation.middleware";
import { HealthController, type ReadinessCheck } from "./health.controller";
import { MetricsController } from "./metrics.controller";
import { TOKENS } from "./tokens";
import { WagerController } from "./wager.controller";
import { WalletsController } from "./wallets.controller";

export interface AppConfig {
  databaseUrl: string;
  poolMax?: number;
  /** `lock_timeout` do PostgreSQL nas conexões do ORM (ausente = sem limite). */
  lockTimeoutMs?: number;
  /** Sem isto a aplicação sobe só com a API HTTP (sem consumidor, relay nem worker de pendências). */
  messaging?: MessagingConfig;
  /** Ausente = `{ mode: "none" }` (sem autenticação; todo chamador é operador). */
  auth?: AuthConfig;
}

function identityProvider(auth: AuthConfig | undefined): Provider {
  if (auth?.mode === "oidc") {
    const { issuer, audience, jwksUrl, providerClaim, operatorRole } = auth;
    return {
      provide: TOKENS.PROVIDER_IDENTITY,
      useFactory: () => new OidcProviderIdentity({ issuer, audience, jwksUrl, providerClaim, operatorRole }),
    };
  }
  return { provide: TOKENS.PROVIDER_IDENTITY, useClass: NoopProviderIdentity };
}

@Injectable()
class OrmLifecycle implements OnApplicationShutdown {
  constructor(@Inject(TOKENS.ORM) private readonly orm: AppOrm) {}

  async onApplicationShutdown(): Promise<void> {
    await this.orm.close();
  }
}

/**
 * Os workers sobem depois que a aplicação está pronta e DESCEM antes de o ORM ser fechado
 * (`beforeApplicationShutdown` roda antes dos `onApplicationShutdown`), para o que está em andamento terminar.
 */
@Injectable()
class WorkersLifecycle implements OnApplicationBootstrap, BeforeApplicationShutdown, OnApplicationShutdown {
  constructor(
    @Inject(TOKENS.WORKERS) private readonly workers: BackgroundWorkers,
    @Inject(TOKENS.SQS_CLIENT) private readonly sqs: SQSClient,
  ) {}

  onApplicationBootstrap(): void {
    this.workers.start();
  }

  async beforeApplicationShutdown(): Promise<void> {
    await this.workers.stop();
  }

  onApplicationShutdown(): void {
    this.sqs.destroy();
  }
}

type UseCaseDeps = [UnitOfWork, Clock, IdGenerator, Observability];
const USE_CASE_INJECT = [TOKENS.UOW, TOKENS.CLOCK, TOKENS.IDS, TOKENS.OBSERVABILITY];

function messagingProviders(messaging: MessagingConfig): Provider[] {
  const { sqs, workers, instanceId } = messaging;

  return [
    { provide: TOKENS.SQS_CLIENT, useFactory: () => createSqsClient({ endpoint: sqs.endpoint, region: sqs.region }) },
    {
      provide: TOKENS.QUEUE_URLS,
      useFactory: (client: SQSClient) =>
        // Rede de segurança no broker: um recebimento acima do TETO do consumidor (que já envia à DLQ antes disso).
        ensureQueues(client, sqs.queues, {
          visibilityTimeoutSeconds: sqs.visibilityTimeoutSeconds,
          maxReceiveCount: sqs.receiveHardLimit + 1,
        }),
      inject: [TOKENS.SQS_CLIENT],
    },
    {
      provide: TOKENS.WORKERS,
      useFactory: (
        client: SQSClient,
        urls: QueueUrls,
        uow: UnitOfWork,
        clock: Clock,
        logger: LoggerPort,
        process: ProcessWagerTransaction,
        reprocess: ReprocessPendingReference,
        metrics: MetricsPort,
        context: LogContextPort,
      ) => {
        return new BackgroundWorkers({
          logger,
          consumer: workers.consumer
            ? new SqsWagerConsumer({
                client,
                queueUrl: urls.requests,
                dlqUrl: urls.requestsDlq,
                process,
                // Tentativas reais, contadas no banco numa transação própria (a do processamento foi desfeita).
                attempts: {
                  record: (consumerName, messageId, error) =>
                    uow.run((repos) => repos.inbox.recordFailedAttempt(consumerName, messageId, error, clock.now())),
                },
                logger,
                maxReceives: sqs.maxReceives,
                receiveHardLimit: sqs.receiveHardLimit,
                waitTimeSeconds: sqs.waitTimeSeconds,
                concurrency: workers.consumerConcurrency,
                prefetch: workers.consumerPrefetch,
                drainTimeoutMs: workers.drainTimeoutMs,
                metrics,
                context,
              })
            : undefined,
          relay: workers.outboxRelay
            ? new OutboxRelay({
                uow,
                publisher: new SqsEventPublisher(client, urls.events),
                clock,
                logger,
                instanceId,
                metrics,
                batchSize: workers.outboxBatchSize,
                runSize: workers.outboxRunSize,
                leaseMs: workers.outboxLeaseMs,
                maxAttempts: workers.outboxMaxAttempts,
              })
            : undefined,
          pendingReference: workers.pendingReference
            ? new PendingReferenceWorker({ uow, reprocess, clock, logger })
            : undefined,
        });
      },
      inject: [
        TOKENS.SQS_CLIENT,
        TOKENS.QUEUE_URLS,
        TOKENS.UOW,
        TOKENS.CLOCK,
        TOKENS.LOGGER,
        TOKENS.PROCESS,
        TOKENS.REPROCESS,
        TOKENS.METRICS,
        TOKENS.LOG_CONTEXT,
      ],
    },
    WorkersLifecycle,
  ];
}

@Module({})
export class AppModule implements NestModule {
  static register(config: AppConfig): DynamicModule {
    const { messaging } = config;

    return {
      module: AppModule,
      controllers: [HealthController, MetricsController, WalletsController, WagerController],
      providers: [
        { provide: TOKENS.ORM, useFactory: () => createOrm(config.databaseUrl, config.poolMax, config.lockTimeoutMs) },
        { provide: TOKENS.UOW, useFactory: (orm: AppOrm) => new MikroOrmUnitOfWork(orm), inject: [TOKENS.ORM] },
        { provide: TOKENS.CLOCK, useClass: SystemClock },
        { provide: TOKENS.IDS, useClass: UuidV7Generator },
        { provide: TOKENS.LOG_CONTEXT, useClass: AsyncLocalLogContext },
        {
          provide: TOKENS.LOGGER,
          useFactory: (context: LogContextPort) => new JsonLogger({ context }),
          inject: [TOKENS.LOG_CONTEXT],
        },
        { provide: TOKENS.METRICS, useClass: PrometheusMetrics },
        {
          provide: TOKENS.OBSERVABILITY,
          useFactory: (metrics: MetricsPort, logger: LoggerPort, context: LogContextPort): Observability => ({
            metrics,
            logger,
            context,
          }),
          inject: [TOKENS.METRICS, TOKENS.LOGGER, TOKENS.LOG_CONTEXT],
        },
        {
          // O lag da outbox é lido no scrape, direto do banco (independe de o relay rodar nesta instância).
          provide: TOKENS.METRICS_BINDING,
          useFactory: (metrics: MetricsPort, uow: UnitOfWork) => {
            if (metrics instanceof PrometheusMetrics) {
              metrics.setOutboxLagSource(async () => {
                const oldest = await uow.run((repos) => repos.outbox.oldestPendingAt());
                return oldest ? Math.max(0, (Date.now() - oldest.getTime()) / 1000) : 0;
              });
              metrics.setOutboxQuarantineSource(() => uow.run((repos) => repos.outbox.countQuarantined()));
            }
            return true;
          },
          inject: [TOKENS.METRICS, TOKENS.UOW],
        },
        {
          provide: TOKENS.CREATE_WALLET,
          useFactory: (...[uow, clock, ids]: UseCaseDeps) => new CreateWallet(uow, clock, ids),
          inject: USE_CASE_INJECT,
        },
        {
          provide: TOKENS.PROCESS,
          useFactory: (...[uow, clock, ids, observability]: UseCaseDeps) =>
            new ProcessWagerTransaction(uow, clock, ids, observability),
          inject: USE_CASE_INJECT,
        },
        {
          provide: TOKENS.REPROCESS,
          useFactory: (...[uow, clock, ids, observability]: UseCaseDeps) =>
            new ReprocessPendingReference(uow, clock, ids, observability),
          inject: USE_CASE_INJECT,
        },
        {
          provide: TOKENS.QUERIES,
          useFactory: (uow: UnitOfWork, logger: LoggerPort, metrics: MetricsPort) => new WalletQueries(uow, logger, metrics),
          inject: [TOKENS.UOW, TOKENS.LOGGER, TOKENS.METRICS],
        },
        {
          provide: TOKENS.READINESS_CHECKS,
          useFactory: (orm: AppOrm, client?: SQSClient, urls?: QueueUrls): ReadinessCheck[] => {
            const checks: ReadinessCheck[] = [
              {
                name: "postgres",
                check: async () => {
                  await orm.em.getConnection().execute("select 1");
                },
              },
            ];
            if (client && urls) {
              checks.push({
                name: "sqs",
                check: async () => {
                  await client.send(new GetQueueAttributesCommand({ QueueUrl: urls.requests, AttributeNames: ["QueueArn"] }));
                },
              });
            }
            return checks;
          },
          inject: messaging ? [TOKENS.ORM, TOKENS.SQS_CLIENT, TOKENS.QUEUE_URLS] : [TOKENS.ORM],
        },
        // Autenticação: OIDC (IdP externo) com AUTH_MODE=oidc; sem isso, NoopProviderIdentity (ARCHITECTURE.md §15).
        identityProvider(config.auth),
        { provide: APP_GUARD, useClass: AuthGuard },
        { provide: APP_FILTER, useClass: ApiExceptionFilter },
        OrmLifecycle,
        ...(messaging ? messagingProviders(messaging) : []),
      ],
    };
  }

  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(CorrelationMiddleware).forRoutes("*");
  }
}
