import { Counter, Gauge, Histogram, Registry } from "prom-client";
import type { MetricsPort } from "../../application/ports";

const SECONDS_BUCKETS = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

/**
 * Métricas Prometheus. Cada instância tem seu próprio registro (nada global), então testes e múltiplas
 * instâncias no mesmo processo não se misturam. Os rótulos têm cardinalidade limitada: status, tipo,
 * origem, motivo e a ROTA (padrão `/wallets/:walletId`), nunca ids.
 */
export class PrometheusMetrics implements MetricsPort {
  private readonly registry = new Registry();
  private outboxLagSource: (() => Promise<number | undefined>) | undefined;
  private outboxQuarantineSource: (() => Promise<number>) | undefined;

  private readonly transactions = new Counter({
    name: "wager_transactions_total",
    help: "Transações decididas, por status e tipo",
    labelNames: ["status", "kind"],
    registers: [this.registry],
  });
  private readonly duplicates = new Counter({
    name: "wager_duplicates_total",
    help: "Duplicatas detectadas (replay HTTP ou redelivery de fila), por origem",
    labelNames: ["source"],
    registers: [this.registry],
  });
  private readonly retries = new Counter({
    name: "retries_total",
    help: "Retentativas, por componente",
    labelNames: ["component"],
    registers: [this.registry],
  });
  private readonly outboxEvents = new Counter({
    name: "outbox_published_events_total",
    help: "Eventos da outbox publicados no broker",
    registers: [this.registry],
  });
  private readonly quarantined = new Counter({
    name: "outbox_quarantined_total",
    help: "Eventos da outbox que esgotaram as tentativas de publicação e foram para a quarentena",
    registers: [this.registry],
  });
  private readonly dlq = new Counter({
    name: "dlq_messages_total",
    help: "Mensagens enviadas à DLQ, por motivo",
    labelNames: ["reason"],
    registers: [this.registry],
  });
  private readonly lockConflicts = new Counter({
    name: "lock_conflicts_total",
    help: "Conflitos de lock (deadlock, serialização, lock_timeout, conflito de versão)",
    labelNames: ["kind"],
    registers: [this.registry],
  });
  private readonly reconciliationDivergences = new Counter({
    name: "reconciliation_divergences_total",
    help: "Divergências entre saldo armazenado e saldo reconstruído pelo ledger",
    registers: [this.registry],
  });
  private readonly lockWaits = new Histogram({
    name: "wallet_lock_wait_seconds",
    help: "Tempo esperando o lock da wallet (contenção em wallet quente)",
    buckets: SECONDS_BUCKETS,
    registers: [this.registry],
  });
  private readonly processing = new Histogram({
    name: "wager_processing_duration_seconds",
    help: "Latência de processamento de uma transação, por origem e resultado",
    labelNames: ["source", "outcome"],
    buckets: SECONDS_BUCKETS,
    registers: [this.registry],
  });
  private readonly http = new Histogram({
    name: "http_request_duration_seconds",
    help: "Latência das requisições HTTP",
    labelNames: ["method", "route", "status"],
    buckets: SECONDS_BUCKETS,
    registers: [this.registry],
  });

  constructor() {
    // O lag é lido no scrape, direto do banco: não depende de o relay estar rodando nesta instância.
    const source = (): (() => Promise<number | undefined>) | undefined => this.outboxLagSource;
    new Gauge({
      name: "outbox_lag_seconds",
      help: "Idade, em segundos, do evento pendente mais antigo da outbox",
      registers: [this.registry],
      async collect() {
        try {
          this.set((await source()?.()) ?? 0);
        } catch {
          // Banco indisponível no scrape: mantém o último valor; o readiness já sinaliza o problema.
        }
      },
    });
    const quarantineSource = (): (() => Promise<number>) | undefined => this.outboxQuarantineSource;
    new Gauge({
      name: "outbox_quarantined_events",
      help: "Eventos da outbox em quarentena, aguardando ação (requeue) — alerta se > 0",
      registers: [this.registry],
      async collect() {
        try {
          this.set((await quarantineSource()?.()) ?? 0);
        } catch {
          // idem ao lag: mantém o último valor
        }
      },
    });
  }

  get contentType(): string {
    return this.registry.contentType;
  }

  render(): Promise<string> {
    return this.registry.metrics();
  }

  setOutboxLagSource(source: () => Promise<number | undefined>): void {
    this.outboxLagSource = source;
  }

  setOutboxQuarantineSource(source: () => Promise<number>): void {
    this.outboxQuarantineSource = source;
  }

  outboxQuarantined(): void {
    this.quarantined.inc();
  }

  reconciliationDivergence(): void {
    this.reconciliationDivergences.inc();
  }

  transactionDecided(status: string, kind: string): void {
    this.transactions.inc({ status, kind });
  }

  duplicateDetected(source: string): void {
    this.duplicates.inc({ source });
  }

  retry(component: string): void {
    this.retries.inc({ component });
  }

  outboxPublished(count: number): void {
    if (count > 0) this.outboxEvents.inc(count);
  }

  deadLettered(reason: string): void {
    this.dlq.inc({ reason });
  }

  lockConflict(kind: string): void {
    this.lockConflicts.inc({ kind });
  }

  lockWait(seconds: number): void {
    this.lockWaits.observe(seconds);
  }

  processingLatency(seconds: number, source: string, outcome: string): void {
    this.processing.observe({ source, outcome }, seconds);
  }

  httpRequest(method: string, route: string, status: number, seconds: number): void {
    this.http.observe({ method, route, status: String(status) }, seconds);
  }
}
