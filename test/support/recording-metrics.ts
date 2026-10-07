import { PrometheusMetrics } from "../../src/infrastructure/observability/metrics";

export interface MetricCall {
  method: string;
  args: unknown[];
}

/** Métricas reais (Prometheus) que também registram cada chamada, para os testes inspecionarem. */
export class RecordingMetrics extends PrometheusMetrics {
  readonly calls: MetricCall[] = [];

  callsOf(method: string): unknown[][] {
    return this.calls.filter((c) => c.method === method).map((c) => c.args);
  }

  get divergences(): number {
    return this.callsOf("reconciliationDivergence").length;
  }

  override reconciliationDivergence(): void {
    this.calls.push({ method: "reconciliationDivergence", args: [] });
    super.reconciliationDivergence();
  }

  override transactionDecided(status: string, kind: string): void {
    this.calls.push({ method: "transactionDecided", args: [status, kind] });
    super.transactionDecided(status, kind);
  }

  override duplicateDetected(source: string): void {
    this.calls.push({ method: "duplicateDetected", args: [source] });
    super.duplicateDetected(source);
  }

  override retry(component: string): void {
    this.calls.push({ method: "retry", args: [component] });
    super.retry(component);
  }

  override outboxPublished(count: number): void {
    this.calls.push({ method: "outboxPublished", args: [count] });
    super.outboxPublished(count);
  }

  override outboxQuarantined(): void {
    this.calls.push({ method: "outboxQuarantined", args: [] });
    super.outboxQuarantined();
  }

  override deadLettered(reason: string): void {
    this.calls.push({ method: "deadLettered", args: [reason] });
    super.deadLettered(reason);
  }

  override lockConflict(kind: string): void {
    this.calls.push({ method: "lockConflict", args: [kind] });
    super.lockConflict(kind);
  }

  override lockWait(seconds: number): void {
    this.calls.push({ method: "lockWait", args: [seconds] });
    super.lockWait(seconds);
  }

  override processingLatency(seconds: number, source: string, outcome: string): void {
    this.calls.push({ method: "processingLatency", args: [seconds, source, outcome] });
    super.processingLatency(seconds, source, outcome);
  }

  override httpRequest(method: string, route: string, status: number, seconds: number): void {
    this.calls.push({ method: "httpRequest", args: [method, route, status, seconds] });
    super.httpRequest(method, route, status, seconds);
  }
}
