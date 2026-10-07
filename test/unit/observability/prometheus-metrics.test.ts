import { describe, expect, it } from "bun:test";
import { PrometheusMetrics } from "../../../src/infrastructure/observability/metrics";

describe("PrometheusMetrics", () => {
  it("transações por status e tipo", async () => {
    const m = new PrometheusMetrics();
    m.transactionDecided("PROCESSED", "BET");
    m.transactionDecided("PROCESSED", "BET");
    m.transactionDecided("REJECTED", "BET");
    const text = await m.render();
    expect(text).toContain('wager_transactions_total{status="PROCESSED",kind="BET"} 2');
    expect(text).toContain('wager_transactions_total{status="REJECTED",kind="BET"} 1');
  });

  it("duplicatas detectadas, retries, DLQ e conflitos de lock", async () => {
    const m = new PrometheusMetrics();
    m.duplicateDetected("http");
    m.duplicateDetected("sqs");
    m.duplicateDetected("sqs");
    m.retry("sqs_consumer");
    m.deadLettered("invalid_message");
    m.lockConflict("deadlock");
    m.reconciliationDivergence();
    const text = await m.render();
    expect(text).toContain('wager_duplicates_total{source="http"} 1');
    expect(text).toContain('wager_duplicates_total{source="sqs"} 2');
    expect(text).toContain('retries_total{component="sqs_consumer"} 1');
    expect(text).toContain('dlq_messages_total{reason="invalid_message"} 1');
    expect(text).toContain('lock_conflicts_total{kind="deadlock"} 1');
    expect(text).toContain("reconciliation_divergences_total 1");
  });

  it("eventos da outbox publicados somam por lote (e lote vazio não conta)", async () => {
    const m = new PrometheusMetrics();
    m.outboxPublished(20);
    m.outboxPublished(5);
    m.outboxPublished(0);
    expect(await m.render()).toContain("outbox_published_events_total 25");
  });

  it("espera pelo lock da wallet e latência de processamento são histogramas", async () => {
    const m = new PrometheusMetrics();
    m.lockWait(0.002);
    m.lockWait(0.4);
    m.processingLatency(0.05, "http", "PROCESSED");
    m.httpRequest("POST", "/wagering/transactions", 200, 0.03);
    const text = await m.render();
    expect(text).toContain("wallet_lock_wait_seconds_count 2");
    expect(text).toMatch(/wallet_lock_wait_seconds_bucket\{le="0\.005"\} 1/);
    expect(text).toContain('wager_processing_duration_seconds_count{source="http",outcome="PROCESSED"} 1');
    expect(text).toContain('http_request_duration_seconds_count{method="POST",route="/wagering/transactions",status="200"} 1');
  });

  it("outbox lag é lido na hora do scrape", async () => {
    const m = new PrometheusMetrics();
    let lag = 12.5;
    m.setOutboxLagSource(async () => lag);
    expect(await m.render()).toContain("outbox_lag_seconds 12.5");
    lag = 0;
    expect(await m.render()).toContain("outbox_lag_seconds 0");
  });

  it("outbox lag: sem fonte ou com fonte falhando, o scrape continua funcionando", async () => {
    const withoutSource = new PrometheusMetrics();
    expect(await withoutSource.render()).toContain("outbox_lag_seconds 0");

    const failing = new PrometheusMetrics();
    failing.setOutboxLagSource(async () => {
      throw new Error("banco fora");
    });
    expect(await failing.render()).toContain("outbox_lag_seconds");
  });

  it("cada instância tem seu próprio registro", async () => {
    const a = new PrometheusMetrics();
    const b = new PrometheusMetrics();
    a.retry("x");
    expect(await b.render()).not.toContain('retries_total{component="x"}');
  });

  it("expõe o content-type do formato texto do Prometheus", () => {
    expect(new PrometheusMetrics().contentType).toContain("text/plain");
  });
});
