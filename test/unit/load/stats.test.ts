import { describe, expect, it } from "bun:test";
import {
  bucketsOf,
  counterDelta,
  deltaBuckets,
  histogramQuantile,
  intendedStarts,
  LatencyRecorder,
  parsePrometheus,
  percentile,
  sumOf,
} from "../../load/stats";

describe("percentile (nearest-rank)", () => {
  const oneToHundred = Array.from({ length: 100 }, (_, i) => i + 1);

  it("p50, p95, p99 e p100 de 1..100", () => {
    expect(percentile(oneToHundred, 50)).toBe(50);
    expect(percentile(oneToHundred, 95)).toBe(95);
    expect(percentile(oneToHundred, 99)).toBe(99);
    expect(percentile(oneToHundred, 100)).toBe(100);
  });

  it("p0 devolve o mínimo; um único valor vale para qualquer percentil", () => {
    expect(percentile(oneToHundred, 0)).toBe(1);
    expect(percentile([42], 99)).toBe(42);
  });

  it("amostra vazia → NaN (nunca um número inventado)", () => {
    expect(percentile([], 50)).toBeNaN();
  });

  it("não interpola: o resultado é sempre um valor realmente observado", () => {
    expect(percentile([10, 20, 30, 40], 50)).toBe(20);
    expect(percentile([10, 20, 30, 40], 75)).toBe(30);
  });
});

describe("LatencyRecorder", () => {
  it("resume entradas fora de ordem", () => {
    const recorder = new LatencyRecorder();
    for (const v of [5, 1, 3, 2, 4]) recorder.record(v);

    expect(recorder.count).toBe(5);
    expect(recorder.summary()).toEqual({ count: 5, min: 1, mean: 3, p50: 3, p90: 5, p95: 5, p99: 5, p999: 5, max: 5 });
  });

  it("p99 de 1000 amostras destaca a cauda", () => {
    const recorder = new LatencyRecorder();
    for (let i = 0; i < 990; i++) recorder.record(10);
    for (let i = 0; i < 10; i++) recorder.record(500);
    const s = recorder.summary();
    expect(s.p50).toBe(10);
    expect(s.p99).toBe(10);
    expect(s.p999).toBe(500);
    expect(s.max).toBe(500);
    expect(s.mean).toBeCloseTo(14.9, 5);
  });

  it("vazio → resumo com NaN, sem lançar", () => {
    const s = new LatencyRecorder().summary();
    expect(s.count).toBe(0);
    expect(s.p99).toBeNaN();
  });
});

describe("parsePrometheus", () => {
  const text = [
    "# HELP wager_transactions_total Transações decididas",
    "# TYPE wager_transactions_total counter",
    'wager_transactions_total{status="PROCESSED",kind="BET"} 120',
    'wager_transactions_total{status="REJECTED",kind="BET"} 3',
    "outbox_lag_seconds 1.5",
    "",
    'wallet_lock_wait_seconds_bucket{le="0.005"} 10',
    'wallet_lock_wait_seconds_bucket{le="+Inf"} 12',
    "wallet_lock_wait_seconds_sum 0.25",
    'odd_label{a="x \\"y\\" z"} 7',
    "tiny 1e-3",
  ].join("\n");

  it("lê contadores com rótulos, gauges sem rótulos e ignora comentários e linhas vazias", () => {
    const samples = parsePrometheus(text);
    expect(samples.find((s) => s.name === "outbox_lag_seconds")?.value).toBe(1.5);
    expect(samples.filter((s) => s.name === "wager_transactions_total")).toEqual([
      { name: "wager_transactions_total", labels: { status: "PROCESSED", kind: "BET" }, value: 120 },
      { name: "wager_transactions_total", labels: { status: "REJECTED", kind: "BET" }, value: 3 },
    ]);
  });

  it("entende +Inf, notação científica e aspas escapadas nos rótulos", () => {
    const samples = parsePrometheus(text);
    expect(samples.find((s) => s.labels["le"] === "+Inf")?.value).toBe(12);
    expect(samples.find((s) => s.name === "tiny")?.value).toBe(0.001);
    expect(samples.find((s) => s.name === "odd_label")?.labels["a"]).toBe('x "y" z');
  });

  it("sumOf soma as séries que casam com o filtro", () => {
    const samples = parsePrometheus(text);
    expect(sumOf(samples, "wager_transactions_total")).toBe(123);
    expect(sumOf(samples, "wager_transactions_total", { status: "REJECTED" })).toBe(3);
    expect(sumOf(samples, "nao_existe")).toBe(0);
  });
});

describe("counterDelta", () => {
  const before = parsePrometheus('retries_total{component="a"} 2\nretries_total{component="b"} 1');
  const after = parsePrometheus('retries_total{component="a"} 7\nretries_total{component="b"} 1');

  it("diferença entre dois scrapes", () => {
    expect(counterDelta(before, after, "retries_total")).toBe(5);
    expect(counterDelta(before, after, "retries_total", { component: "b" })).toBe(0);
  });

  it("contador que voltou a zero (instância reiniciada) conta o valor atual, nunca negativo", () => {
    const restarted = parsePrometheus('retries_total{component="a"} 1');
    expect(counterDelta(before, restarted, "retries_total", { component: "a" })).toBe(1);
  });
});

describe("histogramas", () => {
  const scrape = (a: number, b: number, inf: number) =>
    parsePrometheus(
      [`h_bucket{le="0.1"} ${a}`, `h_bucket{le="1"} ${b}`, `h_bucket{le="+Inf"} ${inf}`].join("\n"),
    ).map((s) => ({ ...s, name: "h_bucket" }));

  it("bucketsOf agrupa por limite e soma séries (várias instâncias)", () => {
    const merged = bucketsOf([...scrape(10, 20, 30), ...scrape(5, 5, 5)], "h");
    expect(merged).toEqual([
      { le: 0.1, count: 15 },
      { le: 1, count: 25 },
      { le: Number.POSITIVE_INFINITY, count: 35 },
    ]);
  });

  it("deltaBuckets subtrai dois scrapes", () => {
    const delta = deltaBuckets(bucketsOf(scrape(10, 20, 30), "h"), bucketsOf(scrape(60, 90, 130), "h"));
    expect(delta.map((b) => b.count)).toEqual([50, 70, 100]);
  });

  it("histogramQuantile interpola dentro do bucket (como o PromQL)", () => {
    const buckets = [
      { le: 0.1, count: 50 },
      { le: 1, count: 90 },
      { le: Number.POSITIVE_INFINITY, count: 100 },
    ];
    expect(histogramQuantile(0.5, buckets)).toBeCloseTo(0.1, 10);
    expect(histogramQuantile(0.7, buckets)).toBeCloseTo(0.55, 10);
    expect(histogramQuantile(0.99, buckets)).toBe(1); // cai no +Inf: o melhor que se sabe é o último limite finito
  });

  it("sem observações → NaN", () => {
    expect(histogramQuantile(0.99, [{ le: 1, count: 0 }, { le: Number.POSITIVE_INFINITY, count: 0 }])).toBeNaN();
  });
});

describe("intendedStarts (carga em malha aberta)", () => {
  it("distribui as requisições em intervalos iguais", () => {
    expect(intendedStarts(4, 1000)).toEqual([0, 250, 500, 750]);
  });

  it("a quantidade é taxa × duração", () => {
    expect(intendedStarts(100, 2000)).toHaveLength(200);
    expect(intendedStarts(100, 2000).at(-1)).toBeCloseTo(1990, 5);
  });

  it("taxa ou duração zero → nada a enviar", () => {
    expect(intendedStarts(0, 1000)).toEqual([]);
    expect(intendedStarts(10, 0)).toEqual([]);
  });
});
