/**
 * Estatísticas do teste de carga. Puras e testadas (test/unit/load/stats.test.ts): um número de latência só vale
 * se o cálculo que o produziu for confiável.
 */

/** Percentil por *nearest-rank*: sempre um valor realmente observado, nunca interpolado. `sortedAsc` ordenado. */
export function percentile(sortedAsc: ArrayLike<number>, p: number): number {
  if (sortedAsc.length === 0) return Number.NaN;
  const rank = Math.ceil((p / 100) * sortedAsc.length);
  return sortedAsc[Math.min(sortedAsc.length, Math.max(1, rank)) - 1]!;
}

export interface LatencySummary {
  count: number;
  min: number;
  mean: number;
  p50: number;
  p90: number;
  p95: number;
  p99: number;
  p999: number;
  max: number;
}

/** Guarda TODAS as amostras (sem bucketização): o p99/p99.9 reportado é o da amostra real. */
export class LatencyRecorder {
  private readonly values: number[] = [];

  record(ms: number): void {
    this.values.push(ms);
  }

  get count(): number {
    return this.values.length;
  }

  summary(): LatencySummary {
    const sorted = Float64Array.from(this.values).sort();
    const count = sorted.length;
    if (count === 0) {
      const nan = Number.NaN;
      return { count: 0, min: nan, mean: nan, p50: nan, p90: nan, p95: nan, p99: nan, p999: nan, max: nan };
    }
    let sum = 0;
    for (const v of sorted) sum += v;
    return {
      count,
      min: sorted[0]!,
      mean: sum / count,
      p50: percentile(sorted, 50),
      p90: percentile(sorted, 90),
      p95: percentile(sorted, 95),
      p99: percentile(sorted, 99),
      p999: percentile(sorted, 99.9),
      max: sorted[count - 1]!,
    };
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Formato texto do Prometheus (o que o /metrics devolve)
// ---------------------------------------------------------------------------------------------------------------

export interface PromSample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

const LINE = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{((?:[^}"]|"(?:[^"\\]|\\.)*")*)\})?\s+(\S+)/;
const LABEL = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g;

function unescapeLabel(value: string): string {
  return value.replace(/\\(["\\n])/g, (_match, char: string) => (char === "n" ? "\n" : char));
}

function parseValue(raw: string): number {
  if (raw === "+Inf") return Number.POSITIVE_INFINITY;
  if (raw === "-Inf") return Number.NEGATIVE_INFINITY;
  return Number(raw); // "NaN" → NaN
}

export function parsePrometheus(text: string): PromSample[] {
  const samples: PromSample[] = [];
  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#")) continue;
    const match = LINE.exec(line);
    if (!match) continue;

    const labels: Record<string, string> = {};
    for (const label of (match[2] ?? "").matchAll(LABEL)) labels[label[1]!] = unescapeLabel(label[2]!);
    samples.push({ name: match[1]!, labels, value: parseValue(match[3]!) });
  }
  return samples;
}

function matches(sample: PromSample, name: string, filter: Record<string, string>): boolean {
  return sample.name === name && Object.entries(filter).every(([key, value]) => sample.labels[key] === value);
}

export function sumOf(samples: PromSample[], name: string, filter: Record<string, string> = {}): number {
  return samples.filter((s) => matches(s, name, filter)).reduce((total, s) => total + s.value, 0);
}

/** Variação de um contador entre dois scrapes. Se o contador "voltou" (processo reiniciado), vale o valor atual. */
export function counterDelta(
  before: PromSample[],
  after: PromSample[],
  name: string,
  filter: Record<string, string> = {},
): number {
  const was = sumOf(before, name, filter);
  const now = sumOf(after, name, filter);
  return now < was ? now : now - was;
}

export interface Bucket {
  le: number;
  count: number;
}

/** Buckets cumulativos de um histograma, somando séries com o mesmo limite (várias instâncias). */
export function bucketsOf(samples: PromSample[], baseName: string): Bucket[] {
  const byLimit = new Map<number, number>();
  for (const sample of samples) {
    if (sample.name !== `${baseName}_bucket`) continue;
    const le = parseValue(sample.labels["le"] ?? "NaN");
    byLimit.set(le, (byLimit.get(le) ?? 0) + sample.value);
  }
  return [...byLimit.entries()].map(([le, count]) => ({ le, count })).sort((a, b) => a.le - b.le);
}

export function deltaBuckets(before: Bucket[], after: Bucket[]): Bucket[] {
  const was = new Map(before.map((b) => [b.le, b.count]));
  return after.map((b) => {
    const previous = was.get(b.le) ?? 0;
    return { le: b.le, count: b.count < previous ? b.count : b.count - previous };
  });
}

/** Quantil de um histograma por interpolação linear dentro do bucket (mesma ideia do `histogram_quantile` do PromQL). */
export function histogramQuantile(q: number, buckets: Bucket[]): number {
  const total = buckets.at(-1)?.count ?? 0;
  if (total === 0) return Number.NaN;

  const rank = q * total;
  const index = buckets.findIndex((b) => b.count >= rank);
  if (index === -1) return Number.NaN;

  const lower = index > 0 ? buckets[index - 1]!.le : 0;
  const lowerCount = index > 0 ? buckets[index - 1]!.count : 0;
  const upper = buckets[index]!;
  if (!Number.isFinite(upper.le)) return lower; // caiu no +Inf: o melhor que se sabe é o último limite finito

  const span = upper.count - lowerCount;
  const fraction = span === 0 ? 1 : (rank - lowerCount) / span;
  return lower + (upper.le - lower) * fraction;
}

/**
 * Instantes programados (ms desde o início) de uma carga em MALHA ABERTA: a requisição k deve começar em k/taxa,
 * quer o sistema consiga acompanhar ou não. A latência é medida a partir desse instante programado, o que evita a
 * *coordinated omission* (um sistema saturado que atrasa o próprio gerador de carga pareceria mais rápido do que é).
 */
export function intendedStarts(ratePerSecond: number, durationMs: number): number[] {
  const count = Math.floor((ratePerSecond * durationMs) / 1000);
  if (ratePerSecond <= 0 || count <= 0) return [];
  return Array.from({ length: count }, (_, i) => (i * 1000) / ratePerSecond);
}
