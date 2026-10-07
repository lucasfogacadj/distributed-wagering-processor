/**
 * Teste de carga — `bun run test:load`.
 *
 * Sobe instâncias REAIS da aplicação (processos `bun`) contra PostgreSQL e LocalStack em containers, gera carga HTTP
 * (malha fechada e malha aberta) e carga pela fila SQS, coleta métricas do cliente e do servidor e, no fim de cada
 * cenário, VERIFICA as invariantes de dinheiro: um teste de carga que corrompe saldo e reporta uma vazão bonita
 * seria enganoso. Metodologia e leitura dos números: LOAD_TEST.md.
 *
 * Variáveis: LOAD_PROFILE=smoke|standard · LOAD_SCENARIOS=spread-1x,hot-3x,… · LOAD_CONCURRENCY · LOAD_MEASURE_SEC ·
 * LOAD_WARMUP_SEC · LOAD_OPEN_FRACTION · LOAD_WALLETS · LOAD_SQS_MESSAGES.
 */
import { SendMessageCommand } from "@aws-sdk/client-sqs";
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import os from "node:os";
import type { Pool } from "pg";
import { createClusterEnv, type AppInstance, type ClusterEnv } from "../support/cluster";
import { brl, createWalletVia, sleep, wagerBody, wagerMessage, type TestWallet } from "../support/wager-fixtures";
import {
  bucketsOf,
  counterDelta,
  deltaBuckets,
  histogramQuantile,
  intendedStarts,
  LatencyRecorder,
  parsePrometheus,
  sumOf,
  type Bucket,
  type LatencySummary,
  type PromSample,
} from "./stats";

// ---------------------------------------------------------------------------------------------------------------
// Configuração
// ---------------------------------------------------------------------------------------------------------------

interface Profile {
  warmupSec: number;
  measureSec: number;
  concurrency: number;
  openFraction: number;
  wallets: number;
  sqsMessages: number;
}

const PROFILES: Record<string, Profile> = {
  smoke: { warmupSec: 1, measureSec: 4, concurrency: 16, openFraction: 0.7, wallets: 50, sqsMessages: 300 },
  standard: { warmupSec: 5, measureSec: 20, concurrency: 64, openFraction: 0.7, wallets: 500, sqsMessages: 3000 },
};

function number(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} inválido: ${raw}`);
  return value;
}

const profileName = process.env["LOAD_PROFILE"] ?? "standard";
const profile = PROFILES[profileName];
if (!profile) throw new Error(`LOAD_PROFILE desconhecido: ${profileName} (use ${Object.keys(PROFILES).join(" | ")})`);

const CONFIG: Profile & { profile: string } = {
  profile: profileName,
  warmupSec: number("LOAD_WARMUP_SEC", profile.warmupSec),
  measureSec: number("LOAD_MEASURE_SEC", profile.measureSec),
  concurrency: number("LOAD_CONCURRENCY", profile.concurrency),
  openFraction: number("LOAD_OPEN_FRACTION", profile.openFraction),
  wallets: number("LOAD_WALLETS", profile.wallets),
  sqsMessages: number("LOAD_SQS_MESSAGES", profile.sqsMessages),
};

interface Scenario {
  name: string;
  description: string;
  instances: number;
  /** Fração das requisições que vai para UMA única wallet (0 = espalhado, 1 = tudo em uma wallet). */
  hotShare: number;
  channel: "http" | "sqs";
}

const SCENARIOS: Scenario[] = [
  { name: "spread-1x", description: "tráfego espalhado entre as wallets, 1 instância", instances: 1, hotShare: 0, channel: "http" },
  { name: "spread-3x", description: "tráfego espalhado entre as wallets, 3 instâncias", instances: 3, hotShare: 0, channel: "http" },
  { name: "hot-3x", description: "100% do tráfego em UMA wallet, 3 instâncias", instances: 3, hotShare: 1, channel: "http" },
  { name: "mixed-3x", description: "90% espalhado + 10% em uma wallet quente, 3 instâncias", instances: 3, hotShare: 0.1, channel: "http" },
  { name: "sqs-3x", description: "carga pela fila SQS FIFO (ponta a ponta), 3 instâncias", instances: 3, hotShare: 0, channel: "sqs" },
];

const selected = process.env["LOAD_SCENARIOS"]?.split(",").map((s) => s.trim());
const PLAN = selected ? SCENARIOS.filter((s) => selected.includes(s.name)) : SCENARIOS;
if (PLAN.length === 0) throw new Error(`LOAD_SCENARIOS não casa com nenhum cenário: ${SCENARIOS.map((s) => s.name).join(", ")}`);

/**
 * Instâncias do teste de carga usam o pool padrão da aplicação (o cluster de testes usa 10 para economizar).
 * LOAD_DISABLE_RELAY=true desliga o relay da outbox: experimento de atribuição (quanto do custo é a publicação?).
 */
/**
 * LOAD_RELAY_INSTANCES=N sobe N instâncias DEDICADAS à publicação (só o relay: sem tráfego, consumidor nem worker de
 * pendências) e desliga o relay nas instâncias que atendem a carga: separa a CPU/event loop da publicação da escrita.
 */
const RELAY_INSTANCES = Number(process.env["LOAD_RELAY_INSTANCES"] ?? 0);
if (!Number.isInteger(RELAY_INSTANCES) || RELAY_INSTANCES < 0) throw new Error("LOAD_RELAY_INSTANCES inválido");
const INSTANCE_ENV: Record<string, string> = {
  POOL_MAX: "20",
  ...(process.env["LOAD_DISABLE_RELAY"] === "true" || RELAY_INSTANCES > 0 ? { OUTBOX_RELAY_ENABLED: "false" } : {}),
};

async function startDedicatedRelays(env: ClusterEnv, scenario: Scenario): Promise<void> {
  if (RELAY_INSTANCES === 0) return;
  await Promise.all(
    Array.from({ length: RELAY_INSTANCES }, (_, i) =>
      env.startOne({ id: `${scenario.name}-relay-${i + 1}`, consumer: false, pendingReference: false, outboxRelay: true, outboxBatchSize: 50, extraEnv: { POOL_MAX: "10" } }),
    ),
  );
}
/**
 * LOAD_SQS_PRELOAD=true: no cenário da fila, primeiro enfileira TUDO (sem consumidor), depois sobe as instâncias que
 * consomem e mede a taxa de DRENAGEM do backlog. Sem isso, produtor, consumidores e relay disputam o mesmo broker e a
 * "vazão de consumo" fica limitada pelo produtor, não pelo consumidor.
 */
const SQS_PRELOAD = process.env["LOAD_SQS_PRELOAD"] === "true";
const RICH_BALANCE = "1000000.00";
const HOT_BALANCE = "999999999999.00";
const MAX_INFLIGHT = 2_000;
const DRAIN_CAP_SEC = 120;

// ---------------------------------------------------------------------------------------------------------------
// Tipos do relatório
// ---------------------------------------------------------------------------------------------------------------

interface ServerDelta {
  transactions: Record<string, number>;
  duplicates: number;
  retries: Record<string, number>;
  dlq: number;
  lockConflicts: Record<string, number>;
  lockWait: { count: number; meanMs: number; p50Ms: number; p95Ms: number; p99Ms: number };
  serverLatency: { count: number; p50Ms: number; p95Ms: number; p99Ms: number };
  pgDeadlocks: number;
  pgCommits: number;
  pgRollbacks: number;
}

interface PhaseResult {
  mode: "closed-loop" | "open-loop";
  durationSec: number;
  concurrency?: number;
  targetRate?: number;
  attempted: number;
  /** 200 (processada) — o que conta como vazão. */
  ok: number;
  /** 202 / 422: respostas de negócio válidas, não erros. */
  business: number;
  /** Rede, timeout e 5xx. */
  errors: number;
  errorRate: number;
  byStatus: Record<string, number>;
  throughput: number;
  latency: LatencySummary;
  /** Só na malha aberta: requisições que o gerador não conseguiu disparar porque havia 2000 em voo. */
  generatorSkipped?: number;
  /** Só na malha aberta: atraso máximo do disparo em relação ao instante programado. */
  generatorMaxLagMs?: number;
  server: ServerDelta;
  lockWaiters: { avg: number; max: number };
  outboxLagSec: { avg: number; max: number };
  outboxBacklogAtEnd: number;
  outboxDrainSec: number | null;
}

interface InvariantCheck {
  name: string;
  ok: boolean;
  detail: string;
}

interface ScenarioResult {
  scenario: Scenario;
  setupSec: number;
  phases: PhaseResult[];
  sqs?: SqsResult;
  invariants: InvariantCheck[];
}

interface SqsResult {
  messages: number;
  producerSec: number;
  producerRate: number;
  totalSec: number;
  consumptionRate: number;
  endToEnd: LatencySummary;
  processed: number;
  server: ServerDelta;
  outboxLagSec: { avg: number; max: number };
  outboxDrainSec: number | null;
}

// ---------------------------------------------------------------------------------------------------------------
// Coleta de métricas do servidor
// ---------------------------------------------------------------------------------------------------------------

interface Snapshot {
  scrapes: PromSample[][];
  pg: { deadlocks: number; commits: number; rollbacks: number };
}

async function scrape(instance: AppInstance): Promise<PromSample[]> {
  const response = await fetch(`${instance.baseUrl}/metrics`);
  return parsePrometheus(await response.text());
}

async function snapshot(instances: AppInstance[], pool: Pool): Promise<Snapshot> {
  const [scrapes, pg] = await Promise.all([
    Promise.all(instances.map(scrape)),
    pool.query<{ deadlocks: string; commits: string; rollbacks: string }>(
      `SELECT deadlocks::text, xact_commit::text AS commits, xact_rollback::text AS rollbacks
         FROM pg_stat_database WHERE datname = current_database()`,
    ),
  ]);
  const row = pg.rows[0]!;
  return { scrapes, pg: { deadlocks: Number(row.deadlocks), commits: Number(row.commits), rollbacks: Number(row.rollbacks) } };
}

function mergeBuckets(lists: Bucket[][]): Bucket[] {
  const byLimit = new Map<number, number>();
  for (const list of lists) for (const b of list) byLimit.set(b.le, (byLimit.get(b.le) ?? 0) + b.count);
  return [...byLimit.entries()].map(([le, count]) => ({ le, count })).sort((a, b) => a.le - b.le);
}

function serverDelta(before: Snapshot, after: Snapshot): ServerDelta {
  const n = before.scrapes.length;
  const sum = (name: string, filter: Record<string, string> = {}) =>
    Array.from({ length: n }, (_, i) => counterDelta(before.scrapes[i]!, after.scrapes[i]!, name, filter)).reduce((a, b) => a + b, 0);
  const histogram = (base: string) =>
    mergeBuckets(Array.from({ length: n }, (_, i) => deltaBuckets(bucketsOf(before.scrapes[i]!, base), bucketsOf(after.scrapes[i]!, base))));

  const labelValues = (name: string, label: string) =>
    [...new Set(after.scrapes.flat().filter((s) => s.name === name).map((s) => s.labels[label] ?? ""))];
  const breakdown = (name: string, label: string) =>
    Object.fromEntries(labelValues(name, label).map((v) => [v, sum(name, { [label]: v })]).filter(([, v]) => (v as number) > 0));

  const lockBuckets = histogram("wallet_lock_wait_seconds");
  const lockCount = sum("wallet_lock_wait_seconds_count");
  const lockSum = sum("wallet_lock_wait_seconds_sum");
  const procBuckets = histogram("wager_processing_duration_seconds");
  const ms = (seconds: number) => seconds * 1000;

  return {
    transactions: breakdown("wager_transactions_total", "status"),
    duplicates: sum("wager_duplicates_total"),
    retries: breakdown("retries_total", "component"),
    dlq: sum("dlq_messages_total"),
    lockConflicts: breakdown("lock_conflicts_total", "kind"),
    lockWait: {
      count: lockCount,
      meanMs: lockCount > 0 ? ms(lockSum / lockCount) : Number.NaN,
      p50Ms: ms(histogramQuantile(0.5, lockBuckets)),
      p95Ms: ms(histogramQuantile(0.95, lockBuckets)),
      p99Ms: ms(histogramQuantile(0.99, lockBuckets)),
    },
    serverLatency: {
      count: procBuckets.at(-1)?.count ?? 0,
      p50Ms: ms(histogramQuantile(0.5, procBuckets)),
      p95Ms: ms(histogramQuantile(0.95, procBuckets)),
      p99Ms: ms(histogramQuantile(0.99, procBuckets)),
    },
    pgDeadlocks: after.pg.deadlocks - before.pg.deadlocks,
    pgCommits: after.pg.commits - before.pg.commits,
    pgRollbacks: after.pg.rollbacks - before.pg.rollbacks,
  };
}

/** Amostra um valor a cada `intervalMs` até `stop()`. */
function sampler(read: () => Promise<number>, intervalMs: number): { stop(): Promise<{ avg: number; max: number }> } {
  const values: number[] = [];
  let running = true;
  const loop = (async () => {
    while (running) {
      try {
        values.push(await read());
      } catch {
        // uma amostra perdida não invalida o experimento
      }
      await sleep(intervalMs);
    }
  })();
  return {
    async stop() {
      running = false;
      await loop;
      return values.length === 0
        ? { avg: Number.NaN, max: Number.NaN }
        : { avg: values.reduce((a, b) => a + b, 0) / values.length, max: Math.max(...values) };
    },
  };
}

const lockWaiters = (pool: Pool) => async () =>
  Number(
    (await pool.query<{ n: string }>(
      "SELECT count(*)::text AS n FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'",
    )).rows[0]!.n,
  );

const outboxLag = (instance: AppInstance) => async () => sumOf(await scrape(instance), "outbox_lag_seconds");

async function outboxPending(pool: Pool): Promise<number> {
  return Number((await pool.query<{ n: string }>("SELECT count(*)::text AS n FROM outbox_messages WHERE published_at IS NULL")).rows[0]!.n);
}

/** Quanto demora para a outbox esvaziar depois que a carga termina (o "outbox lag" de pós-carga). */
async function drainOutbox(pool: Pool): Promise<number | null> {
  const started = performance.now();
  while ((performance.now() - started) / 1000 < DRAIN_CAP_SEC) {
    if ((await outboxPending(pool)) === 0) return (performance.now() - started) / 1000;
    await sleep(250);
  }
  return null;
}

// ---------------------------------------------------------------------------------------------------------------
// Carga HTTP
// ---------------------------------------------------------------------------------------------------------------

interface Workload {
  instances: AppInstance[];
  wallets: TestWallet[];
  hot: TestWallet | undefined;
  hotShare: number;
  tag: string;
  pool: Pool;
}

let requestSeq = 0;

/**
 * Contabilidade de TODAS as requisições do cenário (aquecimento incluído), para a verificação final ser exata:
 * `confirmadas ≤ gravadas no banco ≤ confirmadas + sem resposta definitiva`. Uma requisição sem resposta
 * (timeout, conexão cortada) pode ou não ter sido gravada — mas nunca mais de uma vez.
 */
const totals = { acknowledged: 0, unknown: 0 };

function pickWallet(w: Workload): TestWallet {
  if (w.hot && Math.random() < w.hotShare) return w.hot;
  return w.wallets[Math.floor(Math.random() * w.wallets.length)]!;
}

async function sendBet(w: Workload, instance: AppInstance, wallet: TestWallet): Promise<number> {
  const external = `ld-${w.tag}-${requestSeq++}`;
  try {
    const response = await fetch(`${instance.baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": `load:${external}` },
      body: JSON.stringify({
        providerId: "load",
        externalTransactionId: external,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "r",
        gameId: "g",
        kind: "BET",
        money: { amount: "1.00", currency: "BRL" },
      }),
      signal: AbortSignal.timeout(30_000),
    });
    await response.arrayBuffer(); // libera a conexão
    if (response.status === 200 || response.status === 202 || response.status === 422) totals.acknowledged += 1;
    else totals.unknown += 1;
    return response.status;
  } catch {
    totals.unknown += 1;
    return 0; // rede ou timeout
  }
}

class Tally {
  attempted = 0;
  ok = 0;
  business = 0;
  errors = 0;
  readonly byStatus: Record<string, number> = {};
  readonly latency = new LatencyRecorder();

  record(status: number, ms: number): void {
    this.attempted += 1;
    this.byStatus[String(status)] = (this.byStatus[String(status)] ?? 0) + 1;
    if (status === 200) this.ok += 1;
    else if (status === 202 || status === 422) this.business += 1;
    else this.errors += 1;
    this.latency.record(ms);
  }
}

async function measured(
  w: Workload,
  mode: PhaseResult["mode"],
  durationSec: number,
  run: (tally: Tally) => Promise<{ generatorSkipped?: number; generatorMaxLagMs?: number }>,
  extra: Pick<PhaseResult, "concurrency" | "targetRate">,
): Promise<PhaseResult> {
  const tally = new Tally();
  const before = await snapshot(w.instances, w.pool);
  const waiters = sampler(lockWaiters(w.pool), 250);
  const lag = sampler(outboxLag(w.instances[0]!), 1_000);

  const generator = await run(tally);

  const after = await snapshot(w.instances, w.pool);
  const waiterStats = await waiters.stop();
  const lagStats = await lag.stop();
  const backlog = await outboxPending(w.pool);
  const drain = await drainOutbox(w.pool);

  return {
    mode,
    durationSec,
    ...extra,
    attempted: tally.attempted,
    ok: tally.ok,
    business: tally.business,
    errors: tally.errors,
    errorRate: tally.attempted > 0 ? tally.errors / tally.attempted : 0,
    byStatus: tally.byStatus,
    throughput: tally.ok / durationSec,
    latency: tally.latency.summary(),
    ...generator,
    server: serverDelta(before, after),
    lockWaiters: waiterStats,
    outboxLagSec: lagStats,
    outboxBacklogAtEnd: backlog,
    outboxDrainSec: drain,
  };
}

/** Malha fechada: N clientes em paralelo, cada um envia a próxima assim que recebe a resposta → vazão máxima. */
async function closedLoop(w: Workload): Promise<PhaseResult> {
  const state = { stop: false, measuring: false };
  let rr = 0;
  let tally: Tally | undefined;

  const workers = Array.from({ length: CONFIG.concurrency }, async () => {
    while (!state.stop) {
      const instance = w.instances[rr++ % w.instances.length]!;
      const wallet = pickWallet(w);
      const counted = state.measuring;
      const started = performance.now();
      const status = await sendBet(w, instance, wallet);
      if (counted && tally) tally.record(status, performance.now() - started);
    }
  });

  await sleep(CONFIG.warmupSec * 1000);
  const result = await measured(
    w,
    "closed-loop",
    CONFIG.measureSec,
    async (t) => {
      tally = t;
      state.measuring = true;
      await sleep(CONFIG.measureSec * 1000);
      state.measuring = false;
      state.stop = true;
      await Promise.all(workers);
      return {};
    },
    { concurrency: CONFIG.concurrency },
  );
  return result;
}

/** Malha aberta: taxa constante; a latência conta desde o instante PROGRAMADO (evita coordinated omission). */
async function openLoop(w: Workload, ratePerSecond: number): Promise<PhaseResult> {
  return measured(
    w,
    "open-loop",
    CONFIG.measureSec,
    async (tally) => {
      const offsets = intendedStarts(ratePerSecond, CONFIG.measureSec * 1000);
      const inflight = new Set<Promise<void>>();
      const lagRecorder = new LatencyRecorder();
      let skipped = 0;
      let rr = 0;
      const t0 = performance.now();

      for (const offset of offsets) {
        const target = t0 + offset;
        const wait = target - performance.now();
        if (wait > 2) await sleep(wait);
        lagRecorder.record(Math.max(0, performance.now() - target));
        if (inflight.size >= MAX_INFLIGHT) {
          skipped += 1;
          continue;
        }
        const instance = w.instances[rr++ % w.instances.length]!;
        const wallet = pickWallet(w);
        const request = (async () => {
          const status = await sendBet(w, instance, wallet);
          tally.record(status, performance.now() - target);
        })();
        inflight.add(request);
        void request.finally(() => inflight.delete(request));
      }
      await Promise.all(inflight);
      return { generatorSkipped: skipped, generatorMaxLagMs: lagRecorder.summary().max };
    },
    { targetRate: ratePerSecond },
  );
}

// ---------------------------------------------------------------------------------------------------------------
// Verificação de correção
// ---------------------------------------------------------------------------------------------------------------

async function verifyInvariants(pool: Pool, provider: string, acknowledged: number, unknown = 0): Promise<InvariantCheck[]> {
  const one = async (sql: string, params: unknown[] = []) => Number((await pool.query<{ n: string }>(sql, params)).rows[0]!.n);
  const checks: InvariantCheck[] = [];

  const ledgerDivergent = await one(
    `SELECT count(*)::text AS n FROM wallets w
      WHERE w.balance <> COALESCE((SELECT SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END)
                                     FROM ledger_entries l WHERE l.wallet_id = w.id), 0)`,
  );
  checks.push({ name: "wallet.balance == saldo reconstruído pelo ledger", ok: ledgerDivergent === 0, detail: `${ledgerDivergent} wallets divergentes` });

  const txDivergent = await one(
    `SELECT count(*)::text AS n FROM (
       SELECT w.id FROM wallets w LEFT JOIN wager_transactions t ON t.wallet_id = w.id AND t.status = 'PROCESSED'
        GROUP BY w.id, w.balance
       HAVING w.balance <> COALESCE(SUM(CASE t.kind WHEN 'OPENING' THEN t.amount WHEN 'WIN' THEN t.amount WHEN 'REFUND' THEN t.amount
                                                      WHEN 'BET' THEN -t.amount ELSE 0 END), 0)) d`,
  );
  checks.push({ name: "saldo == recomputação independente (só pela tabela de transações)", ok: txDivergent === 0, detail: `${txDivergent} wallets divergentes` });

  const processed = await one("SELECT count(*)::text AS n FROM wager_transactions WHERE status = 'PROCESSED' AND kind <> 'LOSS'");
  const entries = await one("SELECT count(*)::text AS n FROM ledger_entries");
  checks.push({ name: "um lançamento por transação aplicada (nenhum débito duplicado ou perdido)", ok: processed === entries, detail: `${processed} aplicadas, ${entries} lançamentos` });

  const stuck = await one("SELECT count(*)::text AS n FROM wager_transactions WHERE kind <> 'OPENING' AND status NOT IN ('PROCESSED','REJECTED')");
  checks.push({ name: "nenhuma transação presa em estado não terminal", ok: stuck === 0, detail: `${stuck} presas` });

  const persisted = await one("SELECT count(*)::text AS n FROM wager_transactions WHERE provider_id = $1", [provider]);
  checks.push({
    name: "requisições confirmadas ao cliente ≤ gravadas no banco ≤ confirmadas + sem resposta definitiva (nada gravado em dobro)",
    ok: persisted >= acknowledged && persisted <= acknowledged + unknown,
    detail: `${acknowledged} confirmadas + ${unknown} sem resposta definitiva (rede/timeout/5xx) → ${persisted} no banco`,
  });

  return checks;
}

// ---------------------------------------------------------------------------------------------------------------
// Cenários
// ---------------------------------------------------------------------------------------------------------------

async function createWallets(instances: AppInstance[], count: number, balance: string): Promise<TestWallet[]> {
  const wallets: TestWallet[] = [];
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(32, count) }, async () => {
      while (next < count) {
        const index = next++;
        wallets[index] = await createWalletVia(instances[index % instances.length]!, balance);
      }
    }),
  );
  return wallets;
}

async function runHttpScenario(scenario: Scenario): Promise<ScenarioResult> {
  totals.acknowledged = 0;
  totals.unknown = 0;
  const setupStarted = performance.now();
  const env = await createClusterEnv();
  try {
    const instances = await env.start(scenario.instances, { id: scenario.name, extraEnv: INSTANCE_ENV });
    await startDedicatedRelays(env, scenario);
    const wallets = await createWallets(instances, scenario.hotShare === 1 ? 1 : CONFIG.wallets, RICH_BALANCE);
    const hot = scenario.hotShare > 0 ? (scenario.hotShare === 1 ? wallets[0]! : await createWalletVia(instances[0]!, HOT_BALANCE)) : undefined;
    const workload: Workload = { instances, wallets, hot, hotShare: scenario.hotShare, tag: scenario.name, pool: env.pool };
    const setupSec = (performance.now() - setupStarted) / 1000;

    log(`  ▸ ${scenario.name}: malha fechada (${CONFIG.concurrency} clientes, ${CONFIG.warmupSec}s de aquecimento + ${CONFIG.measureSec}s medidos)…`);
    const closed = await closedLoop(workload);
    log(`    ${closed.throughput.toFixed(0)} req/s · p99 ${closed.latency.p99.toFixed(0)} ms · erros ${closed.errors}`);

    const rate = Math.max(1, Math.round(closed.throughput * CONFIG.openFraction));
    log(`  ▸ ${scenario.name}: malha aberta a ${rate} req/s (${Math.round(CONFIG.openFraction * 100)}% do máximo), ${CONFIG.measureSec}s…`);
    const open = await openLoop(workload, rate);
    log(`    ${open.throughput.toFixed(0)} req/s · p99 ${open.latency.p99.toFixed(0)} ms · erros ${open.errors}`);

    const invariants = await verifyInvariants(env.pool, "load", totals.acknowledged, totals.unknown);
    return { scenario, setupSec, phases: [closed, open], invariants };
  } finally {
    await env.close();
  }
}

async function runSqsScenario(scenario: Scenario): Promise<ScenarioResult> {
  const setupStarted = performance.now();
  const env = await createClusterEnv();
  try {
    const instances = await env.start(scenario.instances, { id: scenario.name, extraEnv: INSTANCE_ENV, ...(SQS_PRELOAD ? { consumer: false } : {}) });
    await startDedicatedRelays(env, scenario);
    const wallets = await createWallets(instances, CONFIG.wallets, RICH_BALANCE);
    const setupSec = (performance.now() - setupStarted) / 1000;
    const total = CONFIG.sqsMessages;
    log(`  ▸ ${scenario.name}: ${total} mensagens pela fila FIFO (grupo = wallet)…`);

    const before = await snapshot(instances, env.pool);
    const lag = sampler(outboxLag(instances[0]!), 1_000);
    const sentAt = new Map<string, number>();
    let next = 0;
    const t0 = performance.now();

    await Promise.all(
      Array.from({ length: 50 }, async () => {
        while (next < total) {
          const index = next++;
          const wallet = wallets[Math.floor(Math.random() * wallets.length)]!;
          const external = `ld-sqs-${index}`;
          const message = wagerMessage(wagerBody(wallet, { providerId: "load-sqs", externalTransactionId: external, money: brl("1.00") }));
          sentAt.set(external, Date.now());
          await env.queues.client.send(
            new SendMessageCommand({
              QueueUrl: env.queues.urls.requests,
              MessageBody: JSON.stringify(message),
              MessageGroupId: wallet.id,
              MessageDeduplicationId: randomUUID(),
            }),
          );
        }
      }),
    );
    const producerSec = (performance.now() - t0) / 1000;
    let consumeStart = t0;
    if (SQS_PRELOAD) {
      // Backlog pronto; agora sobem os consumidores e a medição passa a ser a drenagem.
      await env.start(scenario.instances, { id: `${scenario.name}-consumer`, extraEnv: INSTANCE_ENV });
      consumeStart = performance.now();
    }

    const processedCount = async () =>
      Number(
        (await env.pool.query<{ n: string }>("SELECT count(*)::text AS n FROM wager_transactions WHERE provider_id = 'load-sqs' AND status IN ('PROCESSED','REJECTED')"))
          .rows[0]!.n,
      );
    let processed = 0;
    const deadline = performance.now() + 240_000;
    while (performance.now() < deadline) {
      processed = await processedCount();
      if (processed >= total) break;
      await sleep(250);
    }
    const totalSec = (performance.now() - consumeStart) / 1000;

    const after = await snapshot(instances, env.pool);
    const lagStats = await lag.stop();
    const drain = await drainOutbox(env.pool);

    const rows = await env.pool.query<{ external_transaction_id: string; processed_at: Date }>(
      "SELECT external_transaction_id, processed_at FROM wager_transactions WHERE provider_id = 'load-sqs' AND processed_at IS NOT NULL",
    );
    const endToEnd = new LatencyRecorder();
    for (const row of rows.rows) {
      const sent = sentAt.get(row.external_transaction_id);
      if (sent !== undefined) endToEnd.record(new Date(row.processed_at).getTime() - sent);
    }

    const sqs: SqsResult = {
      messages: total,
      producerSec,
      producerRate: total / producerSec,
      totalSec,
      consumptionRate: processed / totalSec,
      endToEnd: endToEnd.summary(),
      processed,
      server: serverDelta(before, after),
      outboxLagSec: lagStats,
      outboxDrainSec: drain,
    };
    log(`    produtor ${sqs.producerRate.toFixed(0)} msg/s · consumo ${sqs.consumptionRate.toFixed(0)} msg/s · ponta a ponta p99 ${sqs.endToEnd.p99.toFixed(0)} ms`);

    const invariants = await verifyInvariants(env.pool, "load-sqs", processed);
    invariants.push({ name: "todas as mensagens foram processadas", ok: processed === total, detail: `${processed}/${total}` });
    invariants.push({ name: "nada foi para a DLQ", ok: sqs.server.dlq === 0, detail: `${sqs.server.dlq} mensagens` });
    return { scenario, setupSec, phases: [], sqs, invariants };
  } finally {
    await env.close();
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Ambiente e relatório
// ---------------------------------------------------------------------------------------------------------------

function log(message: string): void {
  process.stdout.write(`${message}\n`);
}

async function describeEnvironment(env: ClusterEnv): Promise<Record<string, string | number>> {
  const cpu = os.cpus();
  const docker = Bun.spawnSync(["docker", "version", "--format", "{{.Server.Version}} ({{.Server.Os}}/{{.Server.Arch}})"]);
  const settings = await env.pool.query<{ name: string; setting: string; unit: string | null }>(
    "SELECT name, setting, unit FROM pg_settings WHERE name IN ('max_connections','shared_buffers','synchronous_commit','fsync','wal_level')",
  );
  const pg = await env.pool.query<{ v: string }>("SELECT version() AS v");
  return {
    data: new Date().toISOString(),
    sistema: `${os.type()} ${os.release()} (${os.arch()})`,
    cpu: `${cpu[0]?.model.trim() ?? "?"} × ${cpu.length} núcleos lógicos`,
    memoria: `${(os.totalmem() / 1024 ** 3).toFixed(1)} GB`,
    bun: Bun.version,
    docker: docker.success ? docker.stdout.toString().trim() : "indisponível",
    postgres: pg.rows[0]!.v.split(" on ")[0]!,
    postgresConfig: settings.rows.map((r) => `${r.name}=${r.setting}${r.unit ?? ""}`).join(", "),
    localstack: "3.8 (SQS)",
    app: `instâncias = processos bun · POOL_MAX=${INSTANCE_ENV.POOL_MAX} · LOCK_TIMEOUT_MS=15000 · outbox lote=50 lease=30s · SQS visibility=3s`,
    cliente: "gerador de carga no MESMO host que o banco e as instâncias (compete por CPU)",
  };
}

const f = (n: number, digits = 1) => (Number.isFinite(n) ? n.toFixed(digits) : "—");
const pct = (n: number) => `${(n * 100).toFixed(2)}%`;

function renderPhase(p: PhaseResult): string {
  const header =
    p.mode === "closed-loop"
      ? `**Malha fechada** — ${p.concurrency} clientes, ${p.durationSec}s medidos (vazão máxima sustentada)`
      : `**Malha aberta** — taxa constante de ${p.targetRate} req/s, ${p.durationSec}s (latência desde o instante programado)`;
  const lines = [
    header,
    "",
    "| métrica | valor |",
    "|---|---|",
    `| requisições | ${p.attempted} (200: ${p.ok} · 202/422: ${p.business} · erros: ${p.errors}) |`,
    `| **vazão** (200/s) | **${f(p.throughput, 0)}** |`,
    `| latência p50 / p95 / p99 / p99.9 / máx (ms) | ${f(p.latency.p50)} / ${f(p.latency.p95)} / **${f(p.latency.p99)}** / ${f(p.latency.p999)} / ${f(p.latency.max)} |`,
    `| **taxa de erro** (rede, timeout, 5xx) | **${pct(p.errorRate)}** |`,
    `| status HTTP | ${Object.entries(p.byStatus).map(([s, n]) => `${s === "0" ? "rede/timeout" : s}: ${n}`).join(" · ")} |`,
  ];
  if (p.mode === "open-loop") {
    lines.push(`| gerador | ${p.generatorSkipped ?? 0} requisições não disparadas (limite de ${MAX_INFLIGHT} em voo) · atraso máximo de disparo ${f(p.generatorMaxLagMs ?? Number.NaN)} ms |`);
  }
  lines.push(
    `| latência no servidor p50 / p95 / p99 (ms) | ${f(p.server.serverLatency.p50Ms)} / ${f(p.server.serverLatency.p95Ms)} / ${f(p.server.serverLatency.p99Ms)} (estimada pelo histograma, exclui rede e fila do cliente) |`,
    `| **conflitos de concorrência** | deadlocks PostgreSQL: **${p.server.pgDeadlocks}** · \`lock_conflicts_total\`: ${Object.keys(p.server.lockConflicts).length === 0 ? "0" : JSON.stringify(p.server.lockConflicts)} · retries: ${Object.keys(p.server.retries).length === 0 ? "0" : JSON.stringify(p.server.retries)} |`,
    `| espera pelo lock da wallet (ms) | média ${f(p.server.lockWait.meanMs)} · p50 ${f(p.server.lockWait.p50Ms)} · p95 ${f(p.server.lockWait.p95Ms)} · p99 ${f(p.server.lockWait.p99Ms)} (${p.server.lockWait.count} esperas) |`,
    `| sessões esperando lock (amostra a cada 250 ms) | média ${f(p.lockWaiters.avg, 2)} · máx ${f(p.lockWaiters.max, 0)} |`,
    `| **outbox lag** durante a carga (s) | média ${f(p.outboxLagSec.avg)} · máx ${f(p.outboxLagSec.max)} · pendentes ao fim: ${p.outboxBacklogAtEnd} · **drenou ${p.outboxDrainSec === null ? `em mais de ${DRAIN_CAP_SEC}s (não drenou)` : `${f(p.outboxDrainSec)}s após o fim da carga`}** |`,
    `| transações decididas (servidor) | ${JSON.stringify(p.server.transactions)} · duplicatas: ${p.server.duplicates} · DLQ: ${p.server.dlq} |`,
    `| commits / rollbacks no PostgreSQL | ${p.server.pgCommits} / ${p.server.pgRollbacks} |`,
  );
  return lines.join("\n");
}

function renderScenario(r: ScenarioResult): string {
  const out = [`### \`${r.scenario.name}\` — ${r.scenario.description}`, "", `Preparação (instâncias + wallets): ${f(r.setupSec)}s`, ""];
  for (const phase of r.phases) out.push(renderPhase(phase), "");
  if (r.sqs) {
    const s = r.sqs;
    out.push(
      `**Fila SQS FIFO** — ${s.messages} mensagens, grupo = wallet`,
      "",
      "| métrica | valor |",
      "|---|---|",
      `| produtor | ${f(s.producerRate, 0)} msg/s (${f(s.producerSec)}s) |`,
      `| **vazão de consumo** (processadas/s, do 1º envio ao último processado) | **${f(s.consumptionRate, 0)}** (${s.processed}/${s.messages} em ${f(s.totalSec)}s) |`,
      `| **latência ponta a ponta** envio → decisão, p50 / p95 / p99 / máx (ms) | ${f(s.endToEnd.p50, 0)} / ${f(s.endToEnd.p95, 0)} / **${f(s.endToEnd.p99, 0)}** / ${f(s.endToEnd.max, 0)} |`,
      `| conflitos / retries / DLQ | deadlocks PG: ${s.server.pgDeadlocks} · lock_conflicts: ${JSON.stringify(s.server.lockConflicts)} · retries: ${JSON.stringify(s.server.retries)} · DLQ: ${s.server.dlq} |`,
      `| **outbox lag** (s) | média ${f(s.outboxLagSec.avg)} · máx ${f(s.outboxLagSec.max)} · drenou ${s.outboxDrainSec === null ? "não drenou" : `${f(s.outboxDrainSec)}s após o último processamento`} |`,
      "",
    );
  }
  out.push("**Invariantes verificadas ao fim:**", "");
  for (const check of r.invariants) out.push(`- ${check.ok ? "✅" : "❌"} ${check.name} — ${check.detail}`);
  return out.join("\n");
}

function renderReport(environment: Record<string, string | number>, results: ScenarioResult[]): string {
  const summary = results.map((r) => {
    const closed = r.phases.find((p) => p.mode === "closed-loop");
    const open = r.phases.find((p) => p.mode === "open-loop");
    const ok = r.invariants.every((c) => c.ok);
    return r.sqs
      ? `| \`${r.scenario.name}\` | fila | ${f(r.sqs.consumptionRate, 0)} msg/s | — | ${f(r.sqs.endToEnd.p50, 0)} / ${f(r.sqs.endToEnd.p95, 0)} / ${f(r.sqs.endToEnd.p99, 0)} (ponta a ponta) | — | ${ok ? "✅" : "❌"} |`
      : `| \`${r.scenario.name}\` | HTTP | ${f(closed?.throughput ?? Number.NaN, 0)} req/s | ${f(open?.targetRate ?? Number.NaN, 0)} req/s | ${f(open?.latency.p50 ?? Number.NaN)} / ${f(open?.latency.p95 ?? Number.NaN)} / ${f(open?.latency.p99 ?? Number.NaN)} | ${pct(Math.max(closed?.errorRate ?? 0, open?.errorRate ?? 0))} | ${ok ? "✅" : "❌"} |`;
  });

  return [
    "# Relatório do teste de carga",
    "",
    "## Ambiente",
    "",
    ...Object.entries(environment).map(([k, v]) => `- **${k}**: ${v}`),
    "",
    "## Configuração",
    "",
    "```json",
    JSON.stringify(CONFIG, null, 2),
    "```",
    "",
    "## Resumo",
    "",
    "| cenário | canal | vazão máxima (malha fechada) | taxa da malha aberta | latência p50 / p95 / p99 (ms) na malha aberta | erro | invariantes |",
    "|---|---|---|---|---|---|---|",
    ...summary,
    "",
    "## Detalhe por cenário",
    "",
    ...results.flatMap((r) => [renderScenario(r), ""]),
  ].join("\n");
}

// ---------------------------------------------------------------------------------------------------------------

async function main(): Promise<void> {
  log(`Teste de carga — perfil "${CONFIG.profile}" — cenários: ${PLAN.map((s) => s.name).join(", ")}\n`);
  const results: ScenarioResult[] = [];
  let environment: Record<string, string | number> = {};

  for (const scenario of PLAN) {
    log(`■ ${scenario.name} — ${scenario.description}`);
    const result = scenario.channel === "sqs" ? await runSqsScenario(scenario) : await runHttpScenario(scenario);
    results.push(result);
    for (const check of result.invariants) log(`    ${check.ok ? "✅" : "❌"} ${check.name} — ${check.detail}`);
    log("");
  }

  // O ambiente é descrito com um banco descartável (versão e configuração do PostgreSQL do compose).
  const probe = await createClusterEnv();
  try {
    environment = await describeEnvironment(probe);
  } finally {
    await probe.close();
  }

  const stamp = new Date().toISOString().replaceAll(":", "-").replace(/\..+/, "");
  mkdirSync("load-reports", { recursive: true });
  const markdown = renderReport(environment, results);
  writeFileSync(`load-reports/load-${stamp}.md`, markdown);
  writeFileSync(`load-reports/load-${stamp}.json`, JSON.stringify({ environment, config: CONFIG, results }, null, 2));
  log(`Relatório: load-reports/load-${stamp}.md (e .json)`);

  const failed = results.some((r) => r.invariants.some((c) => !c.ok));
  log(failed ? "\n❌ INVARIANTES VIOLADAS — o resultado de vazão não é confiável." : "\n✅ Todas as invariantes de dinheiro se mantiveram sob carga.");
  process.exit(failed ? 1 : 0);
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`);
  process.exit(1);
});
