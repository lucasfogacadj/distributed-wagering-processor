/**
 * Microbenchmark da vazão de PUBLICAÇÃO da outbox — `bun run test:load:outbox`.
 *
 * Isola o relay (sem HTTP, sem competição por CPU com a carga): semeia eventos e mede quantos o relay publica por
 * segundo no LocalStack, variando:
 *  - a forma do backlog: UMA wallet com muitos eventos × MUITAS wallets com poucos eventos;
 *  - o nº de relays concorrentes (1 e 3);
 *  - `runSize` (eventos consecutivos de uma wallet por rodada): 1 reproduz o comportamento antigo (um evento por
 *    wallet por rodada) e 20 é o padrão atual. A diferença entre as duas colunas é o efeito da publicação em sequência.
 *
 * Variáveis: BENCH_BETS_HOT (apostas na wallet única, padrão 400) · BENCH_WALLETS (padrão 100) · BENCH_BETS_SPREAD (padrão 4).
 */
import { randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { OutboxRelay } from "../../src/application/outbox-relay";
import { WagerTransactionKind } from "../../src/domain/wager-transaction";
import { SqsEventPublisher } from "../../src/infrastructure/messaging/sqs-event-publisher";
import { createHarness } from "../support/harness";
import { CapturingLogger } from "../support/http-harness";
import { createTestQueues } from "../support/sqs";

const intEnv = (name: string, fallback: number) => {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} inválido`);
  return value;
};

const HOT_BETS = intEnv("BENCH_BETS_HOT", 400);
const SPREAD_WALLETS = intEnv("BENCH_WALLETS", 100);
const SPREAD_BETS = intEnv("BENCH_BETS_SPREAD", 4);
const brl = (amount: string) => ({ amount, currency: "BRL" });

const h = await createHarness();
const queues = await createTestQueues();
const publisher = new SqsEventPublisher(queues.client, queues.urls.events);

async function seed(wallets: number, bets: number): Promise<number> {
  await h.pool.query("UPDATE outbox_messages SET published_at = now() WHERE published_at IS NULL");
  const made = await Promise.all(
    Array.from({ length: wallets }, async () => {
      const playerId = randomUUID();
      const wallet = await h.createWallet.execute({ playerId, initialBalance: brl("1000000.00"), correlationId: "bench" });
      return { id: wallet.id, playerId };
    }),
  );
  for (let round = 0; round < bets; round++) {
    await Promise.all(
      made.map((w) => {
        const external = randomUUID();
        return h.process.execute({
          idempotencyKey: `bench:${external}`,
          providerId: "bench",
          externalTransactionId: external,
          playerId: w.playerId,
          walletId: w.id,
          roundId: "r",
          gameId: "g",
          kind: WagerTransactionKind.Bet,
          money: brl("1.00"),
          correlationId: "bench",
        });
      }),
    );
  }
  return Number((await h.pool.query<{ n: string }>("SELECT count(*)::text AS n FROM outbox_messages WHERE published_at IS NULL")).rows[0]!.n);
}

interface Row {
  caso: string;
  relays: number;
  runSize: number;
  eventos: number;
  segundos: number;
  eventosPorSegundo: number;
}

async function measure(caso: string, wallets: number, bets: number, relays: number, runSize: number): Promise<Row> {
  const eventos = await seed(wallets, bets);
  const started = performance.now();
  await Promise.all(
    Array.from({ length: relays }, (_, i) =>
      new OutboxRelay({
        uow: h.uow,
        publisher,
        clock: h.clock,
        logger: new CapturingLogger(),
        instanceId: `bench-${i}`,
        batchSize: 200,
        runSize,
        leaseMs: 30_000,
      }).drain(),
    ),
  );
  const segundos = (performance.now() - started) / 1000;
  const row = { caso, relays, runSize, eventos, segundos, eventosPorSegundo: eventos / segundos };
  process.stdout.write(`${caso} · ${relays} relay(s) · runSize=${runSize}: ${eventos} eventos em ${segundos.toFixed(2)}s → ${Math.round(row.eventosPorSegundo)} eventos/s\n`);
  return row;
}

const rows: Row[] = [];
for (const runSize of [1, 20]) {
  for (const relays of [1, 3]) rows.push(await measure(`1 wallet × ${HOT_BETS} apostas`, 1, HOT_BETS, relays, runSize));
  for (const relays of [1, 3]) rows.push(await measure(`${SPREAD_WALLETS} wallets × ${SPREAD_BETS} apostas`, SPREAD_WALLETS, SPREAD_BETS, relays, runSize));
}

const byKey = (caso: string, relays: number, runSize: number) => rows.find((r) => r.caso === caso && r.relays === relays && r.runSize === runSize)!;
const cases = [...new Set(rows.map((r) => r.caso))];
const markdown = [
  "# Vazão de publicação da outbox (microbenchmark)",
  "",
  `Gerado em ${new Date().toISOString()}. Eventos/s publicados no LocalStack por relays em processo, sem carga HTTP concorrente.`,
  "`runSize=1` reproduz o comportamento antigo (1 evento por wallet por rodada); `runSize=20` é o padrão atual.",
  "",
  "| backlog | relays | runSize=1 (eventos/s) | runSize=20 (eventos/s) | ganho |",
  "|---|---|---|---|---|",
  ...cases.flatMap((caso) =>
    [1, 3].map((relays) => {
      const before = byKey(caso, relays, 1).eventosPorSegundo;
      const after = byKey(caso, relays, 20).eventosPorSegundo;
      return `| ${caso} | ${relays} | ${Math.round(before)} | ${Math.round(after)} | ${(after / before).toFixed(1)}× |`;
    }),
  ),
].join("\n");

mkdirSync("load-reports", { recursive: true });
const stamp = new Date().toISOString().replaceAll(":", "-").replace(/\..+/, "");
writeFileSync(`load-reports/outbox-bench-${stamp}.md`, `${markdown}\n`);
process.stdout.write(`\n${markdown}\n\nRelatório: load-reports/outbox-bench-${stamp}.md\n`);

await queues.cleanup();
await h.close();
process.exit(0);
