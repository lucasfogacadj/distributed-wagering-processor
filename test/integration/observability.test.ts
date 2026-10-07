import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { classifyError, lockConflictKind } from "../../src/application/error-classification";
import type { ProcessWagerCommand } from "../../src/application/process-wager-transaction";
import { WagerTransactionKind as K } from "../../src/domain/wager-transaction";
import { AsyncLocalLogContext } from "../../src/infrastructure/observability/log-context";
import { assertLedgerMatchesBalances, createHarness, type Harness } from "../support/harness";
import { CapturingLogger } from "../support/http-harness";
import { RecordingMetrics } from "../support/recording-metrics";

const brl = (amount: string) => ({ amount, currency: "BRL" });

const context = new AsyncLocalLogContext();
const logger = new CapturingLogger(context);
const metrics = new RecordingMetrics();
let h: Harness;

beforeAll(async () => {
  h = await createHarness({ observability: { metrics, logger, context } });
});
afterAll(async () => {
  await h.close();
});

interface W {
  id: string;
  playerId: string;
}

async function newWallet(harness: Harness = h, balance = "100.00"): Promise<W> {
  const playerId = randomUUID();
  const w = await harness.createWallet.execute({ playerId, initialBalance: brl(balance), correlationId: "c" });
  return { id: w.id, playerId };
}

function cmd(w: W, over: Partial<ProcessWagerCommand> = {}): ProcessWagerCommand {
  const ext = over.externalTransactionId ?? randomUUID();
  return {
    idempotencyKey: `provider-a:${ext}`,
    providerId: "provider-a",
    externalTransactionId: ext,
    playerId: w.playerId,
    walletId: w.id,
    roundId: "r1",
    gameId: "g1",
    kind: K.Bet,
    money: brl("25.00"),
    correlationId: "corr-test",
    ...over,
  };
}

/** Chamadas de uma métrica feitas durante `fn`. */
async function observe<T>(method: string, fn: () => Promise<T>): Promise<{ result: T; calls: unknown[][] }> {
  const before = metrics.callsOf(method).length;
  const result = await fn();
  return { result, calls: metrics.callsOf(method).slice(before) };
}

describe("métricas dos casos de uso", () => {
  it("BET processada: conta a transação por status e tipo, mede a latência e a espera pelo lock", async () => {
    const w = await newWallet();
    const decided = await observe("transactionDecided", () => h.process.execute(cmd(w)));
    expect(decided.calls).toEqual([["PROCESSED", "BET"]]);

    const latency = await observe("processingLatency", () => h.process.execute(cmd(w)));
    expect(latency.calls).toHaveLength(1);
    const [seconds, source, outcome] = latency.calls[0]!;
    expect(typeof seconds).toBe("number");
    expect(seconds as number).toBeGreaterThanOrEqual(0);
    expect([source, outcome]).toEqual(["http", "PROCESSED"]);

    const lock = await observe("lockWait", () => h.process.execute(cmd(w)));
    expect(lock.calls).toHaveLength(1);
  });

  it("replay conta duplicata pela origem HTTP; redelivery de fila conta pela origem SQS", async () => {
    const w = await newWallet();
    const c = cmd(w);
    await h.process.execute(c);

    const http = await observe("duplicateDetected", () => h.process.execute(c));
    expect(http.calls).toEqual([["http"]]);

    const withInbox = { ...c, inbox: { messageId: `msg-${randomUUID()}`, consumerName: "wager-transactions-consumer" } };
    await h.process.execute(withInbox);
    const sqs = await observe("duplicateDetected", () => h.process.execute(withInbox));
    expect(sqs.calls).toEqual([["sqs"]]);
  });

  it("rejeição e pendência também são contadas por status", async () => {
    const w = await newWallet(h, "10.00");
    const rejected = await observe("transactionDecided", () => h.process.execute(cmd(w, { money: brl("80.00") })));
    expect(rejected.calls).toEqual([["REJECTED", "BET"]]);

    const parked = await observe("transactionDecided", () =>
      h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: "ainda-nao-chegou" })),
    );
    expect(parked.calls).toEqual([["PENDING_REFERENCE", "REFUND"]]);
  });

  it("reprocessar uma pendência: retry enquanto aguarda, status final quando resolve", async () => {
    const w = await newWallet();
    const bet = cmd(w);
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }));

    const waiting = await observe("retry", () => h.reprocess.execute(parked.transactionId));
    expect(waiting.calls).toEqual([["pending_reference"]]);

    await h.process.execute(bet);
    const resolved = await observe("transactionDecided", () => h.reprocess.execute(parked.transactionId));
    expect(resolved.calls).toEqual([["PROCESSED", "REFUND"]]);
  });

  it("wallet quente: cada transação registra quanto esperou pelo lock", async () => {
    const w = await newWallet(h, "1000.00");
    const { calls } = await observe("lockWait", () => Promise.all(Array.from({ length: 20 }, () => h.process.execute(cmd(w)))));

    expect(calls).toHaveLength(20);
    const waits = calls.map(([seconds]) => seconds as number);
    expect(waits.every((s) => s >= 0)).toBe(true);
    expect(Math.max(...waits)).toBeGreaterThan(0);
    expect(await metrics.render()).toMatch(/wallet_lock_wait_seconds_count \d+/);
    await assertLedgerMatchesBalances(h.pool);
  });
});

describe("conflito de lock", () => {
  it("lock_timeout: quem espera demais falha como transitório, é contado e a retentativa funciona", async () => {
    const localMetrics = new RecordingMetrics();
    const impatient = await createHarness({
      lockTimeoutMs: 300,
      observability: { metrics: localMetrics, logger: new CapturingLogger(), context: new AsyncLocalLogContext() },
    });
    try {
      const w = await newWallet(impatient);
      const holder = await impatient.pool.connect();
      const c = cmd(w);
      let failure: unknown;
      try {
        await holder.query("BEGIN");
        await holder.query("SELECT id FROM wallets WHERE id = $1 FOR UPDATE", [w.id]); // outra instância segurando a wallet
        await impatient.process.execute(c).catch((error: unknown) => {
          failure = error;
        });
      } finally {
        await holder.query("ROLLBACK");
        holder.release();
      }

      expect(failure).toBeDefined();
      expect(lockConflictKind(failure)).toBe("lock_timeout");
      expect(classifyError(failure)).toBe("transient");
      expect(localMetrics.callsOf("lockConflict")).toEqual([["lock_timeout"]]);

      // O timeout desfez tudo: reenviar a mesma requisição processa uma única vez.
      const retry = await impatient.process.execute(c);
      expect(retry).toMatchObject({ status: "PROCESSED", idempotentReplay: false });
      await assertLedgerMatchesBalances(impatient.pool);
    } finally {
      await impatient.close();
    }
  }, 20_000);
});

describe("logs estruturados", () => {
  it("a decisão é logada com correlationId, walletId, providerId e transactionId", async () => {
    const w = await newWallet();
    const result = await context.run({ correlationId: "corr-obs-12345" }, () => h.process.execute(cmd(w)));

    const record = logger.records.find((l) => l.message === "wager_transaction_decided" && l.fields["transactionId"] === result.transactionId);
    expect(record).toBeDefined();
    expect(record!.fields).toMatchObject({
      correlationId: "corr-obs-12345",
      walletId: w.id,
      providerId: "provider-a",
      transactionId: result.transactionId,
      status: "PROCESSED",
      idempotentReplay: false,
      source: "http",
    });
  });

  it("nenhum log contém valores monetários nem payloads financeiros", async () => {
    const w = await newWallet();
    await context.run({ correlationId: "corr-secret" }, () => h.process.execute(cmd(w, { money: brl("7777.77") })));
    await h.process.execute(cmd(w, { money: brl("999999.00") })); // rejeitada por saldo

    const everything = JSON.stringify(logger.records);
    expect(everything).not.toContain("7777.77");
    expect(everything).not.toContain("999999.00");
    expect(everything).not.toContain('"amount"');
    expect(everything).not.toContain('"money"');
  });
});
