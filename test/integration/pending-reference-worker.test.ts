import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import type { ProcessWagerCommand } from "../../src/application/process-wager-transaction";
import { PendingReferenceWorker } from "../../src/application/pending-reference-worker";
import { PENDING_REFERENCE_MAX_ATTEMPTS } from "../../src/domain/pending-reference-schedule";
import { WagerTransactionKind as K } from "../../src/domain/wager-transaction";
import { assertLedgerMatchesBalances, createHarness, FakeClock, type Harness } from "../support/harness";
import { CapturingLogger } from "../support/http-harness";

const brl = (amount: string) => ({ amount, currency: "BRL" });

const clock = new FakeClock();
let h: Harness;
let logger: CapturingLogger;

beforeAll(async () => {
  h = await createHarness({ clock });
});
afterAll(async () => {
  await h.close();
});

interface W {
  id: string;
  playerId: string;
}

async function newWallet(balance = "100.00"): Promise<W> {
  const playerId = randomUUID();
  const w = await h.createWallet.execute({ playerId, initialBalance: brl(balance), correlationId: "c" });
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
    correlationId: "c",
    ...over,
  };
}

const worker = (reprocess: PendingReferenceWorkerReprocess = h.reprocess) => {
  logger = new CapturingLogger();
  return new PendingReferenceWorker({ uow: h.uow, reprocess, clock, logger });
};
type PendingReferenceWorkerReprocess = ConstructorParameters<typeof PendingReferenceWorker>[0]["reprocess"];

const stateOf = async (id: string) =>
  (await h.pool.query<{ status: string; attempts: number; failure_code: string | null }>(
    "SELECT status, attempts, failure_code FROM wager_transactions WHERE id = $1",
    [id],
  )).rows[0]!;

const balanceOf = async (id: string) =>
  (await h.pool.query<{ b: string }>("SELECT balance::text AS b FROM wallets WHERE id = $1", [id])).rows[0]!.b;

describe("PendingReferenceWorker", () => {
  it("reprocessa assim que a referência chega (o dependente é liberado na hora)", async () => {
    const w = await newWallet();
    const bet = cmd(w);
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }));
    await h.process.execute(bet);

    expect(await worker().tick()).toBeGreaterThanOrEqual(1);
    expect((await stateOf(parked.transactionId)).status).toBe("PROCESSED");
    expect(await balanceOf(w.id)).toBe("100.00");
    await assertLedgerMatchesBalances(h.pool);
  });

  it("respeita o backoff exponencial: só reprocessa depois que o prazo vence", async () => {
    const w = await newWallet();
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: "ainda-nao-chegou" }));
    const w1 = worker();

    expect(await w1.tick()).toBe(0); // próxima tentativa só em +1s
    clock.advance(1_100);
    expect(await w1.tick()).toBe(1);
    expect(await stateOf(parked.transactionId)).toMatchObject({ status: "PENDING_REFERENCE", attempts: 1 });

    clock.advance(1_000); // o backoff agora é de 2s: ainda não venceu
    expect(await w1.tick()).toBe(0);
    clock.advance(1_100);
    expect(await w1.tick()).toBe(1);
    expect(await stateOf(parked.transactionId)).toMatchObject({ attempts: 2 });
  });

  it("esgotado o limite vira REJECTED/REFERENCE_NOT_FOUND e o evento é publicado", async () => {
    const w = await newWallet();
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: "nunca-chega" }));
    const w1 = worker();

    // Avançar 6 min vence qualquer backoff (teto de 5 min): a cada tick a transação é reprocessada uma vez.
    // (Conta as iterações desta transação, não o retorno do tick, que inclui pendências de outros testes.)
    let ticks = 0;
    for (let i = 0; i < PENDING_REFERENCE_MAX_ATTEMPTS + 5; i++) {
      clock.advance(6 * 60_000);
      await w1.tick();
      ticks += 1;
      if ((await stateOf(parked.transactionId)).status !== "PENDING_REFERENCE") break;
    }

    expect(ticks).toBe(PENDING_REFERENCE_MAX_ATTEMPTS);
    expect(await stateOf(parked.transactionId)).toMatchObject({ status: "REJECTED", failure_code: "REFERENCE_NOT_FOUND" });
    const events = await h.pool.query("SELECT event_type FROM outbox_messages WHERE aggregate_id = $1 AND event_type = 'WagerTransactionRejected'", [w.id]);
    expect(events.rowCount).toBe(1);
    expect(await balanceOf(w.id)).toBe("100.00");
  });

  it("workers concorrentes sobre a mesma transação não duplicam o efeito", async () => {
    const w = await newWallet();
    const bet = cmd(w);
    await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }));
    await h.process.execute(bet);

    await Promise.all([worker().tick(), worker().tick(), worker().tick()]);

    expect(await balanceOf(w.id)).toBe("100.00");
    const credits = await h.pool.query("SELECT 1 FROM ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT'", [w.id]);
    expect(credits.rowCount).toBe(2); // abertura + um único reembolso
    await assertLedgerMatchesBalances(h.pool);
  });

  it("a falha em uma transação não impede as demais e é registrada", async () => {
    const a = await newWallet();
    const b = await newWallet();
    const betA = cmd(a);
    const betB = cmd(b);
    const parkedA = await h.process.execute(cmd(a, { kind: K.Refund, referenceExternalTransactionId: betA.externalTransactionId }));
    const parkedB = await h.process.execute(cmd(b, { kind: K.Refund, referenceExternalTransactionId: betB.externalTransactionId }));
    await h.process.execute(betA);
    await h.process.execute(betB);

    const flaky = {
      fail: h.reprocess.fail.bind(h.reprocess),
      execute: async (id: string) => {
        if (id === parkedA.transactionId) throw new Error("falha simulada");
        return h.reprocess.execute(id);
      },
    };
    const w1 = worker(flaky);
    await w1.tick();

    expect((await stateOf(parkedB.transactionId)).status).toBe("PROCESSED");
    expect((await stateOf(parkedA.transactionId)).status).toBe("PENDING_REFERENCE"); // continua pendente (não é perdida)
    expect(logger.records.some((l) => l.level === "error" && l.message === "pending_reference_reprocess_failed")).toBe(true);
  });

  it("falha persistente não prende a linha no topo da fila: é adiada por 1 s, sem gastar tentativas", async () => {
    const w = await newWallet();
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: "nunca-chega" }));
    // Nível alto de backoff (attempts = 6 → 64 s): o atraso depois de uma FALHA não pode herdar esse backoff.
    for (let i = 0; i < 6; i++) await h.reprocess.execute(parked.transactionId);

    const calls: string[] = [];
    const poison = {
      fail: h.reprocess.fail.bind(h.reprocess),
      execute: async (id: string) => {
        if (id !== parked.transactionId) return h.reprocess.execute(id);
        calls.push(id);
        throw new Error("falha persistente");
      },
    };
    const w1 = worker(poison);
    const scheduling = async () =>
      (await h.pool.query<{ attempts: number; next_attempt_at: Date }>(
        "SELECT attempts, next_attempt_at FROM wager_transactions WHERE id = $1",
        [parked.transactionId],
      )).rows[0]!;

    clock.advance(6 * 60_000); // vence qualquer backoff
    await w1.tick();
    expect(calls).toEqual([parked.transactionId]);

    // Sem o adiamento, `next_attempt_at` continuaria vencido e a linha seria a primeira da fila em todo tick.
    const after = await scheduling();
    expect(after.attempts).toBe(6); // uma falha de infraestrutura não gasta a espera pela referência
    expect(after.next_attempt_at.getTime()).toBe(clock.now().getTime() + 1_000); // curto e fixo, não 64 s

    await w1.tick(); // sem avançar o relógio: a linha não está mais vencida
    expect(calls).toHaveLength(1);

    clock.advance(1_001); // passado 1 s, a nova tentativa acontece (e falha de novo)
    await w1.tick();
    expect(calls).toHaveLength(2);
    expect((await stateOf(parked.transactionId)).status).toBe("PENDING_REFERENCE");
  });

  it("falha NÃO transitória repetida: depois de maxProcessingFailures a transação vira FAILED/INFRA_PERMANENT, com evento", async () => {
    const w = await newWallet();
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: "nunca-chega-2" }));
    const bug = {
      fail: h.reprocess.fail.bind(h.reprocess),
      execute: async (id: string) => {
        if (id !== parked.transactionId) return h.reprocess.execute(id);
        throw new TypeError("bug: leitura de undefined");
      },
    };
    logger = new CapturingLogger();
    const w1 = new PendingReferenceWorker({ uow: h.uow, reprocess: bug, clock, logger, maxProcessingFailures: 3 });
    const row = async () =>
      (await h.pool.query("SELECT status, failure_code, attempts, processing_failures FROM wager_transactions WHERE id = $1", [parked.transactionId])).rows[0];

    for (let i = 1; i <= 2; i++) {
      clock.advance(6 * 60_000);
      await w1.tick();
      expect(await row()).toEqual({ status: "PENDING_REFERENCE", failure_code: null, attempts: 0, processing_failures: i });
    }
    clock.advance(6 * 60_000);
    await w1.tick();
    expect(await row()).toEqual({ status: "FAILED", failure_code: "INFRA_PERMANENT", attempts: 0, processing_failures: 3 });

    const events = await h.pool.query(
      "SELECT payload->'data'->>'failureCode' AS code FROM outbox_messages WHERE event_type = 'WagerTransactionFailed' AND payload->'data'->>'transactionId' = $1",
      [parked.transactionId],
    );
    expect(events.rows).toEqual([{ code: "INFRA_PERMANENT" }]);
    expect(logger.records.some((l) => l.message === "pending_reference_failed_permanently")).toBe(true);

    // Terminal: o worker não a lista mais, e o replay devolve FAILED.
    clock.advance(6 * 60_000);
    expect(await h.uow.run((r) => r.transactions.listDuePendingReferences(clock.now(), 100))).not.toContain(parked.transactionId);
    await assertLedgerMatchesBalances(h.pool);
  });

  it("falha TRANSITÓRIA não conta para o FAILED (só adia)", async () => {
    const w = await newWallet();
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: "nunca-chega-3" }));
    const flaky = {
      fail: h.reprocess.fail.bind(h.reprocess),
      execute: async (id: string) => {
        if (id !== parked.transactionId) return h.reprocess.execute(id);
        throw Object.assign(new Error("canceling statement due to lock timeout"), { code: "55P03" });
      },
    };
    const w1 = new PendingReferenceWorker({ uow: h.uow, reprocess: flaky, clock, logger: new CapturingLogger(), maxProcessingFailures: 2 });
    for (let i = 0; i < 4; i++) {
      clock.advance(6 * 60_000);
      await w1.tick();
    }
    const { rows } = await h.pool.query("SELECT status, processing_failures FROM wager_transactions WHERE id = $1", [parked.transactionId]);
    expect(rows[0]).toEqual({ status: "PENDING_REFERENCE", processing_failures: 0 });
  });
});
