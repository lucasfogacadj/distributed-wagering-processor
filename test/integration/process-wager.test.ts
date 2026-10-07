import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { IdempotencyConflictError } from "../../src/application/errors";
import type { ProcessWagerCommand } from "../../src/application/process-wager-transaction";
import { InvalidMoneyError } from "../../src/domain/errors";
import { FailureCode } from "../../src/domain/failure-code";
import { PENDING_REFERENCE_MAX_ATTEMPTS } from "../../src/domain/pending-reference-schedule";
import { WagerTransactionKind as K, WagerTransactionStatus as S } from "../../src/domain/wager-transaction";
import { assertLedgerMatchesBalances, createHarness, FlakyIds, type Harness } from "../support/harness";

const brl = (amount: string) => ({ amount, currency: "BRL" });

let h: Harness;
beforeAll(async () => {
  h = await createHarness();
});
afterAll(async () => {
  await h.close();
});

interface TestWallet {
  id: string;
  playerId: string;
}

async function newWallet(balance = "100.00"): Promise<TestWallet> {
  const playerId = randomUUID();
  const w = await h.createWallet.execute({ playerId, initialBalance: brl(balance), correlationId: "c" });
  return { id: w.id, playerId };
}

function cmd(w: TestWallet, over: Partial<ProcessWagerCommand> = {}): ProcessWagerCommand {
  const externalTransactionId = over.externalTransactionId ?? randomUUID();
  return {
    idempotencyKey: `provider-a:${externalTransactionId}`,
    providerId: "provider-a",
    externalTransactionId,
    playerId: w.playerId,
    walletId: w.id,
    roundId: "round-1",
    gameId: "fortune-chimp",
    kind: K.Bet,
    money: brl("25.00"),
    correlationId: "corr-1",
    ...over,
  };
}

const balanceOf = async (walletId: string) =>
  (await h.pool.query<{ b: string }>("SELECT balance::text AS b FROM wallets WHERE id = $1", [walletId])).rows[0]?.b;

const count = async (sql: string, params: unknown[]) =>
  (await h.pool.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${sql}`, params)).rows[0]!.n;

const debitCount = (walletId: string) => count("ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'", [walletId]);

const eventTypes = async (walletId: string) =>
  (await h.pool.query<{ event_type: string }>("SELECT event_type FROM outbox_messages WHERE aggregate_id = $1 ORDER BY event_type", [walletId])).rows.map(
    (r) => r.event_type,
  );

describe("BET", () => {
  it("debita, grava ledger e eventos, e devolve o saldo", async () => {
    const w = await newWallet("100.00");
    const r = await h.process.execute(cmd(w));

    expect(r).toEqual({ transactionId: expect.any(String), status: S.Processed, balance: brl("75.00"), idempotentReplay: false });
    expect(await balanceOf(w.id)).toBe("75.00");

    const entry = await h.pool.query(
      "SELECT amount::text AS amount, balance_before::text AS before, balance_after::text AS after FROM ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'",
      [w.id],
    );
    expect(entry.rows).toEqual([{ amount: "25.00", before: "100.00", after: "75.00" }]);
    expect(await eventTypes(w.id)).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged", "WalletBalanceChanged"]);
    await assertLedgerMatchesBalances(h.pool);
  });

  it("saldo insuficiente → REJECTED sem ledger nem mudança de saldo", async () => {
    const w = await newWallet("50.00");
    const r = await h.process.execute(cmd(w, { money: brl("80.00") }));

    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.InsufficientFunds, balance: brl("50.00"), idempotentReplay: false });
    expect(await balanceOf(w.id)).toBe("50.00");
    expect(await debitCount(w.id)).toBe(0);
    expect(await eventTypes(w.id)).toEqual(["WagerTransactionRejected", "WalletBalanceChanged"]);
  });

  it("zerar o saldo exatamente é permitido", async () => {
    const w = await newWallet("25.00");
    const r = await h.process.execute(cmd(w));
    expect(r).toMatchObject({ status: S.Processed, balance: brl("0.00") });
  });
});

describe("WIN e LOSS", () => {
  it("WIN credita", async () => {
    const w = await newWallet("100.00");
    const r = await h.process.execute(cmd(w, { kind: K.Win, money: brl("30.00") }));
    expect(r).toMatchObject({ status: S.Processed, balance: brl("130.00") });
    await assertLedgerMatchesBalances(h.pool);
  });

  it("LOSS não move saldo, não gera ledger e só publica WagerTransactionProcessed", async () => {
    const w = await newWallet("100.00");
    const r = await h.process.execute(cmd(w, { kind: K.Loss, money: brl("25.00") }));

    expect(r).toMatchObject({ status: S.Processed, balance: brl("100.00") });
    expect(await count("ledger_entries WHERE wallet_id = $1", [w.id])).toBe(1); // só a abertura
    expect(await eventTypes(w.id)).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
  });
});

describe("idempotência", () => {
  it("replay devolve o resultado original e não duplica o débito", async () => {
    const w = await newWallet("100.00");
    const c = cmd(w);
    const first = await h.process.execute(c);
    const events = await eventTypes(w.id);

    const replay = await h.process.execute(c);
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    expect(await debitCount(w.id)).toBe(1);
    expect(await balanceOf(w.id)).toBe("75.00");
    expect(await eventTypes(w.id)).toEqual(events);
  });

  it("replay devolve o saldo observado na época, não o saldo atual", async () => {
    const w = await newWallet("100.00");
    const c = cmd(w);
    await h.process.execute(c);
    await h.process.execute(cmd(w));
    expect(await balanceOf(w.id)).toBe("50.00");

    const replay = await h.process.execute(c);
    expect(replay.balance).toEqual(brl("75.00"));
  });

  it("replay de uma rejeição devolve a mesma rejeição", async () => {
    const w = await newWallet("10.00");
    const c = cmd(w, { money: brl("80.00") });
    const first = await h.process.execute(c);
    const replay = await h.process.execute(c);
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    expect(replay.failureCode).toBe(FailureCode.InsufficientFunds);
  });

  it("mesma key com payload diferente é conflito, não replay", async () => {
    const w = await newWallet("100.00");
    const c = cmd(w);
    await h.process.execute(c);
    await expect(h.process.execute({ ...c, money: brl("26.00") })).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(await debitCount(w.id)).toBe(1);
  });

  it("mesmo (provider, externalTransactionId) com outra key é conflito", async () => {
    const w = await newWallet("100.00");
    const c = cmd(w);
    await h.process.execute(c);
    await expect(h.process.execute({ ...c, idempotencyKey: "outra-key" })).rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  it("valor inválido é recusado antes de qualquer persistência", async () => {
    const w = await newWallet("100.00");
    const before = await count("wager_transactions WHERE wallet_id = $1", [w.id]);
    await expect(h.process.execute(cmd(w, { money: brl("10") }))).rejects.toBeInstanceOf(InvalidMoneyError);
    expect(await count("wager_transactions WHERE wallet_id = $1", [w.id])).toBe(before);
  });
});

describe("concorrência", () => {
  it("a mesma aposta enviada 50 vezes em paralelo gera um único débito", async () => {
    const w = await newWallet("100.00");
    const c = cmd(w);
    const results = await Promise.all(Array.from({ length: 50 }, () => h.process.execute(c)));

    expect(results.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1);
    for (const r of results) expect(r).toMatchObject({ status: S.Processed, balance: brl("75.00") });
    expect(await debitCount(w.id)).toBe(1);
    expect(await balanceOf(w.id)).toBe("75.00");
    await assertLedgerMatchesBalances(h.pool);
  });

  it("cenário obrigatório: duas apostas de 80.00 disputando 100.00", async () => {
    const w = await newWallet("100.00");
    const results = await Promise.all([h.process.execute(cmd(w, { money: brl("80.00") })), h.process.execute(cmd(w, { money: brl("80.00") }))]);

    expect(results.map((r) => r.status).sort()).toEqual([S.Processed, S.Rejected]);
    expect(results.find((r) => r.status === S.Rejected)?.failureCode).toBe(FailureCode.InsufficientFunds);
    expect(await balanceOf(w.id)).toBe("20.00");
    expect(await debitCount(w.id)).toBe(1);
    await assertLedgerMatchesBalances(h.pool);
  });

  it("20 apostas distintas na mesma wallet nunca deixam o saldo negativo", async () => {
    const w = await newWallet("100.00");
    const results = await Promise.all(Array.from({ length: 20 }, () => h.process.execute(cmd(w, { money: brl("10.00") }))));

    expect(results.filter((r) => r.status === S.Processed)).toHaveLength(10);
    expect(results.filter((r) => r.status === S.Rejected)).toHaveLength(10);
    expect(await balanceOf(w.id)).toBe("0.00");
    await assertLedgerMatchesBalances(h.pool);
  });

  it("wallets distintas processam em paralelo sem interferência", async () => {
    const wallets = await Promise.all(Array.from({ length: 10 }, () => newWallet("100.00")));
    const results = await Promise.all(wallets.map((w) => h.process.execute(cmd(w))));
    for (const r of results) expect(r).toMatchObject({ status: S.Processed, balance: brl("75.00") });
    await assertLedgerMatchesBalances(h.pool);
  });
});

describe("rejeições de identidade", () => {
  it("wallet inexistente → REJECTED WALLET_NOT_FOUND, auditável", async () => {
    const c = cmd({ id: randomUUID(), playerId: randomUUID() });
    const r = await h.process.execute(c);
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.WalletNotFound });
    expect(r.balance).toBeUndefined();
    expect(await h.process.execute(c)).toMatchObject({ idempotentReplay: true, failureCode: FailureCode.WalletNotFound });
  });

  it("player diferente do dono da wallet → WALLET_PLAYER_MISMATCH", async () => {
    const w = await newWallet("100.00");
    const r = await h.process.execute(cmd({ ...w, playerId: randomUUID() }));
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.WalletPlayerMismatch });
    expect(await balanceOf(w.id)).toBe("100.00");
  });

  it("moeda diferente da wallet → CURRENCY_MISMATCH", async () => {
    const w = await newWallet("100.00");
    const r = await h.process.execute(cmd(w, { money: { amount: "10.00", currency: "USD" } }));
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.CurrencyMismatch });
    expect(await balanceOf(w.id)).toBe("100.00");
  });
});

describe("REFUND e ROLLBACK", () => {
  it("REFUND devolve o valor da BET", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    const r = await h.process.execute(
      cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Processed, balance: brl("100.00") });
    await assertLedgerMatchesBalances(h.pool);
  });

  it("uma BET só pode ser reembolsada uma vez → ALREADY_REVERSED", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }));
    const second = await h.process.execute(
      cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(second).toMatchObject({ status: S.Rejected, failureCode: FailureCode.AlreadyReversed, balance: brl("100.00") });
  });

  it.each([
    [K.Refund, K.Rollback],
    [K.Rollback, K.Refund],
  ])("%s seguido de %s da MESMA BET → ALREADY_REVERSED: o crédito nunca é duplicado", async (first, second) => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    expect(await balanceOf(w.id)).toBe("75.00");

    const r1 = await h.process.execute(cmd(w, { kind: first, referenceExternalTransactionId: bet.externalTransactionId }));
    expect(r1).toMatchObject({ status: S.Processed, balance: brl("100.00") });

    const r2 = await h.process.execute(cmd(w, { kind: second, referenceExternalTransactionId: bet.externalTransactionId }));
    expect(r2).toMatchObject({ status: S.Rejected, failureCode: FailureCode.AlreadyReversed, balance: brl("100.00") });
    expect(await balanceOf(w.id)).toBe("100.00"); // um único crédito
    await assertLedgerMatchesBalances(h.pool);
  });

  it("REFUND e ROLLBACK da mesma BET disputando ao mesmo tempo: exatamente um é aplicado", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);

    const results = await Promise.all(
      [K.Refund, K.Rollback, K.Refund, K.Rollback].map((kind) =>
        h.process.execute(cmd(w, { kind, referenceExternalTransactionId: bet.externalTransactionId })),
      ),
    );
    expect(results.filter((r) => r.status === S.Processed)).toHaveLength(1);
    expect(await balanceOf(w.id)).toBe("100.00");
    await assertLedgerMatchesBalances(h.pool);
  });

  it("o ROLLBACK de um REFUND continua válido (outra referência) e desfaz o crédito do reembolso", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    const refund = cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId });
    await h.process.execute(refund);
    expect(await balanceOf(w.id)).toBe("100.00");

    const rollback = await h.process.execute(
      cmd(w, { kind: K.Rollback, referenceExternalTransactionId: refund.externalTransactionId }),
    );
    expect(rollback).toMatchObject({ status: S.Processed, balance: brl("75.00") });
    await assertLedgerMatchesBalances(h.pool);
  });

  it("REFUND concorrentes da mesma BET: exatamente um é aplicado", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    const results = await Promise.all(
      Array.from({ length: 6 }, () => h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }))),
    );
    expect(results.filter((r) => r.status === S.Processed)).toHaveLength(1);
    expect(await balanceOf(w.id)).toBe("100.00");
    await assertLedgerMatchesBalances(h.pool);
  });

  it("ROLLBACK de uma WIN debita o valor", async () => {
    const w = await newWallet("100.00");
    const win = cmd(w, { kind: K.Win, money: brl("40.00") });
    await h.process.execute(win);
    const r = await h.process.execute(
      cmd(w, { kind: K.Rollback, money: brl("40.00"), referenceExternalTransactionId: win.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Processed, balance: brl("100.00") });
    await assertLedgerMatchesBalances(h.pool);
  });

  it("ROLLBACK que deixaria o saldo negativo → REVERSAL_WOULD_OVERDRAW (distinto de INSUFFICIENT_FUNDS)", async () => {
    const w = await newWallet("50.00");
    const win = cmd(w, { kind: K.Win, money: brl("50.00") });
    await h.process.execute(win);
    await h.process.execute(cmd(w, { money: brl("100.00") }));
    expect(await balanceOf(w.id)).toBe("0.00");

    const r = await h.process.execute(
      cmd(w, { kind: K.Rollback, money: brl("50.00"), referenceExternalTransactionId: win.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.ReversalWouldOverdraw, balance: brl("0.00") });
    expect(await balanceOf(w.id)).toBe("0.00");
  });

  it("valor diferente do da referência → AMOUNT_MISMATCH", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    const r = await h.process.execute(
      cmd(w, { kind: K.Refund, money: brl("10.00"), referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.AmountMismatch });
  });

  it("REFUND de uma WIN → INVALID_REFERENCE_KIND", async () => {
    const w = await newWallet("100.00");
    const win = cmd(w, { kind: K.Win });
    await h.process.execute(win);
    const r = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: win.externalTransactionId }));
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.InvalidReferenceKind });
  });

  it("referência de outra rodada → REFERENCE_MISMATCH", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w, { roundId: "round-A" });
    await h.process.execute(bet);
    const r = await h.process.execute(
      cmd(w, { kind: K.Refund, roundId: "round-B", referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.ReferenceMismatch });
  });

  it("referência rejeitada não pode ser revertida → REFERENCE_NOT_PROCESSED", async () => {
    const w = await newWallet("10.00");
    const bet = cmd(w, { money: brl("80.00") });
    await h.process.execute(bet);
    const r = await h.process.execute(
      cmd(w, { kind: K.Refund, money: brl("80.00"), referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.ReferenceNotProcessed });
  });
});

describe("WIN com referência", () => {
  it("WIN que referencia uma BET processada credita o prêmio (valor livre)", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    const r = await h.process.execute(
      cmd(w, { kind: K.Win, money: brl("60.00"), referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Processed, balance: brl("135.00") });
    await assertLedgerMatchesBalances(h.pool);
  });

  it("WIN de uma BET rejeitada não credita → REFERENCE_NOT_PROCESSED", async () => {
    const w = await newWallet("10.00");
    const bet = cmd(w, { money: brl("80.00") });
    await h.process.execute(bet);
    const r = await h.process.execute(
      cmd(w, { kind: K.Win, money: brl("160.00"), referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.ReferenceNotProcessed, balance: brl("10.00") });
    expect(await balanceOf(w.id)).toBe("10.00");
  });

  it("WIN de uma BET já reembolsada → ALREADY_REVERSED", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }));
    const r = await h.process.execute(
      cmd(w, { kind: K.Win, money: brl("50.00"), referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.AlreadyReversed, balance: brl("100.00") });
  });

  it("WIN de outra rodada → REFERENCE_MISMATCH", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    await h.process.execute(bet);
    const r = await h.process.execute(
      cmd(w, { kind: K.Win, roundId: "outra-rodada", referenceExternalTransactionId: bet.externalTransactionId }),
    );
    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.ReferenceMismatch });
  });

  it("WIN antes da BET fica PENDING_REFERENCE e é creditada depois", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    const win = cmd(w, { kind: K.Win, money: brl("60.00"), referenceExternalTransactionId: bet.externalTransactionId });

    const parked = await h.process.execute(win);
    expect(parked).toMatchObject({ status: S.PendingReference, balance: brl("100.00") });

    await h.process.execute(bet);
    const resolved = await h.reprocess.execute(parked.transactionId);
    expect(resolved).toMatchObject({ status: S.Processed, balance: brl("135.00") });
    await assertLedgerMatchesBalances(h.pool);
  });

  it("WIN sem referência continua sendo aplicada", async () => {
    const w = await newWallet("100.00");
    expect(await h.process.execute(cmd(w, { kind: K.Win, money: brl("10.00") }))).toMatchObject({
      status: S.Processed,
      balance: brl("110.00"),
    });
  });
});

describe("referência fora de ordem", () => {
  it("REFUND antes da BET fica PENDING_REFERENCE e é resolvido depois", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    const refund = cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId });

    const parked = await h.process.execute(refund);
    expect(parked).toMatchObject({ status: S.PendingReference, balance: brl("100.00"), idempotentReplay: false });
    expect(await eventTypes(w.id)).toContain("WagerTransactionPendingReference");

    expect(await h.process.execute(refund)).toMatchObject({ status: S.PendingReference, idempotentReplay: true });

    await h.process.execute(bet);
    expect(await balanceOf(w.id)).toBe("75.00");

    const due = await h.pool.query<{ due: boolean }>(
      "SELECT next_attempt_at <= now() AS due FROM wager_transactions WHERE id = $1",
      [parked.transactionId],
    );
    expect(due.rows[0]?.due).toBe(true); // a chegada da referência libera o dependente

    const resolved = await h.reprocess.execute(parked.transactionId);
    expect(resolved).toMatchObject({ transactionId: parked.transactionId, status: S.Processed, balance: brl("100.00") });
    expect(await balanceOf(w.id)).toBe("100.00");
    await assertLedgerMatchesBalances(h.pool);
  });

  it("ROLLBACK antes da referência também é reprocessado", async () => {
    const w = await newWallet("100.00");
    const win = cmd(w, { kind: K.Win, money: brl("40.00") });
    const rollback = cmd(w, { kind: K.Rollback, money: brl("40.00"), referenceExternalTransactionId: win.externalTransactionId });

    const parked = await h.process.execute(rollback);
    expect(parked.status).toBe(S.PendingReference);
    await h.process.execute(win);
    const resolved = await h.reprocess.execute(parked.transactionId);
    expect(resolved).toMatchObject({ status: S.Processed, balance: brl("100.00") });
    await assertLedgerMatchesBalances(h.pool);
  });

  it("reprocessar duas vezes não duplica o efeito", async () => {
    const w = await newWallet("100.00");
    const bet = cmd(w);
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: bet.externalTransactionId }));
    await h.process.execute(bet);

    const results = await Promise.all([h.reprocess.execute(parked.transactionId), h.reprocess.execute(parked.transactionId)]);
    expect(results.filter((r) => r !== undefined)).toHaveLength(1);
    expect(await balanceOf(w.id)).toBe("100.00");
    await assertLedgerMatchesBalances(h.pool);
  });

  it("transação que não está pendente não é reprocessada", async () => {
    const w = await newWallet("100.00");
    const done = await h.process.execute(cmd(w));
    expect(await h.reprocess.execute(done.transactionId)).toBeUndefined();
    expect(await h.reprocess.execute(randomUUID())).toBeUndefined();
  });

  it("esgotadas as tentativas → REJECTED REFERENCE_NOT_FOUND e evento publicado", async () => {
    const w = await newWallet("100.00");
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, referenceExternalTransactionId: "nunca-chega" }));

    let calls = 0;
    let last;
    while (calls < PENDING_REFERENCE_MAX_ATTEMPTS + 5) {
      last = await h.reprocess.execute(parked.transactionId);
      calls += 1;
      if (last?.status !== S.PendingReference) break;
    }

    expect(calls).toBe(PENDING_REFERENCE_MAX_ATTEMPTS);
    expect(last).toMatchObject({ status: S.Rejected, failureCode: FailureCode.ReferenceNotFound, balance: brl("100.00") });
    expect(await eventTypes(w.id)).toContain("WagerTransactionRejected");
    expect(await balanceOf(w.id)).toBe("100.00");
  });

  it("referência que existe mas nunca é decidida → REJECTED REFERENCE_UNRESOLVED (distinto de REFERENCE_NOT_FOUND)", async () => {
    const w = await newWallet("100.00");
    const refundExt = randomUUID();
    // O REFUND fica pendente para sempre (a BET nunca chega); o ROLLBACK dele acha a referência, mas sem decisão.
    await h.process.execute(cmd(w, { externalTransactionId: refundExt, kind: K.Refund, referenceExternalTransactionId: "nunca-chega" }));
    const rollback = await h.process.execute(cmd(w, { kind: K.Rollback, referenceExternalTransactionId: refundExt }));
    expect(rollback.status).toBe(S.PendingReference);

    let last;
    for (let i = 0; i < PENDING_REFERENCE_MAX_ATTEMPTS + 5; i++) {
      last = await h.reprocess.execute(rollback.transactionId);
      if (last?.status !== S.PendingReference) break;
    }

    expect(last).toMatchObject({ status: S.Rejected, failureCode: FailureCode.ReferenceUnresolved });
    expect(await balanceOf(w.id)).toBe("100.00");
  });

  it("referência REJECTED libera os dependentes na hora, que são rejeitados como REFERENCE_NOT_PROCESSED", async () => {
    const w = await newWallet("100.00");
    const betExt = randomUUID();
    const parked = await h.process.execute(cmd(w, { kind: K.Refund, money: brl("500.00"), referenceExternalTransactionId: betExt }));
    const isDue = async () =>
      (await h.pool.query<{ due: boolean }>("SELECT next_attempt_at <= now() AS due FROM wager_transactions WHERE id = $1", [parked.transactionId]))
        .rows[0]!.due;
    expect(await isDue()).toBe(false);

    const bet = await h.process.execute(cmd(w, { externalTransactionId: betExt, money: brl("500.00") }));
    expect(bet).toMatchObject({ status: S.Rejected, failureCode: FailureCode.InsufficientFunds });
    expect(await isDue()).toBe(true);

    expect(await h.reprocess.execute(parked.transactionId)).toMatchObject({
      status: S.Rejected,
      failureCode: FailureCode.ReferenceNotProcessed,
    });
  });
});

describe("limite de valor", () => {
  it("crédito que passaria do limite suportado → REJECTED BALANCE_LIMIT_EXCEEDED, sem tocar saldo nem ledger", async () => {
    const w = await newWallet("999999999999999999.00");
    const r = await h.process.execute(cmd(w, { kind: K.Win, money: brl("1.00") }));

    expect(r).toMatchObject({ status: S.Rejected, failureCode: FailureCode.BalanceLimitExceeded, balance: brl("999999999999999999.00") });
    expect(await balanceOf(w.id)).toBe("999999999999999999.00");
    expect(await count("ledger_entries WHERE wallet_id = $1", [w.id])).toBe(1);
  });

  it("valor acima do limite na entrada é inválido (não chega ao banco)", async () => {
    const w = await newWallet();
    await expect(h.process.execute(cmd(w, { money: brl("1000000000000000000.00") }))).rejects.toThrow(InvalidMoneyError);
  });
});

/** Abre uma transação que segura o lock de um walletId inexistente até `release()`. */
async function holdAbsentLock(walletId: string): Promise<{ release: () => Promise<void> }> {
  let free!: () => void;
  const held = new Promise<void>((resolve) => (free = resolve));
  let acquired!: () => void;
  const locked = new Promise<void>((resolve) => (acquired = resolve));
  const holder = h.uow.run(async (repos) => {
    await repos.wallets.lockAbsent(walletId);
    acquired();
    await held;
  });
  await locked;
  return {
    release: async () => {
      free();
      await holder;
    },
  };
}

describe("wallet inexistente", () => {
  it("rejeições concorrentes do mesmo walletId inexistente são serializadas (eventos saem na ordem de commit)", async () => {
    const missing = { id: randomUUID(), playerId: randomUUID() };
    const lock = await holdAbsentLock(missing.id);

    let finished = false;
    const rejection = h.process.execute(cmd(missing)).then((r) => {
      finished = true;
      return r;
    });
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(finished).toBe(false);

    await lock.release();
    expect(await rejection).toMatchObject({ status: S.Rejected, failureCode: FailureCode.WalletNotFound });
  });

  it("o lock é por walletId: outro id inexistente não espera", async () => {
    const lock = await holdAbsentLock(randomUUID());
    const other = await h.process.execute(cmd({ id: randomUUID(), playerId: randomUUID() }));
    expect(other).toMatchObject({ status: S.Rejected, failureCode: FailureCode.WalletNotFound });
    await lock.release();
  });
});

describe("atomicidade", () => {
  it("falha ao gravar o evento desfaz saldo, ledger, transação e outbox", async () => {
    const ids = new FlakyIds();
    const flaky = await createHarness({ ids });
    try {
      const created = await flaky.createWallet.execute({ playerId: randomUUID(), initialBalance: brl("100.00"), correlationId: "c" });
      const w = { id: created.id, playerId: created.playerId };
      const outboxBefore = (await flaky.pool.query("SELECT count(*)::int AS n FROM outbox_messages")).rows[0].n;

      // ids: transação(1), lançamento(2), WagerTransactionProcessed(3), WalletBalanceChanged(4) → falha no 4º
      ids.failOnCall = ids.calls + 4;
      const c = cmd(w);
      await expect(flaky.process.execute(c)).rejects.toThrow("falha injetada");

      const wallet = await flaky.pool.query("SELECT balance::text AS b, version FROM wallets WHERE id = $1", [w.id]);
      expect(wallet.rows[0]).toEqual({ b: "100.00", version: 1 });
      expect((await flaky.pool.query("SELECT count(*)::int AS n FROM wager_transactions WHERE kind = 'BET'")).rows[0].n).toBe(0);
      expect((await flaky.pool.query("SELECT count(*)::int AS n FROM ledger_entries WHERE direction = 'DEBIT'")).rows[0].n).toBe(0);
      expect((await flaky.pool.query("SELECT count(*)::int AS n FROM outbox_messages")).rows[0].n).toBe(outboxBefore);

      // a retentativa da mesma requisição funciona e debita uma única vez
      const retry = await flaky.process.execute(c);
      expect(retry).toMatchObject({ status: S.Processed, balance: brl("75.00"), idempotentReplay: false });
      await assertLedgerMatchesBalances(flaky.pool);
    } finally {
      await flaky.close();
    }
  });
});
