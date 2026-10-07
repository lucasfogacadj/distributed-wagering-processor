import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { WalletAlreadyExistsError } from "../../src/application/errors";
import { InvalidMoneyError } from "../../src/domain/errors";
import { assertLedgerMatchesBalances, createHarness, FlakyIds, type Harness } from "../support/harness";

const brl = (amount: string) => ({ amount, currency: "BRL" });

let h: Harness;
beforeAll(async () => {
  h = await createHarness();
});
afterAll(async () => {
  await h.close();
});

const create = (playerId = randomUUID(), initialBalance = brl("1000.00")) =>
  h.createWallet.execute({ playerId, initialBalance, correlationId: "corr-1" });

describe("CreateWallet", () => {
  it("cria a wallet com version 1, OPENING processada e lançamento CREDIT", async () => {
    const playerId = randomUUID();
    const wallet = await create(playerId);

    expect(wallet).toEqual({ id: expect.any(String), playerId, balance: brl("1000.00"), version: 1 });

    const tx = await h.pool.query("SELECT kind, status, amount::text AS amount FROM wager_transactions WHERE wallet_id = $1", [wallet.id]);
    expect(tx.rows).toEqual([{ kind: "OPENING", status: "PROCESSED", amount: "1000.00" }]);

    const ledger = await h.pool.query(
      "SELECT direction, amount::text AS amount, balance_before::text AS before, balance_after::text AS after FROM ledger_entries WHERE wallet_id = $1",
      [wallet.id],
    );
    expect(ledger.rows).toEqual([{ direction: "CREDIT", amount: "1000.00", before: "0.00", after: "1000.00" }]);

    const events = await h.pool.query("SELECT event_type FROM outbox_messages WHERE aggregate_id = $1", [wallet.id]);
    expect(events.rows).toEqual([{ event_type: "WalletBalanceChanged" }]);

    await assertLedgerMatchesBalances(h.pool);
  });

  it("saldo inicial zero não gera OPENING, lançamento nem evento", async () => {
    const wallet = await create(randomUUID(), brl("0.00"));
    expect(wallet.balance).toEqual(brl("0.00"));
    for (const table of ["wager_transactions", "ledger_entries"]) {
      const { rows } = await h.pool.query(`SELECT count(*)::int AS n FROM ${table} WHERE wallet_id = $1`, [wallet.id]);
      expect(rows[0].n).toBe(0);
    }
    const events = await h.pool.query("SELECT count(*)::int AS n FROM outbox_messages WHERE aggregate_id = $1", [wallet.id]);
    expect(events.rows[0].n).toBe(0);
  });

  it("rejeita wallet duplicada para o mesmo player + moeda", async () => {
    const playerId = randomUUID();
    await create(playerId);
    await expect(create(playerId)).rejects.toBeInstanceOf(WalletAlreadyExistsError);
  });

  it("permite o mesmo player em outra moeda", async () => {
    const playerId = randomUUID();
    await create(playerId);
    const usd = await create(playerId, { amount: "5.00", currency: "USD" });
    expect(usd.balance.currency).toBe("USD");
  });

  it("criações concorrentes do mesmo player resultam em exatamente uma wallet", async () => {
    const playerId = randomUUID();
    const results = await Promise.allSettled(Array.from({ length: 8 }, () => create(playerId)));

    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    for (const r of results.filter((r): r is PromiseRejectedResult => r.status === "rejected")) {
      expect(r.reason).toBeInstanceOf(WalletAlreadyExistsError);
    }
    const { rows } = await h.pool.query("SELECT count(*)::int AS n FROM wallets WHERE player_id = $1", [playerId]);
    expect(rows[0].n).toBe(1);
    await assertLedgerMatchesBalances(h.pool);
  });

  it.each(["-1.00", "10", "abc"])("rejeita saldo inicial inválido %p", async (amount) => {
    await expect(create(randomUUID(), brl(amount))).rejects.toBeInstanceOf(InvalidMoneyError);
  });

  it("é atômico: falha ao gravar o evento desfaz wallet, OPENING e lançamento", async () => {
    const ids = new FlakyIds();
    const flaky = await createHarness({ ids });
    try {
      // ids: wallet(1), OPENING(2), lançamento(3), evento(4) → falha depois dos inserts
      ids.failOnCall = 4;
      await expect(
        flaky.createWallet.execute({ playerId: randomUUID(), initialBalance: brl("100.00"), correlationId: "c" }),
      ).rejects.toThrow("falha injetada");

      for (const table of ["wallets", "wager_transactions", "ledger_entries", "outbox_messages"]) {
        const { rows } = await flaky.pool.query(`SELECT count(*)::int AS n FROM ${table}`);
        expect(rows[0].n).toBe(0);
      }
    } finally {
      await flaky.close();
    }
  });
});
