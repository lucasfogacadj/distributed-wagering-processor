import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { IdempotencyConflictError } from "../../src/application/errors";
import type { ProcessWagerCommand } from "../../src/application/process-wager-transaction";
import { WagerTransactionKind, WagerTransactionStatus as S } from "../../src/domain/wager-transaction";
import { assertLedgerMatchesBalances, createHarness, FlakyIds, type Harness } from "../support/harness";

const CONSUMER = "wager-transactions-consumer";
const brl = (amount: string) => ({ amount, currency: "BRL" });

let h: Harness;
beforeAll(async () => {
  h = await createHarness();
});
afterAll(async () => {
  await h.close();
});

async function newWallet(harness: Harness = h, balance = "100.00") {
  const playerId = randomUUID();
  const w = await harness.createWallet.execute({ playerId, initialBalance: brl(balance), correlationId: "c" });
  return { id: w.id, playerId };
}

function cmd(w: { id: string; playerId: string }, messageId: string, over: Partial<ProcessWagerCommand> = {}): ProcessWagerCommand {
  const ext = over.externalTransactionId ?? randomUUID();
  return {
    idempotencyKey: `provider-a:${ext}`,
    providerId: "provider-a",
    externalTransactionId: ext,
    playerId: w.playerId,
    walletId: w.id,
    roundId: "r1",
    gameId: "g1",
    kind: WagerTransactionKind.Bet,
    money: brl("25.00"),
    correlationId: messageId,
    causationId: messageId,
    inbox: { messageId, consumerName: CONSUMER },
    ...over,
  };
}

const balanceOf = async (harness: Harness, id: string) =>
  (await harness.pool.query<{ b: string }>("SELECT balance::text AS b FROM wallets WHERE id = $1", [id])).rows[0]!.b;

const inboxCount = async (harness: Harness, messageId: string) =>
  (await harness.pool.query<{ n: number }>("SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1", [messageId])).rows[0]!.n;

describe("inbox persistente", () => {
  it("registra a mensagem como processada na mesma transação do efeito financeiro", async () => {
    const w = await newWallet();
    const messageId = `msg-${randomUUID()}`;
    const r = await h.process.execute(cmd(w, messageId));

    expect(r).toMatchObject({ status: S.Processed, balance: brl("75.00"), idempotentReplay: false });
    const { rows } = await h.pool.query(
      "SELECT consumer_name, processed_at IS NOT NULL AS processed FROM inbox_messages WHERE message_id = $1",
      [messageId],
    );
    expect(rows).toEqual([{ consumer_name: CONSUMER, processed: true }]);
    await assertLedgerMatchesBalances(h.pool);
  });

  it("redelivery da mesma mensagem não duplica o débito e devolve o resultado original", async () => {
    const w = await newWallet();
    const messageId = `msg-${randomUUID()}`;
    const c = cmd(w, messageId);
    const first = await h.process.execute(c);
    const again = await h.process.execute(c);

    expect(again).toEqual({ ...first, idempotentReplay: true });
    expect(await balanceOf(h, w.id)).toBe("75.00");
    expect(await inboxCount(h, messageId)).toBe(1);
  });

  it("a mesma mensagem entregue 30 vezes em paralelo gera um único débito", async () => {
    const w = await newWallet();
    const c = cmd(w, `msg-${randomUUID()}`);
    const results = await Promise.all(Array.from({ length: 30 }, () => h.process.execute(c)));

    expect(results.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    expect(await balanceOf(h, w.id)).toBe("75.00");
    expect(await inboxCount(h, c.inbox!.messageId)).toBe(1);
  });

  it("messageId reutilizado com payload diferente é conflito", async () => {
    const w = await newWallet();
    const messageId = `msg-${randomUUID()}`;
    await h.process.execute(cmd(w, messageId));
    await expect(h.process.execute(cmd(w, messageId, { money: brl("26.00") }))).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(await balanceOf(h, w.id)).toBe("75.00");
  });

  it("a mesma transação em outra mensagem (redelivery do provedor) é um replay", async () => {
    const w = await newWallet();
    const ext = randomUUID();
    const first = await h.process.execute(cmd(w, `msg-${randomUUID()}`, { externalTransactionId: ext }));
    const secondId = `msg-${randomUUID()}`;
    const second = await h.process.execute(cmd(w, secondId, { externalTransactionId: ext }));

    expect(second).toEqual({ ...first, idempotentReplay: true });
    expect(await balanceOf(h, w.id)).toBe("75.00");
    expect(await inboxCount(h, secondId)).toBe(1);
  });

  it("consumidores diferentes têm inboxes independentes para o mesmo messageId", async () => {
    const w1 = await newWallet();
    const w2 = await newWallet();
    const messageId = `msg-${randomUUID()}`;
    await h.process.execute(cmd(w1, messageId, { inbox: { messageId, consumerName: "consumer-a" } }));
    await h.process.execute(cmd(w2, messageId, { inbox: { messageId, consumerName: "consumer-b" } }));
    expect(await inboxCount(h, messageId)).toBe(2);
  });

  it("é atômico: falha depois do efeito desfaz também o registro da inbox", async () => {
    const ids = new FlakyIds();
    const flaky = await createHarness({ ids });
    try {
      const w = await newWallet(flaky);
      const messageId = `msg-${randomUUID()}`;
      const c = cmd(w, messageId);

      // ids: transação(1), lançamento(2), WagerTransactionProcessed(3), WalletBalanceChanged(4)
      ids.failOnCall = ids.calls + 4;
      await expect(flaky.process.execute(c)).rejects.toThrow("falha injetada");

      expect(await inboxCount(flaky, messageId)).toBe(0);
      expect(await balanceOf(flaky, w.id)).toBe("100.00");

      // A retentativa (redelivery) processa normalmente e debita uma única vez.
      const retry = await flaky.process.execute(c);
      expect(retry).toMatchObject({ status: S.Processed, balance: brl("75.00"), idempotentReplay: false });
      expect(await inboxCount(flaky, messageId)).toBe(1);
      await assertLedgerMatchesBalances(flaky.pool);
    } finally {
      await flaky.close();
    }
  });
});
