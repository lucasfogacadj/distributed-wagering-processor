import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import type { Pool, PoolClient } from "pg";
import { migrateDown, migrateUp } from "../../src/infrastructure/database/migrator";
import { createTestDatabase, type TestDatabase } from "../support/db";

const INTEGRITY = "23000";
const CHECK_VIOLATION = "23514";
const UNIQUE_VIOLATION = "23505";

let db: TestDatabase;
let pool: Pool;

beforeAll(async () => {
  db = await createTestDatabase();
  pool = db.pool;
});

afterAll(async () => {
  await db.drop();
});

type Queryable = Pick<Pool | PoolClient, "query">;

async function expectPgError(
  promise: Promise<unknown>,
  expected: { code: string; constraint?: string; message?: string },
): Promise<void> {
  let error: (Error & { code?: string; constraint?: string }) | undefined;
  try {
    await promise;
  } catch (e) {
    error = e as Error & { code?: string; constraint?: string };
  }
  expect(error).toBeDefined();
  expect(error?.code).toBe(expected.code);
  if (expected.constraint) expect(error?.constraint).toBe(expected.constraint);
  if (expected.message) expect(error?.message).toContain(expected.message);
}

/** Cria wallet + OPENING + lançamento CREDIT na mesma transação, como o caso de uso fará. */
async function openWallet(balance = "100.00", currency = "BRL") {
  const walletId = randomUUID();
  const playerId = randomUUID();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
       VALUES ($1, $2, $3, $4, 1, now(), now())`,
      [walletId, playerId, currency, balance],
    );
    if (balance !== "0.00") {
      const txId = randomUUID();
      await insertTx(client, {
        id: txId,
        walletId,
        playerId,
        providerId: "internal",
        externalId: `opening:${walletId}`,
        idempotencyKey: `opening:${walletId}`,
        kind: "OPENING",
        amount: balance,
        currency,
        status: "PROCESSED",
      });
      await insertEntry(client, { walletId, transactionId: txId, direction: "CREDIT", amount: balance, before: "0.00", after: balance, currency });
    }
    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
  return { walletId, playerId };
}

interface TxOverrides {
  id?: string;
  walletId: string;
  playerId: string;
  providerId?: string;
  externalId?: string;
  idempotencyKey?: string;
  kind?: string;
  amount?: string;
  currency?: string;
  status?: string;
  failureCode?: string | null;
  referenceExternalId?: string | null;
  referenceTransactionId?: string | null;
  roundId?: string;
}

async function insertTx(q: Queryable, o: TxOverrides): Promise<string> {
  const id = o.id ?? randomUUID();
  const externalId = o.externalId ?? `ext-${id}`;
  const status = o.status ?? "PENDING";
  await q.query(
    `INSERT INTO wager_transactions
       (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id,
        kind, amount, currency, reference_external_transaction_id, reference_transaction_id,
        status, failure_code, processed_at, created_at)
     VALUES ($1,$2,$3,$4,'hash',$5,$6,$7,'game',$8,$9,$10,$11,$12,$13,$14,
        CASE WHEN $13 = 'PROCESSED' THEN now() END, now())`,
    [
      id,
      o.providerId ?? "provider-a",
      externalId,
      o.idempotencyKey ?? `provider-a:${externalId}`,
      o.walletId,
      o.playerId,
      o.roundId ?? "round-1",
      o.kind ?? "BET",
      o.amount ?? "10.00",
      o.currency ?? "BRL",
      o.referenceExternalId ?? null,
      o.referenceTransactionId ?? null,
      status,
      o.failureCode ?? null,
    ],
  );
  return id;
}

async function insertEntry(
  q: Queryable,
  o: { walletId: string; transactionId: string; direction: string; amount: string; before: string; after: string; currency?: string },
): Promise<void> {
  await q.query(
    `INSERT INTO ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8, now())`,
    [randomUUID(), o.walletId, o.transactionId, o.direction, o.amount, o.currency ?? "BRL", o.before, o.after],
  );
}

describe("migrations", () => {
  it("são reversíveis: down remove tudo e up recria", async () => {
    const fresh = await createTestDatabase({ migrate: false });
    try {
      await migrateUp(fresh.pool);
      const tables = async () =>
        (
          await fresh.pool.query<{ table_name: string }>(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'schema_migrations' ORDER BY 1`,
          )
        ).rows.map((r) => r.table_name);

      expect(await tables()).toEqual(["inbox_attempts", "inbox_messages", "ledger_entries", "outbox_messages", "wager_transactions", "wallets"]);
      await migrateDown(fresh.pool, 4);
      expect(await tables()).toEqual([]);
      await migrateUp(fresh.pool);
      expect((await tables()).length).toBe(6);
    } finally {
      await fresh.drop();
    }
  });

  it("cada migration é reversível isoladamente (0004, 0003 e 0002 saem e voltam)", async () => {
    const fresh = await createTestDatabase({ migrate: false });
    try {
      await migrateUp(fresh.pool);
      const seqColumn = async () =>
        (await fresh.pool.query("SELECT 1 FROM information_schema.columns WHERE table_name = 'outbox_messages' AND column_name = 'seq'")).rowCount;
      /** Colunas indexadas (a cláusula WHERE também menciona `kind`, por isso não basta procurar a palavra). */
      const reversalIndexColumns = async () => {
        const def = (await fresh.pool.query("SELECT indexdef FROM pg_indexes WHERE indexname = 'uq_wager_tx_single_reversal'")).rows[0]?.indexdef as string;
        return /USING btree \(([^)]*)\)/.exec(def)?.[1];
      };

      const column = async (table: string, name: string) =>
        (await fresh.pool.query("SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2", [table, name])).rowCount;
      const pendingIndexDef = async () =>
        (await fresh.pool.query("SELECT indexdef FROM pg_indexes WHERE indexname = 'ix_outbox_pending'")).rows[0]?.indexdef as string;

      expect(await column("outbox_messages", "quarantined_at")).toBe(1);
      expect(await column("wager_transactions", "processing_failures")).toBe(1);
      expect(await pendingIndexDef()).toContain("quarantined_at IS NULL");
      expect(await migrateDown(fresh.pool, 1)).toEqual(["0004_failure_handling"]);
      expect(await column("outbox_messages", "quarantined_at")).toBe(0);
      expect(await column("wager_transactions", "processing_failures")).toBe(0);
      expect(await pendingIndexDef()).not.toContain("quarantined_at");

      expect(await reversalIndexColumns()).toBe("reference_transaction_id"); // por referência, de qualquer tipo
      expect(await migrateDown(fresh.pool, 1)).toEqual(["0003_single_reversal_per_reference"]);
      expect(await reversalIndexColumns()).toBe("reference_transaction_id, kind"); // volta a ser por (referência, tipo)
      expect(await seqColumn()).toBe(1);

      expect(await migrateDown(fresh.pool, 1)).toEqual(["0002_outbox_ordering"]);
      expect(await seqColumn()).toBe(0);

      expect(await migrateUp(fresh.pool)).toEqual(["0002_outbox_ordering", "0003_single_reversal_per_reference", "0004_failure_handling"]);
      expect(await seqColumn()).toBe(1);
      expect(await reversalIndexColumns()).toBe("reference_transaction_id");
    } finally {
      await fresh.drop();
    }
  });

  it("migrateUp é idempotente (reaplicar não falha nem duplica)", async () => {
    expect(await migrateUp(pool)).toEqual([]);
    const { rows } = await pool.query("SELECT count(*)::int AS n FROM schema_migrations");
    expect(rows[0].n).toBe(4);
  });

  it("instâncias concorrentes migrando ao mesmo tempo não colidem", async () => {
    const fresh = await createTestDatabase({ migrate: false });
    try {
      await Promise.all([migrateUp(fresh.pool), migrateUp(fresh.pool), migrateUp(fresh.pool)]);
      const { rows } = await fresh.pool.query("SELECT count(*)::int AS n FROM schema_migrations");
      expect(rows[0].n).toBe(4);
    } finally {
      await fresh.drop();
    }
  });
});

describe("wallets", () => {
  it("criação atômica com OPENING e lançamento é aceita", async () => {
    const { walletId } = await openWallet("1000.00");
    const { rows } = await pool.query("SELECT balance FROM wallets WHERE id = $1", [walletId]);
    expect(rows[0].balance).toBe("1000.00");
  });

  it("rejeita saldo negativo", async () => {
    const { walletId } = await openWallet("10.00");
    await expectPgError(pool.query("UPDATE wallets SET balance = -0.01 WHERE id = $1", [walletId]), {
      code: CHECK_VIOLATION,
      constraint: "ck_wallets_balance_non_negative",
    });
  });

  it("rejeita segunda wallet para o mesmo player + moeda", async () => {
    const { playerId } = await openWallet("0.00");
    await expectPgError(
      pool.query(
        `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at) VALUES ($1,$2,'BRL',0,1,now(),now())`,
        [randomUUID(), playerId],
      ),
      { code: UNIQUE_VIOLATION, constraint: "uq_wallets_player_currency" },
    );
  });

  it("permite o mesmo player em outra moeda", async () => {
    const { playerId } = await openWallet("0.00", "BRL");
    await pool.query(
      `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at) VALUES ($1,$2,'USD',0,1,now(),now())`,
      [randomUUID(), playerId],
    );
  });

  it("não aceita alterar o saldo sem lançamento correspondente no ledger", async () => {
    const { walletId } = await openWallet("100.00");
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("UPDATE wallets SET balance = 50.00, version = version + 1 WHERE id = $1", [walletId]);
      await expectPgError(client.query("COMMIT"), { code: INTEGRITY, message: "diverge" });
    } finally {
      await client.query("ROLLBACK").catch(() => undefined);
      client.release();
    }
    const { rows } = await pool.query("SELECT balance FROM wallets WHERE id = $1", [walletId]);
    expect(rows[0].balance).toBe("100.00");
  });

  it("não aceita wallet criada com saldo sem lançamento de abertura", async () => {
    await expectPgError(
      pool.query(
        `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at) VALUES ($1,$2,'BRL',5.00,1,now(),now())`,
        [randomUUID(), randomUUID()],
      ),
      { code: INTEGRITY, message: "diverge" },
    );
  });

  it("movimentação consistente (débito + lançamento) é aceita", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const txId = await insertTx(client, { walletId, playerId, status: "PROCESSED", amount: "80.00" });
      await client.query("UPDATE wallets SET balance = 20.00, version = version + 1 WHERE id = $1", [walletId]);
      await insertEntry(client, { walletId, transactionId: txId, direction: "DEBIT", amount: "80.00", before: "100.00", after: "20.00" });
      await client.query("COMMIT");
    } finally {
      client.release();
    }
    const { rows } = await pool.query("SELECT balance FROM wallets WHERE id = $1", [walletId]);
    expect(rows[0].balance).toBe("20.00");
  });
});

describe("ledger_entries", () => {
  it("UPDATE é proibido (append-only)", async () => {
    const { walletId } = await openWallet("100.00");
    await expectPgError(pool.query("UPDATE ledger_entries SET amount = 1.00 WHERE wallet_id = $1", [walletId]), {
      code: INTEGRITY,
      message: "append-only",
    });
  });

  it("DELETE é proibido (append-only)", async () => {
    const { walletId } = await openWallet("100.00");
    await expectPgError(pool.query("DELETE FROM ledger_entries WHERE wallet_id = $1", [walletId]), {
      code: INTEGRITY,
      message: "append-only",
    });
  });

  it("TRUNCATE é proibido (append-only)", async () => {
    await expectPgError(pool.query("TRUNCATE ledger_entries"), { code: INTEGRITY, message: "append-only" });
  });

  it("rejeita aritmética inconsistente", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const txId = await insertTx(pool, { walletId, playerId, status: "PROCESSED" });
    await expectPgError(
      insertEntry(pool, { walletId, transactionId: txId, direction: "DEBIT", amount: "10.00", before: "100.00", after: "85.00" }),
      { code: CHECK_VIOLATION, constraint: "ck_ledger_entries_arithmetic" },
    );
  });

  it("rejeita valor zero", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const txId = await insertTx(pool, { walletId, playerId, status: "PROCESSED" });
    await expectPgError(
      insertEntry(pool, { walletId, transactionId: txId, direction: "CREDIT", amount: "0.00", before: "100.00", after: "100.00" }),
      { code: CHECK_VIOLATION, constraint: "ck_ledger_entries_amount_positive" },
    );
  });

  it("no máximo um lançamento por transação", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const txId = await insertTx(pool, { walletId, playerId, status: "PROCESSED" });
    await insertEntry(pool, { walletId, transactionId: txId, direction: "DEBIT", amount: "10.00", before: "100.00", after: "90.00" });
    await expectPgError(
      insertEntry(pool, { walletId, transactionId: txId, direction: "DEBIT", amount: "10.00", before: "90.00", after: "80.00" }),
      { code: UNIQUE_VIOLATION, constraint: "uq_ledger_entries_transaction" },
    );
  });

  it("exige continuidade da cadeia: balance_before = balance_after do lançamento anterior", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const txId = await insertTx(pool, { walletId, playerId, status: "PROCESSED" });
    await expectPgError(
      insertEntry(pool, { walletId, transactionId: txId, direction: "DEBIT", amount: "10.00", before: "50.00", after: "40.00" }),
      { code: INTEGRITY, message: "cadeia" },
    );
  });

  it("moeda do lançamento deve ser a da wallet", async () => {
    const { walletId, playerId } = await openWallet("100.00", "BRL");
    const txId = await insertTx(pool, { walletId, playerId, status: "PROCESSED" });
    await expectPgError(
      insertEntry(pool, { walletId, transactionId: txId, direction: "DEBIT", amount: "10.00", before: "100.00", after: "90.00", currency: "USD" }),
      { code: "23503" },
    );
  });
});

describe("wager_transactions", () => {
  it("rejeita a mesma idempotency key", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    await insertTx(pool, { walletId, playerId, idempotencyKey: "dup-key" });
    await expectPgError(insertTx(pool, { walletId, playerId, idempotencyKey: "dup-key" }), {
      code: UNIQUE_VIOLATION,
      constraint: "uq_wager_tx_idempotency_key",
    });
  });

  it("rejeita o mesmo (provider, externalTransactionId)", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    await insertTx(pool, { walletId, playerId, externalId: "same-ext" });
    await expectPgError(insertTx(pool, { walletId, playerId, externalId: "same-ext", idempotencyKey: "another-key" }), {
      code: UNIQUE_VIOLATION,
      constraint: "uq_wager_tx_provider_external",
    });
  });

  it("mesmo externalTransactionId em outro provider é aceito", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    await insertTx(pool, { walletId, playerId, providerId: "provider-a", externalId: "shared" });
    await insertTx(pool, { walletId, playerId, providerId: "provider-b", externalId: "shared", idempotencyKey: "provider-b:shared" });
  });

  it("REFUND e ROLLBACK exigem referência externa", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    await expectPgError(insertTx(pool, { walletId, playerId, kind: "REFUND" }), {
      code: CHECK_VIOLATION,
      constraint: "ck_wager_tx_reference_required",
    });
  });

  it("rejeita kind desconhecido", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    await expectPgError(insertTx(pool, { walletId, playerId, kind: "DEPOSIT" }), {
      code: CHECK_VIOLATION,
      constraint: "ck_wager_tx_kind",
    });
  });

  it("REJECTED exige failure_code", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    await expectPgError(insertTx(pool, { walletId, playerId, status: "REJECTED" }), {
      code: CHECK_VIOLATION,
      constraint: "ck_wager_tx_failure_code_required",
    });
  });

  it("estado terminal é imutável", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const id = await insertTx(pool, { walletId, playerId, status: "REJECTED", failureCode: "INSUFFICIENT_FUNDS" });
    await expectPgError(pool.query("UPDATE wager_transactions SET status = 'PROCESSED' WHERE id = $1", [id]), {
      code: INTEGRITY,
      message: "terminal",
    });
  });

  it("transações nunca são excluídas", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const id = await insertTx(pool, { walletId, playerId });
    await expectPgError(pool.query("DELETE FROM wager_transactions WHERE id = $1", [id]), {
      code: INTEGRITY,
      message: "não podem ser excluídas",
    });
  });

  it("PENDING → PENDING_REFERENCE → PROCESSED é permitido", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const betId = await insertTx(pool, { walletId, playerId, status: "PROCESSED", externalId: "bet-x" });
    const id = await insertTx(pool, { walletId, playerId, kind: "REFUND", referenceExternalId: "bet-x" });
    await pool.query("UPDATE wager_transactions SET status = 'PENDING_REFERENCE' WHERE id = $1", [id]);
    await pool.query(
      "UPDATE wager_transactions SET status = 'PROCESSED', processed_at = now(), reference_transaction_id = $2 WHERE id = $1",
      [id, betId],
    );
  });

  it("reversão PROCESSED exige reference_transaction_id", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    await expectPgError(
      insertTx(pool, { walletId, playerId, kind: "REFUND", referenceExternalId: "x", status: "PROCESSED" }),
      { code: CHECK_VIOLATION, constraint: "ck_wager_tx_processed_reversal_has_reference" },
    );
  });

  it("uma referência só pode ser revertida uma vez (REFUND duas vezes colide)", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const betId = await insertTx(pool, { walletId, playerId, status: "PROCESSED", externalId: "bet-1" });
    await insertTx(pool, {
      walletId, playerId, kind: "REFUND", status: "PROCESSED", referenceExternalId: "bet-1", referenceTransactionId: betId,
    });
    await expectPgError(
      insertTx(pool, {
        walletId, playerId, kind: "REFUND", status: "PROCESSED", referenceExternalId: "bet-1", referenceTransactionId: betId,
      }),
      { code: UNIQUE_VIOLATION, constraint: "uq_wager_tx_single_reversal" },
    );
  });

  it("REFUND e ROLLBACK da mesma referência também colidem: nenhum tipo duplica o crédito de outro", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const betId = await insertTx(pool, { walletId, playerId, status: "PROCESSED", externalId: "bet-2" });
    await insertTx(pool, {
      walletId, playerId, kind: "ROLLBACK", status: "PROCESSED", referenceExternalId: "bet-2", referenceTransactionId: betId,
    });
    await expectPgError(
      insertTx(pool, {
        walletId, playerId, kind: "REFUND", status: "PROCESSED", referenceExternalId: "bet-2", referenceTransactionId: betId,
      }),
      { code: UNIQUE_VIOLATION, constraint: "uq_wager_tx_single_reversal" },
    );
  });

  it("reversões rejeitadas não contam como reversão", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const betId = await insertTx(pool, { walletId, playerId, status: "PROCESSED", externalId: "bet-3" });
    await insertTx(pool, {
      walletId, playerId, kind: "ROLLBACK", status: "REJECTED", failureCode: "REVERSAL_WOULD_OVERDRAW", referenceExternalId: "bet-3",
    });
    await insertTx(pool, {
      walletId, playerId, kind: "REFUND", status: "REJECTED", failureCode: "ALREADY_REVERSED", referenceExternalId: "bet-3",
    });
    await insertTx(pool, {
      walletId, playerId, kind: "REFUND", status: "PROCESSED", referenceExternalId: "bet-3", referenceTransactionId: betId,
    });
  });

  it("o ROLLBACK de um REFUND é válido: a referência dele é o próprio REFUND, outra referência", async () => {
    const { walletId, playerId } = await openWallet("100.00");
    const betId = await insertTx(pool, { walletId, playerId, status: "PROCESSED", externalId: "bet-4" });
    const refundId = await insertTx(pool, {
      walletId, playerId, kind: "REFUND", status: "PROCESSED", externalId: "refund-4", referenceExternalId: "bet-4", referenceTransactionId: betId,
    });
    await insertTx(pool, {
      walletId, playerId, kind: "ROLLBACK", status: "PROCESSED", referenceExternalId: "refund-4", referenceTransactionId: refundId,
    });
  });

});

describe("inbox_messages", () => {
  it("deduplica por (consumer_name, message_id)", async () => {
    const messageId = randomUUID();
    const insert = () =>
      pool.query(
        `INSERT INTO inbox_messages (message_id, consumer_name, payload_hash, received_at) VALUES ($1,'wager-consumer','h',now())`,
        [messageId],
      );
    await insert();
    await expectPgError(insert(), { code: UNIQUE_VIOLATION });
  });

  it("mesmo message_id em outro consumidor é aceito", async () => {
    const messageId = randomUUID();
    for (const consumer of ["a", "b"]) {
      await pool.query(
        `INSERT INTO inbox_messages (message_id, consumer_name, payload_hash, received_at) VALUES ($1,$2,'h',now())`,
        [messageId, consumer],
      );
    }
  });
});

describe("outbox_messages", () => {
  it("rejeita attempts negativo", async () => {
    await expectPgError(
      pool.query(
        `INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts)
         VALUES ($1,$2,'WalletBalanceChanged','{}',now(),-1)`,
        [randomUUID(), randomUUID()],
      ),
      { code: CHECK_VIOLATION },
    );
  });

  it("armazena o envelope como jsonb", async () => {
    const id = randomUUID();
    await pool.query(
      `INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at)
       VALUES ($1,$2,'WalletBalanceChanged',$3::jsonb,now())`,
      [id, randomUUID(), JSON.stringify({ eventId: id, data: { money: { amount: "25.00", currency: "BRL" } } })],
    );
    const { rows } = await pool.query("SELECT payload->'data'->'money'->>'amount' AS amount, attempts FROM outbox_messages WHERE id = $1", [id]);
    expect(rows[0]).toEqual({ amount: "25.00", attempts: 0 });
  });

  it("um evento não pode estar publicado e em quarentena ao mesmo tempo", async () => {
    await expectPgError(
      pool.query(
        `INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at, published_at, quarantined_at)
         VALUES ($1,$2,'WalletBalanceChanged','{}',now(),now(),now())`,
        [randomUUID(), randomUUID()],
      ),
      { code: CHECK_VIOLATION, constraint: "ck_outbox_quarantine_unpublished" },
    );
  });
});

describe("inbox_attempts", () => {
  it("uma linha por (consumer_name, message_id), com attempts >= 1", async () => {
    const messageId = randomUUID();
    const insert = (attempts: number) =>
      pool.query(
        `INSERT INTO inbox_attempts (consumer_name, message_id, attempts, last_error, first_failed_at, last_failed_at)
         VALUES ('c', $1, $2, 'erro', now(), now())`,
        [messageId, attempts],
      );
    await expectPgError(insert(0), { code: CHECK_VIOLATION, constraint: "ck_inbox_attempts_positive" });
    await insert(1);
    await expectPgError(insert(1), { code: UNIQUE_VIOLATION });
  });
});

describe("wager_transactions.processing_failures", () => {
  it("rejeita contagem negativa", async () => {
    const { walletId, playerId } = await openWallet("10.00");
    const id = await insertTx(pool, { walletId, playerId, kind: "REFUND", referenceExternalId: "x", status: "PENDING_REFERENCE" });
    await expectPgError(
      pool.query("UPDATE wager_transactions SET processing_failures = -1 WHERE id = $1", [id]),
      { code: CHECK_VIOLATION, constraint: "ck_wager_tx_processing_failures" },
    );
  });
});
