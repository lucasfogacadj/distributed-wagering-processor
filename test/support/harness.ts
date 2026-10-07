import { expect } from "bun:test";
import type { Pool } from "pg";
import { CreateWallet } from "../../src/application/create-wallet";
import { NO_OBSERVABILITY } from "../../src/application/observability";
import type { Clock, IdGenerator, Observability } from "../../src/application/ports";
import { ProcessWagerTransaction } from "../../src/application/process-wager-transaction";
import { ReprocessPendingReference } from "../../src/application/reprocess-pending-reference";
import { createOrm } from "../../src/infrastructure/persistence/orm";
import { MikroOrmUnitOfWork } from "../../src/infrastructure/persistence/unit-of-work";
import { SystemClock, UuidV7Generator } from "../../src/infrastructure/system";
import { createTestDatabase, type TestDatabase } from "./db";

export class FakeClock implements Clock {
  constructor(private current = new Date("2026-07-29T15:00:00.000Z")) {}

  now(): Date {
    return new Date(this.current);
  }

  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

/** Gera ids reais, mas pode falhar na N-ésima chamada para provar atomicidade. */
export class FlakyIds implements IdGenerator {
  calls = 0;
  failOnCall: number | undefined;
  private readonly inner = new UuidV7Generator();

  next(): string {
    this.calls += 1;
    if (this.calls === this.failOnCall) throw new Error("falha injetada");
    return this.inner.next();
  }
}

export interface HarnessOptions {
  clock?: Clock;
  ids?: IdGenerator;
  observability?: Partial<Observability>;
  /** `lock_timeout` do PostgreSQL nas conexões do ORM (padrão: sem limite). */
  lockTimeoutMs?: number;
}

export async function createHarness(options: HarnessOptions = {}) {
  const db: TestDatabase = await createTestDatabase();
  const orm = await createOrm(db.url, 20, options.lockTimeoutMs);
  const uow = new MikroOrmUnitOfWork(orm);
  const clock = options.clock ?? new SystemClock();
  const ids = options.ids ?? new UuidV7Generator();
  const observability: Observability = { ...NO_OBSERVABILITY, ...options.observability };

  return {
    pool: db.pool,
    uow,
    clock,
    createWallet: new CreateWallet(uow, clock, ids),
    process: new ProcessWagerTransaction(uow, clock, ids, observability),
    reprocess: new ReprocessPendingReference(uow, clock, ids, observability),
    async close(): Promise<void> {
      await orm.close();
      await db.drop();
    },
  };
}

export type Harness = Awaited<ReturnType<typeof createHarness>>;

/** Invariante final de todos os testes: wallet.balance == saldo reconstruído pelo ledger. */
export async function assertLedgerMatchesBalances(pool: Pool): Promise<void> {
  const { rows } = await pool.query(
    `SELECT w.id, w.balance::text AS balance
       FROM wallets w
      WHERE w.balance <> COALESCE(
              (SELECT SUM(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END)
                 FROM ledger_entries l WHERE l.wallet_id = w.id), 0)`,
  );
  expect(rows).toEqual([]);
}
