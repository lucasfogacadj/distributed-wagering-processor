import { LockMode, type SqlEntityManager } from "@mikro-orm/postgresql";
import { ConcurrentModificationError, WalletAlreadyExistsError } from "../../application/errors";
import { InboxMessage } from "../../domain/inbox-message";
import type {
  InboxRepository,
  LedgerPageItem,
  LedgerRepository,
  OutboxRepository,
  Repositories,
  StoredTransaction,
  TransactionOutcome,
  WagerTransactionRepository,
  WalletRepository,
} from "../../application/ports";
import { Money as MoneyValue, type Money } from "../../domain/money";
import { OutboxMessage } from "../../domain/outbox-message";
import type { WagerTransaction, WagerTransactionKind } from "../../domain/wager-transaction";
import type { Wallet } from "../../domain/wallet";
import type { WalletLedgerEntry } from "../../domain/wallet-ledger-entry";
import { ledgerEntryFromRow, ledgerEntryToRow, outboxToRow, transactionFromRow, walletFromRow, walletToRow } from "./mappers";
import { LedgerEntrySchema, OutboxSchema, WagerTransactionSchema, WalletSchema } from "./schemas";

const LOCK = { lockMode: LockMode.PESSIMISTIC_WRITE, refresh: true } as const;

class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async findById(id: string): Promise<Wallet | undefined> {
    const row = await this.em.findOne(WalletSchema, { id }, { refresh: true });
    return row ? walletFromRow(row) : undefined;
  }

  async findByIdForUpdate(id: string): Promise<Wallet | undefined> {
    const row = await this.em.findOne(WalletSchema, { id }, LOCK);
    return row ? walletFromRow(row) : undefined;
  }

  async lockAbsent(id: string): Promise<void> {
    // Advisory lock por wallet (não global): colisões de hash só serializam um pouco mais, nunca liberam a ordem.
    await this.em.execute("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", [id]);
  }

  async findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | undefined> {
    const row = await this.em.findOne(WalletSchema, { playerId, currency }, { refresh: true });
    return row ? walletFromRow(row) : undefined;
  }

  async add(wallet: Wallet): Promise<void> {
    try {
      this.em.persist(this.em.create(WalletSchema, walletToRow(wallet)));
      await this.em.flush();
    } catch (error) {
      if (isUniqueViolation(error, "uq_wallets_player_currency")) {
        throw new WalletAlreadyExistsError(`Já existe wallet ${wallet.currency} para o player ${wallet.playerId}`);
      }
      throw error;
    }
  }

  /** A guarda `version = anterior` é um segundo cinto de segurança sobre o lock pessimista. */
  async save(wallet: Wallet): Promise<void> {
    const updated = await this.em.nativeUpdate(
      WalletSchema,
      { id: wallet.id, version: wallet.version - 1 },
      { balance: wallet.balance.toJSON().amount, version: wallet.version, updatedAt: wallet.updatedAt },
    );
    if (updated !== 1) {
      throw new ConcurrentModificationError(`Wallet ${wallet.id} foi alterada concorrentemente (version ${wallet.version - 1})`);
    }
  }
}

class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async insertIfAbsent(tx: WagerTransaction, options: { resultBalance?: Money } = {}): Promise<boolean> {
    const rows = await this.em.execute<{ id: string }[]>(
      `INSERT INTO wager_transactions
         (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id, round_id, game_id,
          kind, amount, currency, reference_external_transaction_id, reference_transaction_id, status, failure_code,
          result_balance, attempts, next_attempt_at, processed_at, created_at)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
       ON CONFLICT DO NOTHING
       RETURNING id`,
      [
        tx.id,
        tx.providerId,
        tx.externalTransactionId,
        tx.idempotencyKey,
        tx.payloadHash,
        tx.walletId,
        tx.playerId,
        tx.roundId,
        tx.gameId,
        tx.kind,
        tx.money.toJSON().amount,
        tx.money.currency,
        tx.referenceExternalTransactionId ?? null,
        tx.referenceTransactionId ?? null,
        tx.status,
        tx.failureCode ?? null,
        options.resultBalance?.toJSON().amount ?? null,
        0,
        null,
        tx.processedAt ?? null,
        tx.createdAt,
      ],
    );
    return rows.length > 0;
  }

  async findById(id: string): Promise<StoredTransaction | undefined> {
    return this.load({ id }, false);
  }

  async findByIdForUpdate(id: string): Promise<StoredTransaction | undefined> {
    return this.load({ id }, true);
  }

  async findByIdempotencyKey(key: string): Promise<StoredTransaction | undefined> {
    return this.load({ idempotencyKey: key }, false);
  }

  async findByProviderExternal(providerId: string, externalTransactionId: string): Promise<StoredTransaction | undefined> {
    return this.load({ providerId, externalTransactionId }, false);
  }

  async save(tx: WagerTransaction, outcome: TransactionOutcome): Promise<void> {
    const updated = await this.em.nativeUpdate(
      WagerTransactionSchema,
      { id: tx.id },
      {
        status: tx.status,
        failureCode: tx.failureCode ?? null,
        referenceTransactionId: tx.referenceTransactionId ?? null,
        processedAt: tx.processedAt ?? null,
        resultBalance: outcome.resultBalance?.toJSON().amount ?? null,
        nextAttemptAt: outcome.nextAttemptAt ?? null,
        ...(outcome.attempts !== undefined ? { attempts: outcome.attempts } : {}),
      },
    );
    // Defesa em profundidade, como no `wallets.save`: a transação sempre existe (inserida antes) e está travada.
    if (updated !== 1) {
      throw new ConcurrentModificationError(`Transação ${tx.id} não pôde ser atualizada (linhas afetadas: ${updated})`);
    }
  }

  async hasProcessedReversal(referenceTransactionId: string): Promise<boolean> {
    const rows = await this.em.execute<unknown[]>(
      `SELECT 1 FROM wager_transactions
        WHERE reference_transaction_id = ? AND kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED' LIMIT 1`,
      [referenceTransactionId],
    );
    return rows.length > 0;
  }

  async listDuePendingReferences(now: Date, limit: number): Promise<string[]> {
    const rows = await this.em.execute<{ id: string }[]>(
      `SELECT id FROM wager_transactions
        WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= ?
        ORDER BY next_attempt_at, id
        LIMIT ?`,
      [now, limit],
    );
    return rows.map((r) => r.id);
  }

  async releaseDependents(providerId: string, referenceExternalTransactionId: string, now: Date): Promise<void> {
    await this.em.execute(
      `UPDATE wager_transactions SET next_attempt_at = ?
        WHERE status = 'PENDING_REFERENCE' AND provider_id = ? AND reference_external_transaction_id = ?`,
      [now, providerId, referenceExternalTransactionId],
      "run",
    );
  }

  async recordProcessingFailure(id: string, nextAttemptAt: Date): Promise<number | undefined> {
    const rows = await this.em.execute<{ processing_failures: number }[]>(
      `UPDATE wager_transactions
          SET processing_failures = processing_failures + 1, next_attempt_at = ?
        WHERE id = ? AND status = 'PENDING_REFERENCE'
        RETURNING processing_failures`,
      [nextAttemptAt, id],
    );
    return rows[0]?.processing_failures;
  }

  private async load(where: Record<string, string>, forUpdate: boolean): Promise<StoredTransaction | undefined> {
    const row = await this.em.findOne(WagerTransactionSchema, where, forUpdate ? LOCK : { refresh: true });
    return row ? transactionFromRow(row) : undefined;
  }
}

class MikroOrmLedgerRepository implements LedgerRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async add(entry: WalletLedgerEntry): Promise<void> {
    this.em.persist(this.em.create(LedgerEntrySchema, ledgerEntryToRow(entry)));
    await this.em.flush();
  }

  async listByWallet(
    walletId: string,
    options: { beforeSeq?: string; limit: number },
  ): Promise<{ items: LedgerPageItem[]; hasMore: boolean }> {
    const rows = await this.em.execute<LedgerSqlRow[]>(
      `SELECT seq::text AS seq, id, wallet_id, transaction_id, direction, amount::text AS amount, currency,
              balance_before::text AS balance_before, balance_after::text AS balance_after, created_at
         FROM ledger_entries
        WHERE wallet_id = ? ${options.beforeSeq !== undefined ? "AND seq < ?::bigint" : ""}
        ORDER BY seq DESC
        LIMIT ?`,
      [walletId, ...(options.beforeSeq !== undefined ? [options.beforeSeq] : []), options.limit + 1],
    );

    return {
      items: rows.slice(0, options.limit).map((r) => ({
        seq: r.seq,
        entry: ledgerEntryFromRow({
          id: r.id,
          walletId: r.wallet_id,
          transactionId: r.transaction_id,
          direction: r.direction,
          amount: r.amount,
          currency: r.currency,
          balanceBefore: r.balance_before,
          balanceAfter: r.balance_after,
          // SQL cru não passa pelo mapeamento de tipos do ORM: timestamptz chega como string.
          createdAt: new Date(r.created_at),
        }),
      })),
      hasMore: rows.length > options.limit,
    };
  }

  async reconcile(walletId: string): Promise<{ stored: Money; calculated: Money; entries: number } | undefined> {
    // Um único SELECT: saldo e ledger vêm do mesmo snapshot. Créditos e débitos são somados em separado
    // (ambos não negativos) para que um ledger corrompido ainda possa ser representado e comparado.
    const rows = await this.em.execute<
      { stored: string; currency: string; credits: string; debits: string; entries: number }[]
    >(
      `SELECT w.balance::text AS stored, w.currency,
              COALESCE(SUM(l.amount) FILTER (WHERE l.direction = 'CREDIT'), 0)::numeric(20,2)::text AS credits,
              COALESCE(SUM(l.amount) FILTER (WHERE l.direction = 'DEBIT'), 0)::numeric(20,2)::text AS debits,
              COUNT(l.id)::int AS entries
         FROM wallets w
         LEFT JOIN ledger_entries l ON l.wallet_id = w.id
        WHERE w.id = ?
        GROUP BY w.id, w.balance, w.currency`,
      [walletId],
    );
    const row = rows[0];
    if (!row) return undefined;

    const money = (amount: string) => MoneyValue.from({ amount, currency: row.currency });
    return {
      stored: money(row.stored),
      calculated: money(row.credits).subtract(money(row.debits)),
      entries: row.entries,
    };
  }
}

interface LedgerSqlRow {
  seq: string;
  id: string;
  wallet_id: string;
  transaction_id: string;
  direction: string;
  amount: string;
  currency: string;
  balance_before: string;
  balance_after: string;
  created_at: Date | string;
}

class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async add(message: OutboxMessage): Promise<void> {
    this.em.persist(this.em.create(OutboxSchema, outboxToRow(message)));
    await this.em.flush();
  }

  /**
   * A reivindicação é por SEQUÊNCIA de wallet, em quatro passos de um único comando:
   *  1. `heads`: a cabeça de cada wallet (o evento pendente mais antigo), achada por *loose index scan* recursivo
   *     sobre o índice parcial (aggregate_id, seq) — custo proporcional ao nº de wallets pendentes, não ao backlog;
   *  2. `claimable`: trava (`FOR UPDATE SKIP LOCKED`) só as cabeças elegíveis (sem backoff, sem lease alheio),
   *     da mais antiga para a mais nova. Quem trava a cabeça é dono da wallet: nenhum outro publisher consegue
   *     chegar à cauda, porque a elegibilidade exige a cabeça;
   *  3. `picked`: dessas wallets, os primeiros `perAggregate` eventos de cada uma, no máximo `limit` no total;
   *  4. `UPDATE`: grava o lease nesses eventos. As condições `published_at IS NULL` são reavaliadas sob concorrência
   *     (READ COMMITTED), então um evento que acabou de ser marcado por um dono lento não é reivindicado de novo.
   */
  async claimBatch(options: { limit: number; perAggregate: number; leaseMs: number; owner: string }): Promise<OutboxMessage[]> {
    const rows = await this.em.execute<OutboxSqlRow[]>(
      `WITH RECURSIVE heads AS (
         (SELECT o.id, o.aggregate_id
            FROM outbox_messages o
           WHERE o.published_at IS NULL AND o.quarantined_at IS NULL
           ORDER BY o.aggregate_id, o.seq
           LIMIT 1)
         UNION ALL
         SELECT n.id, n.aggregate_id
           FROM heads h
          CROSS JOIN LATERAL (
                SELECT o.id, o.aggregate_id
                  FROM outbox_messages o
                 WHERE o.published_at IS NULL AND o.quarantined_at IS NULL AND o.aggregate_id > h.aggregate_id
                 ORDER BY o.aggregate_id, o.seq
                 LIMIT 1) n
       ),
       claimable AS (
         SELECT o.id, o.aggregate_id, o.seq AS head_seq
           FROM outbox_messages o
           JOIN heads h ON h.id = o.id
          WHERE o.published_at IS NULL AND o.quarantined_at IS NULL
            AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= now())
            AND (o.locked_until IS NULL OR o.locked_until < now())
          ORDER BY o.seq
          LIMIT ?
            FOR UPDATE OF o SKIP LOCKED
       ),
       run AS (
         SELECT m.id, c.head_seq,
                row_number() OVER (PARTITION BY m.aggregate_id ORDER BY m.seq) AS pos
           FROM outbox_messages m
           JOIN claimable c ON c.aggregate_id = m.aggregate_id
          WHERE m.published_at IS NULL AND m.quarantined_at IS NULL
       ),
       picked AS (
         SELECT id FROM run WHERE pos <= ? ORDER BY head_seq, pos LIMIT ?
       )
       UPDATE outbox_messages u
          SET locked_until = now() + (?::int * interval '1 millisecond'), locked_by = ?
         FROM picked p
        WHERE u.id = p.id AND u.published_at IS NULL AND u.quarantined_at IS NULL
        RETURNING u.id, u.aggregate_id, u.event_type, u.payload, u.occurred_at, u.attempts,
                  u.next_attempt_at, u.published_at, u.seq::text AS seq`,
      [options.limit, options.perAggregate, options.limit, options.leaseMs, options.owner],
    );

    return rows
      .sort((a, b) => (BigInt(a.seq) < BigInt(b.seq) ? -1 : BigInt(a.seq) > BigInt(b.seq) ? 1 : 0))
      .map((r) =>
        OutboxMessage.rehydrate({
          id: r.id,
          aggregateId: r.aggregate_id,
          eventType: r.event_type,
          // SQL cru não passa pelo mapeamento de tipos do ORM: jsonb pode chegar como texto e timestamptz como string.
          payload: typeof r.payload === "string" ? JSON.parse(r.payload) : r.payload,
          occurredAt: new Date(r.occurred_at),
          attempts: r.attempts,
          ...(r.next_attempt_at !== null ? { nextAttemptAt: new Date(r.next_attempt_at) } : {}),
          ...(r.published_at !== null ? { publishedAt: new Date(r.published_at) } : {}),
        }),
      );
  }

  async markPublished(ids: string[], owner: string, at: Date): Promise<number> {
    if (ids.length === 0) return 0;
    // Os ids vão como um único JSON: o mesmo comando serve para 1 ou para 200 eventos.
    const rows = await this.em.execute<{ id: string }[]>(
      `UPDATE outbox_messages
          SET published_at = ?, locked_until = NULL, locked_by = NULL
        WHERE id IN (SELECT value::uuid FROM jsonb_array_elements_text(?::jsonb))
          AND locked_by = ? AND published_at IS NULL
        RETURNING id`,
      [at, JSON.stringify(ids), owner],
    );
    return rows.length;
  }

  async unlock(ids: string[], owner: string): Promise<void> {
    if (ids.length === 0) return;
    await this.em.execute(
      `UPDATE outbox_messages
          SET locked_until = NULL, locked_by = NULL
        WHERE id IN (SELECT value::uuid FROM jsonb_array_elements_text(?::jsonb))
          AND locked_by = ? AND published_at IS NULL`,
      [JSON.stringify(ids), owner],
      "run",
    );
  }

  async releaseForRetry(message: OutboxMessage, owner: string): Promise<void> {
    await this.em.execute(
      `UPDATE outbox_messages
          SET attempts = ?, next_attempt_at = ?, last_error = ?, locked_until = NULL, locked_by = NULL
        WHERE id = ? AND locked_by = ? AND published_at IS NULL`,
      [message.attempts, message.nextAttemptAt ?? null, message.lastError ?? null, message.id, owner],
      "run",
    );
  }

  async quarantine(message: OutboxMessage, owner: string): Promise<void> {
    await this.em.execute(
      `UPDATE outbox_messages
          SET attempts = ?, next_attempt_at = NULL, quarantined_at = ?, last_error = ?, locked_until = NULL, locked_by = NULL
        WHERE id = ? AND locked_by = ? AND published_at IS NULL`,
      [message.attempts, message.quarantinedAt ?? null, message.lastError ?? null, message.id, owner],
      "run",
    );
  }

  async countQuarantined(): Promise<number> {
    const rows = await this.em.execute<{ n: number }[]>(
      "SELECT count(*)::int AS n FROM outbox_messages WHERE quarantined_at IS NOT NULL",
    );
    return rows[0]?.n ?? 0;
  }

  async requeueQuarantined(ids?: string[]): Promise<number> {
    const rows = await this.em.execute<{ id: string }[]>(
      `UPDATE outbox_messages
          SET quarantined_at = NULL, attempts = 0, next_attempt_at = NULL
        WHERE quarantined_at IS NOT NULL
          ${ids ? "AND id IN (SELECT value::uuid FROM jsonb_array_elements_text(?::jsonb))" : ""}
        RETURNING id`,
      ids ? [JSON.stringify(ids)] : [],
    );
    return rows.length;
  }

  async oldestPendingAt(): Promise<Date | undefined> {
    // ORDER BY seq LIMIT 1 usa o índice parcial dos pendentes: custo constante mesmo com backlog grande.
    const rows = await this.em.execute<{ oldest: Date | string | null }[]>(
      "SELECT occurred_at AS oldest FROM outbox_messages WHERE published_at IS NULL AND quarantined_at IS NULL ORDER BY seq LIMIT 1",
    );
    const oldest = rows[0]?.oldest;
    return oldest ? new Date(oldest) : undefined;
  }
}

interface OutboxSqlRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown> | string;
  occurred_at: Date | string;
  attempts: number;
  next_attempt_at: Date | string | null;
  published_at: Date | string | null;
  seq: string;
}

class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly em: SqlEntityManager) {}

  async insertIfAbsent(message: InboxMessage): Promise<boolean> {
    const rows = await this.em.execute<{ message_id: string }[]>(
      `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT DO NOTHING
       RETURNING message_id`,
      [message.consumerName, message.messageId, message.payloadHash, message.receivedAt, message.processedAt ?? null],
    );
    return rows.length > 0;
  }

  async recordFailedAttempt(consumerName: string, messageId: string, error: string, at: Date): Promise<number> {
    const rows = await this.em.execute<{ attempts: number }[]>(
      `INSERT INTO inbox_attempts (consumer_name, message_id, attempts, last_error, first_failed_at, last_failed_at)
       VALUES (?, ?, 1, ?, ?, ?)
       ON CONFLICT (consumer_name, message_id) DO UPDATE
         SET attempts = inbox_attempts.attempts + 1, last_error = EXCLUDED.last_error, last_failed_at = EXCLUDED.last_failed_at
       RETURNING attempts`,
      [consumerName, messageId, error.slice(0, 500), at, at],
    );
    return rows[0]!.attempts;
  }

  async find(consumerName: string, messageId: string): Promise<InboxMessage | undefined> {
    const rows = await this.em.execute<
      { payload_hash: string; received_at: Date | string; processed_at: Date | string | null }[]
    >(
      `SELECT payload_hash, received_at, processed_at FROM inbox_messages WHERE consumer_name = ? AND message_id = ?`,
      [consumerName, messageId],
    );
    const row = rows[0];
    if (!row) return undefined;
    return InboxMessage.rehydrate({
      messageId,
      consumerName,
      payloadHash: row.payload_hash,
      receivedAt: new Date(row.received_at),
      ...(row.processed_at !== null ? { processedAt: new Date(row.processed_at) } : {}),
    });
  }
}

export function createRepositories(em: SqlEntityManager): Repositories {
  return {
    wallets: new MikroOrmWalletRepository(em),
    transactions: new MikroOrmWagerTransactionRepository(em),
    ledger: new MikroOrmLedgerRepository(em),
    outbox: new MikroOrmOutboxRepository(em),
    inbox: new MikroOrmInboxRepository(em),
  };
}

function isUniqueViolation(error: unknown, constraint: string): boolean {
  if (typeof error !== "object" || error === null) return false;
  const e = error as { code?: string; constraint?: string; message?: string };
  return e.code === "23505" && (e.constraint === constraint || (e.message ?? "").includes(constraint));
}
