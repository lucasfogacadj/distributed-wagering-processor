import type { MoneyProps } from "../domain/money";
import type { StoredTransaction } from "./ports";
import type { LoggerPort, MetricsPort, UnitOfWork } from "./ports";
import type { Wallet } from "../domain/wallet";
import type { WalletLedgerEntry } from "../domain/wallet-ledger-entry";
import { decodeCursor, encodeCursor } from "./cursor";
import { TransactionNotFoundError, WalletNotFoundError } from "./errors";

export interface WalletView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  direction: string;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string;
}

export interface LedgerPage {
  items: LedgerEntryView[];
  nextCursor: string | null;
}

export interface TransactionView {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  status: string;
  failureCode?: string;
  money: MoneyProps;
  /** Saldo observado quando a transação foi decidida. */
  balance?: MoneyProps;
  referenceExternalTransactionId?: string;
  createdAt: string;
  processedAt?: string;
}

export interface ReconciliationView {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

export const DEFAULT_LEDGER_LIMIT = 50;
export const MAX_LEDGER_LIMIT = 200;

export class WalletQueries {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly logger: LoggerPort,
    private readonly metrics: MetricsPort,
  ) {}

  getWallet(walletId: string): Promise<WalletView> {
    return this.uow.run(async (repos) => {
      const wallet = await repos.wallets.findById(walletId);
      if (!wallet) throw new WalletNotFoundError(`Wallet ${walletId} não encontrada`);
      return walletView(wallet);
    });
  }

  listLedger(walletId: string, options: { cursor?: string | undefined; limit?: number } = {}): Promise<LedgerPage> {
    const limit = options.limit ?? DEFAULT_LEDGER_LIMIT;
    const beforeSeq = options.cursor !== undefined ? decodeCursor(options.cursor) : undefined;

    return this.uow.run(async (repos) => {
      if (!(await repos.wallets.findById(walletId))) throw new WalletNotFoundError(`Wallet ${walletId} não encontrada`);

      const page = await repos.ledger.listByWallet(walletId, { ...(beforeSeq !== undefined ? { beforeSeq } : {}), limit });
      const last = page.items.at(-1);
      return {
        items: page.items.map((i) => ledgerEntryView(i.entry)),
        nextCursor: page.hasMore && last ? encodeCursor(last.seq) : null,
      };
    });
  }

  getTransaction(transactionId: string): Promise<TransactionView> {
    return this.uow.run(async (repos) => {
      const stored = await repos.transactions.findById(transactionId);
      if (!stored) throw new TransactionNotFoundError(`Transação ${transactionId} não encontrada`);
      return transactionView(stored);
    });
  }

  getTransactionByProvider(providerId: string, externalTransactionId: string): Promise<TransactionView> {
    return this.uow.run(async (repos) => {
      const stored = await repos.transactions.findByProviderExternal(providerId, externalTransactionId);
      if (!stored) {
        throw new TransactionNotFoundError(`Transação ${externalTransactionId} do provedor ${providerId} não encontrada`);
      }
      return transactionView(stored);
    });
  }

  /**
   * Compara o saldo armazenado com o reconstruído pelo ledger. Divergências NÃO são corrigidas:
   * são logadas, contabilizadas em métrica e sinalizadas na resposta.
   */
  reconcile(walletId: string): Promise<ReconciliationView> {
    return this.uow.run(async (repos) => {
      const snapshot = await repos.ledger.reconcile(walletId);
      if (!snapshot) throw new WalletNotFoundError(`Wallet ${walletId} não encontrada`);

      const difference = snapshot.stored.subtract(snapshot.calculated);
      const consistent = difference.isZero();
      if (!consistent) {
        this.metrics.reconciliationDivergence();
        this.logger.warn("reconciliation_divergence", {
          walletId,
          checkedEntries: snapshot.entries,
          difference: difference.toString(), // texto simples: valores monetários estruturados não vão para o log
        });
      }

      return {
        walletId,
        storedBalance: snapshot.stored.toJSON(),
        calculatedBalance: snapshot.calculated.toJSON(),
        difference: difference.toJSON(),
        consistent,
        checkedEntries: snapshot.entries,
      };
    });
  }
}

function walletView(w: Wallet): WalletView {
  return { id: w.id, playerId: w.playerId, balance: w.balance.toJSON(), version: w.version };
}

function ledgerEntryView(e: WalletLedgerEntry): LedgerEntryView {
  return {
    id: e.id,
    transactionId: e.transactionId,
    direction: e.direction,
    money: e.money.toJSON(),
    balanceBefore: e.balanceBefore.toJSON(),
    balanceAfter: e.balanceAfter.toJSON(),
    createdAt: e.createdAt.toISOString(),
  };
}

function transactionView({ transaction: t, resultBalance }: StoredTransaction): TransactionView {
  return {
    transactionId: t.id,
    providerId: t.providerId,
    externalTransactionId: t.externalTransactionId,
    walletId: t.walletId,
    playerId: t.playerId,
    roundId: t.roundId,
    gameId: t.gameId,
    kind: t.kind,
    status: t.status,
    ...(t.failureCode ? { failureCode: t.failureCode } : {}),
    money: t.money.toJSON(),
    ...(resultBalance ? { balance: resultBalance.toJSON() } : {}),
    ...(t.referenceExternalTransactionId ? { referenceExternalTransactionId: t.referenceExternalTransactionId } : {}),
    createdAt: t.createdAt.toISOString(),
    ...(t.processedAt ? { processedAt: t.processedAt.toISOString() } : {}),
  };
}
