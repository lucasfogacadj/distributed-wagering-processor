import { InvalidTransactionError, InvalidTransactionStateError } from "./errors";
import { FailureCode } from "./failure-code";
import { Money } from "./money";
import { LedgerDirection } from "./wallet-ledger-entry";

export enum WagerTransactionKind {
  Opening = "OPENING",
  Bet = "BET",
  Win = "WIN",
  Loss = "LOSS",
  Refund = "REFUND",
  Rollback = "ROLLBACK",
}

/**
 * Transições válidas:
 *   PENDING           → PENDING_REFERENCE | PROCESSED | REJECTED | FAILED
 *   PENDING_REFERENCE → PENDING_REFERENCE | PROCESSED | REJECTED | FAILED
 *   PROCESSED, REJECTED, FAILED são terminais.
 */
export enum WagerTransactionStatus {
  Pending = "PENDING",
  PendingReference = "PENDING_REFERENCE",
  Processed = "PROCESSED",
  Rejected = "REJECTED",
  Failed = "FAILED",
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

export interface WagerTransactionState extends CreateWagerTransactionProps {
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    /** id no provedor — não o id interno */
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
  ) {}

  /** Nasce em PENDING. OPENING é interno e não pode ser criado por aqui. */
  static create(p: CreateWagerTransactionProps): WagerTransaction {
    if (p.kind === WagerTransactionKind.Opening) {
      throw new InvalidTransactionError("OPENING é interno e não pode ser submetido");
    }
    const needsReference = p.kind === WagerTransactionKind.Refund || p.kind === WagerTransactionKind.Rollback;
    if (needsReference && !p.referenceExternalTransactionId) {
      throw new InvalidTransactionError(`${p.kind} exige referenceExternalTransactionId`);
    }
    if (p.kind === WagerTransactionKind.Bet && p.referenceExternalTransactionId) {
      throw new InvalidTransactionError("BET não aceita referência");
    }
    const allowsZero = p.kind === WagerTransactionKind.Loss;
    if (p.money.isNegative() || (p.money.isZero() && !allowsZero)) {
      throw new InvalidTransactionError(`Valor inválido para ${p.kind}: ${p.money}`);
    }
    return WagerTransaction.build({ ...p, status: WagerTransactionStatus.Pending });
  }

  /** Crédito interno de abertura da wallet: já nasce PROCESSED, com chaves determinísticas por wallet. */
  static createOpening(p: { id: string; walletId: string; playerId: string; money: Money; at: Date }): WagerTransaction {
    if (!p.money.isPositive()) {
      throw new InvalidTransactionError(`Valor inválido para OPENING: ${p.money}`);
    }
    const key = `opening:${p.walletId}`;
    const tx = WagerTransaction.build({
      id: p.id,
      providerId: "internal",
      externalTransactionId: key,
      idempotencyKey: key,
      payloadHash: key,
      walletId: p.walletId,
      playerId: p.playerId,
      roundId: "opening",
      gameId: "opening",
      kind: WagerTransactionKind.Opening,
      money: p.money,
      createdAt: p.at,
      status: WagerTransactionStatus.Pending,
    });
    tx.markProcessed(undefined, p.at);
    return tx;
  }

  /** Reconstrução a partir da persistência — não revalida regras de transição. */
  static rehydrate(s: WagerTransactionState): WagerTransaction {
    return WagerTransaction.build(s);
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertNotTerminal("markProcessed");
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
  }

  markPendingReference(): void {
    this.assertNotTerminal("markPendingReference");
    this._status = WagerTransactionStatus.PendingReference;
  }

  reject(code: FailureCode): void {
    this.assertNotTerminal("reject");
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
  }

  fail(code: FailureCode): void {
    this.assertNotTerminal("fail");
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
  }

  isTerminal(): boolean {
    return (
      this._status === WagerTransactionStatus.Processed ||
      this._status === WagerTransactionStatus.Rejected ||
      this._status === WagerTransactionStatus.Failed
    );
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  /** Reversões sempre; WIN apenas quando o provedor informou a BET de origem. */
  hasReferenceToResolve(): boolean {
    return this.requiresReference() || (this.kind === WagerTransactionKind.Win && this.referenceExternalTransactionId !== undefined);
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /** ROLLBACK é o inverso do lançamento da transação referenciada. */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
      case WagerTransactionKind.Opening:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new InvalidTransactionError("ROLLBACK exige a transação referenciada");
        }
        return reference.ledgerDirectionFor() === LedgerDirection.Debit
          ? LedgerDirection.Credit
          : LedgerDirection.Debit;
      }
      case WagerTransactionKind.Loss:
        throw new InvalidTransactionError("LOSS não movimenta saldo");
    }
  }

  private assertNotTerminal(action: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(`${action} inválido: transação ${this.id} está ${this._status}`);
    }
  }

  private static build(x: WagerTransactionState | (CreateWagerTransactionProps & { status: WagerTransactionStatus })): WagerTransaction {
    const s = x as WagerTransactionState;
    return new WagerTransaction(
      s.id,
      s.providerId,
      s.externalTransactionId,
      s.idempotencyKey,
      s.payloadHash,
      s.walletId,
      s.playerId,
      s.roundId,
      s.gameId,
      s.kind,
      s.money,
      s.referenceExternalTransactionId,
      s.createdAt,
      s.status,
      s.referenceTransactionId,
      s.failureCode,
      s.processedAt,
    );
  }
}
