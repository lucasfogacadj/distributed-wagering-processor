import { BalanceLimitExceededError, CurrencyMismatchError, InsufficientFundsError } from "./errors";
import { FailureCode } from "./failure-code";
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from "./wager-transaction";

export type ReferenceDecision =
  | { outcome: "apply" }
  | { outcome: "wait" }
  | { outcome: "reject"; code: FailureCode };

const APPLY: ReferenceDecision = { outcome: "apply" };
const WAIT: ReferenceDecision = { outcome: "wait" };
const reject = (code: FailureCode): ReferenceDecision => ({ outcome: "reject", code });

/** Regras puras de negócio das referências. Sem I/O: quem chama carrega os dados. */
export class WagerPolicy {
  /**
   * Decide o que fazer com uma reversão diante da referência encontrada.
   * @param alreadyReversed a referência já foi revertida (REFUND ou ROLLBACK PROCESSED). Uma referência é
   *   revertida no máximo uma vez, de qualquer tipo: REFUND e ROLLBACK creditariam o mesmo valor duas vezes.
   *   Vale também para uma WIN que aponta para a BET: pagar o prêmio de uma aposta já devolvida duplicaria crédito.
   */
  static evaluateReference(
    tx: WagerTransaction,
    reference: WagerTransaction | undefined,
    alreadyReversed: boolean,
  ): ReferenceDecision {
    if (!tx.hasReferenceToResolve()) return APPLY;
    if (!reference) return WAIT;

    if (
      reference.providerId !== tx.providerId ||
      reference.playerId !== tx.playerId ||
      reference.walletId !== tx.walletId ||
      reference.roundId !== tx.roundId ||
      reference.money.currency !== tx.money.currency
    ) {
      return reject(FailureCode.ReferenceMismatch);
    }

    if (!WagerPolicy.isReferenceable(tx.kind, reference.kind)) {
      return reject(FailureCode.InvalidReferenceKind);
    }

    switch (reference.status) {
      case WagerTransactionStatus.Processed:
        break;
      case WagerTransactionStatus.Rejected:
      case WagerTransactionStatus.Failed:
        return reject(FailureCode.ReferenceNotProcessed);
      default:
        return WAIT;
    }

    // Reversão é total; o prêmio (WIN) pode ter qualquer valor.
    if (tx.requiresReference() && !tx.money.equals(reference.money)) return reject(FailureCode.AmountMismatch);
    if (alreadyReversed) return reject(FailureCode.AlreadyReversed);
    return APPLY;
  }

  /** Traduz um erro de domínio da wallet em failureCode; undefined = não é regra de negócio. */
  static failureCodeFor(tx: WagerTransaction, error: unknown): FailureCode | undefined {
    if (error instanceof InsufficientFundsError) {
      return tx.requiresReference() ? FailureCode.ReversalWouldOverdraw : FailureCode.InsufficientFunds;
    }
    if (error instanceof BalanceLimitExceededError) return FailureCode.BalanceLimitExceeded;
    if (error instanceof CurrencyMismatchError) return FailureCode.CurrencyMismatch;
    return undefined;
  }

  /** WIN só referencia BET; REFUND só referencia BET; ROLLBACK referencia BET, WIN ou REFUND. */
  private static isReferenceable(source: WagerTransactionKind, target: WagerTransactionKind): boolean {
    if (source === WagerTransactionKind.Win) return target === WagerTransactionKind.Bet;
    if (source === WagerTransactionKind.Refund) return target === WagerTransactionKind.Bet;
    return (
      target === WagerTransactionKind.Bet ||
      target === WagerTransactionKind.Win ||
      target === WagerTransactionKind.Refund
    );
  }
}
