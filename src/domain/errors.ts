/** Erro base do domínio. `code` é estável e legível por máquina. */
export abstract class DomainError extends Error {
  abstract readonly code: string;

  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class InvalidMoneyError extends DomainError {
  readonly code = "INVALID_MONEY";
}

export class CurrencyMismatchError extends DomainError {
  readonly code = "CURRENCY_MISMATCH";
}

export class InsufficientFundsError extends DomainError {
  readonly code = "INSUFFICIENT_FUNDS";
}

/** Crédito que levaria o saldo acima do limite suportado pelo sistema. */
export class BalanceLimitExceededError extends DomainError {
  readonly code = "BALANCE_LIMIT_EXCEEDED";
}

export class InvalidLedgerEntryError extends DomainError {
  readonly code = "INVALID_LEDGER_ENTRY";
}

/** Transação malformada (kind/referência/valor incompatíveis). */
export class InvalidTransactionError extends DomainError {
  readonly code = "INVALID_TRANSACTION";
}

/** Mensagem de inbox/outbox usada fora do seu ciclo de vida (ex.: publicar duas vezes). */
export class InvalidMessageStateError extends DomainError {
  readonly code = "INVALID_MESSAGE_STATE";
}

/** Tentativa de transição a partir de estado terminal: erro de programação, não caminho de negócio. */
export class InvalidTransactionStateError extends DomainError {
  readonly code = "INVALID_TRANSACTION_STATE";
}
