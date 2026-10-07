import { DomainError } from "../domain/errors";

/** Mesma idempotency key (ou mesmo externalTransactionId) com payload diferente: conflito, não replay. */
export class IdempotencyConflictError extends DomainError {
  readonly code = "IDEMPOTENCY_CONFLICT";
}

export class WalletAlreadyExistsError extends DomainError {
  readonly code = "WALLET_ALREADY_EXISTS";
}

export class ResourceNotFoundError extends DomainError {
  readonly code: string = "RESOURCE_NOT_FOUND";
}

export class WalletNotFoundError extends ResourceNotFoundError {
  override readonly code = "WALLET_NOT_FOUND";
}

export class TransactionNotFoundError extends ResourceNotFoundError {
  override readonly code = "TRANSACTION_NOT_FOUND";
}

/** Parâmetro de consulta inválido (cursor, limit): 400, não adianta reenviar igual. */
export class InvalidQueryError extends DomainError {
  readonly code = "INVALID_QUERY";
}

/** A guarda de versão (optimistic lock) detectou uma escrita concorrente. Transitório: pode reenviar. */
export class ConcurrentModificationError extends DomainError {
  readonly code = "CONCURRENT_MODIFICATION";
}
