import {
  DomainError,
  InvalidLedgerEntryError,
  InvalidMessageStateError,
  InvalidMoneyError,
  InvalidTransactionError,
  InvalidTransactionStateError,
} from "../domain/errors";
import { ConcurrentModificationError, InvalidQueryError } from "./errors";

/**
 * - transient: tentar de novo tende a funcionar (HTTP 503 / SQS retry com backoff);
 * - invalid:   a entrada é inaceitável e reenviar igual não ajuda (HTTP 400 / SQS DLQ);
 * - business:  conflito ou regra de negócio terminal (HTTP 4xx / SQS ack);
 * - unknown:   bug ou falha não prevista (HTTP 500 / SQS retry até a DLQ).
 */
export type ErrorClass = "transient" | "invalid" | "business" | "unknown";

const TRANSIENT_CODES = new Set([
  // PostgreSQL: serialização, deadlock, lock indisponível, shutdown, conexões, classe 08
  "40001",
  "40P01",
  "55P03",
  "57P01",
  "57P02",
  "57P03",
  "53300",
  "53400",
  "08000",
  "08001",
  "08003",
  "08004",
  "08006",
  "08007",
  // rede
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
  "EPIPE",
  "ENOTFOUND",
  "EAI_AGAIN",
]);

const TRANSIENT_MESSAGE = /timeout exceeded when trying to connect|connection terminated|connection (?:refused|reset)/i;
const MAX_CAUSE_DEPTH = 5;

function isTransientInfrastructure(error: unknown): boolean {
  let current: unknown = error;
  for (let depth = 0; depth < MAX_CAUSE_DEPTH && current instanceof Error; depth++) {
    const code = (current as { code?: unknown }).code;
    if (typeof code === "string" && TRANSIENT_CODES.has(code)) return true;
    if (TRANSIENT_MESSAGE.test(current.message)) return true;
    current = current.cause;
  }
  return false;
}

export type LockConflictKind = "deadlock" | "serialization" | "lock_timeout" | "optimistic_version";

/** Se o erro é um conflito de lock, qual; é o que alimenta a métrica `lock_conflicts_total`. */
export function lockConflictKind(error: unknown): LockConflictKind | undefined {
  if (error instanceof ConcurrentModificationError) return "optimistic_version";

  let current: unknown = error;
  for (let depth = 0; depth < MAX_CAUSE_DEPTH && current instanceof Error; depth++) {
    switch ((current as { code?: unknown }).code) {
      case "40P01":
        return "deadlock";
      case "40001":
        return "serialization";
      case "55P03":
        return "lock_timeout";
    }
    current = current.cause;
  }
  return undefined;
}

export function classifyError(error: unknown): ErrorClass {
  if (error instanceof ConcurrentModificationError) return "transient";
  if (isTransientInfrastructure(error)) return "transient";

  // Estados impossíveis e lançamentos com aritmética errada indicam bug, mesmo sendo DomainError: a entrada
  // já passou pela validação, então o problema não é do chamador.
  if (
    error instanceof InvalidTransactionStateError ||
    error instanceof InvalidMessageStateError ||
    error instanceof InvalidLedgerEntryError
  ) {
    return "unknown";
  }

  if (error instanceof InvalidMoneyError || error instanceof InvalidTransactionError || error instanceof InvalidQueryError) {
    return "invalid";
  }
  if (error instanceof DomainError) return "business";
  return "unknown";
}
