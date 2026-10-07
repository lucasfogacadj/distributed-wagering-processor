import { createHash } from "node:crypto";
import type { MoneyProps } from "../domain/money";

/** Campos de negócio que entram no hash. O header Idempotency-Key e metadados de transporte ficam de fora. */
export interface PayloadFields {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string | undefined;
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, v]) => v !== undefined)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([k, v]) => [k, canonicalize(v)]),
    );
  }
  return value;
}

/** JSON canônico: chaves ordenadas recursivamente, sem espaços, campos undefined omitidos. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalize(value));
}

/**
 * payloadHash = SHA-256 (hex) do JSON canônico dos campos de negócio.
 * `money` é normalizado para { amount, currency } com escala 2, então "25.00" e uma re-serialização
 * equivalente produzem o mesmo hash.
 */
export function payloadHash(fields: PayloadFields): string {
  const normalized = { ...fields, money: { amount: fields.money.amount, currency: fields.money.currency } };
  return createHash("sha256").update(canonicalJson(normalized)).digest("hex");
}
