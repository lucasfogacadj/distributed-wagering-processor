import { z } from "zod";
import { InvalidQueryError } from "../../application/errors";
import { DEFAULT_LEDGER_LIMIT, MAX_LEDGER_LIMIT } from "../../application/queries";
import { WagerTransactionKind } from "../../domain/wager-transaction";

export interface ValidationIssue {
  path: string;
  message: string;
}

export class RequestValidationError extends Error {
  readonly code = "INVALID_REQUEST";

  constructor(readonly issues: ValidationIssue[]) {
    super("Requisição inválida");
    this.name = "RequestValidationError";
  }
}

/** A forma só é checada aqui; formato monetário e regras de negócio ficam com o domínio (Money.from). */
const moneySchema = z.object({ amount: z.string(), currency: z.string() });
const shortText = z.string().min(1).max(128);

export const createWalletSchema = z.object({
  playerId: z.string().uuid(),
  initialBalance: moneySchema,
});

/** OPENING é interno: não pode ser submetido pela API. */
export const submitTransactionSchema = z.object({
  providerId: shortText,
  externalTransactionId: shortText,
  playerId: z.string().uuid(),
  walletId: z.string().uuid(),
  roundId: shortText,
  gameId: shortText,
  kind: z.enum([
    WagerTransactionKind.Bet,
    WagerTransactionKind.Win,
    WagerTransactionKind.Loss,
    WagerTransactionKind.Refund,
    WagerTransactionKind.Rollback,
  ]),
  money: moneySchema,
  referenceExternalTransactionId: shortText.optional(),
});

export const uuidSchema = z.string().uuid();

export function parse<S extends z.ZodType>(schema: S, value: unknown): z.output<S> {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new RequestValidationError(
      result.error.issues.map((i) => ({ path: i.path.join("."), message: i.message })),
    );
  }
  return result.data;
}

/** O header Idempotency-Key é obrigatório e é a fonte da verdade da idempotência. */
export function parseIdempotencyKey(header: unknown): string {
  if (typeof header !== "string" || !/^[\x20-\x7E]{1,255}$/.test(header) || header.trim() === "") {
    throw new RequestValidationError([
      { path: "Idempotency-Key", message: "header obrigatório: 1 a 255 caracteres ASCII imprimíveis" },
    ]);
  }
  return header;
}

export function parseLedgerQuery(query: { cursor?: unknown; limit?: unknown }): { cursor?: string; limit: number } {
  const { cursor, limit } = query;
  if (cursor !== undefined && typeof cursor !== "string") throw new InvalidQueryError("cursor inválido");

  let parsedLimit = DEFAULT_LEDGER_LIMIT;
  if (limit !== undefined) {
    if (typeof limit !== "string" || !/^\d+$/.test(limit) || Number(limit) < 1 || Number(limit) > MAX_LEDGER_LIMIT) {
      throw new InvalidQueryError(`limit deve ser um inteiro entre 1 e ${MAX_LEDGER_LIMIT}`);
    }
    parsedLimit = Number(limit);
  }
  return { ...(cursor !== undefined ? { cursor } : {}), limit: parsedLimit };
}
