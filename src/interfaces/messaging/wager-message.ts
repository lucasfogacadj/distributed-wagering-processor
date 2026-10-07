import { z } from "zod";
import type { ProcessWagerCommand } from "../../application/process-wager-transaction";
import { submitTransactionSchema } from "../http/dto";

export interface MessageIssue {
  path: string;
  message: string;
}

/** Mensagem malformada: erro permanente (reenviar igual não adianta) → DLQ. Nunca ecoa o conteúdo. */
export class MessageParseError extends Error {
  readonly code = "INVALID_MESSAGE";

  constructor(readonly issues: MessageIssue[]) {
    super(`Mensagem inválida (campos: ${issues.map((i) => i.path || "<raiz>").join(", ")})`);
    this.name = "MessageParseError";
  }
}

const wagerMessageSchema = z.object({
  messageId: z.string().min(1).max(256),
  type: z.literal("WagerTransactionRequested"),
  occurredAt: z.string().refine((value) => !Number.isNaN(Date.parse(value)), "data/hora inválida"),
  data: submitTransactionSchema.extend({
    idempotencyKey: z.string().regex(/^[\x20-\x7E]{1,255}$/, "1 a 255 caracteres ASCII imprimíveis"),
  }),
});

export interface ParsedWagerMessage {
  /** O mesmo comando do use case HTTP; a identidade da inbox é acrescentada pelo consumidor. */
  command: ProcessWagerCommand;
  inbox: { messageId: string };
}

export function parseWagerMessage(body: string): ParsedWagerMessage {
  let json: unknown;
  try {
    json = JSON.parse(body);
  } catch {
    throw new MessageParseError([{ path: "", message: "JSON malformado" }]);
  }

  const result = wagerMessageSchema.safeParse(json);
  if (!result.success) {
    throw new MessageParseError(result.error.issues.map((i) => ({ path: i.path.join("."), message: i.message })));
  }

  const { messageId, data } = result.data;
  return {
    command: {
      idempotencyKey: data.idempotencyKey,
      providerId: data.providerId,
      externalTransactionId: data.externalTransactionId,
      playerId: data.playerId,
      walletId: data.walletId,
      roundId: data.roundId,
      gameId: data.gameId,
      kind: data.kind,
      money: data.money,
      ...(data.referenceExternalTransactionId !== undefined
        ? { referenceExternalTransactionId: data.referenceExternalTransactionId }
        : {}),
      correlationId: messageId,
      causationId: messageId,
    },
    inbox: { messageId },
  };
}
