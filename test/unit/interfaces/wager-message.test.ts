import { describe, expect, it } from "bun:test";
import { WagerTransactionKind } from "../../../src/domain/wager-transaction";
import { MessageParseError, parseWagerMessage } from "../../../src/interfaces/messaging/wager-message";

const valid = () => ({
  messageId: "msg-123",
  type: "WagerTransactionRequested",
  occurredAt: "2026-07-29T15:00:00.000Z",
  data: {
    providerId: "provider-a",
    externalTransactionId: "transaction-123",
    idempotencyKey: "provider-a:transaction-123",
    playerId: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    walletId: "0192f291-27dd-7d3f-8071-5f8685deef37",
    roundId: "round-987",
    gameId: "fortune-chimp",
    kind: "BET",
    money: { amount: "25.00", currency: "BRL" },
  },
});

const parse = (value: unknown) => parseWagerMessage(typeof value === "string" ? value : JSON.stringify(value));

describe("parseWagerMessage", () => {
  it("converte a mensagem do enunciado no mesmo comando do use case HTTP", () => {
    const { command, inbox } = parse(valid());
    expect(command).toEqual({
      idempotencyKey: "provider-a:transaction-123",
      providerId: "provider-a",
      externalTransactionId: "transaction-123",
      playerId: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
      walletId: "0192f291-27dd-7d3f-8071-5f8685deef37",
      roundId: "round-987",
      gameId: "fortune-chimp",
      kind: WagerTransactionKind.Bet,
      money: { amount: "25.00", currency: "BRL" },
      correlationId: "msg-123",
      causationId: "msg-123",
    });
    expect(inbox).toEqual({ messageId: "msg-123" });
  });

  it("carrega a referência de REFUND/ROLLBACK", () => {
    const msg = valid();
    Object.assign(msg.data, { kind: "REFUND", referenceExternalTransactionId: "transaction-100" });
    expect(parse(msg).command.referenceExternalTransactionId).toBe("transaction-100");
  });

  it("ignora campos extras", () => {
    const msg = { ...valid(), extra: 1 };
    Object.assign(msg.data, { surplus: true });
    expect(() => parse(msg)).not.toThrow();
  });

  it.each([
    ["JSON malformado", "{ não é json"],
    ["corpo não é objeto", "[1,2]"],
    ["tipo desconhecido", { ...valid(), type: "Outra" }],
    ["messageId ausente", (() => { const m: any = valid(); delete m.messageId; return m; })()],
    ["messageId vazio", { ...valid(), messageId: "" }],
    ["data ausente", (() => { const m: any = valid(); delete m.data; return m; })()],
    ["idempotencyKey ausente", (() => { const m: any = valid(); delete m.data.idempotencyKey; return m; })()],
    ["walletId não é uuid", (() => { const m: any = valid(); m.data.walletId = "x"; return m; })()],
    ["kind OPENING (interno)", (() => { const m: any = valid(); m.data.kind = "OPENING"; return m; })()],
    ["kind desconhecido", (() => { const m: any = valid(); m.data.kind = "DEPOSIT"; return m; })()],
    ["money ausente", (() => { const m: any = valid(); delete m.data.money; return m; })()],
    ["amount numérico", (() => { const m: any = valid(); m.data.money.amount = 25; return m; })()],
  ])("rejeita com MessageParseError: %s", (_name, input) => {
    expect(() => parse(input)).toThrow(MessageParseError);
  });

  it("o erro lista os campos inválidos sem ecoar o payload", () => {
    const msg: any = valid();
    msg.data.walletId = "valor-sensivel-12345";
    try {
      parse(msg);
      throw new Error("deveria falhar");
    } catch (error) {
      expect(error).toBeInstanceOf(MessageParseError);
      expect((error as MessageParseError).issues.map((i) => i.path)).toContain("data.walletId");
      expect((error as Error).message).not.toContain("valor-sensivel-12345");
    }
  });
});
