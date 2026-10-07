import { describe, expect, it } from "bun:test";
import { canonicalJson, payloadHash } from "../../../src/application/payload-hash";

const fields = {
  providerId: "provider-a",
  externalTransactionId: "transaction-123",
  playerId: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  walletId: "0192f291-27dd-7d3f-8071-5f8685deef37",
  roundId: "round-987",
  gameId: "fortune-chimp",
  kind: "BET",
  money: { amount: "25.00", currency: "BRL" },
};

describe("canonicalJson", () => {
  it("ordena as chaves recursivamente", () => {
    expect(canonicalJson({ b: 1, a: { d: 1, c: 2 } })).toBe('{"a":{"c":2,"d":1},"b":1}');
  });

  it("preserva a ordem dos arrays", () => {
    expect(canonicalJson({ a: [3, 1, 2] })).toBe('{"a":[3,1,2]}');
  });

  it("omite campos undefined", () => {
    expect(canonicalJson({ a: 1, b: undefined })).toBe('{"a":1}');
  });
});

describe("payloadHash", () => {
  it("é SHA-256 em hexadecimal (64 caracteres)", () => {
    expect(payloadHash(fields)).toMatch(/^[0-9a-f]{64}$/);
  });

  it("é determinístico e independe da ordem das chaves", () => {
    const reordered = {
      money: { currency: "BRL", amount: "25.00" },
      kind: "BET",
      gameId: "fortune-chimp",
      roundId: "round-987",
      walletId: fields.walletId,
      playerId: fields.playerId,
      externalTransactionId: "transaction-123",
      providerId: "provider-a",
    };
    expect(payloadHash(reordered)).toBe(payloadHash(fields));
  });

  it("muda quando qualquer campo de negócio muda", () => {
    const base = payloadHash(fields);
    expect(payloadHash({ ...fields, money: { amount: "25.01", currency: "BRL" } })).not.toBe(base);
    expect(payloadHash({ ...fields, money: { amount: "25.00", currency: "USD" } })).not.toBe(base);
    expect(payloadHash({ ...fields, kind: "WIN" })).not.toBe(base);
    expect(payloadHash({ ...fields, roundId: "round-1" })).not.toBe(base);
    expect(payloadHash({ ...fields, referenceExternalTransactionId: "x" })).not.toBe(base);
  });

  it("referência ausente e undefined produzem o mesmo hash", () => {
    expect(payloadHash({ ...fields, referenceExternalTransactionId: undefined })).toBe(payloadHash(fields));
  });
});
