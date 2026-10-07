import { describe, expect, it } from "bun:test";
import {
  ConcurrentModificationError,
  IdempotencyConflictError,
  InvalidQueryError,
  ResourceNotFoundError,
  WalletAlreadyExistsError,
} from "../../../src/application/errors";
import { classifyError, lockConflictKind } from "../../../src/application/error-classification";
import {
  CurrencyMismatchError,
  InvalidLedgerEntryError,
  InvalidMessageStateError,
  InvalidMoneyError,
  InvalidTransactionError,
  InvalidTransactionStateError,
} from "../../../src/domain/errors";

const withCode = (code: string, message = "x") => Object.assign(new Error(message), { code });

describe("lockConflictKind", () => {
  it("identifica deadlock, serialização, timeout de lock e conflito de versão", () => {
    expect(lockConflictKind(withCode("40P01"))).toBe("deadlock");
    expect(lockConflictKind(withCode("40001"))).toBe("serialization");
    expect(lockConflictKind(withCode("55P03"))).toBe("lock_timeout");
    expect(lockConflictKind(new ConcurrentModificationError("x"))).toBe("optimistic_version");
  });

  it("enxerga o código na cadeia de causas (erros do ORM embrulham o do driver)", () => {
    expect(lockConflictKind(new Error("wrapper", { cause: withCode("55P03") }))).toBe("lock_timeout");
  });

  it("outros erros não são conflito de lock", () => {
    expect(lockConflictKind(withCode("ECONNREFUSED"))).toBeUndefined();
    expect(lockConflictKind(new Error("boom"))).toBeUndefined();
    expect(lockConflictKind(null)).toBeUndefined();
  });
});

describe("classifyError", () => {
  it("falhas transitórias: lock otimista, deadlock, serialização, conexão", () => {
    expect(classifyError(new ConcurrentModificationError("x"))).toBe("transient");
    expect(classifyError(withCode("40P01"))).toBe("transient");
    expect(classifyError(withCode("40001"))).toBe("transient");
    expect(classifyError(withCode("55P03"))).toBe("transient");
    expect(classifyError(withCode("57P01"))).toBe("transient");
    expect(classifyError(withCode("ECONNREFUSED"))).toBe("transient");
    expect(classifyError(withCode("ECONNRESET"))).toBe("transient");
    expect(classifyError(withCode("ETIMEDOUT"))).toBe("transient");
  });

  it("enxerga o código transitório na cadeia de causas (erros do ORM embrulham o do driver)", () => {
    const wrapped = new Error("wrapper", { cause: withCode("40P01") });
    expect(classifyError(wrapped)).toBe("transient");
    expect(classifyError(new Error("a", { cause: new Error("b", { cause: withCode("ECONNRESET") }) }))).toBe("transient");
  });

  it("timeout de pool de conexões é transitório", () => {
    expect(classifyError(new Error("timeout exceeded when trying to connect"))).toBe("transient");
    expect(classifyError(new Error("Connection terminated unexpectedly"))).toBe("transient");
  });

  it("entradas inválidas (não adianta reenviar igual)", () => {
    for (const error of [
      new InvalidMoneyError("x"),
      new InvalidTransactionError("x"),
      new InvalidQueryError("x"),
    ]) {
      expect(classifyError(error)).toBe("invalid");
    }
  });

  it("lançamento de ledger inválido é bug, não entrada inválida", () => {
    expect(classifyError(new InvalidLedgerEntryError("x"))).toBe("unknown");
  });

  it("conflitos e regras de negócio são terminais", () => {
    expect(classifyError(new IdempotencyConflictError("x"))).toBe("business");
    expect(classifyError(new WalletAlreadyExistsError("x"))).toBe("business");
    expect(classifyError(new ResourceNotFoundError("x"))).toBe("business");
    expect(classifyError(new CurrencyMismatchError("x"))).toBe("business");
  });

  it("estados impossíveis são bugs, não negócio", () => {
    expect(classifyError(new InvalidTransactionStateError("x"))).toBe("unknown");
    expect(classifyError(new InvalidMessageStateError("x"))).toBe("unknown");
  });

  it("desconhecidos", () => {
    expect(classifyError(new Error("boom"))).toBe("unknown");
    expect(classifyError(withCode("23505"))).toBe("unknown");
    expect(classifyError(null)).toBe("unknown");
    expect(classifyError("texto")).toBe("unknown");
  });
});
