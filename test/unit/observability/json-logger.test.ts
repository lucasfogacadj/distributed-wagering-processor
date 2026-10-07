import { describe, expect, it } from "bun:test";
import { AsyncLocalLogContext } from "../../../src/infrastructure/observability/log-context";
import { JsonLogger, redact } from "../../../src/infrastructure/observability/json-logger";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function setup() {
  const lines: string[] = [];
  const context = new AsyncLocalLogContext();
  const logger = new JsonLogger({ context, sink: (line) => lines.push(line), now: () => new Date("2026-07-29T15:00:00.000Z") });
  return { lines, context, logger, records: () => lines.map((l) => JSON.parse(l)) };
}

describe("JsonLogger", () => {
  it("escreve uma linha JSON por evento, com nível, instante ISO e mensagem", () => {
    const { logger, lines, records } = setup();
    logger.info("evento_a", { status: "PROCESSED" });
    logger.warn("evento_b");
    logger.error("evento_c");

    expect(lines).toHaveLength(3);
    expect(lines.every((l) => !l.includes("\n"))).toBe(true);
    expect(records()[0]).toEqual({ level: "info", time: "2026-07-29T15:00:00.000Z", message: "evento_a", status: "PROCESSED" });
    expect(records().map((r) => r.level)).toEqual(["info", "warn", "error"]);
  });

  it("acrescenta o contexto corrente (correlationId, messageId, transactionId, walletId, providerId)", () => {
    const { logger, context, records } = setup();
    context.run({ correlationId: "corr-1", messageId: "msg-1" }, () => {
      context.add({ transactionId: "tx-1", walletId: "w-1", providerId: "provider-a" });
      logger.info("dentro");
    });
    logger.info("fora");

    expect(records()[0]).toMatchObject({
      message: "dentro",
      correlationId: "corr-1",
      messageId: "msg-1",
      transactionId: "tx-1",
      walletId: "w-1",
      providerId: "provider-a",
    });
    expect(records()[1]).not.toHaveProperty("correlationId");
  });

  it("campos explícitos prevalecem sobre o contexto", () => {
    const { logger, context, records } = setup();
    context.run({ walletId: "do-contexto" }, () => logger.info("x", { walletId: "explicito" }));
    expect(records()[0].walletId).toBe("explicito");
  });

  it("o contexto não vaza entre tarefas assíncronas concorrentes", async () => {
    const { logger, context, records } = setup();
    await Promise.all(
      ["a", "b", "c", "d"].map((id, i) =>
        context.run({ correlationId: id }, async () => {
          await sleep(10 * (4 - i));
          context.add({ transactionId: `tx-${id}` });
          await sleep(5);
          logger.info("fim");
        }),
      ),
    );

    for (const r of records()) expect(r.transactionId).toBe(`tx-${r.correlationId}`);
    expect(records()).toHaveLength(4);
  });

  it("add() em um escopo filho não altera o escopo pai", () => {
    const { logger, context, records } = setup();
    context.run({ correlationId: "pai" }, () => {
      context.run({}, () => context.add({ transactionId: "so-do-filho" }));
      logger.info("pai");
    });
    expect(records()[0]).not.toHaveProperty("transactionId");
  });

  it("add() fora de um escopo é ignorado, sem lançar", () => {
    const { context } = setup();
    expect(() => context.add({ walletId: "x" })).not.toThrow();
    expect(context.snapshot()).toEqual({});
  });
});

describe("redact", () => {
  it("mascara chaves sensíveis em qualquer profundidade, inclusive dentro de arrays", () => {
    const out = redact({
      walletId: "w-1",
      money: { amount: "25.00", currency: "BRL" },
      nested: { password: "x", token: "y", authorization: "Bearer z", secret: "s", keep: "ok" },
      list: [{ amount: "1.00", id: 1 }],
      payload: { qualquer: "coisa" },
      balanceAfter: "75.00",
    });
    expect(out).toEqual({
      walletId: "w-1",
      money: "[REDACTED]",
      nested: { password: "[REDACTED]", token: "[REDACTED]", authorization: "[REDACTED]", secret: "[REDACTED]", keep: "ok" },
      list: [{ amount: "[REDACTED]", id: 1 }],
      payload: "[REDACTED]",
      balanceAfter: "[REDACTED]",
    });
  });

  it("preserva identificadores e metadados operacionais", () => {
    const fields = { correlationId: "c", messageId: "m", transactionId: "t", walletId: "w", providerId: "p", status: "PROCESSED", attempts: 3 };
    expect(redact(fields)).toEqual(fields);
  });

  it("serializa erros sem estourar e tolera referências circulares", () => {
    const circular: Record<string, unknown> = { a: 1 };
    circular["self"] = circular;
    expect(() => JSON.stringify(redact({ error: new Error("falhou"), circular }))).not.toThrow();
    expect((redact({ error: new Error("falhou") }) as { error: { message: string } }).error.message).toBe("falhou");
  });

  it("o logger aplica a redação nos campos antes de escrever", () => {
    const { logger, lines } = setup();
    logger.info("x", { money: { amount: "99.99", currency: "BRL" }, walletId: "w-1" });
    expect(lines[0]).not.toContain("99.99");
    expect(lines[0]).toContain("w-1");
  });
});
