import { describe, expect, it } from "bun:test";
import { InboxMessage } from "../../../src/domain/inbox-message";
import { OutboxMessage } from "../../../src/domain/outbox-message";
import { WalletBalanceChanged } from "../../../src/domain/events";
import { InvalidMessageStateError } from "../../../src/domain/errors";
import { Money } from "../../../src/domain/money";
import { Wallet } from "../../../src/domain/wallet";

const t0 = new Date("2026-07-29T15:00:00.000Z");
const plus = (ms: number) => new Date(t0.getTime() + ms);

describe("InboxMessage", () => {
  const receive = () =>
    InboxMessage.receive({ messageId: "msg-1", consumerName: "wager-consumer", payloadHash: "h", receivedAt: t0 });

  it("nasce não processada", () => {
    const m = receive();
    expect(m.isProcessed()).toBe(false);
    expect(m.processedAt).toBeUndefined();
  });

  it("markProcessed registra o instante", () => {
    const m = receive();
    m.markProcessed(plus(5));
    expect(m.isProcessed()).toBe(true);
    expect(m.processedAt).toEqual(plus(5));
  });

  it("não pode ser processada duas vezes", () => {
    const m = receive();
    m.markProcessed(plus(5));
    expect(() => m.markProcessed(plus(6))).toThrow(InvalidMessageStateError);
  });

  it("rehydrate preserva o estado", () => {
    const m = InboxMessage.rehydrate({
      messageId: "m",
      consumerName: "c",
      payloadHash: "h",
      receivedAt: t0,
      processedAt: plus(1),
    });
    expect(m.isProcessed()).toBe(true);
  });
});

describe("OutboxMessage", () => {
  const event = () => {
    const wallet = Wallet.open({ id: "w1", playerId: "p1", initialBalance: Money.from({ amount: "100.00", currency: "BRL" }), now: t0 });
    const entry = wallet.debit({
      entryId: "e1",
      transactionId: "t1",
      money: Money.from({ amount: "25.00", currency: "BRL" }),
      at: t0,
    });
    return WalletBalanceChanged.from(wallet, entry, { eventId: "ev1", correlationId: "c1", occurredAt: t0 });
  };

  it("enqueue copia o envelope serializado e nasce pendente e devida", () => {
    const o = OutboxMessage.enqueue(event());
    expect(o.id).toBe("ev1");
    expect(o.aggregateId).toBe("w1");
    expect(o.eventType).toBe("WalletBalanceChanged");
    expect(o.payload["eventId"]).toBe("ev1");
    expect(o.attempts).toBe(0);
    expect(o.isPending()).toBe(true);
    expect(o.isDue(t0)).toBe(true);
  });

  it("scheduleRetry incrementa tentativas com backoff exponencial", () => {
    const o = OutboxMessage.enqueue(event());
    o.scheduleRetry(t0);
    expect(o.attempts).toBe(1);
    expect(o.nextAttemptAt).toEqual(plus(1_000));
    o.scheduleRetry(t0);
    expect(o.nextAttemptAt).toEqual(plus(2_000));
    o.scheduleRetry(t0);
    expect(o.nextAttemptAt).toEqual(plus(4_000));
  });

  it("o backoff tem teto de 5 minutos", () => {
    const o = OutboxMessage.enqueue(event());
    for (let i = 0; i < 30; i++) o.scheduleRetry(t0);
    expect(o.nextAttemptAt).toEqual(plus(300_000));
  });

  it("isDue respeita nextAttemptAt", () => {
    const o = OutboxMessage.enqueue(event());
    o.scheduleRetry(t0);
    expect(o.isDue(plus(999))).toBe(false);
    expect(o.isDue(plus(1_000))).toBe(true);
  });

  it("markPublished encerra o ciclo", () => {
    const o = OutboxMessage.enqueue(event());
    o.markPublished(plus(10));
    expect(o.isPending()).toBe(false);
    expect(o.publishedAt).toEqual(plus(10));
    expect(o.isDue(plus(100))).toBe(false);
  });

  it("rehydrate preserva tentativas, agenda e publicação", () => {
    const o = OutboxMessage.rehydrate({
      id: "ev9",
      aggregateId: "w1",
      eventType: "WalletBalanceChanged",
      payload: { eventId: "ev9" },
      occurredAt: t0,
      attempts: 3,
      nextAttemptAt: plus(4_000),
      publishedAt: plus(9_000),
    });
    expect(o.attempts).toBe(3);
    expect(o.nextAttemptAt).toEqual(plus(4_000));
    expect(o.isPending()).toBe(false);
  });

  it("não publica nem reagenda uma mensagem já publicada", () => {
    const o = OutboxMessage.enqueue(event());
    o.markPublished(plus(10));
    expect(() => o.markPublished(plus(11))).toThrow(InvalidMessageStateError);
    expect(() => o.scheduleRetry(plus(11))).toThrow(InvalidMessageStateError);
    expect(() => o.quarantine(plus(11), "erro")).toThrow(InvalidMessageStateError);
  });

  it("scheduleRetry guarda o último erro (truncado) para diagnóstico", () => {
    const o = OutboxMessage.enqueue(event());
    o.scheduleRetry(t0, "SenderFault: " + "x".repeat(1_000));
    expect(o.lastError?.startsWith("SenderFault: ")).toBe(true);
    expect(o.lastError).toHaveLength(500);
  });

  it("hasExhaustedAttempts só é verdadeiro ao atingir o limite", () => {
    const o = OutboxMessage.enqueue(event());
    for (let i = 0; i < 2; i++) o.scheduleRetry(t0);
    expect(o.hasExhaustedAttempts(3)).toBe(false);
    o.scheduleRetry(t0);
    expect(o.hasExhaustedAttempts(3)).toBe(true);
  });

  it("quarentena tira o evento do ciclo: deixa de estar pendente e de vencer, e não pode ser publicada nem reagendada", () => {
    const o = OutboxMessage.enqueue(event());
    o.scheduleRetry(t0, "falhou");
    o.quarantine(plus(5), "esgotou");
    expect(o.isQuarantined()).toBe(true);
    expect(o.isPending()).toBe(false);
    expect(o.isDue(plus(10_000_000))).toBe(false);
    expect(o.quarantinedAt).toEqual(plus(5));
    expect(o.nextAttemptAt).toBeUndefined();
    expect(o.lastError).toBe("esgotou");
    expect(() => o.markPublished(plus(6))).toThrow(InvalidMessageStateError);
    expect(() => o.scheduleRetry(plus(6))).toThrow(InvalidMessageStateError);
  });

  it("rehydrate preserva a quarentena e o último erro", () => {
    const o = OutboxMessage.rehydrate({
      id: "ev10",
      aggregateId: "w1",
      eventType: "WalletBalanceChanged",
      payload: { eventId: "ev10" },
      occurredAt: t0,
      attempts: 10,
      quarantinedAt: plus(1),
      lastError: "boom",
    });
    expect(o.isQuarantined()).toBe(true);
    expect(o.lastError).toBe("boom");
  });
});
