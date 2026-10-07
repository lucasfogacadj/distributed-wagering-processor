import { describe, expect, it } from "bun:test";
import { GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import { createClusterEnv, type ApiResponse, type ClusterEnv } from "../support/cluster";
import { assertLedgerMatchesBalances } from "../support/harness";
import { approximateMessages, waitFor } from "../support/sqs";
import {
  balanceOf,
  brl,
  count,
  createWalletVia,
  debitsOf,
  sendMessages,
  submitVia,
  wagerBody,
  wagerMessage,
  type TestWallet,
} from "../support/wager-fixtures";

/**
 * Shutdown gracioso com processos reais, pelo gatilho NATIVO da plataforma em que o teste roda:
 *  - Linux/macOS: SIGTERM (o que `docker stop` e o Kubernetes mandam);
 *  - Windows: mensagem IPC (não há SIGTERM entre processos no Windows — ver o controle negativo abaixo).
 * Nos dois casos o caminho é o mesmo `app.close()`: parar de aceitar, terminar o que está em andamento, devolver a
 * visibilidade do que não começou e fechar o ORM.
 */
async function withEnv(fn: (env: ClusterEnv) => Promise<void>): Promise<void> {
  const env = await createClusterEnv();
  try {
    await fn(env);
  } finally {
    await env.close();
  }
}

async function notVisible(env: ClusterEnv): Promise<number> {
  const { Attributes } = await env.queues.client.send(
    new GetQueueAttributesCommand({ QueueUrl: env.queues.urls.requests, AttributeNames: ["ApproximateNumberOfMessagesNotVisible"] }),
  );
  return Number(Attributes?.["ApproximateNumberOfMessagesNotVisible"] ?? "0");
}

const WALLETS = 4;
const MESSAGES_PER_WALLET = 100;
const TRIGGER = process.platform === "win32" ? "ipc" : "SIGTERM";

describe(`shutdown gracioso (${process.platform}, gatilho ${TRIGGER})`, () => {
  it("com fila e HTTP em andamento: sai com 0, nada fica invisível, nada se perde nem duplica", async () => {
    await withEnv(async (env) => {
      const seed = await env.startOne({ id: "seed", consumer: false, outboxRelay: false, pendingReference: false });
      const wallets: TestWallet[] = [];
      for (let i = 0; i < WALLETS; i++) wallets.push(await createWalletVia(seed, "1000.00"));

      const messages = wallets.flatMap((w) =>
        Array.from({ length: MESSAGES_PER_WALLET }, () => wagerMessage(wagerBody(w, { money: brl("1.00") }))),
      );
      await sendMessages(env.queues, messages);

      const victim = await env.startOne({ id: "victim", outboxRelay: false, pendingReference: false });
      const queueDebits = async () =>
        count(env.pool, "ledger_entries WHERE direction = 'DEBIT' AND wallet_id = ANY($1)", [wallets.map((w) => w.id)]);
      await waitFor(async () => (await queueDebits()) >= 20, { timeoutMs: 30_000, what: "consumo em andamento" });

      // Requisições HTTP em voo no momento do shutdown (wallet própria, para não disputar com a fila).
      const httpWallet = await createWalletVia(seed, "1000.00");
      const bodies = Array.from({ length: 30 }, () => wagerBody(httpWallet, { money: brl("1.00") }));
      const inFlight = bodies.map((body) => submitVia(victim, body).catch((error: unknown) => error));

      const exitCode = await victim.shutdown();
      const responses = await Promise.all(inFlight);
      const consumedByVictim = await queueDebits();

      // 1. Saiu limpo, pelo caminho gracioso e pelo gatilho da plataforma, sem handler abandonado.
      expect(exitCode).toBe(0);
      const logs = victim.records();
      expect(logs.some((l) => l["message"] === "shutdown_complete" && l["trigger"] === TRIGGER)).toBe(true);
      expect(logs.some((l) => l["message"] === "sqs_consumer_handlers_still_running")).toBe(false);

      // 2. Nada ficou preso no visibility timeout: o que estava em andamento terminou (ack) ou voltou à fila na hora.
      expect(await notVisible(env)).toBe(0);
      expect(consumedByVictim).toBeLessThan(WALLETS * MESSAGES_PER_WALLET); // o shutdown pegou a fila no meio

      // 3. HTTP: toda resposta recebida é uma decisão válida e está persistida; nenhuma requisição aceita virou 5xx.
      const answered = responses.filter((r): r is ApiResponse => typeof (r as ApiResponse | undefined)?.status === "number");
      for (const r of answered) expect([200, 422]).toContain(r.status);
      for (const r of answered.filter((x) => x.status === 200)) {
        expect(await count(env.pool, "wager_transactions WHERE id = $1 AND status = 'PROCESSED'", [r.body.transactionId])).toBe(1);
      }
      expect(await count(env.pool, "wager_transactions WHERE status = 'PENDING'")).toBe(0); // nada pela metade

      // 4. Outra instância assume e termina: cada mensagem aplicada exatamente uma vez.
      await env.startOne({ id: "rescuer", outboxRelay: false, pendingReference: false });
      await waitFor(
        async () => (await approximateMessages(env.queues.client, env.queues.urls.requests)) === 0 && (await notVisible(env)) === 0,
        { timeoutMs: 60_000, what: "fila drenada pela segunda instância" },
      );
      for (const w of wallets) {
        expect(await debitsOf(env.pool, w.id)).toBe(MESSAGES_PER_WALLET);
        expect(await balanceOf(env.pool, w.id)).toBe("900.00");
      }
      expect(await count(env.pool, "inbox_messages")).toBe(WALLETS * MESSAGES_PER_WALLET);
      await assertLedgerMatchesBalances(env.pool);
    });
  }, 180_000);

  it("shutdown pedido DURANTE o boot: sai com 0 em segundos (sem esperar SIGKILL) e não processa nada", async () => {
    await withEnv(async (env) => {
      const seed = await env.startOne({ id: "seed", consumer: false, outboxRelay: false, pendingReference: false });
      const w = await createWalletVia(seed, "100.00");
      await sendMessages(env.queues, [wagerMessage(wagerBody(w, { money: brl("1.00") }))]);

      // Regressão: no container o app é o PID 1, e um SIGTERM antes de existir handler era IGNORADO pelo kernel — o
      // `docker stop` só terminava no SIGKILL, 30 s depois, com o app já consumindo a fila.
      const booting = await env.startOne({ id: "booting", waitReady: false, outboxRelay: false, pendingReference: false });
      await new Promise((resolve) => setTimeout(resolve, 300));
      const started = performance.now();
      const exitCode = await booting.shutdown();

      expect(exitCode).toBe(0);
      expect(performance.now() - started).toBeLessThan(10_000);
      const logs = booting.records();
      expect(logs.some((l) => l["message"] === "shutdown_requested" && l["phase"] === "booting")).toBe(true);
      expect(logs.some((l) => l["message"] === "shutdown_complete")).toBe(true);
      expect(logs.some((l) => l["message"] === "listening")).toBe(false);
      expect(await count(env.pool, "inbox_messages")).toBe(0);
    });
  }, 60_000);

  it.if(process.platform === "win32")(
    "controle negativo (Windows): kill('SIGTERM') NÃO é gracioso — o processo é terminado sem rodar o shutdown",
    async () => {
      await withEnv(async (env) => {
        const instance = await env.startOne({ id: "terminated", outboxRelay: false, pendingReference: false });
        await instance.kill("SIGTERM");
        expect(instance.alive).toBe(false);
        const messages = instance.records().map((l) => l["message"]);
        expect(messages).not.toContain("shutdown_requested");
        expect(messages).not.toContain("shutdown_complete");
      });
    },
    60_000,
  );
});
