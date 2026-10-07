import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createClusterEnv, type AppInstance, type ClusterEnv } from "../support/cluster";
import { assertLedgerMatchesBalances } from "../support/harness";
import { approximateMessages, drainQueue, waitFor } from "../support/sqs";
import {
  brl,
  count,
  createWalletVia,
  keyFor,
  sendMessages,
  sleep,
  submitVia,
  wagerBody,
  wagerMessage,
  type TestWallet,
} from "../support/wager-fixtures";

let env: ClusterEnv;
beforeAll(async () => {
  env = await createClusterEnv();
}, 60_000);
afterAll(async () => {
  await env.close();
});

interface HttpJob {
  body: ReturnType<typeof wagerBody>;
  attempts: number;
  status?: number;
}

/** Cliente de provedor: reenvia a MESMA requisição (mesma Idempotency-Key) até receber uma resposta definitiva. */
async function runHttpJob(job: HttpJob, pick: () => AppInstance | undefined, deadlineMs: number): Promise<void> {
  const key = keyFor(job.body);
  while (Date.now() < deadlineMs) {
    const instance = pick();
    if (instance) {
      job.attempts += 1;
      try {
        const response = await submitVia(instance, job.body, key);
        if (response.status === 200 || response.status === 422) {
          job.status = response.status;
          return;
        }
        // 5xx / 503: falha transitória → reenviar
      } catch {
        // conexão recusada ou cortada: a instância caiu → reenviar
      }
    }
    await sleep(250);
  }
  throw new Error(`Requisição ${String(job.body.externalTransactionId)} não obteve resposta definitiva`);
}

describe("queda e reinício do serviço inteiro no meio da carga", () => {
  it("SIGKILL em todas as instâncias → reiniciar → a consistência final é provada", async () => {
    const first = await env.start(3, { id: "before" });
    let alive: AppInstance[] = first;
    const pick = () => {
      const candidates = alive.filter((i) => i.alive);
      return candidates[Math.floor(Math.random() * candidates.length)];
    };

    // --- cenário: 12 wallets com saldo folgado + 1 wallet pobre (20.00) que só comporta 4 apostas de 5.00 ---
    const rich: TestWallet[] = await Promise.all(Array.from({ length: 12 }, () => createWalletVia(first[0]!, "1000.00")));
    const poor = await createWalletVia(first[0]!, "20.00");
    const initialTotal = 12 * 1000 + 20;

    // --- carga: tudo que o "provedor" vai enviar ---
    const sqsMessages: ReturnType<typeof wagerMessage>[] = [];
    let plannedBets = 0;
    let plannedRefunds = 0;

    for (const w of rich) {
      for (let i = 0; i < 20; i++) {
        sqsMessages.push(wagerMessage(wagerBody(w, { money: brl("5.00") })));
        plannedBets += 1;
      }
      // 2 pares por wallet com o REFUND ANTES da BET (referência fora de ordem)
      for (let i = 0; i < 2; i++) {
        const bet = wagerBody(w, { money: brl("7.00") });
        const refund = wagerBody(w, { kind: "REFUND", money: brl("7.00"), referenceExternalTransactionId: bet.externalTransactionId });
        sqsMessages.push(wagerMessage(refund), wagerMessage(bet));
        plannedBets += 1;
        plannedRefunds += 1;
      }
    }
    for (let i = 0; i < 10; i++) {
      sqsMessages.push(wagerMessage(wagerBody(poor, { money: brl("5.00") })));
      plannedBets += 1;
    }
    const httpJobs: HttpJob[] = rich.flatMap((w) =>
      Array.from({ length: 8 }, () => ({ body: wagerBody(w, { money: brl("3.00") }), attempts: 0 })),
    );
    plannedBets += httpJobs.length;
    const planned = sqsMessages.length + httpJobs.length;

    // --- disparar a carga e, no meio dela, matar TODAS as instâncias com SIGKILL ---
    const deadline = Date.now() + 150_000;
    const jobs = httpJobs.map((job) => runHttpJob(job, pick, deadline));
    const sending = sendMessages(env.queues, sqsMessages);

    await waitFor(async () => (await count(env.pool, "wager_transactions WHERE kind <> 'OPENING'")) >= 60, {
      timeoutMs: 60_000,
      intervalMs: 20,
      what: "carga em andamento",
    });
    await Promise.all(first.map((i) => i.kill()));
    const decidedAtKill = await count(env.pool, "wager_transactions WHERE kind <> 'OPENING'");
    expect(decidedAtKill).toBeLessThan(planned); // a queda aconteceu NO MEIO do trabalho
    expect(first.every((i) => !i.alive)).toBe(true);

    await sending; // as mensagens continuam chegando à fila enquanto o serviço está fora
    await sleep(500);

    // --- reiniciar o serviço (3 processos novos, mesmo banco, mesmas filas) ---
    alive = await env.start(3, { id: "after" });

    await Promise.all(jobs); // os clientes HTTP reenviam até a resposta definitiva
    const open = () => count(env.pool, "wager_transactions WHERE status IN ('PENDING','PENDING_REFERENCE')");
    await waitFor(
      async () =>
        (await count(env.pool, "wager_transactions WHERE kind <> 'OPENING'")) === planned &&
        (await open()) === 0 &&
        (await count(env.pool, "outbox_messages WHERE published_at IS NULL")) === 0 &&
        (await approximateMessages(env.queues.client, env.queues.urls.requests)) === 0,
      { timeoutMs: 150_000, intervalMs: 250, what: "convergência após o reinício" },
    );

    // --- invariantes finais ---
    // 1) saldo materializado == ledger, em todas as wallets
    await assertLedgerMatchesBalances(env.pool);

    // 2) cada requisição virou exatamente UMA transação, e todas chegaram a um estado terminal de negócio
    expect(await count(env.pool, "wager_transactions WHERE kind <> 'OPENING'")).toBe(planned);
    expect(await count(env.pool, "wager_transactions WHERE kind <> 'OPENING' AND status NOT IN ('PROCESSED','REJECTED')")).toBe(0);

    // 3) recomputação INDEPENDENTE do ledger: saldo = abertura + créditos - débitos, só pela tabela de transações
    const divergent = await env.pool.query(
      `SELECT w.id
         FROM wallets w
         LEFT JOIN wager_transactions t ON t.wallet_id = w.id AND t.status = 'PROCESSED'
        GROUP BY w.id, w.balance
       HAVING w.balance <> COALESCE(SUM(CASE t.kind
                                          WHEN 'OPENING' THEN t.amount WHEN 'WIN' THEN t.amount WHEN 'REFUND' THEN t.amount
                                          WHEN 'BET' THEN -t.amount ELSE 0 END), 0)`,
    );
    expect(divergent.rows).toEqual([]);

    // 4) nenhum débito/crédito duplicado: um lançamento por transação aplicada que mexe em saldo, e nenhum a mais
    expect(await count(env.pool, "wager_transactions WHERE status = 'PROCESSED' AND kind <> 'LOSS'")).toBe(
      await count(env.pool, "ledger_entries"),
    );
    expect(
      await count(env.pool, "wager_transactions t WHERE t.kind <> 'LOSS' AND t.status = 'PROCESSED' AND NOT EXISTS (SELECT 1 FROM ledger_entries l WHERE l.transaction_id = t.id)"),
    ).toBe(0);
    expect(await count(env.pool, "wager_transactions t WHERE t.status = 'REJECTED' AND EXISTS (SELECT 1 FROM ledger_entries l WHERE l.transaction_id = t.id)")).toBe(0);

    // 5) a wallet pobre comporta exatamente 4 apostas de 5.00 (a disputa foi decidida pelo saldo, não pela ordem)
    expect(await count(env.pool, "wager_transactions WHERE wallet_id = $1 AND status = 'PROCESSED' AND kind = 'BET'", [poor.id])).toBe(4);
    expect(await count(env.pool, "wager_transactions WHERE wallet_id = $1 AND status = 'REJECTED' AND failure_code = 'INSUFFICIENT_FUNDS'", [poor.id])).toBe(6);
    const poorBalance = await env.pool.query("SELECT balance::text AS b FROM wallets WHERE id = $1", [poor.id]);
    expect(poorBalance.rows[0].b).toBe("0.00");

    // 6) os REFUNDs que chegaram antes da BET foram todos resolvidos
    expect(await count(env.pool, "wager_transactions WHERE kind = 'REFUND' AND status = 'PROCESSED'")).toBe(plannedRefunds);

    // 7) conservação do dinheiro: o que saiu das wallets é exatamente o que as apostas aplicadas custaram
    const processedBets = await env.pool.query("SELECT COALESCE(SUM(amount),0)::text AS s FROM wager_transactions WHERE kind = 'BET' AND status = 'PROCESSED'");
    const refunded = await env.pool.query("SELECT COALESCE(SUM(amount),0)::text AS s FROM wager_transactions WHERE kind = 'REFUND' AND status = 'PROCESSED'");
    const balances = await env.pool.query("SELECT COALESCE(SUM(balance),0)::text AS s FROM wallets");
    expect(Number(balances.rows[0].s)).toBeCloseTo(initialTotal - Number(processedBets.rows[0].s) + Number(refunded.rows[0].s), 2);

    // 8) o provedor nunca ficou sem resposta definitiva
    expect(httpJobs.every((j) => j.status === 200 || j.status === 422)).toBe(true);
    const resent = httpJobs.filter((j) => j.attempts > 1).length;
    console.log(
      JSON.stringify({
        chaos: { planned, plannedBets, plannedRefunds, decidedAtKill, httpJobs: httpJobs.length, httpJobsResent: resent },
      }),
    );

    // 9) a outbox foi toda publicada e nenhum evento se perdeu (duplicatas, se houver, têm o mesmo eventId)
    const outboxRows = await count(env.pool, "outbox_messages");
    const events = await drainQueue(env.queues.client, env.queues.urls.events, outboxRows, 60_000);
    const delivered = new Set(events.map((m) => JSON.parse(m.body).eventId as string));
    const expectedIds = (await env.pool.query<{ id: string }>("SELECT id FROM outbox_messages")).rows.map((r) => r.id);
    expect(expectedIds.filter((id) => !delivered.has(id))).toEqual([]);

    // 10) nada foi parar na DLQ
    expect(await approximateMessages(env.queues.client, env.queues.urls.requestsDlq)).toBe(0);
  }, 300_000);
});
