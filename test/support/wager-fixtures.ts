import { randomUUID } from "node:crypto";
import { SendMessageCommand } from "@aws-sdk/client-sqs";
import type { Pool } from "pg";
import type { AppInstance } from "./cluster";
import type { TestQueues } from "./sqs";

export const brl = (amount: string) => ({ amount, currency: "BRL" });

export interface TestWallet {
  id: string;
  playerId: string;
}

export async function createWalletVia(instance: AppInstance, balance = "100.00"): Promise<TestWallet> {
  const playerId = randomUUID();
  const response = await instance.request("POST", "/wallets", { playerId, initialBalance: brl(balance) });
  if (response.status !== 201) throw new Error(`POST /wallets → ${response.status}: ${JSON.stringify(response.body)}`);
  return { id: response.body.id as string, playerId };
}

/** Corpo de POST /wagering/transactions (e `data` da mensagem SQS). */
export function wagerBody(w: TestWallet, over: Record<string, unknown> = {}) {
  return {
    providerId: "provider-a",
    externalTransactionId: randomUUID(),
    playerId: w.playerId,
    walletId: w.id,
    roundId: "round-1",
    gameId: "fortune-chimp",
    kind: "BET",
    money: brl("25.00"),
    ...over,
  };
}

export const keyFor = (body: { externalTransactionId: unknown }) => `provider-a:${String(body.externalTransactionId)}`;

export function submitVia(instance: AppInstance, body: ReturnType<typeof wagerBody>, key: string = keyFor(body)) {
  return instance.request("POST", "/wagering/transactions", body, { "Idempotency-Key": key });
}

/** Mensagem WagerTransactionRequested (§10 do enunciado) com o mesmo conteúdo do corpo HTTP. */
export function wagerMessage(body: ReturnType<typeof wagerBody>) {
  return {
    messageId: `msg-${randomUUID()}`,
    type: "WagerTransactionRequested",
    occurredAt: new Date().toISOString(),
    data: { ...body, idempotencyKey: keyFor(body) },
  };
}

export async function sendMessages(queues: TestQueues, messages: { data: { walletId: unknown } }[], concurrency = 20): Promise<void> {
  for (let i = 0; i < messages.length; i += concurrency) {
    await Promise.all(
      messages.slice(i, i + concurrency).map((message) =>
        queues.client.send(
          new SendMessageCommand({
            QueueUrl: queues.urls.requests,
            MessageBody: JSON.stringify(message),
            // O grupo é a wallet: a unidade de concorrência. A ordem do grupo é só otimização.
            MessageGroupId: String(message.data.walletId),
            MessageDeduplicationId: randomUUID(),
          }),
        ),
      ),
    );
  }
}

export async function count(pool: Pool, sql: string, params: unknown[] = []): Promise<number> {
  return (await pool.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${sql}`, params)).rows[0]!.n;
}

export const balanceOf = async (pool: Pool, walletId: string) =>
  (await pool.query<{ b: string }>("SELECT balance::text AS b FROM wallets WHERE id = $1", [walletId])).rows[0]!.b;

export const debitsOf = (pool: Pool, walletId: string) =>
  count(pool, "ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'", [walletId]);

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
