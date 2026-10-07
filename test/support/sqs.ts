import { randomUUID } from "node:crypto";
import {
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  type SQSClient,
} from "@aws-sdk/client-sqs";
import { createSqsClient } from "../../src/infrastructure/messaging/sqs";
import { ensureQueues, type QueueNames, type QueueUrls } from "../../src/infrastructure/messaging/sqs";

export const LOCALSTACK_ENDPOINT = process.env["SQS_ENDPOINT"] ?? "http://localhost:4566";

export interface TestQueues {
  client: SQSClient;
  names: QueueNames;
  urls: QueueUrls;
  cleanup(): Promise<void>;
}

/** Filas com nomes únicos por suíte: os testes rodam contra o LocalStack real sem interferir entre si. */
export async function createTestQueues(options: { visibilityTimeoutSeconds?: number; maxReceiveCount?: number } = {}): Promise<TestQueues> {
  const suffix = randomUUID().replaceAll("-", "").slice(0, 12);
  const names: QueueNames = {
    requests: `test-requests-${suffix}.fifo`,
    requestsDlq: `test-requests-dlq-${suffix}.fifo`,
    events: `test-events-${suffix}.fifo`,
  };
  const client = createSqsClient({ endpoint: LOCALSTACK_ENDPOINT, region: "us-east-1" });
  const urls = await ensureQueues(client, names, {
    visibilityTimeoutSeconds: options.visibilityTimeoutSeconds ?? 30,
    maxReceiveCount: options.maxReceiveCount ?? 6,
  });

  return {
    client,
    names,
    urls,
    async cleanup() {
      for (const url of Object.values(urls)) {
        await client.send(new DeleteQueueCommand({ QueueUrl: url })).catch(() => undefined);
      }
      client.destroy();
    },
  };
}

export interface ReceivedMessage {
  body: string;
  attributes: Record<string, string>;
}

/** Lê (e apaga) mensagens até juntar `expected` ou estourar o prazo. Devolve o que conseguiu ler. */
export async function drainQueue(
  client: SQSClient,
  queueUrl: string,
  expected: number,
  timeoutMs = 10_000,
): Promise<ReceivedMessage[]> {
  const received: ReceivedMessage[] = [];
  const deadline = Date.now() + timeoutMs;

  while (received.length < expected && Date.now() < deadline) {
    const { Messages = [] } = await client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        VisibilityTimeout: 30,
        MessageSystemAttributeNames: ["All"],
        MessageAttributeNames: ["All"],
      }),
    );
    for (const m of Messages) {
      received.push({
        body: m.Body ?? "",
        attributes: Object.fromEntries(
          Object.entries(m.MessageAttributes ?? {}).map(([k, v]) => [k, v.StringValue ?? ""]),
        ),
      });
      await client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle! }));
    }
  }
  return received;
}

/** Espera uma condição assíncrona ficar verdadeira, com mensagem clara se estourar o prazo. */
export async function waitFor(
  condition: () => Promise<boolean>,
  options: { timeoutMs?: number; intervalMs?: number; what?: string } = {},
): Promise<void> {
  const deadline = Date.now() + (options.timeoutMs ?? 15_000);
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, options.intervalMs ?? 100));
  }
  throw new Error(`Tempo esgotado esperando: ${options.what ?? "condição"}`);
}

/** Mensagens que ainda estão na fila (visíveis ou em voo), sem apagá-las. */
export async function approximateMessages(client: SQSClient, queueUrl: string): Promise<number> {
  const { GetQueueAttributesCommand } = await import("@aws-sdk/client-sqs");
  const { Attributes = {} } = await client.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"],
    }),
  );
  return Object.values(Attributes).reduce((sum, v) => sum + Number(v), 0);
}
