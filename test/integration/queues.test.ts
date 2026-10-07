import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { GetQueueAttributesCommand, SendMessageCommand } from "@aws-sdk/client-sqs";
import { ensureQueues } from "../../src/infrastructure/messaging/sqs";
import { createTestQueues, drainQueue, type TestQueues } from "../support/sqs";

let q: TestQueues;
beforeAll(async () => {
  q = await createTestQueues({ visibilityTimeoutSeconds: 12, maxReceiveCount: 6 });
});
afterAll(async () => {
  await q.cleanup();
});

const attributes = async (url: string) =>
  (await q.client.send(new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: ["All"] }))).Attributes ?? {};

describe("ensureQueues", () => {
  it("cria as três filas FIFO com os nomes pedidos", () => {
    expect(q.urls.requests.endsWith(q.names.requests)).toBe(true);
    expect(q.urls.requestsDlq.endsWith(q.names.requestsDlq)).toBe(true);
    expect(q.urls.events.endsWith(q.names.events)).toBe(true);
  });

  it("todas são FIFO sem deduplicação por conteúdo (a deduplicação é por MessageDeduplicationId)", async () => {
    for (const url of [q.urls.requests, q.urls.requestsDlq, q.urls.events]) {
      const attrs = await attributes(url);
      expect(attrs["FifoQueue"]).toBe("true");
      expect(attrs["ContentBasedDeduplication"]).toBe("false");
    }
  });

  it("a fila principal redireciona para a DLQ após maxReceiveCount recebimentos", async () => {
    const attrs = await attributes(q.urls.requests);
    const redrive = JSON.parse(attrs["RedrivePolicy"] ?? "{}");
    expect(redrive.maxReceiveCount).toBe(6);
    expect(redrive.deadLetterTargetArn).toContain(q.names.requestsDlq);
    expect(attrs["VisibilityTimeout"]).toBe("12");
  });

  it("é idempotente: chamar de novo devolve as mesmas filas", async () => {
    const again = await ensureQueues(q.client, q.names, { visibilityTimeoutSeconds: 12, maxReceiveCount: 6 });
    expect(again).toEqual(q.urls);
  });

  it("envia e recebe por grupo preservando a ordem dentro do grupo", async () => {
    for (let i = 0; i < 5; i++) {
      await q.client.send(
        new SendMessageCommand({
          QueueUrl: q.urls.events,
          MessageBody: JSON.stringify({ n: i }),
          MessageGroupId: "grupo-a",
          MessageDeduplicationId: `ordem-${i}`,
        }),
      );
    }
    const received = await drainQueue(q.client, q.urls.events, 5);
    expect(received.map((m) => JSON.parse(m.body).n)).toEqual([0, 1, 2, 3, 4]);
  });
});
