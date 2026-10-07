import { CreateQueueCommand, GetQueueAttributesCommand, SQSClient } from "@aws-sdk/client-sqs";

export interface SqsConfig {
  /** Endpoint do LocalStack/MiniStack. Ausente = AWS real (credenciais da cadeia padrão). */
  endpoint?: string | undefined;
  region: string;
}

export interface QueueNames {
  /** Entrada: WagerTransactionRequested. */
  requests: string;
  /** DLQ da fila de entrada. */
  requestsDlq: string;
  /** Saída: eventos de integração publicados pela outbox. */
  events: string;
}

export interface QueueUrls {
  requests: string;
  requestsDlq: string;
  events: string;
}

export interface QueueOptions {
  visibilityTimeoutSeconds: number;
  /** Rede de segurança no broker; o consumidor já envia à DLQ explicitamente antes de chegar aqui. */
  maxReceiveCount: number;
}

export const DEFAULT_QUEUE_NAMES: QueueNames = {
  requests: "wager-transactions.fifo",
  requestsDlq: "wager-transactions-dlq.fifo",
  events: "wallet-events.fifo",
};

export function createSqsClient(config: SqsConfig): SQSClient {
  return new SQSClient({
    region: config.region,
    ...(config.endpoint
      ? { endpoint: config.endpoint, credentials: { accessKeyId: "test", secretAccessKey: "test" } }
      : {}),
  });
}

async function createFifoQueue(client: SQSClient, name: string, attributes: Record<string, string>): Promise<string> {
  const { QueueUrl } = await client.send(
    new CreateQueueCommand({
      QueueName: name,
      // Sem deduplicação por conteúdo: quem publica informa o MessageDeduplicationId. E a FIFO é só
      // otimização — a correção vem do banco (inbox, constraints), não do broker.
      Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false", ...attributes },
    }),
  );
  if (!QueueUrl) throw new Error(`SQS não devolveu a URL da fila ${name}`);
  return QueueUrl;
}

/** Cria (idempotentemente) a fila de entrada, a DLQ com redrive e a fila de eventos. */
export async function ensureQueues(client: SQSClient, names: QueueNames, options: QueueOptions): Promise<QueueUrls> {
  const visibility = String(options.visibilityTimeoutSeconds);

  const requestsDlq = await createFifoQueue(client, names.requestsDlq, { VisibilityTimeout: visibility });
  const { Attributes } = await client.send(
    new GetQueueAttributesCommand({ QueueUrl: requestsDlq, AttributeNames: ["QueueArn"] }),
  );
  const deadLetterTargetArn = Attributes?.["QueueArn"];
  if (!deadLetterTargetArn) throw new Error(`SQS não devolveu o ARN da DLQ ${names.requestsDlq}`);

  const requests = await createFifoQueue(client, names.requests, {
    VisibilityTimeout: visibility,
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn, maxReceiveCount: options.maxReceiveCount }),
  });
  const events = await createFifoQueue(client, names.events, { VisibilityTimeout: visibility });

  return { requests, requestsDlq, events };
}
