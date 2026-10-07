import { SendMessageBatchCommand, type SQSClient } from "@aws-sdk/client-sqs";
import type { EventPublisher } from "../../application/ports";
import type { OutboxMessage } from "../../domain/outbox-message";

/** Limite de entradas de um `SendMessageBatch`. */
export const SQS_MAX_BATCH = 10;

/**
 * Publica os envelopes dos eventos na fila FIFO de saída, em lote.
 *  - MessageGroupId = aggregateId (a wallet): ordem preservada por wallet, paralelismo entre wallets;
 *  - MessageDeduplicationId = eventId: o broker descarta republicações próximas (otimização). A garantia
 *    real é a do consumidor, que deduplica pelo eventId;
 *  - as entradas de um lote seguem a ordem da lista e o relay só envia lotes de UMA wallet, então a FIFO
 *    mantém a ordem dentro do grupo.
 *
 * Falha parcial (`Failed` não vazio): devolve o tamanho do PREFIXO que entrou. Entradas depois da primeira
 * falha podem ter sido aceitas pelo broker; elas serão republicadas depois (mesmo eventId, deduplicável).
 * Em troca, o relay nunca marca como publicado um evento que vem depois de um que falhou.
 */
export class SqsEventPublisher implements EventPublisher {
  constructor(
    private readonly client: SQSClient,
    private readonly queueUrl: string,
  ) {}

  async publishBatch(messages: OutboxMessage[]): Promise<number> {
    if (messages.length === 0) return 0;
    if (messages.length > SQS_MAX_BATCH) {
      throw new Error(`Um lote do SQS aceita no máximo ${SQS_MAX_BATCH} mensagens (recebeu ${messages.length})`);
    }

    const { Successful = [], Failed = [] } = await this.client.send(
      new SendMessageBatchCommand({
        QueueUrl: this.queueUrl,
        Entries: messages.map((message, index) => ({
          Id: String(index),
          MessageBody: JSON.stringify(message.payload),
          MessageGroupId: message.aggregateId,
          MessageDeduplicationId: message.id,
          MessageAttributes: { eventType: { DataType: "String", StringValue: message.eventType } },
        })),
      }),
    );

    const accepted = new Set(Successful.map((entry) => entry.Id));
    let prefix = 0;
    while (prefix < messages.length && accepted.has(String(prefix))) prefix += 1;

    if (prefix === 0) {
      const first = Failed[0];
      throw new Error(`SendMessageBatch falhou${first ? `: ${first.Code ?? "?"} ${first.Message ?? ""}`.trimEnd() : ""}`);
    }
    return prefix;
  }
}
