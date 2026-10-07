import {
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from "@aws-sdk/client-sqs";
import { describeError, errorLogFields } from "../../application/describe-error";
import { classifyError } from "../../application/error-classification";
import { FailureCode } from "../../domain/failure-code";
import { PollingLoop } from "../../application/polling-loop";
import type { ProcessWagerCommand, ProcessWagerResult } from "../../application/process-wager-transaction";
import { NO_CONTEXT, NO_METRICS } from "../../application/observability";
import type { LogContextPort, LoggerPort, MetricsPort } from "../../application/ports";
import { MessageParseError, parseWagerMessage, type ParsedWagerMessage } from "./wager-message";

export type SqsSender = Pick<SQSClient, "send">;

export interface WagerConsumerOptions {
  client: SqsSender;
  queueUrl: string;
  dlqUrl: string;
  /**
   * O mesmo use case da entrada HTTP. `recordFailure`, se presente, encerra a transação como FAILED (auditável) quando
   * a mensagem vai à DLQ por falha permanente.
   */
  process: {
    execute(command: ProcessWagerCommand): Promise<ProcessWagerResult>;
    recordFailure?(command: ProcessWagerCommand, code: FailureCode): Promise<ProcessWagerResult>;
  };
  /**
   * Contador persistente de tentativas reais (inbox_attempts). Sem ele, o consumidor usa o ApproximateReceiveCount do
   * SQS, que também conta as devoluções sem processamento (cauda de grupo, shutdown).
   */
  attempts?: { record(consumerName: string, messageId: string, error: string): Promise<number> };
  logger: LoggerPort;
  /** Escopo da inbox: a deduplicação é por (consumerName, messageId). */
  consumerName?: string;
  /** Tentativas de processamento que FALHARAM permitidas antes de enviar a mensagem à DLQ. */
  maxReceives?: number;
  /**
   * Teto de recebimentos (ApproximateReceiveCount) usado só quando a tentativa não pôde ser contada no banco (ex.:
   * PostgreSQL fora): a mensagem continua em retry com backoff até este teto (padrão 3 × maxReceives) em vez de ir
   * para a DLQ cedo por uma queda de infraestrutura.
   */
  receiveHardLimit?: number;
  retryBaseDelaySeconds?: number;
  retryMaxDelaySeconds?: number;
  waitTimeSeconds?: number;
  /**
   * Laços de recebimento concorrentes nesta instância (padrão 1). Cada um recebe até 10 mensagens, processa e apaga
   * sem esperar os outros. A ordem por wallet não depende disso: a fila FIFO não entrega uma mensagem de um grupo
   * enquanto houver outra do mesmo grupo em andamento, e dentro de um lote o grupo é sequencial.
   */
  concurrency?: number;
  /**
   * Prefetch (padrão false): cada laço pede o PRÓXIMO lote enquanto processa o atual, escondendo a latência do
   * recebimento. É seguro para a ordem: a fila FIFO não devolve um grupo que ainda tem mensagem em andamento. As
   * mensagens prefetchadas esperam invisíveis durante o processamento do lote atual (conta no visibility timeout) e,
   * no shutdown, as que ninguém vai processar têm a visibilidade devolvida.
   */
  prefetch?: boolean;
  /** Quanto o shutdown espera pelas mensagens em andamento antes de devolver a visibilidade delas. */
  drainTimeoutMs?: number;
  /** Espera extra, depois de devolver a visibilidade por estouro do drain, pelos handlers que ainda estão vivos. */
  stopGraceMs?: number;
  metrics?: MetricsPort;
  /** Cada mensagem roda no seu escopo de log: messageId e correlationId acompanham todos os logs dela. */
  context?: LogContextPort;
}

type Outcome = "done" | "retry";

const DEFAULTS = {
  consumerName: "wager-transactions-consumer",
  maxReceives: 5,
  retryBaseDelaySeconds: 2,
  retryMaxDelaySeconds: 300,
  waitTimeSeconds: 5,
  drainTimeoutMs: 20_000,
  stopGraceMs: 5_000,
} as const;

/** Teto de recebimentos quando a tentativa não pôde ser contada no banco. O `maxReceiveCount` do broker fica acima dele. */
export function defaultReceiveHardLimit(maxReceives: number): number {
  return maxReceives * 3;
}

/** true se `work` terminou em até `ms`; false se o prazo estourou antes (o trabalho continua rodando). */
async function within(work: Promise<void>, ms: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work.then(() => true),
      new Promise<boolean>((resolve) => {
        timer = setTimeout(() => resolve(false), ms);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/**
 * Consumidor da fila de entrada. Reaproveita o use case da API HTTP; a inbox persistente deduplica
 * redeliveries dentro da mesma transação SQL do efeito financeiro.
 *
 * Decisão por resultado:
 *  - processado (inclusive REJECTED / PENDING_REFERENCE)   → ack, SOMENTE depois do commit;
 *  - erro de negócio (ex.: conflito de idempotência)       → ack (terminal);
 *  - mensagem/entrada inválida (erro permanente)           → DLQ imediata, com o motivo;
 *  - falha transitória ou desconhecida                     → retry com backoff exponencial (visibility timeout)
 *                                                            até `maxReceives` tentativas REAIS (contadas em
 *                                                            inbox_attempts), depois DLQ;
 *  - desconhecida que esgotou as tentativas (permanente)   → transação FAILED/INFRA_PERMANENT + evento, depois DLQ.
 */
export class SqsWagerConsumer {
  private readonly consumerName: string;
  private readonly maxReceives: number;
  private readonly receiveHardLimit: number;
  private readonly retryBaseDelaySeconds: number;
  private readonly retryMaxDelaySeconds: number;
  private readonly waitTimeSeconds: number;
  private readonly drainTimeoutMs: number;
  private readonly stopGraceMs: number;
  private readonly loops: PollingLoop[];
  /** Estado por laço: o recebimento antecipado (prefetch) que ainda não foi consumido. */
  private readonly lanes: { next?: Promise<Message[]> }[];
  private readonly prefetch: boolean;
  private readonly inFlight = new Map<string, string>(); // MessageId do SQS → receipt handle
  private accepting = false;

  constructor(private readonly options: WagerConsumerOptions) {
    this.consumerName = options.consumerName ?? DEFAULTS.consumerName;
    this.maxReceives = options.maxReceives ?? DEFAULTS.maxReceives;
    this.receiveHardLimit = options.receiveHardLimit ?? defaultReceiveHardLimit(this.maxReceives);
    if (this.receiveHardLimit < this.maxReceives) throw new Error("receiveHardLimit deve ser >= maxReceives");
    this.retryBaseDelaySeconds = options.retryBaseDelaySeconds ?? DEFAULTS.retryBaseDelaySeconds;
    this.retryMaxDelaySeconds = options.retryMaxDelaySeconds ?? DEFAULTS.retryMaxDelaySeconds;
    this.waitTimeSeconds = options.waitTimeSeconds ?? DEFAULTS.waitTimeSeconds;
    this.drainTimeoutMs = options.drainTimeoutMs ?? DEFAULTS.drainTimeoutMs;
    this.stopGraceMs = options.stopGraceMs ?? DEFAULTS.stopGraceMs;

    const concurrency = options.concurrency ?? 1;
    if (!Number.isInteger(concurrency) || concurrency < 1) throw new Error("concurrency deve ser um inteiro >= 1");
    this.prefetch = options.prefetch ?? false;
    this.lanes = Array.from({ length: concurrency }, () => ({}));
    this.loops = this.lanes.map(
      (lane) =>
        new PollingLoop({
          name: "sqs-wager-consumer",
          work: () => this.poll(lane),
          idleDelayMs: 50, // o long polling já espera; isto só evita girar em falso
          errorDelayMs: 2_000,
          onError: (error) => options.logger.error("sqs_poll_failed", { error: describeError(error) }),
        }),
    );
  }

  start(): void {
    this.accepting = true;
    for (const loop of this.loops) loop.start();
  }

  /**
   * SIGTERM: para de buscar mensagens, conclui as que já estão em processamento (até `drainTimeoutMs`) e
   * devolve a visibilidade das que não chegaram a começar, para outra instância pegá-las de imediato.
   */
  async stop(): Promise<void> {
    this.accepting = false;
    const stopped = Promise.all(this.loops.map((loop) => loop.stop())).then(() => undefined);

    if (!(await within(stopped, this.drainTimeoutMs))) {
      this.options.logger.warn("sqs_consumer_drain_timeout", { inFlight: this.inFlight.size });
      await this.release([...this.inFlight.entries()].map(([MessageId, ReceiptHandle]) => ({ MessageId, ReceiptHandle })));

      // Uma transação em andamento não é cancelável: os handlers seguem até terminar. Damos um segundo teto antes de
      // devolver o controle, porque logo depois o ORM é fechado e o cliente SQS destruído, e um handler ainda vivo
      // falharia com "conexão fechada". Se ainda houver algum, fica registrado (a inbox e o lock da wallet garantem
      // que uma eventual reentrega não duplica o efeito).
      if (!(await within(stopped, this.stopGraceMs))) {
        this.options.logger.error("sqs_consumer_handlers_still_running", { inFlight: this.inFlight.size });
      }
    }
    // Com ou sem timeout: com `accepting` desligado nenhum laço consome mais o prefetch, então ele volta à fila aqui.
    await this.releasePrefetched();
  }

  private receive(): Promise<Message[]> {
    return this.options.client
      .send(
        new ReceiveMessageCommand({
          QueueUrl: this.options.queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: this.waitTimeSeconds,
          MessageSystemAttributeNames: ["All"],
        }),
      )
      .then((response) => response.Messages ?? []);
  }

  private async poll(lane: { next?: Promise<Message[]> }): Promise<boolean> {
    if (!this.accepting) return false;

    const pending = lane.next;
    lane.next = undefined;
    const Messages = await (pending ?? this.receive());
    if (Messages.length === 0) return false;

    if (this.prefetch && this.accepting) {
      const next = this.receive();
      next.catch(() => undefined); // o erro é tratado quando o resultado for consumido; evita rejeição não tratada
      lane.next = next;
    }

    // A fila FIFO pode entregar várias mensagens do mesmo grupo (wallet) no mesmo lote: dentro do grupo a
    // ordem é sequencial; entre grupos diferentes, em paralelo.
    const groups = new Map<string, Message[]>();
    for (const message of Messages) {
      const group = message.Attributes?.["MessageGroupId"] ?? "sem-grupo";
      groups.set(group, [...(groups.get(group) ?? []), message]);
    }
    // Os acks do lote saem juntos, num único DeleteMessageBatch, DEPOIS de todos os commits: segue valendo "ack só
    // depois do commit". Se o processo morrer antes do delete, a redelivery é deduplicada pela inbox.
    const acks: Message[] = [];
    try {
      await Promise.all([...groups.values()].map((group) => this.processGroup(group, acks)));
    } finally {
      // O que já foi confirmado (commit feito) é apagado mesmo que algo inesperado tenha escapado de um grupo.
      await this.flushAcks(acks);
    }
    return true;
  }

  private async processGroup(messages: Message[], acks: Message[]): Promise<void> {
    for (let i = 0; i < messages.length; i++) {
      if (!this.accepting) {
        await this.release(messages.slice(i)); // shutdown: o que não começou volta imediatamente à fila
        return;
      }
      let outcome: Outcome;
      try {
        outcome = await this.handle(messages[i]!, acks);
      } catch (error) {
        // Falha inesperada FORA do tratamento normal (o handle já converte erros de processamento em ack/retry/DLQ).
        // Esta mensagem fica para o visibility timeout; o resto do grupo volta já, para não furar a ordem, e os
        // outros grupos do lote seguem normalmente (um grupo com problema não derruba o lote).
        this.options.logger.error("sqs_group_failed", { sqsMessageId: messages[i]!.MessageId, ...errorLogFields(error) });
        await this.release(messages.slice(i + 1));
        return;
      }
      if (outcome === "retry") {
        // Para não furar a ordem do grupo, o restante do lote também volta para a fila.
        await this.release(messages.slice(i + 1));
        return;
      }
    }
  }

  private handle(message: Message, acks: Message[]): Promise<Outcome> {
    const sqsId = message.MessageId ?? "desconhecido";
    // Antes de ler o corpo só se conhece o id do SQS; o messageId de negócio entra no contexto depois do parse.
    return (this.options.context ?? NO_CONTEXT).run({ correlationId: sqsId, sqsMessageId: sqsId }, () => this.handleInContext(message, acks));
  }

  private async handleInContext(message: Message, acks: Message[]): Promise<Outcome> {
    const sqsId = message.MessageId ?? "desconhecido";
    const receiveCount = Number(message.Attributes?.["ApproximateReceiveCount"] ?? "1");
    this.inFlight.set(sqsId, message.ReceiptHandle ?? "");

    try {
      let parsed: ParsedWagerMessage;
      try {
        parsed = parseWagerMessage(message.Body ?? "");
      } catch (error) {
        if (error instanceof MessageParseError) return await this.deadLetter(message, "invalid_message", error, acks);
        throw error;
      }

      (this.options.context ?? NO_CONTEXT).add({ messageId: parsed.inbox.messageId, correlationId: parsed.inbox.messageId });

      const command: ProcessWagerCommand = {
        ...parsed.command,
        inbox: { messageId: parsed.inbox.messageId, consumerName: this.consumerName },
      };
      try {
        const result = await this.options.process.execute(command);
        this.options.logger.info("sqs_message_processed", {
          messageId: parsed.inbox.messageId,
          transactionId: result.transactionId,
          status: result.status,
          idempotentReplay: result.idempotentReplay,
          receiveCount,
        });
        return this.ack(message, acks);
      } catch (error) {
        return await this.onFailure(message, command, receiveCount, error, acks);
      }
    } finally {
      this.inFlight.delete(sqsId);
    }
  }

  private async onFailure(
    message: Message,
    command: ProcessWagerCommand,
    receiveCount: number,
    error: unknown,
    acks: Message[],
  ): Promise<Outcome> {
    const messageId = command.inbox!.messageId;
    const errorClass = classifyError(error);
    switch (errorClass) {
      case "business":
        // Conflito/regra terminal: reenviar não muda nada. Ack para não ficar em loop.
        this.options.logger.warn("sqs_message_business_error", { messageId, error: describeError(error) });
        return this.ack(message, acks);
      case "invalid":
        return this.deadLetter(message, "invalid_payload", error, acks);
      default: {
        const attempt = await this.countAttempt(messageId, receiveCount, error);
        const exhausted = attempt.recorded ? attempt.count >= this.maxReceives : attempt.count >= this.receiveHardLimit;
        if (!exhausted) return this.retryLater(message, messageId, attempt.count, error);

        // Falha que não é transitória e se repetiu até o limite é permanente: a transação é encerrada como FAILED
        // (auditável, com evento) antes de a mensagem ir para a DLQ. Esgotamento por falha TRANSITÓRIA (ex.: banco fora)
        // não grava nada: a mensagem na DLQ pode ser reenviada quando a infraestrutura voltar.
        if (errorClass === "unknown") await this.markFailed(command);
        const reason = attempt.recorded ? "retries_exhausted" : "retries_exhausted_unrecorded";
        return this.deadLetter(message, reason, error, acks, attempt.count);
      }
    }
  }

  /**
   * Conta esta tentativa que falhou. Com o contador persistente, só tentativas reais contam; se ele estiver indisponível
   * (o próprio banco caiu), cai para o ApproximateReceiveCount, comparado com o teto mais alto (`receiveHardLimit`).
   */
  private async countAttempt(messageId: string, receiveCount: number, error: unknown): Promise<{ count: number; recorded: boolean }> {
    if (!this.options.attempts) return { count: receiveCount, recorded: true };
    try {
      return { count: await this.options.attempts.record(this.consumerName, messageId, describeError(error)), recorded: true };
    } catch (recordError) {
      this.options.logger.warn("sqs_attempt_record_failed", { messageId, receiveCount, error: describeError(recordError) });
      return { count: receiveCount, recorded: false };
    }
  }

  private async markFailed(command: ProcessWagerCommand): Promise<void> {
    if (!this.options.process.recordFailure) return;
    try {
      await this.options.process.recordFailure(command, FailureCode.InfraPermanent);
    } catch (failError) {
      // A mensagem vai para a DLQ de qualquer forma (nada se perde); só não ficou o registro FAILED.
      this.options.logger.error("sqs_record_failed_status_failed", { messageId: command.inbox?.messageId, error: describeError(failError) });
    }
  }

  /** Só chamado depois do commit: apenas enfileira o ack, que sai em lote no fim do recebimento (flushAcks). */
  private ack(message: Message, acks: Message[]): Outcome {
    acks.push(message);
    return "done";
  }

  /** Apaga as mensagens confirmadas em lotes de 10. Falhas só são registradas: a redelivery é deduplicada pela inbox. */
  private async flushAcks(acks: Message[]): Promise<void> {
    for (let i = 0; i < acks.length; i += 10) {
      const batch = acks.slice(i, i + 10);
      try {
        const { Failed = [] } = await this.options.client.send(
          new DeleteMessageBatchCommand({
            QueueUrl: this.options.queueUrl,
            Entries: batch.map((message, index) => ({ Id: String(index), ReceiptHandle: message.ReceiptHandle! })),
          }),
        );
        for (const failed of Failed) {
          this.options.logger.error("sqs_ack_failed", {
            sqsMessageId: batch[Number(failed.Id)]?.MessageId,
            error: `${failed.Code ?? "erro"}: ${failed.Message ?? ""}`,
          });
        }
      } catch (error) {
        this.options.logger.error("sqs_ack_failed", { sqsMessageIds: batch.map((m) => m.MessageId), error: describeError(error) });
      }
    }
  }

  /** O backoff segue o nº de tentativas que FALHARAM (não os recebimentos), para a cauda devolvida não inflar a espera. */
  private async retryLater(message: Message, messageId: string, attempt: number, error: unknown): Promise<Outcome> {
    const delay = Math.min(this.retryBaseDelaySeconds * 2 ** (Math.max(attempt, 1) - 1), this.retryMaxDelaySeconds);
    this.options.logger.warn("sqs_message_retry", { messageId, attempt, retryInSeconds: delay, error: describeError(error) });
    (this.options.metrics ?? NO_METRICS).retry("sqs_consumer");
    try {
      await this.options.client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: this.options.queueUrl,
          ReceiptHandle: message.ReceiptHandle!,
          VisibilityTimeout: delay,
        }),
      );
    } catch (visibilityError) {
      this.options.logger.error("sqs_visibility_change_failed", { error: describeError(visibilityError) });
    }
    return "retry";
  }

  private async deadLetter(message: Message, reason: string, error: unknown, acks: Message[], attempts?: number): Promise<Outcome> {
    (this.options.metrics ?? NO_METRICS).deadLettered(reason);
    this.options.logger.error("sqs_message_dead_lettered", {
      sqsMessageId: message.MessageId,
      reason,
      receiveCount: message.Attributes?.["ApproximateReceiveCount"],
      ...(attempts !== undefined ? { attempts } : {}),
      error: describeError(error),
    });
    try {
      await this.options.client.send(
        new SendMessageCommand({
          QueueUrl: this.options.dlqUrl,
          MessageBody: message.Body ?? "",
          MessageGroupId: message.Attributes?.["MessageGroupId"] ?? "dlq",
          // O id do SQS é único por mensagem: reenviar à DLQ depois de um ack perdido não duplica.
          MessageDeduplicationId: message.MessageId ?? crypto.randomUUID(),
          MessageAttributes: {
            reason: { DataType: "String", StringValue: reason },
            error: { DataType: "String", StringValue: describeError(error).slice(0, 256) },
            originalMessageId: { DataType: "String", StringValue: message.MessageId ?? "" },
            receiveCount: { DataType: "String", StringValue: message.Attributes?.["ApproximateReceiveCount"] ?? "1" },
            ...(attempts !== undefined ? { attempts: { DataType: "String", StringValue: String(attempts) } } : {}),
          },
        }),
      );
    } catch (sendError) {
      // Sem a DLQ não dá para descartar com segurança: a mensagem fica e será entregue de novo.
      this.options.logger.error("sqs_dlq_send_failed", { error: describeError(sendError) });
      return "retry";
    }
    return this.ack(message, acks);
  }

  /** Shutdown: o que foi recebido por antecipação e não será processado volta à fila imediatamente. */
  private async releasePrefetched(): Promise<void> {
    for (const lane of this.lanes) {
      const next = lane.next;
      lane.next = undefined;
      if (!next) continue;
      try {
        await this.release(await next);
      } catch (error) {
        this.options.logger.error("sqs_prefetch_release_failed", { error: describeError(error) });
      }
    }
  }

  /** Devolve a visibilidade (timeout 0): a mensagem fica disponível para qualquer instância na hora. */
  private async release(messages: Pick<Message, "MessageId" | "ReceiptHandle">[]): Promise<void> {
    await Promise.all(
      messages.map(async (message) => {
        if (!message.ReceiptHandle) return;
        try {
          await this.options.client.send(
            new ChangeMessageVisibilityCommand({
              QueueUrl: this.options.queueUrl,
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: 0,
            }),
          );
        } catch (error) {
          this.options.logger.error("sqs_release_failed", { sqsMessageId: message.MessageId, error: describeError(error) });
        }
      }),
    );
  }
}
