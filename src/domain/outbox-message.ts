import { InvalidMessageStateError } from "./errors";
import { IntegrationEvent } from "./integration-event";

const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 300_000;

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
  quarantinedAt?: Date;
  lastError?: string;
}

/** Tamanho máximo do erro guardado para diagnóstico (a coluna é text; isto só evita guardar corpos inteiros). */
const MAX_ERROR_LENGTH = 500;

/**
 * Ciclo de vida: pendente → publicada, ou pendente → (retries) → em quarentena.
 * A quarentena tira um evento venenoso da sequência da sua wallet para não bloquear os seguintes para sempre; é uma
 * quebra de ordem DELIBERADA e auditável (o evento continua na tabela, com o último erro, e pode ser devolvido à fila).
 */
export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
    private _quarantinedAt?: Date,
    private _lastError?: string,
  ) {}

  /** O id da mensagem é o eventId: consumidores deduplicam por ele. */
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      { ...event.toJSON() },
      event.occurredAt,
      0,
    );
  }

  static rehydrate(s: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      s.id,
      s.aggregateId,
      s.eventType,
      s.payload,
      s.occurredAt,
      s.attempts,
      s.nextAttemptAt,
      s.publishedAt,
      s.quarantinedAt,
      s.lastError,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  get quarantinedAt(): Date | undefined {
    return this._quarantinedAt;
  }

  /** Último erro de publicação (diagnóstico de retries e da quarentena). */
  get lastError(): string | undefined {
    return this._lastError;
  }

  /** Ainda aguardando publicação: nem publicada nem em quarentena. */
  isPending(): boolean {
    return this._publishedAt === undefined && this._quarantinedAt === undefined;
  }

  isQuarantined(): boolean {
    return this._quarantinedAt !== undefined;
  }

  /** Esgotou as tentativas permitidas: deve ir para a quarentena em vez de ganhar mais um backoff. */
  hasExhaustedAttempts(maxAttempts: number): boolean {
    return this._attempts >= maxAttempts;
  }

  isDue(now: Date): boolean {
    if (!this.isPending()) return false;
    return this._nextAttemptAt === undefined || this._nextAttemptAt.getTime() <= now.getTime();
  }

  markPublished(at: Date): void {
    this.assertPending("markPublished");
    this._publishedAt = at;
  }

  /** Incrementa attempts e agenda a próxima tentativa: 1s, 2s, 4s… com teto de 5 min. */
  scheduleRetry(now: Date, error?: string): void {
    this.assertPending("scheduleRetry");
    this._attempts += 1;
    const delay = Math.min(BASE_DELAY_MS * 2 ** (this._attempts - 1), MAX_DELAY_MS);
    this._nextAttemptAt = new Date(now.getTime() + delay);
    if (error !== undefined) this._lastError = error.slice(0, MAX_ERROR_LENGTH);
  }

  /** Tira o evento da sequência da wallet: não será mais tentado até ser devolvido à fila explicitamente. */
  quarantine(at: Date, error: string): void {
    this.assertPending("quarantine");
    this._quarantinedAt = at;
    this._nextAttemptAt = undefined;
    this._lastError = error.slice(0, MAX_ERROR_LENGTH);
  }

  private assertPending(action: string): void {
    if (!this.isPending()) {
      const state = this._publishedAt !== undefined ? "já foi publicada" : "está em quarentena";
      throw new InvalidMessageStateError(`${action} inválido: mensagem ${this.id} ${state}`);
    }
  }
}
