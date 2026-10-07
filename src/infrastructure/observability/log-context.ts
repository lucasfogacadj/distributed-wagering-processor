import { AsyncLocalStorage } from "node:async_hooks";
import type { LogContextPort } from "../../application/ports";

/**
 * Contexto de log por fluxo assíncrono (AsyncLocalStorage): cada requisição HTTP ou mensagem SQS roda em
 * seu próprio escopo, então requisições concorrentes nunca misturam correlationId/transactionId.
 */
export class AsyncLocalLogContext implements LogContextPort {
  private readonly storage = new AsyncLocalStorage<Record<string, string>>();

  /** O escopo filho herda os campos do pai, mas o que ele acrescentar não volta para o pai. */
  run<T>(fields: Record<string, string>, fn: () => T): T {
    return this.storage.run({ ...this.storage.getStore(), ...fields }, fn);
  }

  add(fields: Record<string, string>): void {
    const store = this.storage.getStore();
    if (store) Object.assign(store, fields);
  }

  snapshot(): Record<string, string> {
    return { ...this.storage.getStore() };
  }
}
