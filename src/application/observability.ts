import type { LogContextPort, LoggerPort, MetricsPort, Observability } from "./ports";

/** Implementações nulas: os casos de uso funcionam sem observabilidade (testes unitários, scripts). */
export const NO_METRICS: MetricsPort = {
  reconciliationDivergence: () => undefined,
  transactionDecided: () => undefined,
  duplicateDetected: () => undefined,
  retry: () => undefined,
  outboxPublished: () => undefined,
  outboxQuarantined: () => undefined,
  deadLettered: () => undefined,
  lockConflict: () => undefined,
  lockWait: () => undefined,
  processingLatency: () => undefined,
  httpRequest: () => undefined,
};

export const NO_LOGGER: LoggerPort = {
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
};

export const NO_CONTEXT: LogContextPort = {
  run: (_fields, fn) => fn(),
  add: () => undefined,
  snapshot: () => ({}),
};

export const NO_OBSERVABILITY: Observability = { metrics: NO_METRICS, logger: NO_LOGGER, context: NO_CONTEXT };
