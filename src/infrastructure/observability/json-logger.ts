import { NO_CONTEXT } from "../../application/observability";
import type { LogContextPort, LoggerPort } from "../../application/ports";

type Level = "info" | "warn" | "error";

const REDACTED = "[REDACTED]";
/**
 * Chaves cujo valor nunca vai para o log, em qualquer profundidade: valores monetários, saldos, payloads e
 * credenciais. É uma rede de segurança — quem loga já evita passar esses dados.
 */
const SENSITIVE_KEY = /^(amount|money|initialBalance|payload|body|password|secret|token|authorization|cookie|balance\w*)$/i;

export function redact(value: unknown, seen: WeakSet<object> = new WeakSet()): unknown {
  if (value === null || typeof value !== "object") return value;
  if (value instanceof Date) return value.toISOString();
  if (seen.has(value)) return "[Circular]";
  seen.add(value);

  if (value instanceof Error) {
    return { name: value.name, message: value.message, ...(value.stack ? { stack: value.stack } : {}) };
  }
  if (Array.isArray(value)) return value.map((item) => redact(item, seen));

  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, SENSITIVE_KEY.test(key) ? REDACTED : redact(item, seen)]),
  );
}

export interface JsonLoggerOptions {
  context?: LogContextPort;
  /** Destino das linhas (padrão: stdout). */
  sink?: (line: string) => void;
  now?: () => Date;
}

/** Log estruturado: uma linha JSON por evento, já com o contexto do fluxo corrente. */
export class JsonLogger implements LoggerPort {
  private readonly context: LogContextPort;
  private readonly sink: (line: string) => void;
  private readonly now: () => Date;

  constructor(options: JsonLoggerOptions = {}) {
    this.context = options.context ?? NO_CONTEXT;
    this.sink = options.sink ?? ((line) => void process.stdout.write(`${line}\n`));
    this.now = options.now ?? (() => new Date());
  }

  info(message: string, fields: Record<string, unknown> = {}): void {
    this.write("info", message, fields);
  }

  warn(message: string, fields: Record<string, unknown> = {}): void {
    this.write("warn", message, fields);
  }

  error(message: string, fields: Record<string, unknown> = {}): void {
    this.write("error", message, fields);
  }

  private write(level: Level, message: string, fields: Record<string, unknown>): void {
    const record = {
      level,
      time: this.now().toISOString(),
      message,
      ...this.context.snapshot(),
      ...(redact(fields) as Record<string, unknown>),
    };
    this.sink(JSON.stringify(record));
  }
}
