/** Texto curto e estável de um erro para logs: `Nome: mensagem` (ou o valor, se não for um Error). */
export function describeError(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

/** Campos de log de um erro inesperado: o texto curto mais a stack, quando houver. */
export function errorLogFields(error: unknown): { error: string; stack?: string } {
  return {
    error: describeError(error),
    ...(error instanceof Error && error.stack ? { stack: error.stack } : {}),
  };
}
