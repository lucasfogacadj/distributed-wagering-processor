/**
 * Reprocessamento de transações PENDING_REFERENCE.
 *
 * Limite: 10 tentativas com backoff exponencial 1s, 2s, 4s … (teto de 5 min), o que cobre ~8,5 minutos
 * de espera pela referência. Uma referência que não chegou nesse prazo dificilmente chegará: o provedor
 * já terá feito timeout/retry por conta própria, e manter a transação pendente indefinidamente deixaria
 * o fluxo do jogador sem resposta final. Esgotado o limite a transação vira REJECTED/REFERENCE_NOT_FOUND.
 */
export const PENDING_REFERENCE_MAX_ATTEMPTS = 10;

const BASE_DELAY_MS = 1_000;
const MAX_DELAY_MS = 300_000;

/** Atraso até a próxima tentativa, dado o número de tentativas já feitas. */
export function pendingReferenceDelayMs(attempts: number): number {
  return Math.min(BASE_DELAY_MS * 2 ** attempts, MAX_DELAY_MS);
}
