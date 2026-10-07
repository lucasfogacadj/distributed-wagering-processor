/**
 * Taxonomia estável de códigos de falha, legível por máquina.
 * O provedor decide: reenviar (transitório), corrigir payload ou desistir (negócio).
 */
export enum FailureCode {
  // Regras de negócio sobre o saldo
  InsufficientFunds = "INSUFFICIENT_FUNDS",
  /** Reversão que deixaria o saldo negativo — operacionalmente distinto de uma aposta sem saldo. */
  ReversalWouldOverdraw = "REVERSAL_WOULD_OVERDRAW",
  /** Crédito que levaria o saldo acima do limite suportado (|saldo| < 10^18). */
  BalanceLimitExceeded = "BALANCE_LIMIT_EXCEEDED",

  // Wallet / identidade
  CurrencyMismatch = "CURRENCY_MISMATCH",
  WalletNotFound = "WALLET_NOT_FOUND",
  WalletPlayerMismatch = "WALLET_PLAYER_MISMATCH",

  // Referências
  ReferenceNotFound = "REFERENCE_NOT_FOUND",
  /** A referência existe, mas continuou sem decisão final até esgotar as tentativas. */
  ReferenceUnresolved = "REFERENCE_UNRESOLVED",
  ReferenceMismatch = "REFERENCE_MISMATCH",
  InvalidReferenceKind = "INVALID_REFERENCE_KIND",
  ReferenceNotProcessed = "REFERENCE_NOT_PROCESSED",
  AlreadyReversed = "ALREADY_REVERSED",
  AmountMismatch = "AMOUNT_MISMATCH",

  // Infraestrutura (terminal, auditável)
  InfraPermanent = "INFRA_PERMANENT",
}
