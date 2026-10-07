import type { InboxMessage } from "../domain/inbox-message";
import type { Money } from "../domain/money";
import type { OutboxMessage } from "../domain/outbox-message";
import type { WagerTransaction, WagerTransactionKind } from "../domain/wager-transaction";
import type { Wallet } from "../domain/wallet";
import type { WalletLedgerEntry } from "../domain/wallet-ledger-entry";

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  next(): string;
}

/** Transação reidratada + o que a tabela guarda além do agregado (snapshot do resultado e agendamento). */
export interface StoredTransaction {
  transaction: WagerTransaction;
  /** Saldo observado quando a transação foi decidida: devolvido nos replays (regra 7.7). */
  resultBalance?: Money;
  attempts: number;
  nextAttemptAt?: Date;
}

export interface TransactionOutcome {
  resultBalance?: Money | undefined;
  attempts?: number;
  nextAttemptAt?: Date | null;
}

export interface WalletRepository {
  findById(id: string): Promise<Wallet | undefined>;
  /** SELECT … FOR UPDATE: serializa por wallet entre instâncias. Deve ser o primeiro lock da transação. */
  findByIdForUpdate(id: string): Promise<Wallet | undefined>;
  /**
   * Lock exclusivo, até o fim da transação, sobre o id de uma wallet que NÃO existe (não há linha para o FOR UPDATE).
   * Serializa as rejeições concorrentes desse walletId: a ordem de `seq` dos eventos dele vira a ordem de commit.
   */
  lockAbsent(id: string): Promise<void>;
  findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | undefined>;
  /** Lança WalletAlreadyExistsError se (playerId, currency) já existir. */
  add(wallet: Wallet): Promise<void>;
  /** UPDATE guardado por versão. Só chamar quando o saldo mudou. */
  save(wallet: Wallet): Promise<void>;
}

export interface WagerTransactionRepository {
  /** INSERT … ON CONFLICT DO NOTHING. true = inseriu; false = idempotency key ou (provider, externalId) já existe. */
  insertIfAbsent(tx: WagerTransaction, options?: { resultBalance?: Money }): Promise<boolean>;
  findById(id: string): Promise<StoredTransaction | undefined>;
  findByIdForUpdate(id: string): Promise<StoredTransaction | undefined>;
  findByIdempotencyKey(key: string): Promise<StoredTransaction | undefined>;
  findByProviderExternal(providerId: string, externalTransactionId: string): Promise<StoredTransaction | undefined>;
  save(tx: WagerTransaction, outcome: TransactionOutcome): Promise<void>;
  /** Esta referência já foi revertida (REFUND ou ROLLBACK PROCESSED)? Uma referência é revertida uma única vez. */
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;
  /** Ids das transações PENDING_REFERENCE cujo próximo reprocessamento já venceu (sem lock; o use case confere de novo). */
  listDuePendingReferences(now: Date, limit: number): Promise<string[]>;
  /** Torna imediatamente elegíveis as transações PENDING_REFERENCE que aguardam esta referência. */
  releaseDependents(providerId: string, referenceExternalTransactionId: string, now: Date): Promise<void>;
  /**
   * Registra uma falha NÃO transitória no reprocessamento de uma PENDING_REFERENCE e adia a próxima tentativa.
   * Devolve o total de falhas, ou undefined se a transação não está mais PENDING_REFERENCE (outra instância decidiu).
   */
  recordProcessingFailure(id: string, nextAttemptAt: Date): Promise<number | undefined>;
}

export interface LedgerPageItem {
  /** Posição estável (identity) usada pelo cursor. */
  seq: string;
  entry: WalletLedgerEntry;
}

export interface LedgerRepository {
  add(entry: WalletLedgerEntry): Promise<void>;
  /** Mais novo primeiro. Com `beforeSeq`, só lançamentos estritamente anteriores a ele. */
  listByWallet(walletId: string, options: { beforeSeq?: string; limit: number }): Promise<{ items: LedgerPageItem[]; hasMore: boolean }>;
  /** Saldo armazenado e saldo reconstruído pelo ledger, lidos no MESMO snapshot (um único SELECT). */
  reconcile(walletId: string): Promise<{ stored: Money; calculated: Money; entries: number } | undefined>;
}

export interface LoggerPort {
  info(message: string, fields?: Record<string, unknown>): void;
  warn(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

/**
 * Quem está chamando a API. Os direitos são EXPLÍCITOS — a ausência de um campo nunca concede acesso:
 *  - `providerId`: provedor de jogos; submete e consulta só as transações do PRÓPRIO providerId e lê saldo de wallets;
 *  - `operator`: backoffice do operador; cria wallets, lê o ledger, reconcilia e pode agir por qualquer provedor.
 * Um token válido sem nenhum dos dois autentica, mas não autoriza nada (403).
 */
export interface ProviderIdentity {
  providerId?: string;
  operator?: boolean;
  /** Identificador do chamador no IdP (ex.: `sub` do token), para auditoria. */
  subject?: string;
}

/**
 * Ponto de extensão de autenticação. A API não implementa autenticação própria: um adaptador desta porta
 * integra um Identity Provider externo (Keycloak, Zitadel…) — valida o token JWT/OIDC ou a chave do
 * chamador e devolve a identidade. `undefined` = credenciais ausentes ou inválidas (HTTP 401).
 * Se o IdP estiver inacessível, o adaptador lança: a API responde 503 (transitório).
 */
export interface ProviderIdentityPort {
  authenticate(headers: Record<string, string | undefined>): Promise<ProviderIdentity | undefined>;
}

/** Métricas operacionais. Rótulos de baixa cardinalidade: nunca ids de wallet, transação ou mensagem. */
export interface MetricsPort {
  reconciliationDivergence(): void;
  /** Transações por status (e tipo) no momento em que são decididas. */
  transactionDecided(status: string, kind: string): void;
  /** Replay HTTP ou redelivery de fila reconhecidos como duplicata. */
  duplicateDetected(source: string): void;
  retry(component: string): void;
  /** Eventos da outbox efetivamente publicados no broker (vazão do relay). */
  outboxPublished(count: number): void;
  /** Evento da outbox que esgotou as tentativas e foi para a quarentena. */
  outboxQuarantined(): void;
  /** Mensagens enviadas à DLQ, por motivo. */
  deadLettered(reason: string): void;
  /** Deadlock, falha de serialização, lock_timeout ou conflito de versão. */
  lockConflict(kind: string): void;
  /** Tempo esperando o lock da wallet (wallet quente). */
  lockWait(seconds: number): void;
  processingLatency(seconds: number, source: string, outcome: string): void;
  httpRequest(method: string, route: string, status: number, seconds: number): void;
}

/**
 * Contexto de log do fluxo corrente (correlationId, messageId, transactionId, walletId, providerId).
 * Todo log emitido dentro de `run` o recebe automaticamente.
 */
export interface LogContextPort {
  run<T>(fields: Record<string, string>, fn: () => T): T;
  /** Acrescenta campos ao escopo corrente (ex.: o transactionId, que só se conhece depois). */
  add(fields: Record<string, string>): void;
  snapshot(): Record<string, string>;
}

export interface Observability {
  metrics: MetricsPort;
  logger: LoggerPort;
  context: LogContextPort;
}

export interface OutboxRepository {
  add(message: OutboxMessage): Promise<void>;
  /**
   * Reivindica eventos para publicar, sem bloquear outros publishers (`FOR UPDATE SKIP LOCKED` + lease em
   * `locked_until`). A unidade de reivindicação é a SEQUÊNCIA de cada wallet: quem trava a cabeça (o evento mais
   * antigo ainda não publicado, fora de backoff e sem lease alheio) leva também os seguintes da mesma wallet, até
   * `perAggregate`, e `limit` eventos no total. Assim a ordem por wallet é preservada e uma wallet nunca é dividida
   * entre publishers. As wallets são atendidas da mais antiga para a mais nova. O relógio é o do banco, a única
   * referência comum entre instâncias. Devolve os eventos em ordem de `seq`.
   */
  claimBatch(options: { limit: number; perAggregate: number; leaseMs: number; owner: string }): Promise<OutboxMessage[]>;
  /** Marca como publicados, em UMA instrução, os eventos cujo lease é do `owner`. Devolve quantos foram marcados. */
  markPublished(ids: string[], owner: string, at: Date): Promise<number>;
  /** Persiste attempts/nextAttemptAt/lastError calculados pelo domínio e libera o lease. */
  releaseForRetry(message: OutboxMessage, owner: string): Promise<void>;
  /** Persiste a quarentena decidida pelo domínio (quarantinedAt, attempts, lastError) e libera o lease. */
  quarantine(message: OutboxMessage, owner: string): Promise<void>;
  /** Quantos eventos estão em quarentena (base da métrica `outbox_quarantined_events`). */
  countQuarantined(): Promise<number>;
  /**
   * Devolve à fila eventos em quarentena (todos, ou só os `ids`), zerando attempts. Voltam para a posição original
   * (`seq`) da wallet: se eventos posteriores já saíram, este sai DEPOIS deles. Devolve quantos voltaram.
   */
  requeueQuarantined(ids?: string[]): Promise<number>;
  /** Libera o lease de eventos que não foram publicados nem entram em backoff (a cauda atrás de uma falha). */
  unlock(ids: string[], owner: string): Promise<void>;
  /** Instante do evento pendente mais antigo (base da métrica de outbox lag). */
  oldestPendingAt(): Promise<Date | undefined>;
}

/** Porta de saída para o broker. At-least-once: o mesmo evento pode ser publicado mais de uma vez. */
export interface EventPublisher {
  /**
   * Publica, EM ORDEM, uma sequência de eventos de uma mesma wallet (no máximo 10, o limite de um lote do SQS).
   * Devolve quantos do PREFIXO foram publicados (0..n); lança se nem o primeiro pôde ser publicado. Os que não
   * entraram no prefixo serão tentados de novo, na mesma ordem.
   */
  publishBatch(messages: OutboxMessage[]): Promise<number>;
}

/** Deduplicação persistente de mensagens por (consumerName, messageId). */
export interface InboxRepository {
  /** INSERT … ON CONFLICT DO NOTHING. false = a mensagem já havia sido registrada (redelivery). */
  insertIfAbsent(message: InboxMessage): Promise<boolean>;
  find(consumerName: string, messageId: string): Promise<InboxMessage | undefined>;
  /**
   * Conta uma tentativa de processamento que FALHOU (UPSERT em `inbox_attempts`) e devolve o total desta mensagem.
   * Só tentativas reais contam: uma mensagem devolvida à fila sem ter sido processada não passa por aqui.
   */
  recordFailedAttempt(consumerName: string, messageId: string, error: string, at: Date): Promise<number>;
}

export interface Repositories {
  wallets: WalletRepository;
  transactions: WagerTransactionRepository;
  ledger: LedgerRepository;
  outbox: OutboxRepository;
  inbox: InboxRepository;
}

/** Uma transação SQL. Tudo que `work` grava é confirmado junto ou descartado junto. */
export interface UnitOfWork {
  run<T>(work: (repos: Repositories) => Promise<T>): Promise<T>;
}
