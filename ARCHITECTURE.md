# Arquitetura e decisões

Este documento explica **o que foi decidido, por quê, o que foi descartado e onde estão os limites**. Setup e comandos
estão no [README](./README.md).

## 1. Visão geral e princípios

1. **O banco é a fonte da verdade e o árbitro final.** Unicidade, imutabilidade, não-negatividade e consistência
   saldo↔ledger estão no schema (§10), não só no código. Ordenação e deduplicação do SQS são otimização.
2. **Idempotência é persistente e decidida pelo banco** (`INSERT … ON CONFLICT DO NOTHING`), nunca por cache em memória.
3. **A wallet é a unidade de concorrência.** Um `SELECT … FOR UPDATE` na linha da wallet serializa as operações dela
   entre quaisquer instâncias; wallets diferentes andam em paralelo. Não existe lock global.
4. **Tudo que precisa ser atômico está na mesma transação SQL:** inbox, transação, saldo, ledger e outbox.
5. **Nenhum evento sai antes do commit.** A publicação é feita por um relay que lê a outbox já confirmada.
6. **At-least-once em toda parte, com consumidores idempotentes.** Duplicatas são esperadas e seguras.

## 2. Camadas e fronteiras

```
   interfaces/ (HTTP, consumidor SQS)      ← traduzem protocolo em comandos; zod só aqui
        │
   application/ (casos de uso, portas)     ← orquestram; dependem só de portas e do domínio
        │
   domain/ (Money, Wallet, WagerTransaction, Ledger, Inbox/Outbox, eventos, WagerPolicy)
                                           ← puro: sem Nest, ORM, zod, AWS
   infrastructure/ (MikroORM, SQS, migrator, logs, métricas)  ← implementa as portas
```

- Casos de uso: `CreateWallet`, `ProcessWagerTransaction` (**único** ponto de entrada de transações, usado pelo HTTP e
  pelo SQS), `ReprocessPendingReference`, `WalletQueries`. `WagerSettler` concentra a decisão e a aplicação, compartilhada
  entre a entrada e o reprocessamento.
- Portas: `UnitOfWork`, repositórios (`Wallet`, `WagerTransaction`, `Ledger`, `Outbox`, `Inbox`), `Clock`, `IdGenerator`,
  `EventPublisher`, `ProviderIdentityPort`, `LoggerPort`, `MetricsPort`, `LogContextPort`.
- O domínio expõe construtores `private` e factories estáticas (`create`, `from`, `open`, `rehydrate`). `rehydrate`
  reconstrói estado já persistido **sem** revalidar transições.
- A validação de **forma** (JSON, tipos, UUIDs) é feita em zod na borda; as regras de **dinheiro e negócio** ficam no
  domínio (`Money.from`, `WagerTransaction.create`).

## 3. Stack e escolhas

### ORM: MikroORM (preferencial no enunciado)

- **Por quê:** Unit of Work e Identity Map explícitos, `EntityManager.transactional()` e `LockMode` mapeiam direto na
  estratégia de concorrência (§9). Cada operação roda em um **fork** do `EntityManager` (Identity Map próprio) dentro
  de uma transação `READ COMMITTED`.
- **Mapeamento separado do domínio:** `EntitySchema` em `infrastructure/persistence/schemas.ts` descreve linhas
  (`WalletRow`, `WagerTransactionRow`…); mappers convertem de/para as classes de domínio (via `rehydrate`). O domínio
  não tem decorators nem conhece o ORM.
- **SQL cru onde o ORM não expressa:** `INSERT … ON CONFLICT DO NOTHING RETURNING` (idempotência e inbox), CTE com
  `FOR UPDATE SKIP LOCKED` (outbox), `UPDATE` de liberação de dependentes, o `SELECT` único da reconciliação e a
  paginação por `seq`. Tudo via `em.execute`, que respeita a transação do fork. *SQL cru não passa pelo mapeamento de
  tipos do ORM* (um bug real: `timestamptz` chegava como string; os mappers convertem).
- **Verificado na prática:** o MikroORM v7 funciona sobre o Bun (spike descartável antes de construir), incluindo
  `numeric` como string exata e `FOR UPDATE`.

### Migrations: SQL puro com runner próprio

`migrations/NNNN_nome.up.sql` e `.down.sql`, aplicados por `migrator.ts` sob **advisory lock** (várias instâncias
subindo juntas não colidem), cada uma em sua transação. **Por quê não o Migrator do MikroORM:** o essencial do schema
são triggers e constraints, que são SQL de qualquer forma; o runner próprio é pequeno, previsível sob Bun e totalmente
reversível (testado: `down` até zerar e `up` de novo, e cada migration isoladamente).

### Bun + NestJS

- `emitDecoratorMetadata` não é confiável no Bun: toda injeção usa `@Inject(TOKEN)` explícito.
- O Nest fica na borda (módulo, controllers, guard, filtro, middleware, ciclo de vida). O logger do Nest é desligado:
  todo log passa pelo `LoggerPort` (JSON).

## 4. Dinheiro (`Money`)

- **Nunca `number`/float.** Internamente é um `bigint` de **centavos**; a aritmética é exata (inclusive acima de
  `Number.MAX_SAFE_INTEGER`, testado).
- **Entrada e saída são strings decimais com escala fixa de 2**: `^\d+\.\d{2}$`. Rejeitados: `NaN`, `Infinity`, notação
  científica, vazio, 0/1/3+ casas decimais, sinal, vírgula, `number` no lugar de string, moeda fora de `^[A-Z]{3}$`.
  **Excesso de casas é rejeitado, nunca arredondado** (arredondar silenciosamente é perder dinheiro).
- **Imutável:** toda operação devolve uma nova instância. Operar moedas diferentes lança `CurrencyMismatchError`
  (`equals` entre moedas diferentes devolve `false`).
- **Persistência:** `numeric(20,2)` mapeado como **string** no ORM (representação exata, nunca passa por `number`) e
  moeda em `char(3)` separada, reidratados como `Money`. O JSON dos eventos carrega `MoneyProps` (string decimal).
- **Limite:** valores absolutos abaixo de 10^18 (o que cabe em `numeric(20,2)`). Uma entrada acima disso é
  `INVALID_MONEY` (HTTP 400 / DLQ `invalid_payload`), nunca um erro do banco; um crédito válido que levaria o saldo
  acima do limite é `REJECTED/BALANCE_LIMIT_EXCEEDED`.
- O modelo é **multi-moeda** (uma wallet por `(playerId, currency)`); os conflitos de moeda são testados. Os exemplos
  usam BRL.

## 5. Modelo de domínio

### Wallet (aggregate root)

`version` começa em `1` e **só incrementa quando o saldo muda**. `debit`/`credit` devolvem o lançamento de ledger
correspondente: saldo e ledger nascem juntos, e a wallet não muda de estado se a operação for recusada. Saldo
insuficiente lança `InsufficientFundsError` sem alterar nada.

### WagerTransaction — máquina de estados

| De | Para | Quando |
|---|---|---|
| `PENDING` | `PROCESSED` | aplicada |
| `PENDING` | `REJECTED` | regra de negócio violada (com `failureCode`) |
| `PENDING` | `PENDING_REFERENCE` | reversão cuja referência ainda não existe/foi decidida |
| `PENDING_REFERENCE` | `PROCESSED` / `REJECTED` | reprocessada (worker ou chegada da referência) |
| `PENDING_REFERENCE` | `PENDING_REFERENCE` | continua aguardando (nova tentativa agendada) |
| `PENDING` / `PENDING_REFERENCE` | `FAILED` | erro permanente de infraestrutura (ver §18) |
| `PROCESSED`, `REJECTED`, `FAILED` | — | **terminais**: tentar transicionar lança `InvalidTransactionStateError` |

`OPENING` é interno (`createOpening`, já nasce `PROCESSED`); `create` o recusa, e a API/fila também (o schema zod não
aceita o `kind`). O **schema reforça** a máquina: um trigger impede alterar transação terminal, alterar campos de
identidade, excluir e voltar `PENDING_REFERENCE → PENDING`.

### Ledger

`WalletLedgerEntry` não tem campos mutáveis nem métodos de transição (a imutabilidade é estrutural, e o banco a repete
com triggers). `create` valida a aritmética `balanceBefore ± money = balanceAfter`, valor positivo e saldo não negativo.
No máximo **um lançamento por transação**; `LOSS` e qualquer `REJECTED` não geram lançamento.

### Eventos (`IntegrationEvent<T>`)

Classe abstrata com subclasses concretas; `eventType` e `version` ficam **no tipo**. `data` carrega `MoneyProps`, nunca
`Money`. `aggregateId` é a **walletId** (ordenação por wallet e `MessageGroupId`). `WalletBalanceChanged` só é gerado
quando o saldo muda (inclui a abertura da wallet).

## 6. Regras de negócio e interpretações adicionais

O enunciado pede que "qualquer interpretação adicional seja documentada". São estas:

| # | Interpretação | Motivo |
|---|---|---|
| 1 | **Uma referência é revertida no máximo uma vez, por qualquer tipo** (`REFUND` ou `ROLLBACK`). O enunciado diz "pelo mesmo tipo"; a leitura literal permitiria `REFUND` **e** `ROLLBACK` da mesma `BET`, creditando o valor duas vezes. Isso fere o invariante global "não duplicar créditos". A segunda reversão é `REJECTED/ALREADY_REVERSED`. O `ROLLBACK` de um `REFUND` continua válido (a referência dele é o `REFUND`). | Segurança financeira; foi um defeito achado e corrigido (migration `0003`), com teste. |
| 2 | `LOSS` aceita valor zero; todos os outros `kind` exigem valor positivo. | `LOSS` não move saldo. |
| 3 | `BET` não aceita referência. `WIN` pode trazer `referenceExternalTransactionId`; quando traz, ela é **validada como uma reversão** (mesmo provider, player, wallet, moeda e rodada; só pode apontar para `BET`; a `BET` precisa estar `PROCESSED` e não revertida) e aguarda a `BET` se ela ainda não chegou. O valor do prêmio é livre. Sem referência, a `WIN` é aplicada como antes. | O enunciado diz que a `WIN` "pode referenciar"; ignorar uma referência informada permitiria pagar prêmio de aposta rejeitada ou já devolvida (crédito duplicado). |
| 4 | `ROLLBACK` referencia `BET`, `WIN` ou `REFUND`; o efeito é o **inverso** do lançamento da referência. `REFUND` só referencia `BET`. O valor precisa ser **igual** ao da referência. | Regras 3 e 5 do enunciado. |
| 5 | Mesmo provider, player, wallet, moeda e rodada entre a reversão e a referência, senão `REFERENCE_MISMATCH`. | Regra 2. |
| 6 | Transações rejeitadas por wallet inexistente, player divergente ou moeda divergente **são persistidas** (auditáveis). Por isso `wager_transactions.wallet_id`/`player_id` não têm FK. | "Toda rejeição permanece auditável." |
| 7 | Payload inválido (400) **não** vira transação: nunca chegou a existir para o domínio. | Não há o que auditar além do log. |
| 8 | A `Idempotency-Key` é única **globalmente**. O padrão recomendado é `{providerId}:{externalTransactionId}`. O mesmo `(providerId, externalTransactionId)` com outra key é conflito (409). | O header é a fonte da verdade. |
| 9 | O replay devolve o resultado **da decisão** (status e saldo observado). Para uma `PENDING_REFERENCE` resolvida depois, devolve o **estado atual** (a decisão mais recente). | Faz o replay refletir o que de fato aconteceu. |
| 10 | `playerId` e `walletId` são UUID por contrato. Saldo inicial zero não gera `OPENING`, lançamento nem evento. | Contrato da API. |

## 7. Códigos de falha (`failureCode`)

Estáveis e legíveis por máquina. Presentes no corpo de `422` (e nos eventos `WagerTransactionRejected`).

| Código | Quando | O provedor deve… |
|---|---|---|
| `INSUFFICIENT_FUNDS` | `BET` sem saldo | desistir desta transação (a mesma key devolve a mesma rejeição); pode tentar outra, com **novo** `externalTransactionId`, quando houver saldo |
| `REVERSAL_WOULD_OVERDRAW` | reversão que deixaria o saldo negativo — **distinto** de `INSUFFICIENT_FUNDS`, porque operacionalmente é outra situação (o jogador já gastou o valor) | tratar manualmente/conciliar |
| `BALANCE_LIMIT_EXCEEDED` | crédito que levaria o saldo acima do limite suportado (10^18, §4) | tratar manualmente |
| `CURRENCY_MISMATCH` | moeda da operação ≠ moeda da wallet | corrigir o payload |
| `WALLET_NOT_FOUND` | wallet inexistente | corrigir o `walletId` |
| `WALLET_PLAYER_MISMATCH` | a wallet não pertence ao `playerId` informado | corrigir o payload |
| `REFERENCE_NOT_FOUND` | a referência não chegou após esgotar as tentativas (§12.4) | desistir ou reenviar a referência e depois a reversão |
| `REFERENCE_UNRESOLVED` | a referência existe, mas continuou sem decisão final (ela própria `PENDING_REFERENCE`) até esgotar as tentativas (§12.4) | resolver a referência e reenviar a reversão com novo `externalTransactionId` |
| `REFERENCE_MISMATCH` | provider/player/wallet/moeda/rodada diferentes da referência | corrigir o payload |
| `INVALID_REFERENCE_KIND` | `REFUND` de algo que não é `BET`; `ROLLBACK` de algo que não é `BET`/`WIN`/`REFUND` | corrigir o payload |
| `REFERENCE_NOT_PROCESSED` | a referência foi `REJECTED`/`FAILED` (não há o que reverter) | desistir |
| `ALREADY_REVERSED` | a referência já foi revertida (§6, item 1) | desistir |
| `AMOUNT_MISMATCH` | valor ≠ valor da referência (reversão parcial está fora do escopo) | corrigir o payload |
| `INFRA_PERMANENT` | estado **`FAILED`**: o processamento falhou de forma **não transitória** até esgotar as tentativas (mensagem da fila, §12.2; ou reprocessamento de uma `PENDING_REFERENCE`, §12.4). Terminal e auditável, com evento `WagerTransactionFailed` | **não** reenviar a mesma key (devolve o mesmo resultado); investigar e, se for o caso, reenviar com **novo** `externalTransactionId` |

## 8. Idempotência

**Chave:** o header `Idempotency-Key` (HTTP) ou `data.idempotencyKey` (SQS). **Hash do payload:**

```
payloadHash = SHA-256 (hex) de JSON.stringify( canonicalize({
  providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind,
  money: { amount, currency }, referenceExternalTransactionId   // omitido se ausente
}) )
```

`canonicalize` ordena as chaves **recursivamente** e omite `undefined`. O header e metadados de transporte **não entram**
no hash; campos extras do corpo são ignorados. Resultado: reordenar as chaves não muda o hash; mudar qualquer campo de
negócio muda.

**Algoritmo** (uma transação SQL):

1. (fila) registra a mensagem na inbox: `INSERT … ON CONFLICT DO NOTHING`. Já existe → `messageId` com outro hash é
   conflito; com o mesmo hash é redelivery → devolve o resultado original.
2. `INSERT` da transação em `PENDING` com `ON CONFLICT DO NOTHING RETURNING id`. Duas requisições simultâneas da mesma
   key: a segunda **espera** a primeira confirmar (índice único) e então não insere.
3. Não inseriu → lê a existente: mesmo `payloadHash` ⇒ **replay** (mesmo resultado, `idempotentReplay: true`); hash
   diferente ⇒ **conflito** (409). `(provider, externalId)` já usado com outra key ⇒ conflito.
4. Inseriu → toma o lock da wallet e decide (§9).

O saldo observado é gravado na linha (`result_balance`), então o replay devolve o saldo **daquele momento**, não o atual.

## 9. Concorrência

**Estratégia: lock pessimista por wallet** — `SELECT … FOR UPDATE` (`LockMode.PESSIMISTIC_WRITE`) na linha da wallet,
dentro da transação do caso de uso.

**Por que pessimista e não otimista:** numa **wallet quente** (muitas apostas simultâneas) o controle otimista faz a
maioria das tentativas falhar e refazer o trabalho (tempestade de retries). Com o lock, a segunda aposta **espera** e
decide com o saldo já atualizado. Medido nos testes: 150 apostas de 10,00 sobre 1000,00 espalhadas em 3 processos
resultam em exatamente 100 aplicadas e 50 rejeitadas, saldo 0,00. A alternativa de `UPDATE … SET balance = balance - x
WHERE balance >= x` foi descartada porque as regras de referência/reversão e o encadeamento do ledger precisam do estado
completo sob lock.

**Camadas de defesa** (a primeira é a estratégia; as demais pegam falhas de implementação):

1. o lock da wallet (serialização);
2. `UPDATE wallets … WHERE version = <anterior>`: se alguém escreveu sem o lock, falha com `ConcurrentModificationError`
   (transitório);
3. `CHECK (balance >= 0)` e o trigger deferido saldo↔ledger no schema (§10).

**Ordem de locks e deadlock.** Cada transação trava **uma** wallet. A ordem global é *wallet → linha da transação*:
o reprocessamento lê a transação sem lock, trava a wallet e só então a linha. Como nenhum caminho trava duas wallets
nem inverte essa ordem, não há ciclo possível. O `INSERT` da transação nova acontece antes do lock da wallet, mas essa
linha ainda não é visível a ninguém.

**Wallet inexistente.** Sem linha não há `FOR UPDATE`; a rejeição `WALLET_NOT_FOUND` toma então um advisory lock de
transação sobre o **id** dessa wallet (`pg_advisory_xact_lock(hashtextextended(walletId, 0))`, por wallet, não global).
Assim as rejeições concorrentes do mesmo `walletId` também são serializadas e a ordem de `seq` dos eventos delas
continua sendo a ordem de commit, premissa do relay da outbox (§12).

**`lock_timeout`** (`LOCK_TIMEOUT_MS`, padrão 10 s, aplicado a cada conexão do pool): quem espera demais por uma wallet
falha com `55P03`, tratado como **transitório** (HTTP 503 + `Retry-After`, retry na fila) e contado em
`lock_conflicts_total{kind="lock_timeout"}`. Isso impede que uma transação travada prenda o resto indefinidamente.

**Várias instâncias:** nada depende de estado em memória; a serialização vem do banco. Provado com 3 processos reais
(§16).

## 10. Garantias no schema

| Garantia | Mecanismo (nome no schema) |
|---|---|
| Saldo nunca negativo | `ck_wallets_balance_non_negative`; `ck_ledger_entries_balances_non_negative` |
| Uma wallet por `(player, moeda)` | `uq_wallets_player_currency` |
| Idempotência persistente | `uq_wager_tx_idempotency_key`, `uq_wager_tx_provider_external` |
| Ledger imutável | triggers `BEFORE UPDATE/DELETE/TRUNCATE` em `ledger_entries` |
| Aritmética do lançamento | `ck_ledger_entries_arithmetic` (`antes ± valor = depois`), `ck_ledger_entries_amount_positive` |
| No máximo um lançamento por transação | `uq_ledger_entries_transaction` |
| Moeda do lançamento = moeda da wallet | FK composta `(wallet_id, currency) → wallets(id, currency)` |
| Cadeia do ledger contínua | trigger `BEFORE INSERT`: `balance_before` = `balance_after` do lançamento anterior |
| **`wallet.balance` = soma do ledger** | trigger **deferido** (no `COMMIT`): o saldo da wallet precisa ser o `balance_after` do último lançamento. Com a cadeia acima, isso implica a igualdade com a soma. Custo O(log n) por commit. |
| Uma reversão por referência | índice único parcial `uq_wager_tx_single_reversal (reference_transaction_id) WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK')` |
| Reversão exige referência; `PROCESSED` exige consistência | `ck_wager_tx_reference_required`, `ck_wager_tx_processed_*`, `ck_wager_tx_failure_code_required` |
| Estado terminal imutável; transações não são excluídas | trigger `wager_transactions_guard` |
| Dedup de mensagens | PK `(consumer_name, message_id)` em `inbox_messages` |
| Ordem de publicação por wallet | `seq` (identity) + índices parciais na outbox |

Cada linha tem um teste que **tenta violá-la** e confere que o banco recusa (`test/integration/schema.test.ts`).

## 11. Ledger e reconciliação

- `GET /wallets/:id/ledger`: mais novo primeiro, paginado por **cursor opaco** (base64url de `{v, seq}`); `seq` é a
  identity da tabela. Os lançamentos de uma wallet são inseridos sob o lock dela, então para uma mesma wallet a ordem
  de `seq` é a ordem de commit. O cursor é estável quando chegam lançamentos novos entre as páginas (testado).
- `POST /wallets/:id/reconciliation`: lê saldo armazenado e créditos/débitos do ledger **no mesmo `SELECT`** (mesmo
  snapshot). **Divergências não são corrigidas**: são logadas (`reconciliation_divergence`), contadas em
  `reconciliation_divergences_total` e sinalizadas (`consistent: false`, `difference`). O caminho de divergência é
  testado desligando o trigger de propósito. Custo O(n) por wallet.

## 12. Mensageria

### 12.1 Inbox
`(consumerName, messageId)` com o hash do payload, gravada **na mesma transação** do efeito. Nasce já processada porque
só é confirmada junto com ele; se a transação desfaz, a linha some, e a redelivery processa de novo.

### 12.2 Consumidor SQS
Reaproveita `ProcessWagerTransaction`. Long polling; mensagens do mesmo `MessageGroupId` (a wallet) em sequência, grupos
diferentes em paralelo.

| Resultado | Ação |
|---|---|
| Processada (inclusive `REJECTED` e `PENDING_REFERENCE`) | **ack, somente depois do commit** |
| Erro de negócio terminal (ex.: conflito de idempotência) | ack |
| Mensagem malformada / entrada inválida (erro permanente) | **DLQ imediata**, com `reason`, `error`, `originalMessageId`, `receiveCount` |
| Falha transitória (deadlock, `lock_timeout`, conexão…) ou desconhecida | retry com backoff exponencial via visibility timeout (`2·2ⁿ⁻¹` s, teto 300 s; `n` = tentativas que falharam) |
| Tentativas esgotadas (`SQS_MAX_RECEIVES`, padrão 5) por falha **transitória** | DLQ, `reason=retries_exhausted`; **nada é gravado** — reenviar da DLQ quando a infraestrutura voltar processa normalmente |
| Tentativas esgotadas por falha **desconhecida** (permanente: bug, dado que o banco recusa) | transação **`FAILED`/`INFRA_PERMANENT`** + inbox + evento `WagerTransactionFailed` (numa transação própria), depois DLQ, `reason=retries_exhausted` |

**Tentativas reais, contadas no banco.** O `ApproximateReceiveCount` do SQS também sobe quando a mensagem volta à fila
**sem ter sido processada** (a cauda de um grupo devolvida atrás de um retry; o shutdown). Por isso o limite da DLQ conta
só tentativas que **falharam**, em `inbox_attempts (consumer_name, message_id, attempts, last_error, …)`, num UPSERT em
transação própria (a do processamento foi desfeita). `inbox_messages` continua sendo só deduplicação. Testado: a cauda
devolvida duas vezes atrás da cabeça e que depois falha uma vez **não** vai para a DLQ (iria, com o contador do SQS). Se o
próprio contador estiver fora (o banco caiu), o consumidor cai para o `ApproximateReceiveCount`, comparado com um teto
**mais alto** (`SQS_RECEIVE_HARD_LIMIT`, padrão 3 × `SQS_MAX_RECEIVES`) e `reason=retries_exhausted_unrecorded`: uma queda
do banco não manda mensagens cedo para a DLQ. A mensagem na DLQ leva o atributo `attempts`.

O broker também tem `RedrivePolicy` (`SQS_RECEIVE_HARD_LIMIT + 1`) como rede de segurança. Se o **ack falhar depois do commit**, a
mensagem reaparece e a inbox a reconhece como duplicata (testado). Por isso os acks de cada recebimento (até 10) saem
juntos num `DeleteMessageBatch`, depois de todos os commits do lote, e a instância roda `CONSUMER_CONCURRENCY` (padrão 4)
laços de recebimento independentes: a fila FIFO não entrega um grupo (wallet) com outra mensagem dele em andamento, então
a ordem por wallet não depende do nº de laços. Medido: a drenagem de um backlog foi de ~205 para ~440 msg/s (LOAD_TEST.md §5.5).

**Shutdown gracioso.** Gatilhos: `SIGTERM`/`SIGINT` (Linux, `docker stop`, Kubernetes) e, no **Windows** — onde não existe
`SIGTERM` entre processos (`kill` vira `TerminateProcess`, sem chance de limpeza) —, Ctrl+C, Ctrl+Break (`SIGBREAK`) ou uma
mensagem IPC `{ type: "shutdown" }` de um processo pai (`src/main.ts`). Todos levam ao mesmo `app.close()` e saem com `0`.
O consumidor para de buscar mensagens, conclui a que está em processamento (até `SHUTDOWN_DRAIN_MS`) e
**devolve a visibilidade** (timeout 0) das que não começaram, para outra instância pegá-las na hora. Se o prazo de
drenagem estoura, devolve também a das que estão em andamento e dá **mais um teto** (`stopGraceMs`, 5 s) para os
handlers vivos terminarem, porque uma transação em andamento não é cancelável e logo depois o ORM é fechado; o que
sobrar vira o log `sqs_consumer_handlers_still_running` (a inbox e o lock da wallet impedem efeito duplicado numa
reentrega). Os workers descem **antes** de o ORM ser fechado. Verificado nas duas plataformas:
`test/concurrency/graceful-shutdown.test.ts` (processo real, gatilho nativo: IPC no Windows, `SIGTERM` no Linux) com 400
mensagens e 30 requisições HTTP em voo — sai com `0`, nenhuma mensagem fica invisível, toda resposta HTTP recebida está
persistida, e outra instância termina a fila com exatamente 1 débito por mensagem; um controle negativo mostra que, no
Windows, `kill('SIGTERM')` termina o processo **sem** rodar o shutdown. E em container (3 réplicas, `AUTH_MODE=oidc`)
num host Windows: `docker stop` das três no meio do consumo (82 de 600 mensagens processadas) saiu com `0` em < 1 s cada,
nenhuma mensagem ficou invisível, e ao religar as 600 foram aplicadas exatamente uma vez (inbox = 600, DLQ vazia, outbox
drenada, reconciliação consistente).

**Sinal durante o boot (bug achado nessa validação e corrigido).** No container o app é o PID 1, e o kernel **ignora**
um SIGTERM para o PID 1 sem handler. Os handlers eram instalados depois das migrations e do create do Nest: um `docker
stop` logo após o `docker start` era ignorado, o app terminava de subir, consumia a fila e só morria no SIGKILL, 30 s
depois. Agora `src/shutdown.ts` é o **primeiro import** do `main.ts` (instala os gatilhos antes de carregar o resto);
um pedido durante o boot sai na hora (nada em andamento) ou, se a aplicação já foi criada, fecha assim que o boot termina.
Teste de regressão: `graceful-shutdown.test.ts`, "shutdown pedido DURANTE o boot".

### 12.3 Outbox e relay
Os eventos entram na outbox **na mesma transação** da alteração financeira. O relay (`OutboxRelay`):

- **a unidade de trabalho é a sequência de eventos de uma wallet.** Reivindica com `FOR UPDATE SKIP LOCKED` + **lease**
  (`locked_until`, `locked_by`), usando o relógio do banco (a referência comum entre instâncias): quem trava a **cabeça**
  da wallet (o evento pendente mais antigo, fora de backoff e sem lease alheio) leva também os seguintes dela, até
  `OUTBOX_RUN_SIZE` (20) por wallet e `OUTBOX_BATCH_SIZE` (200) no total. Como a elegibilidade exige a cabeça, **nenhum
  outro publisher alcança a cauda**: uma wallet nunca é dividida entre publishers e a ordem por wallet se mantém (testado
  com 6 reivindicações concorrentes). As cabeças são achadas por *loose index scan* recursivo no índice parcial
  `(aggregate_id, seq)`: custo proporcional ao nº de wallets pendentes, não ao tamanho do backlog;
- publica cada sequência em `SendMessageBatch` de até 10 (`MessageGroupId = walletId`, `MessageDeduplicationId =
  eventId`; a FIFO preserva a ordem dentro do grupo). Wallets diferentes em paralelo; lotes da mesma wallet em série;
- marca tudo como publicado com **um único `UPDATE`**: uma rodada custa **duas transações** no banco (reivindicar e
  marcar), qualquer que seja o nº de eventos;
- falha de publicação: só o **prefixo aceito** é marcado; o primeiro evento que não saiu ganha `attempts++` e
  `next_attempt_at` com backoff (1 s, 2 s, 4 s… teto 5 min) e os de trás apenas têm o lease liberado (a ordem os mantém
  na fila, bloqueio *head-of-line*). Se o broker aceitar entradas *depois* da que falhou, elas serão
  republicadas (mesmo `eventId`): uma exceção rara à ordem estrita, coberta pela deduplicação por `eventId`;
- **quarentena de evento venenoso:** um evento que esgota `OUTBOX_MAX_ATTEMPTS` (padrão 10, ~13 min de falhas seguidas)
  sai da sequência da wallet (`quarantined_at`, com `last_error`) e **a wallet volta a andar** — sem isso um único
  evento que o broker sempre recusa (ex.: `SenderFault`) bloquearia a wallet para sempre. É uma quebra de ordem
  **deliberada e visível**: o evento não é apagado nem dado como publicado, `outbox_quarantined_total` conta,
  `outbox_quarantined_events` (gauge lido no scrape) deve alertar se > 0, e o log `outbox_event_quarantined` leva o erro.
  Os índices de pendentes ignoram quarentenados (a reivindicação e o `outbox_lag_seconds` também). Corrigido o problema,
  `bun run outbox:requeue -- --list | --all | <eventId…>` devolve o evento à fila com `attempts = 0`; ele sai **depois**
  dos que já foram publicados, e o consumidor ordena pelo `walletVersion` / deduplica pelo `eventId`.

Por que isso existe: com um evento por wallet por rodada e uma transação por evento, o teto de publicação era ~100
eventos/s por wallet, menos da metade do que a escrita sustenta (achado do teste de carga). Agora é ~1 500–1 800
eventos/s por wallet no microbenchmark (LOAD_TEST.md §5.4).

| Queda do publisher | Estado | Recuperação |
|---|---|---|
| depois do commit, antes de publicar | eventos pendentes na outbox | outro publicador assume (`SKIP LOCKED`) |
| reivindicou e morreu | lease no banco | o lease expira (`OUTBOX_LEASE_MS`) e outro assume |
| publicou e morreu antes de marcar | evento já no broker | **republicado** com o mesmo `eventId` e corpo; o consumidor deduplica por `eventId` |
| o broker recusa o evento sempre | `attempts` sobe com backoff | **quarentena** em `OUTBOX_MAX_ATTEMPTS`; a wallet segue; `outbox:requeue` depois da correção |

### 12.4 Referências fora de ordem
Uma reversão cuja referência não existe vira `PENDING_REFERENCE` (HTTP `202`, evento
`WagerTransactionPendingReference`). Quando a referência chega e é processada, os dependentes são **liberados na hora**
(`next_attempt_at = now`) — e também quando a referência é **rejeitada**, para que os dependentes recebam
`REFERENCE_NOT_PROCESSED` sem esperar o backoff; o `PendingReferenceWorker` reprocessa as vencidas, com backoff
exponencial 1, 2, 4 … 256 s.

**Limite: 10 tentativas (≈ 8,5 minutos).** Justificativa: o provedor já terá feito timeout/retry por conta própria, e
manter a reversão pendente indefinidamente deixaria o fluxo do jogador sem resposta final. Esgotado o limite →
`REJECTED/REFERENCE_NOT_FOUND` (a referência nunca chegou) ou `REJECTED/REFERENCE_UNRESOLVED` (chegou, mas continuou
sem decisão final) e o evento `WagerTransactionRejected`. O reprocessamento confere o estado de novo sob
lock, então vários workers podem rodar ao mesmo tempo sem duplicar efeitos.

**Falha no reprocessamento.** Se uma tentativa lança erro (infraestrutura ou bug), a transação SQL é desfeita e
`next_attempt_at` continuaria vencido: a linha ficaria no topo da fila (`ORDER BY next_attempt_at LIMIT 20`) e, com 20
linhas assim, nenhuma outra pendência andaria. Por isso o worker, numa transação própria e sob lock, **adia** a linha
por **1 s** (fixo, e não pelo backoff da referência, que no nível mais alto chega a minutos e puniria uma falha
transitória). Como a fila é ordenada por `next_attempt_at`, as pendências vencidas há mais tempo passam na frente.
`attempts` não muda: uma falha de infraestrutura (ex.: `lock_timeout` de uma wallet quente) não pode gastar a espera
pela referência. O erro continua registrado a cada tentativa (`pending_reference_reprocess_failed`, com stack).
Uma falha **não transitória** (bug, dado que o banco recusa) também é contada em `processing_failures`, no banco (vale
entre instâncias); com 5 seguidas a transação vira **`FAILED`/`INFRA_PERMANENT`** com o evento `WagerTransactionFailed`,
em vez de ser tentada a cada segundo para sempre, e quem a aguardava como referência é liberado (`REFERENCE_NOT_PROCESSED`).
Falhas transitórias não contam (testado).

## 13. API HTTP

Mapa único (README, "Status HTTP"). Erros: `{ "error": { "code", "message", "details?" } }`. O `x-correlation-id` é
aceito (se razoável) ou gerado, devolvido na resposta e propagado aos logs. Falhas transitórias (deadlock,
`lock_timeout`, conexão, conflito de versão) viram `503` + `Retry-After`; erros desconhecidos viram `500` **sem vazar
detalhes** (o detalhe vai para o log). `404` de rota inexistente usa o mesmo formato.

## 14. Observabilidade

- **Logs JSON** (uma linha por evento) com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`,
  propagados por `AsyncLocalStorage` (cada requisição/mensagem tem seu escopo; testado com tarefas concorrentes).
- **Sem dados sensíveis:** além de não passar valores monetários para o log, uma redação por nome de chave
  (`amount`, `money`, `payload`, `balance*`, `password`, `token`, `authorization`…) mascara qualquer vazamento, em
  qualquer profundidade. Testes confirmam que valores como `4321.09` nunca aparecem nos logs.
- **Métricas Prometheus** em `GET /metrics`:

| Métrica | O que mede |
|---|---|
| `wager_transactions_total{status,kind}` | transações por status |
| `wager_duplicates_total{source=http\|sqs}` | duplicatas detectadas |
| `retries_total{component}` | retries (`sqs_consumer`, `outbox_relay`, `pending_reference`) |
| `dlq_messages_total{reason}` | mensagens na DLQ (`invalid_message`, `invalid_payload`, `retries_exhausted`, `retries_exhausted_unrecorded`) |
| `outbox_quarantined_total`, `outbox_quarantined_events` | eventos que foram para a quarentena / que estão lá agora (alerta se > 0) |
| `lock_conflicts_total{kind}` | `deadlock`, `serialization`, `lock_timeout`, `optimistic_version` |
| `wallet_lock_wait_seconds` | espera pelo lock da wallet (contenção de wallet quente) |
| `outbox_lag_seconds` | idade do evento pendente mais antigo (lido no scrape, direto do banco) |
| `wager_processing_duration_seconds{source,outcome}`, `http_request_duration_seconds{method,route,status}` | latências |
| `reconciliation_divergences_total` | divergências de saldo |

  Os rótulos têm cardinalidade limitada (a rota é o **padrão** `/wallets/:walletId`, nunca a URL com ids).
- **Health:** `/health/live` não consulta dependências; `/health/ready` verifica PostgreSQL e SQS (timeout de 2 s por
  verificação) e lista qual caiu. Ambos, e `/metrics`, são abertos.

## 15. Autenticação

**Decisão: OIDC com um Identity Provider externo (Keycloak), máquina a máquina.** Nada de tabela de usuários nem hash de
senha na API. `AUTH_MODE=oidc` liga; `AUTH_MODE=none` (padrão fora do compose, usado nos testes que não são de auth)
trata todo chamador como operador.

**IdP (`docker-compose.yml`, realm em `keycloak/realm-wagering.json`):**

- cada **provedor de jogos** é um *client* `client_credentials` (`provider-a`, `provider-b`) com um *hardcoded claim
  mapper* que põe `provider_id` no token;
- o **backoffice do operador** é o client `backoffice`, cuja service account tem o papel de realm `wagering-operator`;
- um *client scope* `wagering-api` põe a audiência (`aud = wagering-api`) e os papéis (`realm_access.roles`);
- `KC_HOSTNAME=http://localhost:8180` fixa o `iss`, então o token tirado do host vale dentro dos containers, que buscam o
  JWKS pela rede interna (`OIDC_JWKS_URL=http://keycloak:8080/…`). Secrets de **desenvolvimento**, versionados de propósito.

**Na API:**

- `OidcProviderIdentity` (`infrastructure/auth`, com `jose`): valida assinatura via JWKS (cacheado, renovado em `kid`
  novo, timeout 3 s), `iss`, `aud`, `exp`/`nbf` e aceita **só algoritmos assimétricos** (um HS256 forjado é recusado).
  Token ausente/inválido/expirado/de outra audiência ⇒ `401`; JWKS inacessível ⇒ lança ⇒ `503` (transitório);
- `AuthGuard` global (`APP_GUARD`); rotas `@Public()` (health e métricas) não consultam o IdP;
- **autorização com direitos explícitos** — a ausência de uma claim nunca concede acesso:

| Rota | Provedor (`provider_id`) | Operador (`wagering-operator`) | Token válido sem nenhum dos dois |
|---|---|---|---|
| `POST /wagering/transactions`, consultas de transação | só do **próprio** `providerId` | qualquer provedor | `403` |
| `GET /wallets/:id` (saldo) | sim | sim | `403` |
| `POST /wallets`, `GET /wallets/:id/ledger`, `POST /wallets/:id/reconciliation` | **`403`** (o ledger expõe transações de outros provedores) | sim | `403` |

Testes: `test/unit/infrastructure/oidc-provider-identity.test.ts` (JWKS local: expirado, outro `iss`/`aud`, chave
desconhecida, HS256 forjado, malformado, JWKS fora ⇒ lança); `test/integration/oidc-keycloak.test.ts` (**tokens reais
do Keycloak**: backoffice cria wallet e lê ledger, provedor aposta e lê saldo, provedor não age como outro nem faz
backoffice, client sem direitos ⇒ 403, token adulterado ⇒ 401); `auth-extension.test.ts` (guard e regras com um
adaptador de teste, IdP fora ⇒ 503).

**Fora de escopo, como no enunciado:** os endpoints de health ficam abertos e as **mensagens da fila são um canal
interno confiável**; mesmo assim, o `providerId` contido na mensagem continua sujeito às validações de domínio. A relação
player↔provedor não é definida pelo enunciado, então o saldo de uma wallet é legível por qualquer provedor autenticado
(o ledger, não).

## 16. Estratégia de testes e evidências

**535 testes** (264 de unidade, 254 de integração e 17 de concorrência multi-processo), todos passando no Windows, onde
roda também o controle negativo de shutdown (16 em Linux). Nenhum substitui PostgreSQL, SQS ou o IdP por mock: cada suíte
de integração usa um banco isolado e filas SQS únicas no LocalStack, e a de OIDC tira tokens do Keycloak real.

- **Testes que provam, e não só passam.** Para as garantias principais, o código foi **quebrado de propósito** para
  confirmar que os testes o detectam (e depois restaurado):

| Mutação | Quem detectou |
|---|---|
| remover o `FOR UPDATE` da wallet | 4 testes de integração (80+80, 20 apostas, `REFUND` concorrentes, reprocessamento duplicado) **e** 3 testes com 3 processos reais |
| relay ignora o lease | publishers concorrentes; lease do publisher morto |
| reivindicação sem `FOR UPDATE … SKIP LOCKED` (publishers repartindo uma wallet) | 3 relays concorrentes; reivindicações concorrentes |
| relay trata lote parcialmente publicado como totalmente publicado (perda silenciosa de evento) | falha parcial no meio do lote |
| shutdown não devolve a visibilidade | teste de shutdown gracioso |
| `ack` antes do commit | retry transitório e DLQ por tentativas esgotadas |

- **Falhas deterministas sem ganchos de teste no código de produção:** um **proxy HTTP na frente do SQS** (`test/support/
  sqs-proxy.ts`) retém uma ação específica (`DeleteMessageBatch`, `SendMessageBatch`) enquanto o teste dá `SIGKILL` no processo,
  cobrindo "morto depois do commit e antes do ack" e "publisher morto antes/depois de publicar".
- **Queda total:** as 3 instâncias levam `SIGKILL` no meio da carga (62–67 de 394 transações decididas), o serviço
  reinicia, clientes HTTP reenviam a mesma requisição, e ao convergir se verifica: ledger = saldo; uma transação por
  requisição; saldo recomputado **de forma independente do ledger** (só pela tabela de transações) = saldo; nenhum
  lançamento duplicado; 4 de 10 apostas na wallet pobre; todos os `REFUND` fora de ordem resolvidos; nenhum evento
  perdido; DLQ vazia.
- **Atomicidade:** falha injetada (id que lança) depois do efeito desfaz saldo, ledger, transação, inbox e outbox.
- **Shutdown gracioso real, nas duas plataformas:** processo real com fila e HTTP em voo, pelo gatilho nativo (IPC no
  Windows, `SIGTERM` no Linux) e controle negativo no Windows; e `docker stop` em container (§12.2).
- **Falhas permanentes:** `FAILED` pela fila (falha desconhecida esgotada) e pelo worker de pendências; quarentena e
  requeue na outbox; a cauda de grupo devolvida não gasta tentativas; banco fora ⇒ teto mais alto, sem DLQ precoce.
- **Autenticação com Keycloak real** (tokens `client_credentials`) e com JWKS local (§15).
- **Teste de carga** (`bun run test:load`, LOAD_TEST.md): processos reais, malha fechada e aberta (sem *coordinated
  omission*), verificação das invariantes de dinheiro ao fim de cada cenário e experimentos de atribuição.
- **Cobertura** (unit + integração): 99,9% das linhas, 93% das funções. Sem piso no `bunfig.toml` (o relatório do Bun
  conta como não chamado o construtor implícito das subclasses de erro; o motivo está no arquivo).

## 17. Modos de falha

| Falha | Comportamento |
|---|---|
| PostgreSQL indisponível | HTTP `503` + `Retry-After`; o consumidor faz retry com backoff; `/health/ready` `503`, `/health/live` `200`; recupera sozinho |
| SQS indisponível | a API HTTP continua processando; a outbox acumula (`outbox_lag_seconds` sobe) e é drenada na volta; o consumidor registra o erro e tenta de novo; `/health/ready` `503` |
| Processo morre no meio de uma requisição | a transação SQL desfaz; o provedor reenvia com a mesma `Idempotency-Key` |
| Processo morre depois do commit | o efeito está no banco; a redelivery/reenvio é reconhecida como replay |
| IdP indisponível (`AUTH_MODE=oidc`) | `503` (transitório), nunca `401` |
| Mensagem envenenada | DLQ imediata (inválida); após `SQS_MAX_RECEIVES` tentativas **reais** (transitória persistente, sem gravar nada); ou `FAILED/INFRA_PERMANENT` + DLQ (falha desconhecida persistente) |
| `PENDING_REFERENCE` cujo reprocessamento sempre falha | `FAILED/INFRA_PERMANENT` após 5 falhas não transitórias (§12.4) |
| Evento da outbox que sempre falha | backoff, depois **quarentena** (`OUTBOX_MAX_ATTEMPTS`): a wallet volta a andar; `outbox_quarantined_events` alerta; `outbox:requeue` (§12.3) |
| Shutdown no Windows | `SIGTERM` não existe entre processos: Ctrl+C, Ctrl+Break ou IPC fazem o shutdown gracioso (§12.2) |

## 18. Trade-offs, limitações e próximos passos

**Limitações conhecidas**

- **Wallet quente tem teto de vazão:** as operações da mesma wallet são serializadas por design. Medido em
  [LOAD_TEST.md](./LOAD_TEST.md) (§5.2): **~126 tx/s** por wallet nesta máquina; abaixo disso o p99 fica em dezenas de
  ms, acima a espera pelo lock cresce (p99 de 3,8 s com 64 clientes) sem falhas.
- **Outbox sob saturação total.** O teste de carga achou a publicação mais lenta que a escrita (~100 eventos/s por
  wallet) e isso foi **corrigido** (§12.3): sequência por wallet + lote ordenado + marcação em lote, ~4× a ~17× mais
  rápido, e o tempo para drenar depois da carga caiu de 34–70 s (ou nunca) para 0–10 s. **Sobra:** em saturação total
  o `outbox_lag_seconds` ainda chega a 7–23 s durante a carga, porque o relay disputa CPU e banco com uma escrita no
  limite (testei relays dedicados com `OUTBOX_RELAY_ENABLED`: não houve ganho consistente num único host, ou seja, o teto é a capacidade agregada de relays e não a separação de processos; LOAD_TEST.md §5.4). Em regime sustentável (70% da vazão máxima) o lag fica ≤ 2,2 s. A correção financeira nunca foi
  afetada (LOAD_TEST.md §5.4).
- **Vazão do consumidor SQS:** a drenagem de backlog subiu de ~205 para ~440 msg/s (3 instâncias, LocalStack) com acks em
  lote e 4 laços por instância; ainda abaixo do caminho HTTP (~750 req/s). O *prefetch* existe (`CONSUMER_PREFETCH`, desligado
  por padrão) mas não mostrou ganho de vazão; falta um teste de fila em malha aberta (LOAD_TEST.md §5.5).
- **Head-of-line na outbox (limitado):** enquanto um evento está em backoff, os seguintes da mesma wallet esperam (ordem
  por wallet). O limite agora é a **quarentena** (§12.3, ~13 min com o padrão), que troca a ordem estrita daquele evento
  pela vazão da wallet — conscientemente e com alerta. O custo da reivindicação cresce com o nº de wallets com eventos
  pendentes (uma busca por wallet), não com o backlog.
- **`FAILED` só existe pela fila e pelo worker de pendências.** Pela API HTTP uma falha desconhecida responde `500` e não
  grava nada: o provedor pode reenviar a mesma key, o que é mais seguro do que encerrar como `FAILED` na primeira falha
  de uma requisição síncrona. Esgotamento por falha **transitória** também não grava `FAILED` (a mensagem na DLQ pode ser
  reenviada quando a infraestrutura voltar).
- **Teto de recebimentos com o banco fora:** se o contador de tentativas (`inbox_attempts`) está inacessível, o consumidor
  usa o `ApproximateReceiveCount` contra `SQS_RECEIVE_HARD_LIMIT`, que conta também as devoluções sem processamento — só
  nesse modo degradado.
- **A idempotency key é global** (não por provedor): dois provedores que escolham a mesma key colidem (409). Recomenda-se
  o prefixo do provedor.
- **Reconciliação é O(n)** por wallet (soma do ledger). Para ledgers muito grandes, manter totais incrementais.
- **A migration `0003` falha** se já existirem duas reversões `PROCESSED` de tipos diferentes na mesma referência
  (estado que a regra proíbe); seria preciso resolver os dados antes.
- **Saldo legível por qualquer provedor autenticado** (§15): o enunciado não define a relação player↔provedor. O ledger
  e as operações de backoffice são só do operador. `/metrics` e health abertos: restrinja por rede em produção.
- **Keycloak em `start-dev`** (HTTP, banco H2 embutido, secrets de desenvolvimento no realm versionado): adequado ao
  desafio, não à produção (lá: `start` com TLS, banco próprio e secrets fora do repositório).
- **LocalStack fixado em 3.8** e `ensureQueues` falha com atributos diferentes de uma fila existente (inclusive o
  `maxReceiveCount`, que passou a ser `SQS_RECEIVE_HARD_LIMIT + 1`: filas criadas por uma versão anterior precisam ser
  recriadas). Não foi testado contra a AWS real.
- Métricas padrão do Node (event loop, memória) não são coletadas; sem OpenTelemetry nem dashboard (opcionais).

**Com mais tempo**

1. Repetir o experimento de relay dedicado com a publicação em outra máquina; repetir o teste de carga com várias execuções, gerador em outra máquina e fila em malha aberta
   (LOAD_TEST.md §6).
2. Totais incrementais por wallet para reconciliação O(1), mantendo o ledger como verdade.
3. Relação player↔provedor (se o negócio a definir) para escopar também a leitura de saldo.
4. Particionamento do ledger por tempo e arquivamento.
