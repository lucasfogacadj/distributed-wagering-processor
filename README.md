# Distributed Wagering Processor

Serviço financeiro distribuído que processa transações de apostas (`BET → WIN | LOSS | REFUND | ROLLBACK`) vindas de
múltiplos provedores de jogos, por **HTTP** e por **SQS**, com saldo materializado e ledger imutável.

O sistema permanece correto quando as mensagens chegam **duplicadas**, **fora de ordem** ou **simultaneamente**, com
**várias instâncias** rodando ao mesmo tempo e mesmo depois de **quedas de processo**. A correção é garantida pelo
**PostgreSQL** (constraints, triggers, locks); o broker é só uma otimização.

> As decisões de projeto, os trade-offs e as limitações estão em **[ARCHITECTURE.md](./ARCHITECTURE.md)**.

## Stack

| Item | Escolha |
|---|---|
| Runtime / gerenciador de pacotes / test runner | **Bun 1.x** |
| Linguagem | **TypeScript** em modo estrito |
| Framework | **NestJS** (só na borda: HTTP, composição e ciclo de vida) |
| Banco | **PostgreSQL 16** |
| ORM | **MikroORM v7** (`EntityManager.transactional()`, `LockMode.PESSIMISTIC_WRITE`) |
| Mensageria | **AWS SQS** (filas FIFO) via **LocalStack 3.8** |
| Autenticação | **OIDC** com **Keycloak 26** (IdP externo, `client_credentials`), JWT validado com `jose` |
| Orquestração local | **Docker Compose** |
| Migrations | SQL versionado e reversível (`migrations/NNNN_*.up.sql` / `.down.sql`) |

## Requisitos

- [Bun](https://bun.sh) 1.x
- Docker + Docker Compose (Postgres, LocalStack e Keycloak; também usados pelos testes de integração e de concorrência)

## Início rápido

```bash
bun install
bun run infra:up        # Postgres (localhost:5433), LocalStack/SQS (localhost:4566) e Keycloak (localhost:8180)
bun run start           # aplica as migrations pendentes e sobe a API em http://localhost:3000 (AUTH_MODE=none)
```

Ao subir, a aplicação cria (se não existirem) as filas `wager-transactions.fifo`, `wager-transactions-dlq.fifo` e
`wallet-events.fifo`, e inicia o consumidor SQS, o relay da outbox e o worker de referências pendentes.

`bun run start` usa `AUTH_MODE=none` (sem autenticação, para explorar a API). Com o IdP:
`AUTH_MODE=oidc OIDC_ISSUER=http://localhost:8180/realms/wagering bun run start` — ou o `docker compose up`, que já sobe
assim (ver "Autenticação" abaixo).

### Exemplo de ponta a ponta

```bash
API=http://localhost:3000
PLAYER=$(bun -e 'console.log(crypto.randomUUID())')

# 1. criar a wallet (gera a transação OPENING e o lançamento CREDIT na mesma transação SQL)
WALLET=$(curl -s -X POST $API/wallets -H 'content-type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}")
WID=$(echo $WALLET | bun -e 'console.log(JSON.parse(await Bun.stdin.text()).id)')

# 2. apostar (a Idempotency-Key é obrigatória e é a fonte da verdade da idempotência)
BODY="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WID\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
curl -s -X POST $API/wagering/transactions -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' -d "$BODY"
# → {"transactionId":"…","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}

# 3. repetir a MESMA requisição devolve o resultado original, sem novo débito
curl -s -X POST $API/wagering/transactions -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' -d "$BODY"
# → …"idempotentReplay":true

# 4. consultas e reconciliação
curl -s "$API/wallets/$WID/ledger?limit=10"
curl -s -X POST "$API/wallets/$WID/reconciliation"
curl -s $API/health/ready
curl -s $API/metrics
```

## Rodando com Docker Compose (várias instâncias)

```bash
docker compose up --build --scale app=3     # 3 réplicas da aplicação + Postgres + LocalStack + Keycloak
```

Cada réplica é um processo independente (pool, consumidor, relay e worker próprios), com `AUTH_MODE=oidc`. As portas do
host são sorteadas na faixa `3000-3010` (`docker compose ps` mostra qual é qual). As migrations no boot são seguras com
várias réplicas subindo juntas (advisory lock). `docker compose stop` envia `SIGTERM`: cada réplica drena o que está em
andamento e devolve a visibilidade das mensagens que não chegou a processar (`stop_grace_period: 30s`).

## Autenticação

OIDC máquina a máquina com o Keycloak do compose (realm `wagering`, em `keycloak/realm-wagering.json`; secrets de
**desenvolvimento**). Detalhes e regras em ARCHITECTURE.md §15.

| Client (`client_credentials`) | Secret | Identidade no token | Pode |
|---|---|---|---|
| `provider-a`, `provider-b` | `<client>-secret` | `provider_id` | submeter/consultar transações **do próprio** provedor; ler saldo |
| `backoffice` | `backoffice-secret` | papel `wagering-operator` | criar wallet, ledger, reconciliação, agir por qualquer provedor |
| `no-rights` | `no-rights-secret` | nenhuma | nada (`403`; existe para os testes) |

```bash
token() { curl -s -X POST http://localhost:8180/realms/wagering/protocol/openid-connect/token \
  -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret \
  | bun -e 'console.log(JSON.parse(await Bun.stdin.text()).access_token)'; }
OPERATOR="Authorization: Bearer $(token backoffice)"
PROVIDER_A="Authorization: Bearer $(token provider-a)"
# use -H "$OPERATOR" para criar wallet / ledger / reconciliação e -H "$PROVIDER_A" para apostar como provider-a
```

Sem token ou com token inválido/expirado → `401`; IdP inacessível → `503`; sem o direito → `403`. Health e `/metrics`
são abertos.

## API

| Método e rota | Descrição | Quem (`AUTH_MODE=oidc`) | Sucesso |
|---|---|---|---|
| `POST /wallets` | Cria a wallet (`playerId` UUID + `initialBalance`) | operador | `201` |
| `GET /wallets/:walletId` | Saldo e `version` | operador ou provedor | `200` |
| `GET /wallets/:walletId/ledger?cursor=&limit=` | Ledger, mais novo primeiro; cursor opaco e estável; `limit` 1–200 (padrão 50) | operador | `200` |
| `POST /wallets/:walletId/reconciliation` | Compara o saldo armazenado com o reconstruído pelo ledger | operador | `200` |
| `POST /wagering/transactions` | Submete uma transação (header `Idempotency-Key` obrigatório) | o próprio provedor ou operador | `200` / `202` / `422` |
| `GET /wagering/transactions/:transactionId` | Consulta por id interno | o próprio provedor ou operador | `200` |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Consulta pelo id do provedor | o próprio provedor ou operador | `200` |
| `GET /health/live` · `GET /health/ready` | Liveness · readiness (PostgreSQL e SQS) — sem autenticação | `200` / `503` |
| `GET /metrics` | Métricas Prometheus — sem autenticação | `200` |

### Status HTTP (o mesmo mapa em todos os endpoints)

| Status | Situação | Corpo | O provedor deve… |
|---|---|---|---|
| `200` | Transação `PROCESSED` (primeira vez ou replay) | resultado | seguir |
| `201` | Wallet criada | wallet | seguir |
| `202` | `PENDING_REFERENCE`: aceita, aguardando a transação referenciada | resultado | aguardar (resolve sozinha) |
| `400` | Payload, header ou consulta inválidos | `{error:{code,message}}` | **corrigir** o payload |
| `401` | Token ausente, inválido ou expirado | `{error}` | obter um token novo |
| `403` | O chamador não tem o direito (outro provedor, operação de operador) | `{error}` | **não** reenviar |
| `404` | Recurso inexistente | `{error}` | — |
| `409` | Conflito: mesma `Idempotency-Key` com payload diferente, ou wallet duplicada | `{error}` | **não** reenviar igual |
| `422` | `REJECTED` por regra de negócio | resultado com `failureCode` | decidir pelo `failureCode` (ver ARCHITECTURE.md §7) |
| `503` | Falha transitória de infraestrutura (`Retry-After: 1`) | `{error}` | **reenviar** com a mesma `Idempotency-Key` |
| `500` | Falha não prevista (sem detalhes internos) | `{error}` | reenviar com cautela |
| `500` | Transação `FAILED` (`INFRA_PERMANENT`): falha permanente de infraestrutura, só pela fila (replay) | resultado com `status: "FAILED"` | **não** reenviar a mesma key (terminal) |

O replay devolve **o mesmo status HTTP** do original (uma rejeição repetida continua `422`).

## Mensagens SQS

A fila de entrada é `wager-transactions.fifo`. Use a **wallet como `MessageGroupId`** (unidade de concorrência) e um
`MessageDeduplicationId` único por envio. Corpo (§10 do enunciado):

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-07-29T15:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

```bash
# com a AWS CLI apontando para o LocalStack
QUEUE=$(aws --endpoint-url http://localhost:4566 sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text)
aws --endpoint-url http://localhost:4566 sqs send-message --queue-url "$QUEUE" \
  --message-group-id "$WID" --message-deduplication-id "$(bun -e 'console.log(crypto.randomUUID())')" \
  --message-body file://mensagem.json
```

O consumidor usa **o mesmo caso de uso** da API HTTP. Erros de negócio são terminais (ack); falhas transitórias têm
retry com backoff; entradas inválidas e tentativas esgotadas vão para `wager-transactions-dlq.fifo` com o motivo (e o nº
de `attempts`) em atributos da mensagem. As tentativas contadas são as **reais**, guardadas no banco (`inbox_attempts`),
e não o `ApproximateReceiveCount`. Uma falha **desconhecida** que esgota as tentativas encerra a transação como
`FAILED/INFRA_PERMANENT` antes da DLQ. Os eventos de integração (`WagerTransactionProcessed`, `WagerTransactionRejected`,
`WagerTransactionFailed`, `WalletBalanceChanged`, `WagerTransactionPendingReference`) saem pela outbox para
`wallet-events.fifo`.

## Variáveis de ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | `postgres://wager:wager@localhost:5433/wager` | Conexão do PostgreSQL |
| `PORT` | `3000` | Porta HTTP |
| `POOL_MAX` | `20` | Tamanho do pool de conexões |
| `RUN_MIGRATIONS` | `true` | Aplica as migrations pendentes ao subir |
| `LOCK_TIMEOUT_MS` | `10000` | `lock_timeout` do PostgreSQL: quem espera mais que isso por uma wallet falha como transitório (503) |
| `INSTANCE_ID` | `<hostname>-<pid>` | Dono do lease da outbox; precisa ser único por instância |
| `SQS_ENABLED` | `true` | `false` sobe só a API HTTP |
| `SQS_ENDPOINT` | `http://localhost:4566` | Endpoint do LocalStack/MiniStack; **vazio** = AWS real |
| `AWS_REGION` | `us-east-1` | Região |
| `QUEUE_REQUESTS` / `QUEUE_REQUESTS_DLQ` / `QUEUE_EVENTS` | `wager-transactions.fifo` / `wager-transactions-dlq.fifo` / `wallet-events.fifo` | Nomes das filas (terminam em `.fifo`) |
| `SQS_MAX_RECEIVES` | `5` | Tentativas de processamento que **falharam** (contadas no banco) antes da DLQ |
| `SQS_RECEIVE_HARD_LIMIT` | `3 × SQS_MAX_RECEIVES` | Teto de recebimentos do SQS usado só se o contador do banco estiver fora; a redrive policy da fila é este + 1 |
| `SQS_VISIBILITY_TIMEOUT` | `30` | Segundos |
| `SQS_WAIT_SECONDS` | `5` | Long polling (0–20) |
| `CONSUMER_ENABLED` | `true` | Liga o consumidor SQS |
| `CONSUMER_PREFETCH` | `false` | Cada laço pede o próximo lote enquanto processa o atual (sem ganho de vazão medido; ver LOAD_TEST.md §5.5) |
| `CONSUMER_CONCURRENCY` | `4` | Laços de recebimento concorrentes por instância (a ordem por wallet é garantida pela fila FIFO) |
| `OUTBOX_RELAY_ENABLED` | `true` | Liga o publisher da outbox |
| `PENDING_REFERENCE_WORKER_ENABLED` | `true` | Liga o worker de referências pendentes |
| `OUTBOX_LEASE_MS` | `30000` | Quanto tempo um publisher é dono dos eventos que reivindicou; se morrer, outro assume depois disso |
| `OUTBOX_BATCH_SIZE` / `OUTBOX_RUN_SIZE` | `200` / `20` | Eventos reivindicados por rodada (total) / eventos consecutivos de uma mesma wallet por rodada |
| `OUTBOX_MAX_ATTEMPTS` | `10` | Tentativas de publicação de um evento antes da **quarentena** (ver `outbox:requeue`) |
| `SHUTDOWN_DRAIN_MS` | `20000` | Quanto o shutdown espera as mensagens em andamento |
| `AUTH_MODE` | `none` | `oidc` valida JWT do IdP; `none` não autentica (todo chamador é operador) |
| `OIDC_ISSUER` | — (obrigatório com `oidc`) | `iss` esperado, ex.: `http://localhost:8180/realms/wagering` |
| `OIDC_JWKS_URL` | `<issuer>/protocol/openid-connect/certs` | Onde buscar as chaves públicas (no compose, pela rede interna) |
| `OIDC_AUDIENCE` | `wagering-api` | `aud` esperado |
| `OIDC_PROVIDER_CLAIM` / `OIDC_OPERATOR_ROLE` | `provider_id` / `wagering-operator` | Claim do provedor / papel de realm do operador |

## Testes

```bash
bun run infra:up              # necessário para integração e concorrência
bun run test:unit             # domínio e aplicação puros (~1 s, sem infraestrutura)
bun run test:int              # PostgreSQL e LocalStack REAIS (nenhum mock de banco/broker)
bun run test:concurrency      # processos `bun` de verdade (3+ instâncias), SIGKILL, queda total
bun run test:all              # as três suítes
bun run test:coverage         # unit + integração com relatório de cobertura
bun run test:load             # teste de carga (~5 min; LOAD_PROFILE=smoke para ~2 min) → load-reports/
LOAD_RELAY_INSTANCES=1 bun run test:load   # idem, com relays dedicados fora das instâncias HTTP
bun run test:load:outbox      # microbenchmark isolado da vazão de publicação da outbox (~30 s)
bun run typecheck             # tsc --noEmit
```

O **teste de carga** (`test:load`) sobe instâncias reais, mede vazão, p50/p95/p99, taxa de erro, conflitos de
concorrência e *outbox lag*, e **falha se alguma invariante de dinheiro for violada**. Metodologia, resultados e análise
(inclusive o que ele não prova) em **[LOAD_TEST.md](./LOAD_TEST.md)**.

| Suíte | O que prova |
|---|---|
| `test/unit` | `Money` (escala, entradas inválidas, moedas), invariantes da `Wallet`, regras de `BET/WIN/LOSS/REFUND/ROLLBACK`, máquina de estados, política de referências, `payloadHash`, cursor, classificação de erros, config, logger |
| `test/integration` | Migrations e **cada constraint do schema** (uma violação provocada por constraint); atomicidade (falha injetada desfaz saldo, ledger, inbox e outbox); idempotência e replay; inbox e redelivery; consumidor SQS (retry, DLQ, tentativas reais no banco, `FAILED` por falha permanente, shutdown, ack perdido); relay da outbox (lease, ordem, publishers concorrentes, **quarentena e requeue**); worker de pendências (inclusive `FAILED`); API HTTP; autenticação (**Keycloak real** e regras de autorização); métricas e logs |
| `test/concurrency` | **3 processos reais**: mesma aposta 50×, 80+80 em 20 wallets, wallet quente, HTTP+SQS simultâneos; `SIGKILL` depois do commit e antes do ack, publisher morto antes/depois de publicar; **queda e reinício de todas as instâncias no meio da carga** com a consistência final provada; **shutdown gracioso** pelo gatilho nativo da plataforma (IPC no Windows, `SIGTERM` no Linux) com fila e HTTP em voo |

Cada teste de integração cria um **banco isolado** (`CREATE DATABASE`) e filas SQS com nome único, então rodam sem
interferir entre si. A invariante `wallet.balance == saldo reconstruído pelo ledger` é verificada ao fim dos testes de
concorrência.

**Cobertura** (`bun run test:coverage`, unit + integração): **99,9% das linhas, 93% das funções**. O `main.ts` roda em
subprocessos nos testes de concorrência, o que o relatório não enxerga. Não há piso de cobertura no `bunfig.toml` (o
motivo está no próprio arquivo).

## Migrations

```bash
bun run migrate:up              # aplica as pendentes (advisory lock: seguro com várias instâncias)
bun run migrate:down            # reverte a última  (bun run migrate:down 3 reverte as 3 últimas)
```

## Outbox: quarentena

Um evento que o broker recusa `OUTBOX_MAX_ATTEMPTS` vezes seguidas vai para a **quarentena** e deixa de bloquear a
wallet. A métrica `outbox_quarantined_events` (> 0) e o log `outbox_event_quarantined` sinalizam. Corrigida a causa:

```bash
bun run outbox:requeue -- --list          # eventos em quarentena, com o último erro
bun run outbox:requeue -- <eventId…>      # devolve esses à fila (ou --all)
```

## Estrutura

```
src/
  domain/          Money, Wallet, WagerTransaction, WalletLedgerEntry, Inbox/Outbox, eventos, WagerPolicy (puro)
  application/     casos de uso, portas (repositórios, relógio, métricas…), outbox relay, workers
  infrastructure/  MikroORM (schemas, repositórios, unit of work), SQS, migrator, logs, métricas, auth (OIDC e no-op)
  interfaces/      HTTP (controllers, guard, filtro de erros) e mensageria (consumidor SQS)
  main.ts          bootstrap
migrations/        SQL versionado (.up.sql / .down.sql)
keycloak/          realm do IdP (clients, papéis, mappers) importado pelo docker-compose
test/              unit · integration · concurrency · support
```

O domínio não depende de NestJS, do ORM, de zod nem do SDK da AWS; as dependências apontam para dentro.

## Operação e armadilhas

- **LocalStack fixado em 3.8**: as imagens recentes exigem token de conta. MiniStack também serve (`SQS_ENDPOINT`).
- **`QueueNameExists`**: criar uma fila já existente com atributos diferentes (visibility timeout, redrive) é recusado
  pelo SQS. Se mudar `SQS_RECEIVE_HARD_LIMIT` (ou `SQS_MAX_RECEIVES`, de que ele depende) ou `SQS_VISIBILITY_TIMEOUT`,
  recrie as filas (`docker compose restart localstack` as apaga).
- **Windows**: não existe `SIGTERM` entre processos (`kill` termina na hora, sem shutdown). O shutdown gracioso vem de
  **Ctrl+C**, **Ctrl+Break** ou de uma mensagem IPC `{ "type": "shutdown" }` de um processo pai; em container, `docker
  stop` (SIGTERM) funciona normalmente. Testado nas duas formas (`test/concurrency/graceful-shutdown.test.ts`).
- **Postgres na porta 5433** e **Keycloak na 8180** do host (para não colidir com serviços locais); dentro da rede do
  compose são 5432 e 8080.
- **Keycloak** sobe em `start-dev` (HTTP, H2 embutido): só para desenvolvimento. O realm é importado na primeira
  subida; ao mudar `keycloak/realm-wagering.json`, recrie o container (`docker compose rm -sf keycloak`).
