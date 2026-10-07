# Teste de carga

`bun run test:load` — metodologia, resultados e, principalmente, **o que os números provam e o que não provam**.
Relatórios brutos em `load-reports/`, organizados por experimento ([índice](./load-reports/README.md)): **antes** da
correção da outbox [`load-2026-10-07T15-08-07.md`](./load-reports/02-baseline/load-2026-10-07T15-08-07.md); **depois**
[`load-2026-10-07T15-32-28.md`](./load-reports/04-outbox-fix/load-2026-10-07T15-32-28.md) (e os `.json`).

> **Resumo honesto.** Sob carga, **o dinheiro nunca divergiu** em nenhuma das duas rodadas (0 deadlocks, 0 erros, 0
> mensagens na DLQ, ~80 mil transações aplicadas por rodada com o ledger batendo com o saldo em todas). O caminho de
> escrita sustenta **~750 req/s** nesta máquina. A primeira rodada achou um gargalo que os testes funcionais não
> mostravam: **a publicação da outbox era mais lenta que a escrita** (teto de ~100 eventos/s *por wallet*). Foi
> **corrigido** (§5.4): a publicação passou a ser em sequência e em lote, ficou **~4× a ~17× mais rápida** no
> microbenchmark e, sob carga, o tempo para drenar a outbox caiu de **34–70 s (ou nunca) para 0–10 s**. Sob saturação
> total o lag durante a carga ainda chega a 7–23 s; em regime sustentável (70%) fica em ≤ 2,2 s.

## 1. Como rodar

```bash
bun run infra:up                              # PostgreSQL e LocalStack
bun run test:load                             # perfil standard: ~4–5 minutos (eram ~11 antes da correção da outbox)
LOAD_PROFILE=smoke bun run test:load          # perfil curto (~2 min) para validar o ambiente
LOAD_SCENARIOS=hot-3x,sqs-3x bun run test:load
bun run test:load:outbox                      # microbenchmark isolado da vazão de PUBLICAÇÃO da outbox (~30 s)
```

| Variável | Padrão (standard) | Efeito |
|---|---|---|
| `LOAD_PROFILE` | `standard` | `smoke` ou `standard` |
| `LOAD_SCENARIOS` | todos | lista separada por vírgula: `spread-1x,spread-3x,hot-3x,mixed-3x,sqs-3x` |
| `LOAD_CONCURRENCY` | 64 | clientes da malha fechada |
| `LOAD_MEASURE_SEC` / `LOAD_WARMUP_SEC` | 20 / 5 | janela medida e aquecimento (descartado) |
| `LOAD_OPEN_FRACTION` | 0.7 | taxa da malha aberta = fração × vazão máxima da malha fechada |
| `LOAD_WALLETS` / `LOAD_SQS_MESSAGES` | 500 / 3000 | tamanho do cenário |
| `LOAD_DISABLE_RELAY` | — | `true` desliga o relay (experimento de atribuição) |
| `LOAD_RELAY_INSTANCES` | 0 | N instâncias dedicadas só ao relay (o relay fica desligado nas que recebem carga) |
| `LOAD_SQS_PRELOAD` | — | `true`: no `sqs-3x` enfileira tudo antes de subir os consumidores e mede a taxa de drenagem do backlog |

O relatório (`.md` e `.json`) vai para `load-reports/`. O processo termina com **exit 1 se alguma invariante de dinheiro
for violada**: a vazão só é reportada como confiável se a correção se manteve.

## 2. Metodologia

- **Processos reais.** Cada cenário sobe 1 ou 3 instâncias de verdade (`bun run src/main.ts`, `POOL_MAX=20`, todos os
  workers ligados) contra o PostgreSQL e o LocalStack do compose, com **banco e filas isolados por cenário**. Nada é
  mockado.
- **Duas malhas por cenário**, porque medem coisas diferentes:
  1. **Malha fechada** (64 clientes; cada um envia a próxima assim que recebe a resposta): descobre a **vazão máxima**.
     A latência aqui é, em boa parte, tempo de fila — não deve ser lida como latência de serviço. *Verificação de
     sanidade:* pela Lei de Little, 64 clientes / 729 req/s = 88 ms, e o p50 medido foi 86,7 ms.
  2. **Malha aberta** a **70% da vazão máxima** (taxa constante): é a latência de um sistema *sustentável*. A latência é
     contada **desde o instante programado** de cada requisição (`intendedStarts`), e não desde o disparo real. Isso
     evita a *coordinated omission*: sem isso, um sistema saturado atrasaria o próprio gerador e pareceria mais rápido
     do que é.
- **Aquecimento descartado** (5 s) e janela medida de 20 s por fase. Os percentis são calculados sobre **todas** as
  amostras (nearest-rank, sem bucketização no cliente).
- **Cenários:** `spread-1x` e `spread-3x` (500 wallets, tráfego uniforme, 1 e 3 instâncias), `hot-3x` (100% do tráfego em
  **uma** wallet), `mixed-3x` (90% espalhado + 10% em uma wallet quente) e `sqs-3x` (3000 mensagens pela fila FIFO,
  grupo = wallet, medindo o ponta a ponta envio → decisão).
- **Servidor:** antes e depois de cada janela, o teste lê o `/metrics` das instâncias e o `pg_stat_database` e calcula as
  variações: `lock_conflicts_total`, `retries_total`, DLQ, deadlocks do PostgreSQL, a distribuição da espera pelo lock da
  wallet (histograma) e o **outbox lag** (amostrado a cada 1 s). Sessões esperando lock são amostradas a cada 250 ms em
  `pg_stat_activity`.
- **Correção.** Ao fim de cada cenário: saldo = soma do ledger; saldo = recomputação **independente** só pela tabela de
  transações; um lançamento por transação aplicada; nada preso em estado não terminal; e
  `confirmadas ao cliente ≤ gravadas ≤ confirmadas + sem resposta definitiva` (nada gravado em dobro).

## 3. Ambiente

| | |
|---|---|
| Máquina | Intel Core i7-11800H @ 2,30 GHz, 16 núcleos lógicos, 31,8 GB de RAM |
| Sistema | Windows 11 (NT 10.0.26300, x64), Docker 29.8.1 (Docker Desktop, VM Linux) |
| Runtime | Bun 1.4.2 |
| PostgreSQL | 16.15 em container — `fsync=on`, `synchronous_commit=on`, `shared_buffers≈160 MB`, `max_connections=200` |
| Broker | LocalStack 3.8 (emulador de SQS, **não** a AWS) |
| Aplicação | processos `bun`, `POOL_MAX=20`, `LOCK_TIMEOUT_MS=15000`, outbox lote 50 e sequência por wallet 20, SQS visibility 3 s. *O lote de 50 vem do helper de testes; o padrão de produção agora é 200, então o "depois" foi medido com um parâmetro mais conservador.* |
| Gerador de carga | **no mesmo host** que o banco, o LocalStack e as instâncias — compete por CPU com eles |

## 4. Resultados

Duas rodadas `standard` de 2026-10-07, **antes → depois** da correção da outbox (§5.4), uma execução por cenário em cada
uma (ver limitações em §7). Latências em ms. A coluna "depois" é a rodada atual do repositório.

| Cenário | Vazão máx. (malha fechada) | Malha aberta (70%) | p50 / p95 / **p99** na malha aberta (depois) | p99 antes → depois | Erros | Deadlocks PG | Invariantes |
|---|---|---|---|---|---|---|---|
| `spread-1x` (1 instância) | 442 → **521** req/s | 365 req/s | 11,6 / 25,8 / **52,7** | 49,2 → 52,7 | 0 | 0 | ✅ |
| `spread-3x` (3 instâncias) | 729 → **747** req/s | 523 req/s | 13,4 / 22,0 / **34,0** | 58,4 → 34,0 | 0 | 0 | ✅ |
| `hot-3x` (1 wallet) | 126 → **127** req/s | 89 req/s | 10,6 / 17,3 / **26,1** | 41,0 → 26,1 | 0 | 0 | ✅ |
| `mixed-3x` (10% quente) | 633 → **674** req/s | 472 req/s | 13,2 / 22,9 / **36,9** | 51,7 → 36,9 | 0 | 0 | ✅ |
| `sqs-3x` (fila) | 120 → **145** msg/s de consumo | — | ponta a ponta: 9 787 / 15 203 / **16 302** | 20 126 → 16 302 | 0 | 0 | ✅ |

A vazão de escrita não deveria mudar com a correção da outbox, e no `hot-3x` (limitado pela serialização da wallet)
não mudou (126 → 127). Nos demais subiu 2% a 18%: **hipótese, não testada**, de que o relay agora faz muito menos idas
ao banco por evento e disputa menos CPU e conexões com a escrita. Com n = 1 por rodada, diferenças desse tamanho podem
ser ruído.

**Conflitos de concorrência.** `lock_conflicts_total` e deadlocks ficaram em **0** em todos os cenários, nas duas
rodadas: o lock pessimista por wallet transforma a disputa em *espera*, não em falha. O custo aparece na **espera pelo
lock** (números da primeira rodada; a correção da outbox não mexe no caminho do lock):

| Espera pelo lock da wallet | média | p50 | p95 | p99 | sessões esperando (média / máx) |
|---|---|---|---|---|---|
| `spread-3x` (malha fechada) | 8,5 ms | 7,1 | 24,1 | 64,0 | 1,3 / 4 |
| `mixed-3x` (malha fechada) | 65,7 ms | 3,2 | 330,2 | 2 034,7 | 38,4 / 48 |
| `hot-3x` (malha fechada) | **449,1 ms** | 156,9 | 2 158,6 | **3 801,0** | **52,9 / 59** |
| `hot-3x` (malha aberta, 70%) | 1,8 ms | 1,7 | 4,9 | 24,7 | 0,06 / 2 |

## 5. O que os números mostram

### 5.1 A correção se manteve sob carga
~79 mil transações aplicadas em 5 cenários, com 0 erros, 0 deadlocks, 0 mensagens na DLQ e as cinco invariantes
verdadeiras em todos. Sob 64 clientes disputando uma única wallet, as 2 515 apostas da janela foram serializadas sem
nenhuma falha: o `lock_timeout` de 15 s nunca foi atingido (a maior espera foi 5,6 s).

### 5.2 Wallet quente: o teto é a serialização, e é previsível
Com tudo em uma wallet o sistema sustenta **~126 tx/s** (≈ 8 ms por transação, que é o tempo que cada uma segura o lock).
Acima disso a fila de espera cresce: com 64 clientes, p99 de 2,9 s e 53 sessões esperando em média. **Abaixo do teto
(70%) a mesma wallet responde com p99 de 41 ms.** Ou seja, o problema não é a contenção em si, e sim passar da vazão da
wallet. O `mixed-3x` mostra o efeito colateral: com 10% do tráfego na wallet quente, a malha fechada tem p99 de 1,6 s e
máximo de 10,5 s (perto do `lock_timeout` de 15 s), enquanto a malha aberta a 70% tem p99 de 52 ms.
*Estimativa (não testada):* com mais de ~1 900 requisições enfileiradas na mesma wallet (15 s ÷ 8 ms) começariam a
aparecer `lock_timeout` → 503.

### 5.3 Instâncias: ganho real, mas não linear — e este experimento não diz por quê
`spread-1x` → `spread-3x`: 442 → 729 req/s (**1,65×**, não 3×). Como o gerador, o PostgreSQL (em VM), o LocalStack e as 3
instâncias dividem os mesmos 16 núcleos lógicos, **não dá para concluir nada sobre a escalabilidade do código a partir
disso**. Não medi CPU por processo, então atribuir o limite à CPU do host é uma hipótese razoável, não um resultado.

### 5.4 Principal achado — e a correção: a publicação da outbox não acompanhava a escrita

**O problema (primeira rodada).**

| Cenário (malha fechada) | Eventos pendentes ao fim da carga | Tempo para drenar |
|---|---|---|
| `spread-1x` | 18 959 | 34,4 s |
| `spread-3x` | 28 249 | 47,5 s |
| `mixed-3x` | 23 599 | 70,1 s |
| `hot-3x` | 5 799 | **não drenou em 120 s** |

Sob carga máxima o *outbox lag* chegou a 20–24 s e, no `hot-3x`, o backlog nem acabou antes da fase seguinte (por isso o
lag de 149 s que apareceu ali em malha aberta era o backlog da fase anterior, não efeito dessa fase — falha de método
minha: o teto de 120 s de espera impediu o isolamento).

**Causa, medida isoladamente** (sem HTTP, sem competição; mesmos eventos em 1 wallet vs. 100 wallets): com o código
antigo, 1 wallet publicava **~111 eventos/s** (3 relays: ~92, *piorava*) e 100 wallets ~517–644. Consequência direta de
uma decisão de projeto (ARCHITECTURE.md §12.3): para **preservar a ordem por wallet**, o relay só liberava o evento mais
antigo de cada wallet a cada rodada, e cada evento era publicado e marcado numa transação própria. O teto por wallet
era ~100 eventos/s ≈ **55 tx/s por wallet** (2 eventos por transação), menos da metade do que a escrita sustenta.

**A correção.** Três mudanças, sem abrir mão da ordem por wallet, da exclusão mútua nem do at-least-once:

1. **A unidade de reivindicação passou a ser a *sequência* de uma wallet**, e não o evento solto. Quem trava a cabeça de
   uma wallet (`FOR UPDATE SKIP LOCKED`) leva os próximos eventos dela, até `OUTBOX_RUN_SIZE` (20), e `OUTBOX_BATCH_SIZE`
   (200) no total. A cabeça é achada por *loose index scan* recursivo, com custo proporcional ao nº de wallets pendentes
   e não ao tamanho do backlog.
2. **Publicação em lote ordenado:** a sequência de uma wallet sai em `SendMessageBatch` de até 10 (a FIFO preserva a ordem
   dentro do grupo); wallets diferentes em paralelo, lotes da mesma wallet em série.
3. **Marcação em lote:** uma rodada custa **duas transações** no banco (reivindicar e marcar, num único `UPDATE`),
   qualquer que seja o nº de eventos. Antes era uma transação por evento.

Falha parcial de um lote: só o prefixo aceito é marcado; o primeiro evento que não saiu ganha backoff e os de trás só
são destravados (a ordem os mantém na fila). Se o broker aceitar entradas *depois* da que falhou, elas serão republicadas
(mesmo `eventId`, deduplicável) — uma exceção rara e documentada à ordem estrita.

**Efeito, isolado** (`bun run test:load:outbox`; `runSize=1` reproduz "um evento por wallet por rodada" no mesmo código):

| Backlog | Relays | Código antigo (medição avulsa) | `runSize=1` | `runSize=20` (atual) | Ganho sobre o antigo |
|---|---|---|---|---|---|
| 1 wallet × 400 apostas | 1 | 111 ev/s | 142 | **1 761** | **~16×** |
| 1 wallet × 400 apostas | 3 | 92 ev/s | 152 | **1 549** | **~17×** |
| 100 wallets × 4 apostas | 1 | 517 ev/s | 636 | **2 641** | **~5×** |
| 100 wallets × 4 apostas | 3 | 644 ev/s | 585 | **2 618** | **~4×** |

**Efeito, sob a carga HTTP real** (malha fechada = saturação; malha aberta = 70% do máximo):

| Cenário | Pendentes ao fim | Drenou em | Lag máx. durante a carga | Lag máx. a 70% |
|---|---|---|---|---|
| `spread-1x` | 18 959 → 17 748 | 34,4 s → **10,0 s** | 20,7 → 22,7 s | 6,2 → **0,5 s** |
| `spread-3x` | 28 249 → 10 573 | 47,5 s → **4,3 s** | 20,1 → 10,6 s | 13,2 → **2,2 s** |
| `mixed-3x` | 23 599 → 2 188 | 70,1 s → **1,1 s** | 23,4 → 6,8 s | 19,3 → **0,8 s** |
| `hot-3x` | 5 799 → **0** | > 120 s → **0 s** | 23,6 → **0,4 s** | 159 → **0,2 s** |
| `sqs-3x` | — | 0 s → 0 s | 1,4 → 2,1 s | — |

**Experimento: relay em instâncias dedicadas** (`LOAD_RELAY_INSTANCES=N`: N processos só com o relay; as instâncias que atendem a carga ficam com o relay desligado; lote 50 como nas rodadas anteriores). Relatórios `load-2026-10-07T15-42-35` (1 dedicado) e `…T15-44-32` (3 dedicados, só os cenários de 3 instâncias). Saturação (malha fechada), lag máx. durante a carga / tempo para drenar:

| Cenário | Relay embutido (3 relays) | 1 dedicado | 3 dedicados |
|---|---|---|---|
| `spread-1x` | 22,7 s / 10,0 s | **0,9 s / 0,3 s** | — |
| `spread-3x` | 10,6 s / 4,3 s | 14,2 s / 10,1 s | 8,9 s / 2,3 s |
| `mixed-3x` | 6,8 s / 1,1 s | 14,5 s / 9,0 s | 6,9 s / 1,6 s |
| `hot-3x` | 0,4 s / 0 s | 0,0 s / 0 s | — |

Vazão de escrita (req/s): `spread-3x` 747 → 796 → 730; `mixed-3x` 674 → 717 → 655; `spread-1x` 521 → 378 (a queda aparece só com 1 instância; com n = 1 não sei se é ruído ou efeito de o relay dedicado disputar a CPU do host). Invariantes de dinheiro ✅ em todas; a 70% da carga o lag ficou ≤ 3,7 s e o backlog zerou, salvo 750 eventos pendentes no `spread-3x` com 1 dedicado.

**Leitura (n = 1, ambiente compartilhado, indício e não prova):** a hipótese de que *o relay no mesmo processo do HTTP* era o gargalo **não se confirmou**. Um relay dedicado só é capaz de menos que três embutidos (pior em `spread-3x`/`mixed-3x`), e três dedicados ficam no mesmo patamar dos embutidos (lag ligeiramente menor no `spread-3x`, igual no `mixed-3x`). O que o experimento mostra é que o teto de publicação é a **capacidade agregada de relays**, e não a separação de processos; sem sobra de CPU no host (tudo roda no mesmo notebook), separar processos não cria capacidade. Só um teste com a publicação em máquina distinta responderia se a separação ajuda de fato.

**O que ainda sobra (não resolvido).** Em saturação total (`spread-1x`, `spread-3x`, `mixed-3x` em malha fechada) o
lag *durante* a carga ainda chega a 7–23 s e ficam milhares de eventos pendentes: a correção encurtou muito a
recuperação (de 34–70 s para 1–10 s), mas o relay disputa CPU e banco com uma escrita que está no limite. A leitura
*plausível*, não testada, é que o relay roda no mesmo processo (mesmo event loop) que atende o HTTP; instâncias
dedicadas à publicação (`OUTBOX_RELAY_ENABLED` já permite separar) seriam o experimento seguinte. **Em regime
sustentável (70% da vazão máxima) o lag fica em ≤ 2,2 s e o backlog zera.** Nada disso afeta a correção financeira: nada
se perde, a ordem por wallet se mantém e a tabela `outbox_messages` só cresce enquanto a carga excede a publicação (não há
arquivamento).

### 5.5 Fila SQS: de 120 para ~440 msg/s de drenagem
O produtor enfileirou 3 000 mensagens em 6,7 s (447 msg/s) e o consumo levou 24,9 s (120 msg/s); por isso o ponta a ponta
tem p50 de 11 s — **é o backlog da fila, não a latência de uma mensagem em regime sustentável** (falta um teste de fila
em malha aberta). Experimentos de atribuição:

- LocalStack **sozinho**: 769 msg/s de envio concorrente e **533 msg/s** de consumo com um consumidor. Não é ele o teto.
- Cenário `sqs-3x` **sem o relay**: o consumo sobe de 123 para **205 msg/s (+67%)**. A publicação da outbox explica *parte*
  do custo (cada mensagem processada custa ~4 chamadas ao LocalStack: envio, `DeleteMessage` e 2 eventos publicados).
- O restante da diferença (205 msg/s contra os 533 do LocalStack isolado e os 729 req/s do HTTP, que roda o **mesmo**
  caso de uso) **não foi explicado**. Hipóteses *não testadas*: o ciclo do consumidor (receber 10 → processar → apagar →
  receber de novo) não faz *pipelining*; custo extra por mensagem (inbox, logs); contenção do LocalStack com produtor,
  deletes e relay ao mesmo tempo.
- **Depois da correção da outbox** (§5.4) o consumo subiu de 120 para **145 msg/s** e o ponta a ponta p99 caiu de 20,1
  para 16,3 s (n = 1; coerente com a publicação ter ficado mais barata, mas a diferença é pequena). O consumidor continua
  sem ser o ponto mais rápido do sistema, e a causa ainda não estava explicada.

**Correção do consumidor.** Duas mudanças, sem mexer nas garantias (ack só depois do commit, inbox, ordem por wallet):
1. **Acks em lote:** em vez de um `DeleteMessage` por mensagem, os acks de cada recebimento (até 10) saem juntos num
   `DeleteMessageBatch`, depois de todos os commits do lote. Se o processo morrer antes do delete, a redelivery é
   deduplicada pela inbox (testado com `SIGKILL` retendo o `DeleteMessageBatch`).
2. **Concorrência por instância** (`CONSUMER_CONCURRENCY`, padrão 4): vários laços de recebimento independentes. A fila
   FIFO não entrega mensagem de um grupo enquanto houver outra do mesmo grupo em andamento, então a ordem por wallet
   não depende do nº de laços.

**Como medir o consumidor.** No modo padrão, produtor, consumidores e relay disputam o mesmo LocalStack: com mais laços
o *produtor* desacelerou (498 → 309 → 181 msg/s com 1, 4 e 8 laços) e a "vazão de consumo" passou a medir o produtor,
não o consumidor. Por isso o executor ganhou `LOAD_SQS_PRELOAD=true`: enfileira tudo, só então sobe os consumidores e
mede a **taxa de drenagem** do backlog (3 000 mensagens, 3 instâncias, relay embutido):

| Consumidor | Drenagem (msg/s) |
|---|---|
| antes: 1 laço, `DeleteMessage` por mensagem | 205 |
| antes, com 4 laços (só concorrência) | 224 |
| antes, com 8 / 16 laços | 220 / 220 |
| **acks em lote, 1 laço** | **321** |
| **acks em lote, 4 laços (padrão)** | **438** |
| acks em lote, 8 laços | 471 |

Leitura: a concorrência **sozinha não ajudou** (205 → 224 → 220): o teto estava no número de chamadas ao broker, e a
causa era o `DeleteMessage` por mensagem. Com os acks em lote a concorrência passa a render (321 → 438 → 471, com
retorno decrescente). Isso responde a pergunta que estava em aberto, mas por indício: **n = 1 por configuração**, o
LocalStack não é a AWS e a atribuição vem da comparação antes/depois, não de um perfil por chamada. No modo padrão (produtor
concorrente, 4 laços) o consumo medido foi 281 msg/s e o ponta a ponta p50 caiu para 508 ms (p99 1,2 s), contra
p50 de 9,8 s (p99 16,3 s): com o consumidor acompanhando o produtor, o backlog quase não se forma. 
**Prefetch** (`CONSUMER_PREFETCH=true`: cada laço pede o próximo lote enquanto processa o atual). Implementado, com testes
(tudo processado uma vez; no shutdown o que foi prefetchado volta à fila na hora, e removê-lo faz o teste falhar). Medido
com a drenagem de 3 000 mensagens, acks em lote:

| Laços | Sem prefetch (msg/s) | Com prefetch (msg/s) |
|---|---|---|
| 1 | 297 | 297 |
| 4 | 409, 422, 423, 439 (média 423) | 473, 348, 454, 456 (média 433) |

**Sem ganho de vazão demonstrado** (a diferença está dentro da dispersão entre execuções do mesmo cenário, ~±10%, e uma
execução com prefetch ficou 17% abaixo). No modo padrão, o p99 ponta a ponta foi de 1,6 s para 1,0 s com prefetch, mas é
uma execução só. Como o prefetch tem custo (as mensagens recebidas esperam invisíveis durante o processamento do lote
atual, dentro do visibility timeout) e não se pagou, **fica desligado por padrão**; é uma opção, não uma recomendação.
Relatórios do prefetch (`load-reports/07-prefetch/`): `…T16-15-02` a `…T16-18-11`. Relatórios do consumidor
(`load-reports/06-sqs-consumer/`): `…T15-59-05` a `…T16-00-12` (antes) e `…T16-02-32` a `…T16-03-17` (depois).

**Rastreabilidade.** Em `load-reports/` (uma subpasta por experimento, ver o [índice](./load-reports/README.md)): a rodada `standard` **antes** da correção da outbox é
`load-2026-10-07T15-08-07`; **depois**, `load-2026-10-07T15-32-28`. O `sqs-3x` com relay (referência do experimento de
atribuição) é `…T15-09-55` e sem relay (`LOAD_DISABLE_RELAY=true`) é `…T15-12-13`; `…T14-55-39` e `…T14-56-33` são
rodadas `smoke` de validação do executor; o microbenchmark da publicação é `outbox-bench-2026-10-07T15-28-22`.
**Reprodutibilidade:** a comparação `runSize=1` × `runSize=20` do microbenchmark da outbox é reproduzível com
`bun run test:load:outbox`. Já as medições do **código antigo** (111 / 92 / 517 / 644 eventos/s) e do **LocalStack
isolado** (769 msg/s de envio, 533 msg/s de consumo) foram scripts avulsos e **não estão no repositório**; o código antigo
nem existe mais para ser medido de novo.

## 6. Caminhos de correção

1. ~~**Outbox:** marcação em lote e publicação em lote ordenado por wallet~~ — **feito** (§5.4). Resta o experimento de
   repetir o experimento de relay dedicado com a publicação em máquina separada.
2. ~~**Fila:** apagar em lote e mais laços de recebimento~~ — **feito** (§5.5). *Prefetch* implementado e medido: sem ganho de vazão
   demonstrado (§5.5), por isso desligado por padrão. Resta um teste de fila em malha aberta.
3. **Wallet quente:** reduzir as idas e voltas feitas *sob o lock* (hoje cada `flush` é separado: saldo, lançamento,
   transação, 2 eventos), agrupando em menos instruções.
4. **Método:** esperar a drenagem completa entre as fases; repetir cada cenário várias vezes e reportar a variação;
   gerador de carga em outra máquina; teste de fila em malha aberta; perfil de CPU por processo.

## 7. Limitações deste experimento (leia antes de citar um número)

- **Uma execução por configuração (n = 1), em cada rodada.** Só há um sinal fraco de variância: duas rodadas `smoke` de
  `spread-3x` deram 558 e 543 req/s (~3% de diferença). Não há intervalo de confiança; as diferenças pequenas entre
  "antes" e "depois" (vazão de escrita, p99 em malha aberta) **não devem ser lidas como ganho**. As grandes (tempo de
  drenagem da outbox, lag a 70%, vazão de publicação isolada) são de uma ordem de grandeza e estão em acordo entre o
  microbenchmark e a carga real.
- **Ambiente de laptop**: Windows + Docker Desktop (PostgreSQL numa VM), gerador de carga no mesmo host. Os números
  absolutos **não** se transferem para um servidor Linux dedicado; o que se transfere são as *formas* (teto da wallet
  quente, outbox mais lenta que a escrita, custo da espera pelo lock).
- **LocalStack não é a AWS.** A vazão e a latência do SQS real são diferentes.
- A **latência "no servidor"** do relatório vem de histogramas com buckets grossos e interpolação: em malha fechada o p50
  do servidor chega a ser maior que o do cliente (artefato da interpolação). Use as latências do **cliente**.
- A janela de 20 s é curta para efeitos lentos (crescimento da tabela, vacuum, caches).
- Sem OpenTelemetry nem dashboard (opcionais no enunciado).
