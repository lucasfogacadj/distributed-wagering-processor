# Relatórios do teste de carga

Relatórios brutos (`.md` legível + `.json` com os números) das rodadas analisadas em [LOAD_TEST.md](../LOAD_TEST.md),
agrupados pelo experimento. Todas as rodadas são de 2026-10-07, na mesma máquina (ver a seção "Ambiente" de cada
relatório). Novas execuções de `bun run test:load` gravam na raiz desta pasta e são ignoradas pelo `.gitignore`;
mova para uma subpasta as que forem citadas numa análise.

| Pasta | Rodadas | O que é | Onde é discutido |
|---|---|---|---|
| `01-smoke/` | `T14-55-39`, `T14-56-33` | perfil `smoke`, validação do executor | LOAD_TEST.md §5.5 (Rastreabilidade) |
| `02-baseline/` | `T15-08-07` + `standard-run.log` | rodada `standard` **antes** da correção da outbox | §4, §5.4 |
| `03-sqs-attribution/` | `T15-09-55` (com relay), `T15-12-13` (`LOAD_DISABLE_RELAY=true`) | atribuição do custo do consumidor SQS | §5.5 |
| `04-outbox-fix/` | `outbox-bench-T15-28-22`, `T15-32-28` + log | microbenchmark e rodada `standard` **depois** da correção da outbox | §5.4 |
| `05-dedicated-relay/` | `T15-42-35` (1 relay dedicado), `T15-44-32` (3 dedicados) + logs | relay em instâncias dedicadas | §5.4 |
| `06-sqs-consumer/` | `T15-59-05`…`T16-00-12` (antes: 1/4/8/16 laços), `T16-02-32`…`T16-03-04` (acks em lote: 1/4/8 laços), `T16-03-17` (modo padrão) | drenagem do consumidor SQS | §5.5 |
| `06-sqs-consumer/exploratory/` | `T15-57-37`, `T15-57-56`, `T15-58-16` | rodadas exploratórias **não usadas** na análise (mantidas por transparência) | — |
| `07-prefetch/` | `T16-15-02`…`T16-18-11` | prefetch ligado × desligado (1 e 4 laços, repetições) | §5.5 (Prefetch) |
