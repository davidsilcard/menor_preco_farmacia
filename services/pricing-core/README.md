# Pricing Core

Este diretorio representa o destino final do backend operacional de comparacao de medicamentos por `CEP`.

Estado atual da transicao:

- o codigo executavel do `pricing-core` ainda permanece temporariamente na raiz do repositorio
- os entrypoints de fase 1 ja existem em `src.main`, `src.worker_main` e `src.scheduler_main`
- a migracao fisica completa para este diretorio sera feita em uma rodada posterior, quando o monorepo estiver estabilizado

Responsabilidades do `pricing-core`:

- consultas de preco e comparacao por `CEP`
- tracking por `CEP`
- fila de `search_jobs` e `operation_jobs`
- coleta agendada
- retencao de dados operacionais
