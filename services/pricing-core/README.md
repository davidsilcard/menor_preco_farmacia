# Pricing Core

Backend operacional de comparacao de medicamentos por `CEP`.

Responsabilidades:

- consultas de preco e comparacao por `CEP`
- tracking por `CEP`
- fila de `search_jobs` e `operation_jobs`
- coleta agendada
- retencao de dados operacionais
- endpoints HTTP internos para o `assistant-service`

Entry points:

- `uv run python -m src.main`
- `uv run python -m src.worker_main`
- `uv run python -m src.scheduler_main`
- `uv run python -m src.mcp_server`

Observacoes:

- o runtime operacional e `request_scoped` por `CEP`
- `CEP` em `.env` e apenas fallback opcional de bootstrap local
- o `.env` usado pelo compose da fase 1 continua centralizado na raiz do monorepo
