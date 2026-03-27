# Super Melhor Preco Farmacia Monorepo

Este repositorio agora esta organizado como monorepo.

Estrutura atual:

- `services/pricing-core`: backend operacional de comparacao, fila, coleta e retencao por `CEP`
- `services/assistant-service`: servico de atendimento que vai conversar com a LLM e integrar canais
- `apps/frontend`: interface web do cliente
- `infra/`: compose, deploy e operacao da fase 1

Pontos de entrada principais:

- documentacao do monorepo em `MONOREPO.md`
- documentacao do backend em `services/pricing-core/README.md`
- compose da fase 1 em `infra/docker-compose.phase1.yml`

Regra arquitetural:

- `frontend`, `Telegram` e `WhatsApp` falam com o `assistant-service`
- o `assistant-service` fala com o `pricing-core` por HTTP interno
- o `pricing-core` continua MCP-friendly, mas o caminho principal de producao e HTTP interno
