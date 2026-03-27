# Monorepo

## Estrutura alvo

O repositorio passa a adotar a seguinte estrutura logica:

- `services/pricing-core`
- `services/assistant-service`
- `apps/frontend`
- `infra/`

## Papel de cada parte

### `services/pricing-core`

Backend operacional de comparacao, fila, coleta, retencao e consultas orientadas a ferramentas.
O `pricing-core` expoe HTTP interno para o `assistant-service`.
O MCP `stdio` continua opcional para integracoes internas e desenvolvimento local.

### `services/assistant-service`

Servico de atendimento que recebe mensagens do frontend, Telegram e WhatsApp.
Ele chama a LLM, mantem sessao/historico e usa o `pricing-core` por HTTP interno.

### `apps/frontend`

Interface web do cliente.
Nao conversa diretamente com `pricing-core`.

### `infra/`

Arquivos de compose, proxy, deploy e operacao local/producao.

## Estado de transicao

Nesta primeira etapa, o codigo executavel do `pricing-core` ainda fica na raiz do repositorio.
Os diretorios do monorepo sao criados agora para fixar as fronteiras arquiteturais.
A movimentacao fisica completa para `services/pricing-core` fica para a proxima rodada.
