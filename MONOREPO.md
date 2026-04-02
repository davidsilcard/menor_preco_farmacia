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

### `deploy/`

Artefatos de deploy Linux para VPS com varias aplicacoes:

- `app.env.example` externo ao codigo
- units e timer de `systemd`
- exemplo de `Caddyfile`
- guia para layout em `/home/david/apps/<app>` e `/etc/<app>/app.env`

## Estado atual

O backend foi movido fisicamente para `services/pricing-core`.

Na raiz permanecem:

- `infra/`
- `apps/`
- `services/assistant-service`
- documentacao de topo do monorepo

O `.env` principal continua centralizado na raiz para a fase 1, porque o compose sobe os servicos a partir desse ponto.
Para deploy Linux sem containers, o padrao recomendado fica em `deploy/README.md` com configuracao em `/etc/<app>/app.env`.
