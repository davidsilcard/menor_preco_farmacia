# Deploy Linux em VPS

Padrao recomendado para esta VPS com varias aplicacoes:

```text
/home/david/apps/super_melhor_preco_farmacia/           -> codigo versionado do monorepo
/etc/super-melhor-preco-farmacia/app.env                -> segredos e parametros de producao
/etc/systemd/system/super-melhor-preco-farmacia-*.service
/etc/systemd/system/super-melhor-preco-farmacia-scheduler.timer
/etc/caddy/sites/super-melhor-preco-farmacia-api.Caddyfile
```

Separacao de responsabilidades:

- `/home/david/apps/...`: codigo versionado, `services/` e `apps/` do monorepo
- `/etc/<app>/app.env`: segredos e configuracao sensivel de producao
- `systemd`: processo HTTP, worker e agendamento
- `Caddy`: dominio, TLS e roteamento HTTP/HTTPS

## Quando usar este padrao

Use este padrao quando a VPS hospeda varias aplicacoes e voce quer:

- isolar codigo por app
- manter um `.env` por app fora do git
- reiniciar uma app sem afetar as outras
- manter timers e logs separados
- encaixar proxy reverso por dominio ou subdominio

## Layout recomendado desta aplicacao

Para este repositorio, use:

```text
/home/david/apps/super_melhor_preco_farmacia
/etc/super-melhor-preco-farmacia/app.env
```

O `pricing-core` roda a partir de:

```text
/home/david/apps/super_melhor_preco_farmacia/services/pricing-core
```

## Bootstrap da VPS

Pacotes base no Ubuntu:

```bash
sudo apt update
sudo apt install -y git curl caddy python3 python3-venv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Clone do codigo:

```bash
mkdir -p /home/david/apps
cd /home/david/apps
git clone <URL_DO_REPO> super_melhor_preco_farmacia
cd /home/david/apps/super_melhor_preco_farmacia/services/pricing-core
~/.local/bin/uv sync --frozen
```

Arquivo de ambiente:

```bash
sudo mkdir -p /etc/super-melhor-preco-farmacia
sudo cp /home/david/apps/super_melhor_preco_farmacia/deploy/env/app.env.example /etc/super-melhor-preco-farmacia/app.env
sudo nano /etc/super-melhor-preco-farmacia/app.env
sudo chown root:david /etc/super-melhor-preco-farmacia/app.env
sudo chmod 640 /etc/super-melhor-preco-farmacia/app.env
```

## Variaveis obrigatorias

Preencha no minimo:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `INTERNAL_API_AUTH_ENABLED=true`
- `INTERNAL_API_TOKEN`

Observacoes:

- o runtime da aplicacao e `cep-aware`; nao dependa de `CEP` global para producao
- `CEP` pode permanecer vazio
- a retencao operacional deve continuar em `90` dias, salvo excecao explicitamente documentada

## Systemd

Arquivos prontos:

- `deploy/systemd/super-melhor-preco-farmacia-api.service`
- `deploy/systemd/super-melhor-preco-farmacia-worker.service`
- `deploy/systemd/super-melhor-preco-farmacia-scheduler.service`
- `deploy/systemd/super-melhor-preco-farmacia-scheduler.timer`

Instalacao:

```bash
sudo cp /home/david/apps/super_melhor_preco_farmacia/deploy/systemd/super-melhor-preco-farmacia-*.service /etc/systemd/system/
sudo cp /home/david/apps/super_melhor_preco_farmacia/deploy/systemd/super-melhor-preco-farmacia-scheduler.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now super-melhor-preco-farmacia-api.service
sudo systemctl enable --now super-melhor-preco-farmacia-worker.service
sudo systemctl enable --now super-melhor-preco-farmacia-scheduler.timer
```

Validacao:

```bash
systemctl status super-melhor-preco-farmacia-api.service
systemctl status super-melhor-preco-farmacia-worker.service
systemctl status super-melhor-preco-farmacia-scheduler.timer
journalctl -u super-melhor-preco-farmacia-api.service -f
```

## Caddy

Arquivo base:

- `deploy/caddy/super-melhor-preco-farmacia-api.Caddyfile`

Instalacao:

```bash
sudo mkdir -p /etc/caddy/sites
sudo cp /home/david/apps/super_melhor_preco_farmacia/deploy/caddy/super-melhor-preco-farmacia-api.Caddyfile /etc/caddy/sites/
sudo grep -q "import /etc/caddy/sites/\*" /etc/caddy/Caddyfile || echo "import /etc/caddy/sites/*" | sudo tee -a /etc/caddy/Caddyfile
sudo nano /etc/caddy/sites/super-melhor-preco-farmacia-api.Caddyfile
sudo caddy fmt --overwrite /etc/caddy/sites/super-melhor-preco-farmacia-api.Caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

O vhost de exemplo faz proxy para `127.0.0.1:8001`.

## Healthchecks

Depois da subida:

```bash
curl http://127.0.0.1:8001/health/live
curl -H "Authorization: Bearer <SEU_TOKEN_INTERNO>" http://127.0.0.1:8001/health/ready
```

Se o dominio ja estiver apontado e o `Caddy` estiver ativo:

```bash
curl https://farmacia-api.seu-dominio.com/health/live
curl -H "Authorization: Bearer <SEU_TOKEN_INTERNO>" https://farmacia-api.seu-dominio.com/health/ready
```

## Atualizacao de deploy

Rotina segura de atualizacao:

```bash
cd /home/david/apps/super_melhor_preco_farmacia
git pull
cd services/pricing-core
~/.local/bin/uv sync --frozen
sudo systemctl restart super-melhor-preco-farmacia-api.service
sudo systemctl restart super-melhor-preco-farmacia-worker.service
```

## Sobre o `docker compose` da fase 1

O material em `infra/` continua util para ambiente local, testes de stack ou subida containerizada.
Para a sua VPS com varias aplicacoes, o padrao principal recomendado neste repositorio passa a ser:

- codigo em `/home/david/apps/super_melhor_preco_farmacia`
- segredos em `/etc/super-melhor-preco-farmacia/app.env`
- `systemd` para processos e timer
- `Caddy` para HTTP/HTTPS
