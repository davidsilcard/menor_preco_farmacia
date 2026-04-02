# Infra

Infraestrutura inicial da fase 1 do monorepo.

Arquivos atuais:

- `docker-compose.phase1.yml`: `postgres`, `pricing-core-api`, `pricing-core-worker` e `pricing-core-scheduler`
- `.env.phase1.example`: base de configuracao para a VPS

Escopo deste diretório:

- stack containerizada da fase 1
- ambiente local ou VPS baseada em Docker

Se a sua VPS hospeda varias aplicacoes, o padrao recomendado deste repositorio agora esta em `deploy/README.md`, com:

- codigo em `/home/david/apps/super_melhor_preco_farmacia`
- segredos em `/etc/super-melhor-preco-farmacia/app.env`
- `systemd` para API, worker e scheduler
- `Caddy` para dominio e TLS

## Subida completa na VPS

Premissas:

- Ubuntu na VPS
- Docker e plugin `docker compose`
- repositorio clonado em `/opt/super_melhor_preco_farmacia`

Sequencia de comandos:

```bash
sudo apt update
sudo apt install -y git docker.io docker-compose-plugin curl cron
sudo systemctl enable --now docker
sudo systemctl enable --now cron

cd /opt
sudo git clone <URL_DO_REPO> super_melhor_preco_farmacia
sudo chown -R $USER:$USER /opt/super_melhor_preco_farmacia

cd /opt/super_melhor_preco_farmacia
cp infra/.env.phase1.example .env
nano .env

docker compose -f infra/docker-compose.phase1.yml build
docker compose -f infra/docker-compose.phase1.yml up -d postgres pricing-core-api pricing-core-worker
docker compose -f infra/docker-compose.phase1.yml ps
```

No `.env`, troque pelo menos:

- `POSTGRES_PASSWORD`
- `INTERNAL_API_TOKEN`

Validacao basica:

```bash
curl http://127.0.0.1:8001/health/live
curl -H "Authorization: Bearer <SEU_TOKEN_INTERNO>" http://127.0.0.1:8001/health/ready
```

Teste manual do scheduler:

```bash
cd /opt/super_melhor_preco_farmacia
docker compose -f infra/docker-compose.phase1.yml run --rm pricing-core-scheduler
```

## Scheduler

- o servico `pricing-core-scheduler` e `one-shot`
- ele deve ser disparado pelo agendador do host nos horarios corretos
- isso evita repetir coletas varias vezes dentro da mesma janela operacional

Exemplo de `cron` no host Linux:

```cron
0 8 * * * cd /opt/super_melhor_preco_farmacia && docker compose -f infra/docker-compose.phase1.yml run --rm pricing-core-scheduler
0 15 * * * cd /opt/super_melhor_preco_farmacia && docker compose -f infra/docker-compose.phase1.yml run --rm pricing-core-scheduler
```

Comandos uteis:

```bash
docker compose -f infra/docker-compose.phase1.yml logs -f pricing-core-api
docker compose -f infra/docker-compose.phase1.yml logs -f pricing-core-worker
docker compose -f infra/docker-compose.phase1.yml down
docker compose -f infra/docker-compose.phase1.yml up -d
```

Proximas rodadas:

- proxy reverso
- servicos do `assistant-service`
- frontend
