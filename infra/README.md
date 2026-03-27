# Infra

Infraestrutura inicial da fase 1 do monorepo.

Arquivos atuais:

- `docker-compose.phase1.yml`: `postgres`, `pricing-core-api`, `pricing-core-worker` e `pricing-core-scheduler`
- `.env.phase1.example`: base de configuracao para a VPS

Uso basico:

```bash
docker compose -f infra/docker-compose.phase1.yml up -d postgres pricing-core-api pricing-core-worker
```

Scheduler:

- o servico `pricing-core-scheduler` e `one-shot`
- ele deve ser disparado pelo agendador do host nos horarios corretos
- isso evita repetir coletas varias vezes dentro da mesma janela operacional

Exemplo de cron no host Linux:

```cron
0 8 * * * cd /opt/pricing-core && docker compose -f infra/docker-compose.phase1.yml run --rm pricing-core-scheduler
0 15 * * * cd /opt/pricing-core && docker compose -f infra/docker-compose.phase1.yml run --rm pricing-core-scheduler
```

Proximas rodadas:

- proxy reverso
- servicos do `assistant-service`
- frontend
