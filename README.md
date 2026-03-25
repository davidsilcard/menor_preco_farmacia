# Super Melhor Preco Farmacia

Este projeto nao e um backend tradicional para interface final. Ele existe para servir dados estruturados de comparacao de medicamentos para uma LLM usar como ferramenta.

O fluxo alvo e:

1. o cliente usa uma aplicacao
2. a aplicacao envia para uma LLM uma nota fiscal, uma lista de produtos ou uma pergunta livre
3. a LLM interpreta a intencao do usuario
4. a LLM consulta esta API como ferramenta
5. a LLM responde com comparacao de precos, alternativas equivalentes e economia possivel

Em outras palavras: esta API e a camada de dados e comparacao. A experiencia conversacional fica fora dela.

## Objetivo

Permitir que uma LLM responda perguntas como:

- "quanto eu paguei mais caro nestes itens?"
- "onde encontro este mesmo medicamento mais barato em Jaragua do Sul?"
- "essa nota fiscal tem algum item que poderia ser comprado em outra farmacia por menos?"
- "qual farmacia tem o menor preco para este remedio?"
- "estes dois nomes diferentes sao o mesmo produto?"

## Papel da API no ecossistema

Esta API deve ser tratada como um backend orientado a ferramentas para agentes.

Preco de farmacia depende de regiao. Por isso, o CEP deve sempre acompanhar a consulta da LLM/aplicacao.

Ela nao deve:

- decidir a resposta final em linguagem natural
- interpretar PDF ou imagem de nota fiscal
- fazer OCR
- conduzir a conversa com o usuario

Ela deve:

- armazenar catalogos por farmacia
- manter snapshots de preco por CEP
- normalizar produtos
- associar produtos equivalentes entre farmacias
- expor consultas objetivas para comparacao

## MCP: atendimento vs operacao

O MCP deste projeto deve priorizar ferramentas de atendimento.

Por padrao, a LLM de atendimento deve enxergar apenas tools orientadas a responder o cliente, como:

- `search_products`
- `compare_shopping_list`
- `compare_basket`
- `compare_invoice_items`
- `compare_receipt`
- `search_observed_item`
- `compare_canonical_product`
- `get_search_job`

Tools administrativas nao devem ser expostas por default no MCP.

Exemplos de tools administrativas:

- `list_review_matches`
- `list_search_jobs`

Essas tools so devem ser expostas quando houver necessidade operacional explicita e a configuracao `MCP_EXPOSE_ADMIN_TOOLS=true`.

Regra pratica:

- MCP de atendimento: tools minimas, seguras e orientadas a resposta final da LLM
- API HTTP operacional: endpoints de monitoramento, fila, health e revisao
- MCP administrativo: opcional, controlado por flag

## Semantica de resolucao para a LLM

As respostas de atendimento agora expõem `resolution_source` para evitar inferencia ambigua pela LLM.

Valores esperados:

- `canonical_match`: o resultado veio do catalogo canonico diretamente
- `source_product_fallback`: o resultado veio de `SourceProduct` ja raspado e reconciliado
- `queued_enrichment`: ainda nao houve resultado util; a demanda foi registrada e entrou em fila
- `searched_no_results`: a busca sob demanda terminou sem resultado util

Regra de uso:

- `canonical_match` e `source_product_fallback` podem ser tratados como resposta util imediata
- `queued_enrichment` nao e resposta final; a LLM deve informar que a busca foi enfileirada
- `searched_no_results` deve ser tratado como ausencia real de resultado apos processamento, nao como fila pendente

## Arquitetura

O modelo de dados foi desenhado para evitar comparacao por texto puro.

### Entidades principais

- `pharmacies`: cadastro das farmacias monitoradas
- `source_products`: produto como ele existe na farmacia de origem
- `canonical_products`: representacao canonica para comparacao entre farmacias
- `product_matches`: vinculo entre `source_product` e `canonical_product`, com confianca e status de revisao
- `price_snapshots`: historico de preco, disponibilidade, CEP e origem da coleta

### Principio central

Nunca assumir que dois produtos sao iguais apenas porque o nome parece parecido.

A prioridade de matching e:

1. `EAN/GTIN`
2. `ANVISA` / `Registro MS`
3. nome normalizado com atributos estruturados
4. revisao manual quando o match for fraco

## Como a LLM deve usar a API

A LLM deve converter o pedido do usuario em consultas estruturadas.

O contrato de payload por tipo de entrada esta em:

- `contrato-entradas-multimodais.md`

### Exemplo 1: nota fiscal

Entrada do usuario:

- foto de nota fiscal
- PDF de cupom
- texto copiado da nota

Fluxo esperado:

1. OCR ou parser da aplicacao extrai nomes, quantidades e valores
2. a LLM identifica quais itens parecem medicamentos ou produtos comparaveis
3. a LLM consulta esta API para localizar produtos canonicos equivalentes
4. a LLM compara com os ultimos precos disponiveis por farmacia
5. a LLM responde com economia potencial

### Exemplo 2: lista de compras

Entrada do usuario:

- "dipirona 1g 10 comprimidos, ibuprofeno 600mg, vitamina c"

Fluxo esperado:

1. a LLM quebra a lista em itens
2. consulta os produtos e ofertas por produto canonico
3. consolida menor preco por farmacia
4. devolve a recomendacao

### Exemplo 3: pergunta livre

Entrada do usuario:

- "a novalgina infantil esta mais barata na Panvel ou na Drogasil?"

Fluxo esperado:

1. a LLM encontra o produto canonico relevante
2. consulta as ofertas ativas
3. responde com farmacia, preco, confianca do match e observacoes

## Estrutura do projeto

- `src/api/`: rotas HTTP
- `src/core/`: configuracoes globais
- `src/models/`: modelos SQLAlchemy
- `src/services/`: regras de matching e logica de dominio
- `src/scrapers/`: scrapers por farmacia
- `src/main.py`: API FastAPI
- `src/init_db.py`: reset e criacao do banco
- `src/update_reference_data.py`: carga e atualizacao de DCB, regulatorio e CMED
- `data/reference/`: arquivos locais reprocessaveis de referencia

## Farmacias atuais

- Panvel
- FarmaSesi
- Sao Joao
- Farmacia Jaragua
- Drogasil
- Droga Raia
- Drogaria Sao Paulo
- Drogaria Catarinense
- Preco Popular

Todas sao coletadas para o CEP de Jaragua do Sul configurado em `.env`.

## Campos que importam para cruzamento

O sistema tenta capturar, sempre que possivel:

- `source_sku`
- `source_url`
- `raw_name`
- `normalized_name`
- `brand`
- `manufacturer`
- `active_ingredient`
- `dosage`
- `presentation`
- `pack_size`
- `ean_gtin`
- `anvisa_code`
- `price`
- `availability`
- `promotion_text`
- `source_metadata`

Sem esses campos, a comparacao vira heuristica fraca. O foco do projeto e justamente evitar isso.

## Dados regulatorios e CMED

O projeto agora tem uma camada separada de referencia para:

- `DCB`
- registros regulatorios
- precos de referencia da `CMED`

Esses dados nao substituem os scrapers de preco por farmacia. Eles servem para:

- melhorar matching
- reduzir canonicals fracos
- validar precos raspados
- permitir recriar o banco do zero e reimportar referencias oficiais

Tabelas novas:

- `regulatory_products`
- `regulatory_aliases`
- `cmed_price_entries`

Regra operacional:

- o schema pode ser recriado do zero com `src.init_db`
- as referencias podem ser reimportadas quantas vezes forem necessarias
- a atualizacao deve ser tratada como carga idempotente, nao como edicao manual em banco

### Atualizando referencias

Coloque os arquivos em `data/reference/` ou no diretorio configurado por `REFERENCE_DATA_DIR`.

Comandos:

```bash
uv run python -m src.init_db
uv run python -m src.update_reference_data
```

Para substituir integralmente as referencias ja carregadas:

```bash
uv run python -m src.update_reference_data --replace
```

Para pular um dataset:

```bash
uv run python -m src.update_reference_data --skip-cmed
```

Contrato dos arquivos:

- `data/reference/README.md`

## Logs e modo de teste

Para reduzir ruido durante testes locais:

- `UVICORN_ACCESS_LOG=false`: oculta os logs de access do Uvicorn
- `LOG_OPERATION_JOB_REUSED=false`: evita repetir eventos de reuse de fila em nivel `INFO`

Exemplo de uso no `.env`:

```env
UVICORN_ACCESS_LOG=false
LOG_OPERATION_JOB_REUSED=false
```

Subida local:

```bash
uv run python -m src.main
```

### Efeito pratico da camada regulatoria

`DCB`, registros regulatorios e `CMED` nao ficam isolados em backoffice.
Eles ja influenciam o comportamento real da API:

- expandem buscas por aliases regulatorios
- ancoram matching de `SourceProduct` antes de criar canonical novo
- validam preco raspado contra referencia `CMED`
- reduzem ida desnecessaria para fila quando ja existe `SourceProduct` util no banco

## Endpoints atuais

### Status e catalogo

- `GET /`
- `GET /products`
- `GET /prices/{source_product_id}`
- `GET /canonical-products`

### Qualidade de matching

- `GET /matching/review`

Retorna produtos cujo match ainda precisa de revisao.

### Comparacao entre farmacias

- `GET /comparison/canonical-products`
- `GET /comparison/canonical/{canonical_product_id}`

Esses endpoints sao os mais importantes para a LLM quando a pergunta do usuario e comparativa.

### Tool endpoints para LLM

- `GET /tool/search-products?query=...`
- `GET /tool/search-products?query=...&cep=...`
- `POST /tool/compare-shopping-list`
- `POST /tool/compare-invoice-items`
- `POST /tool/compare-receipt`
- `POST /tool/search-observed-item`

### Operacao e coleta

- `GET /ops/schedule`
- `GET /ops/collection-plan`
- `POST /ops/collections/run`
- `POST /ops/cycle/run`
- `GET /ops/health`
- `GET /ops/metrics`
- `GET /ops/scrape-runs`

Esses endpoints ja sao pensados para uso por agente, sem depender de uma interface humana.
Eles tambem aceitam entradas mais sujas, como abreviacoes, nomes parciais e codigos `EAN/GTIN`.

Todos eles retornam o mesmo envelope:

```json
{
  "tool_name": "compare_invoice_items",
  "input": {},
  "confidence": 0.3,
  "warnings": [],
  "result": {}
}
```

Campos importantes dentro de `result`:

- `offers`: ofertas por farmacia com `availability`
- `availability_summary`: resumo estruturado de disponibilidade para a LLM

Estados atuais de disponibilidade por item:

- `has_available_offers`
- `only_unknown_offers`
- `only_out_of_stock_offers`
- `no_offers`

Exemplos de consultas que devem funcionar melhor agora:

- `GET /tool/search-products?query=novalg inf 100ml&cep=89254300`
- `GET /tool/search-products?query=dip sod 1g 10 cpr medley&cep=89254300`
- `GET /tool/search-products?query=7891058464073&cep=89254300`

### Busca por foto da caixa

Se a aplicacao fizer OCR da caixa do remedio, pode enviar os textos observados para:

```json
POST /tool/search-observed-item
{
  "cep": "89254300",
  "source_type": "box_photo",
  "observations": [
    "Novalgina infantil dipirona 100ml",
    "solucao oral",
    "seringa dosadora",
    "lote 12345",
    "validade 12/2027"
  ]
}
```

O endpoint tenta ignorar ruido comum de embalagem, como lote e validade.

## Escopo por CEP

Preco, disponibilidade, fila e tracking devem sempre respeitar `cep`.

Regras atuais:

- consultas de tool exigem `cep`
- comparacoes de oferta exigem `cep`
- snapshots e melhores ofertas sao filtrados por `cep`
- jobs, tracking e rotina agendada sao controlados por `cep`
- retencao operacional e de precos e de `90 dias`

Em endpoints/listagens administrativas, o comportamento esperado e:

- sem `cep`: visao global de backoffice, quando fizer sentido operacional
- com `cep`: visao estritamente filtrada para aquele contexto

Para MCP de atendimento, o uso esperado e sempre com `cep`.

## Exemplo de resposta util para LLM

Um endpoint de comparacao retorna algo neste estilo:

```json
{
  "canonical_product_id": 2,
  "canonical_name": "NOVALGINA INFANTIL DIPIRONA PARA FEBRE E DOR SERINGA SOLUCAO ORAL 100ML",
  "ean_gtin": "7891058464073",
  "availability_summary": {
    "state": "has_available_offers",
    "offer_counts": {
      "available": 2,
      "unknown": 0,
      "out_of_stock": 0
    },
    "best_offer_availability": "available"
  },
  "offers": [
    {
      "pharmacy": "Drogasil",
      "price": 39.99,
      "availability": "available",
      "match_type": "ean_gtin",
      "match_confidence": 1.0,
      "review_status": "auto_approved"
    },
    {
      "pharmacy": "Panvel",
      "price": 48.44,
      "availability": "unknown",
      "match_type": "new_canonical",
      "match_confidence": 0.0,
      "review_status": "new"
    }
  ]
}
```

A LLM pode usar isso para responder:

- qual farmacia esta mais barata
- qual a diferenca de preco
- se o match e confiavel ou ainda precisa cuidado
- se ha estoque confirmado, incerto ou indisponivel

## Regras de disponibilidade para a LLM

Ao interpretar `availability` e `availability_summary`:

- `available`: oferta com estoque confirmado
- `unknown`: oferta encontrada, mas sem confirmacao forte de estoque
- `out_of_stock`: oferta encontrada sem estoque

Regras operacionais atuais:

- `out_of_stock` nao entra como melhor oferta
- `out_of_stock` nao entra no total da cesta
- `unknown` pode aparecer como fallback, mas perde prioridade para `available`
- `warnings` e `availability_summary` devem ser usados juntos na resposta final da LLM

## Instalacao

```bash
uv sync
uv run playwright install firefox
```

## Configuracao

Configure `.env` com:

- `CEP=89254300`
- `SCHEDULED_COLLECTION_SLOTS=08:00,15:00`
- `SCHEDULED_COLLECTION_SLOT_WINDOW_MINUTES=120`
- `PRICE_RETENTION_DAYS=90`
- `REFERENCE_DATA_DIR=data/reference`
- `SCHEDULED_COLLECTION_MAX_ITEMS_PER_CEP=50`
- `SCHEDULED_COLLECTION_ENABLE_BROWSER_SCRAPERS=false`
- `ON_DEMAND_ENABLE_BROWSER_SCRAPERS=false`

## Operacao recomendada

O projeto agora trabalha com um ciclo operacional unico:

1. verifica se esta dentro da janela de coleta das `08:00` ou `15:00`
2. executa a coleta dos itens monitorados por `CEP` quando estiver na janela
3. aplica retencao dos `price_snapshots` com mais de `90` dias
4. devolve relatorio consolidado

### Comandos uteis

Verificar janela atual:

```bash
uv run python -m src.run_operational_cycle --schedule-only
```

Executar o ciclo normal:

```bash
uv run python -m src.run_operational_cycle
```

Forcar coleta fora da janela:

```bash
uv run python -m src.run_operational_cycle --force-collection
```

### Endpoints operacionais

- `GET /ops/schedule`: informa slot atual, proximo slot e janela
- `POST /ops/cycle/run`: executa coleta + retencao
- `GET /ops/collection-plan`: mostra os itens que entrariam no lote
- `GET /ops/metrics`: mostra saude operacional e distribuicao do sistema

### Agendamento no Windows Task Scheduler

Criar duas tarefas, uma para `08:00` e outra para `15:00`, apontando para:

Programa:

```powershell
C:\Users\davidsc\projetos-python\super_melhor_preco_farmacia\.venv\Scripts\python.exe
```

Argumentos:

```powershell
-m src.run_operational_cycle
```

Iniciar em:

```powershell
C:\Users\davidsc\projetos-python\super_melhor_preco_farmacia
```

Se quiser uma execucao manual de suporte fora da janela, usar:

```powershell
-m src.run_operational_cycle --force-collection
```

### Politica operacional atual

- a coleta e orientada por demanda real de `CEP + item monitorado`
- itens sem demanda recente entram em `cooldown` e depois `inactive`
- o sistema nao varre catalogo completo das farmacias
- a retencao padrao de preco e `90 dias`
- a LLM deve sempre informar ao usuario o horario da coleta mais recente quando responder sobre preco

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `CEP`
- `PORT`
- `PANVEL_SEARCH_TERMS`
- `FARMASESI_SEARCH_TERMS`
- `SAO_JOAO_SEARCH_TERMS`
- `FARMACIA_JARAGUA_SEARCH_TERMS`
- `DROGASIL_SEARCH_TERMS`
- `CATARINENSE_SEARCH_TERMS`
- `PRECO_POPULAR_SEARCH_TERMS`
- `DROGA_RAIA_SEARCH_TERMS`
- `DROGARIA_SAO_PAULO_SEARCH_TERMS`

Exemplo:

```env
POSTGRES_DB=precos-farmacia
POSTGRES_USER=admin
POSTGRES_PASSWORD=senha
DB_HOST=127.0.0.1
DB_PORT=5432
CEP=89254300
PORT=8001
PANVEL_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
FARMASESI_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
SAO_JOAO_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
FARMACIA_JARAGUA_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
DROGASIL_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
CATARINENSE_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
PRECO_POPULAR_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
DROGA_RAIA_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
DROGARIA_SAO_PAULO_SEARCH_TERMS=dipirona,paracetamol,ibuprofeno
ON_DEMAND_ENABLE_BROWSER_SCRAPERS=false
```

## Reset e carga inicial

Nesta fase inicial de arquitetura, o banco e recriado do zero.

```bash
uv run python -m src.init_db
```

Isso remove o schema `public`, recria as tabelas e cadastra as farmacias iniciais.

Importante:

- o `CEP` do `.env` define o contexto em que os scrapers coletam os precos
- a LLM/aplicacao deve informar o mesmo CEP em todas as consultas
- se o cliente pedir outro CEP, os scrapers precisam ser executados novamente para esse CEP

## Coleta

### Direcao operacional

A coleta nao deve varrer o catalogo inteiro das farmacias.

A direcao atual do projeto e:

- monitorar demanda real por `CEP`
- rastrear os itens mais pedidos por `CEP`
- coletar nos horarios fixos apenas os itens relevantes daquele `CEP`
- deixar itens sem demanda recente sairem da rotina recorrente

Para isso, a base agora passa a usar o conceito de `tracked_items_by_cep`.

Cada item monitorado por CEP guarda:

- consulta normalizada
- `canonical_product_id` quando ja houver match confiavel
- `request_count_total`
- `last_requested_at`
- `scrape_priority`
- `status`: `active`, `cooldown` ou `inactive`

Ciclo de vida atual:

- `active`: houve demanda recente; entra na coleta recorrente
- `cooldown`: perdeu recencia, mas ainda pode voltar
- `inactive`: ficou fora da janela operacional e deve sair da coleta recorrente

### Panvel

```bash
uv run python -m src.scrapers.panvel
```

### FarmaSesi

```bash
uv run python -m src.scrapers.farmasesi
```

### Sao Joao

```bash
uv run python -m src.scrapers.sao_joao
```

### Farmacia Jaragua

```bash
uv run python -m src.scrapers.farmacia_jaragua
```

### Drogasil

```bash
uv run python -m src.scrapers.drogasil
```

### Droga Raia

```bash
uv run python -m src.scrapers.droga_raia
```

### Drogaria Sao Paulo

```bash
uv run python -m src.scrapers.drogaria_sao_paulo
```

### Drogaria Catarinense

```bash
uv run python -m src.scrapers.drogaria_catarinense
```

### Preco Popular

```bash
uv run python -m src.scrapers.preco_popular
```

## API

```bash
uv run python -m src.main
```

Por padrao:

- `http://127.0.0.1:8000`

Ou a porta definida por `PORT`.

## Testes

Os testes atuais focam nas funcoes de busca e nos envelopes de tool use:

```bash
uv run python -m unittest discover -s tests
```

## Como integrar isso como MCP

Este repositorio agora tem duas formas de integracao:

1. API HTTP em `src.main`
2. servidor MCP por `stdio` em `src.mcp_server`

O MCP pode ser usado diretamente por clientes que falam o protocolo. A API HTTP continua util para integracoes customizadas.

### Subindo o servidor MCP

```bash
uv run python -m src.mcp_server
```

### Tools expostas pelo MCP

- `search_products`
- `compare_shopping_list`
- `compare_basket`
- `compare_invoice_items`
- `compare_receipt`
- `search_observed_item`
- `compare_canonical_product`
- `list_review_matches`
- `get_search_job`
- `list_search_jobs`

Observacoes:

- o `cep` e obrigatorio nas tools de busca e comparacao de preco
- quando a base nao tiver o item, a resposta pode incluir `catalog_request` e `search_job`
- `search_job` permite polling posterior de fila, posicao e ETA estimado
- `search_job.status` pode ser `queued`, `processing`, `completed`, `partial_success` ou `failed`
- `partial_success` significa que a busca terminou, mas uma ou mais farmacias falharam durante a execucao
- `search_job.warnings` traz avisos estruturados para a LLM, como falha parcial por farmacia ou ausencia total de resultados
- scrapers baseadas em browser podem ser puladas na busca sob demanda quando `ON_DEMAND_ENABLE_BROWSER_SCRAPERS=false`
- a API tambem pode devolver `tracked_item` ou `tracked_items`, indicando que aquele item entrou na fila de monitoramento recorrente do `CEP`

### Exemplo de uso em cliente MCP

O cliente deve configurar este comando de `stdio`:

```bash
uv run python -m src.mcp_server
```

### Alternativa HTTP

Se preferir nao usar MCP nativo, a API HTTP ainda pode ser exposta para uma LLM de tres formas:

1. um MCP server externo chama esta API e expoe tools
2. a aplicacao da LLM chama esta API diretamente como tool HTTP
3. um orquestrador converte intents da LLM em chamadas REST

### Ferramentas MCP recomendadas

Se for embrulhar isso como MCP, as tools mais naturais sao:

- `search_products`
- `get_product_history`
- `list_canonical_products`
- `compare_canonical_product`
- `compare_shopping_list`
- `compare_basket`
- `compare_invoice_items`
- `compare_receipt`
- `search_observed_item`
- `list_review_matches`
- `get_search_job`
- `list_search_jobs`

### Exemplo de tool contract

- `compare_canonical_product`
  - input: `canonical_product_id`
  - output: ofertas atuais por farmacia, menor preco, confianca do matching

- `search_products`
  - input: `query`, `cep`
  - output: produtos canonicos, ofertas, `catalog_request` e `search_job` quando nao houver base suficiente

- `compare_shopping_list`
  - input: `cep` + lista de itens em texto
  - output: melhor oferta por item, comparacao entre farmacias e jobs para itens ausentes

- `compare_invoice_items`
  - input: `cep` + itens com descricao e preco pago
  - output: melhor oferta atual, economia potencial e jobs para itens ausentes

- `get_search_job`
  - input: `job_id`
  - output: status da fila, posicao, ETA, warnings estruturados e eventual payload final

### Semantica de search jobs

- `queued`: job aceito e aguardando processamento
- `processing`: job em execucao
- `completed`: job concluido sem falha de scraper
- `partial_success`: job concluido com falha parcial de uma ou mais farmacias
- `failed`: job falhou antes de produzir um resultado utilizavel
- `skipped` aparece no resultado por farmacia quando a origem depende de browser e esse runtime nao esta habilitado para busca sob demanda

Warnings estruturados comuns:

- `partial_scraper_failure`: uma ou mais farmacias falharam no processamento
- `scraper_runtime_unavailable`: parte das farmacias foi pulada por falta de runtime de browser
- `no_results_found`: a busca foi executada, mas nao encontrou produtos

## Limites atuais

- OCR de nota fiscal ainda nao faz parte deste repositorio
- parser de lista do usuario ainda nao faz parte deste repositorio
- matching por nome ainda precisa revisao em alguns casos
- cobertura de farmacias ainda esta no inicio
- o CEP da Drogasil esta em modo best effort; o ideal e endurecer esse fluxo conforme o site evoluir

## Proximos passos recomendados

1. adicionar endpoint de busca semantica/estruturada por item de lista
2. criar endpoint para comparar uma lista inteira de produtos
3. adicionar revisao manual assistida para matches fracos
4. ampliar cobertura para novas farmacias
5. criar um adaptador MCP dedicado sobre a API
