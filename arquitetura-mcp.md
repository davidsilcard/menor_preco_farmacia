# Arquitetura MCP

## Tese do projeto

O usuario final nao conversa com esta API.

O usuario final conversa com uma aplicacao que usa uma LLM.

A LLM usa esta API como ferramenta para responder perguntas sobre economia em farmacias, equivalencia de produtos e comparacao de precos.

## Perfis de MCP

Este projeto passa a ter uma separacao explicita entre MCP de atendimento e operacao interna.

### MCP de atendimento

Deve expor apenas tools necessarias para responder o cliente final.

Exemplos:

- `search_products`
- `compare_shopping_list`
- `compare_basket`
- `compare_invoice_items`
- `compare_receipt`
- `search_observed_item`
- `compare_canonical_product`
- `get_search_job`

### MCP administrativo

Nao deve ser o default.
So pode ser exposto quando houver necessidade operacional explicita.

Exemplos:

- `list_review_matches`
- `list_search_jobs`

Essa exposicao deve ficar atras de configuracao.
No codigo atual, a flag e `MCP_EXPOSE_ADMIN_TOOLS`.

## Camadas

### Camada 1: interface do cliente

Responsavel por:

- upload de nota fiscal
- envio de lista de compras
- envio de foto da caixa do remedio
- perguntas livres

### Camada 2: LLM

Responsavel por:

- interpretar a intencao do usuario
- extrair entidades da nota, lista ou pergunta
- consolidar OCR de caixa ou cupom em payload estruturado
- decidir quando consultar esta API
- montar a resposta final em linguagem natural

### Camada 3: API deste projeto

Responsavel por:

- catalogo por farmacia
- normalizacao de produtos
- matching entre farmacias
- comparacao de precos
- recuperacao de historico

## Exemplos de intents da LLM

- "encontre o produto canonico mais provavel para este item"
- "compare o menor preco entre farmacias para este produto"
- "liste itens cujo match ainda e fraco"
- "mostre o historico de preco do item comprado"
- "encontre este remedio a partir do texto observado na caixa"
- "compare a nota inteira e estime economia total"

## Regra importante

Esta API deve devolver fatos estruturados.

A LLM deve produzir interpretacao.

Se a API tentar responder em linguagem natural, ela mistura responsabilidade errada com a camada de agente.

## Modelo mental correto

- scraping: coleta dados da origem
- OCR/aplicacao: extrai texto de nota ou caixa
- matching: cria confianca sobre equivalencia
- api: expoe fatos estruturados
- llm: usa fatos estruturados para responder ao usuario

## Jobs assíncronos

Quando a base nao tiver o item, a API/MCP pode devolver um `search_job`.

Estados esperados:

- `queued`
- `processing`
- `completed`
- `partial_success`
- `failed`

Interpretacao correta:

- `completed`: a busca terminou sem falhas de scraper
- `partial_success`: a busca terminou, mas parte das farmacias falhou; a LLM deve responder com cautela
- `failed`: a busca nao produziu resultado confiavel

No detalhe por farmacia, uma origem tambem pode aparecer como `skipped` quando depende de browser e esse runtime nao esta habilitado para a busca sob demanda.

O agente nao deve tratar `partial_success` como erro fatal, mas tambem nao deve responder como se a cobertura estivesse completa.

### Origem de resolucao

As tools de atendimento e os payloads de `search_job` passam a expor `resolution_source`.

Semantica:

- `canonical_match`: o item foi resolvido pelo catalogo canonico
- `source_product_fallback`: o item foi resolvido reaproveitando `SourceProduct` ja coletado
- `queued_enrichment`: a tool nao achou resultado util imediato e abriu fila
- `searched_no_results`: a fila rodou e terminou sem resultado util

Isso permite que a LLM diferencie:

- resposta util imediata
- fallback operacional pendente
- ausencia real de resultado apos processamento

## Coleta orientada por demanda

O objetivo nao e varrer o catalogo inteiro das farmacias.

A coleta deve ser guiada por:

- `CEP`
- demanda observada
- produto canonico quando houver match confiavel

Por isso a arquitetura passa a usar um item monitorado por `CEP`, com:

- consulta normalizada
- `canonical_product_id` opcional
- contagem de demanda
- recencia
- prioridade de scraping
- ciclo de vida `active/cooldown/inactive`

Isso permite que a rotina das `08:00` e `15:00` busque apenas os itens relevantes para cada `CEP`.

## Regra de isolamento por CEP

Toda a arquitetura operacional deve assumir `cep` como chave de contexto.

Isso significa:

- leitura de oferta filtrada por `cep`
- snapshot filtrado por `cep`
- fila filtrada por `cep`
- tracking filtrado por `cep`
- health operacional filtravel por `cep`
- retencao aplicada aos dados operacionais por janela maxima de `90 dias`

Uma LLM nao pode receber como verdade operacional dados de outro `cep`.
Se o contexto atual e `89254300`, nenhuma leitura de preco, fila ou item rastreado de outro `cep` deve contaminar a resposta.

## Ciclo operacional

O projeto agora tem um ciclo operacional explicito:

1. identificar se a execucao caiu dentro da janela configurada de coleta
2. montar o lote a partir de `tracked_items_by_cep`
3. rodar a coleta do slot
4. aplicar retencao de `price_snapshots`
5. expor relatorio consolidado para operacao

Configuracao atual recomendada:

- slots: `08:00` e `15:00`
- janela operacional por slot: `120 minutos`
- retencao de precos: `90 dias`

Endpoints operacionais relevantes:

- `GET /ops/schedule`
- `GET /ops/collection-plan`
- `POST /ops/cycle/run`
- `GET /ops/metrics`
- `GET /ops/health`

Isso deixa claro para a LLM e para a operacao humana que:

- um preco nao e "agora"; ele pertence a um ciclo de coleta
- a resposta deve carregar contexto de frescor
- o sistema so coleta demanda relevante do CEP
- produto ja raspado deve ser reaproveitado antes de abrir nova fila desnecessaria

## Worker embarcado vs producao

O comando `uv run python -m src.main` pode subir um worker embarcado para drenar `operation_jobs` no mesmo processo da API.

Esse modo existe para:

- desenvolvimento local
- homologacao
- operacao simples

Mas isso nao deve ser tratado como topologia final obrigatoria.

Em producao maior, o ideal e:

- desligar `EMBED_OPERATION_WORKER`
- manter a API HTTP separada do processamento assíncrono
- rodar o worker em processo dedicado, servico supervisionado ou orquestracao equivalente

Motivos:

- reduzir acoplamento entre latencia da API e execucao de fila
- evitar concorrencia ambigua quando houver multiplas instancias da API
- permitir escala e observabilidade independentes para atendimento e processamento
