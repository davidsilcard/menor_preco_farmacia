# Manual do Operador LLM

## Objetivo

Este backend existe para ser usado por uma LLM como ferramenta.
A LLM nao deve inferir demais a partir dos payloads.
Ela deve seguir este contrato operacional.

## Regra zero

Toda consulta de atendimento precisa de `cep`.

Se o usuario ainda nao informou `cep`, a LLM deve pedir antes de chamar:

- `search_products`
- `compare_shopping_list`
- `compare_basket`
- `compare_invoice_items`
- `compare_receipt`
- `search_observed_item`
- `compare_canonical_product`
- `get_search_job`
- `get_coverage`

Excecao:

- `submit_pharmacy_lead` pode ser usado sem `cep`, mas `cep`, `cidade` e `estado` continuam sendo sinais uteis para expansao futura.

## Leitura correta das respostas

Os campos principais ficam em `result`.

Campos importantes:

- `result.resolution_source`
- `result.results`
- `result.match_mode`
- `result.recommended_match_mode`
- `result.next_action`
- `result.next_action_reason`
- `result.groups`
- `result.catalog_request`
- `result.search_job`
- `result.operation_job`
- `result.tracked_item`

Para sugestao de farmacia:

- `result.lead`
- `result.created`
- `result.next_action`

Para cobertura declarada:

- `result.covered`
- `result.regions`
- `result.region_count`

Em respostas de cesta/lista/nota tambem existem:

- `result.items`
- `result.resolution_source_summary`

## Significado de `resolution_source`

- `canonical_match`
  - o item foi resolvido direto pelo catalogo canonico
- `source_product_fallback`
  - o item foi resolvido reaproveitando `SourceProduct` ja raspado
- `queued_enrichment`
  - nao houve resultado util imediato; a demanda foi registrada e entrou em fila
- `searched_no_results`
  - a fila ja processou a busca e terminou sem resultado util

## O que a LLM deve fazer em cada caso

### `canonical_match`

Pode responder imediatamente.

Se houver `offers`, a resposta pode citar farmacia, preco, frescor e disponibilidade.

Se houver `results` mas `offers` vazios, a LLM deve dizer que o produto foi reconhecido, mas ainda nao ha oferta util para aquele `cep`.

### `source_product_fallback`

Pode responder imediatamente.

Esse caso significa que o sistema aproveitou produto ja coletado, mesmo sem match canonico direto inicial.

Isso e resposta util.
Nao deve ser tratado como erro.

### `queued_enrichment`

Nao e resposta final.

A LLM deve:

1. informar que a busca foi enfileirada
2. capturar os IDs retornados
3. fazer polling posterior em `get_search_job`

IDs relevantes:

- `result.catalog_request.catalog_request_id`
- `result.search_job.job_id`
- `result.operation_job.operation_job_id`
- `result.tracked_item.tracked_item_id`

### `searched_no_results`

Nao significa fila pendente.
Significa que a fila rodou e terminou sem encontrar resultado util.

A LLM deve responder como ausencia real de resultado apos processamento.

## Como interpretar `results`

`results` representa candidatos utilmente resolvidos.

Cada item pode ter:

- `canonical_product_id`
- `canonical_name`
- `display_name`
- `presentation_group`
- `score`
- `offers`
- `data_freshness`
- `availability_summary`

`result.groups` ja devolve os resultados agrupados por apresentacao.

Cada grupo pode trazer:

- `group_label`
- `results_count`
- `offers_count`
- `unique_pharmacies`
- `best_offer`
- `items`

## Quando usar `match_mode`

`search_products` aceita:

- `match_mode = broad`
- `match_mode = strict`

Regra operacional:

- `broad`
  - usar quando o usuario pediu apenas o nome do remedio
  - exemplo: `loratadina`, `glifage`, `dipirona`
  - o backend pode trazer variacoes diferentes de dosagem e apresentacao
  - a LLM deve agrupar por `presentation_group`

- `strict`
  - usar quando o usuario pediu dosagem explicita
  - exemplo: `glifage 500mg`, `dipirona 1g`, `clonazepam gotas 20ml`
  - o backend deve manter a dosagem pedida e remover variacoes de dosagem diferentes
  - variacoes como `XR` podem continuar, desde que a dosagem bata

`strict` nao deve ser usado para esconder variacoes quando o usuario pediu apenas o nome base do remedio.

## Como usar `recommended_match_mode`

O backend tambem expõe `result.recommended_match_mode`.

Regra:

- se vier `broad`, a LLM pode manter busca ampla
- se vier `strict` e o usuario pediu dosagem explicita, a LLM deve preferir `strict`

Esse campo existe para reduzir erro de decisao da LLM.

## Como usar `next_action`

O backend expõe:

- `result.next_action`
- `result.next_action_reason`

Valores esperados:

- `respond_now`
  - ja ha resultado suficiente para responder
- `poll_search_job`
  - a busca foi enfileirada e a LLM deve consultar `get_search_job`
- `ask_user_to_refine`
  - faltam detalhes suficientes para resposta util imediata
- `thank_user`
  - a sugestao de farmacia faltante foi registrada com sucesso

Se `results` vier vazio:

- com `queued_enrichment`: o sistema abriu fila
- com `searched_no_results`: a busca ja terminou sem resultado

## Quando usar `submit_pharmacy_lead`

Use quando o usuario indicar que sente falta de uma farmacia da propria regiao.

Entrada recomendada:

- `website_url`
- `cep` se o usuario souber
- `city` e `state` quando disponiveis
- `pharmacy_name` opcional
- `notes` opcional

Regras:

- nao usar isso no lugar da busca de preco
- nao prometer integracao imediata
- tratar como registro de interesse de cobertura futura

## Quando usar `get_coverage`

Use quando a conversa precisar validar cobertura declarada por regiao antes da busca.

Exemplos:

- o usuario informou cidade, mas ainda nao informou `cep`
- a LLM quer confirmar se `Jaragua do Sul`, `Guaramirim` ou `Schroeder` ja estao declaradas na cobertura
- a LLM quer responder com transparencia sobre regiao ativa vs planejada

Regras:

- `get_coverage` nao substitui `search_products`
- para preco e oferta, a chave continua sendo `cep`
- `cidade` e `estado` sao apoio de contexto, nao chave principal de consulta de mercado

## Como interpretar `offers`

`offers` e a evidencia de oferta real por farmacia.

Cada `offer` pode trazer:

- farmacia
- preco
- disponibilidade
- `captured_at`
- `data_freshness`
- validacao `CMED`

Regras:

- `offers` presentes: resposta real de mercado
- `results` sem `offers`: match sem oferta util para aquele `cep`
- `availability=unknown`: responder com cautela

## Quando fazer polling

Fazer polling apenas quando houver `result.search_job`.

Tool de polling:

- `get_search_job`

Campos relevantes no polling:

- `status`
- `resolution_source`
- `warnings`
- `result_payload`

Estados esperados:

- `queued`
- `processing`
- `completed`
- `partial_success`
- `failed`

## Como ler `ops/metrics`

Campos importantes:

- `requested_cep`
- `active_cep`

Interpretacao correta:

- `requested_cep`: escopo real da consulta
- `active_cep`: CEP padrao configurado na aplicacao

A LLM nao deve assumir que `active_cep` e o CEP efetivamente consultado.

## Erros de uso comuns

- procurar `resolution_source` no topo da resposta em vez de olhar `result.resolution_source`
- procurar `search_job_id` no topo em vez de olhar `result.search_job.job_id`
- tratar `queued_enrichment` como se ja fosse resposta final
- tratar `source_product_fallback` como falha
- confundir `active_cep` com o escopo real da consulta

## Regra final

Uma resposta de atendimento so deve ser tratada como resolvida imediatamente quando houver:

- `canonical_match`, ou
- `source_product_fallback`

Se vier `queued_enrichment`, a LLM ainda esta no meio do fluxo, nao no fim.
