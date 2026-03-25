# Checklist de Conformidade para LLM

## Objetivo

Validar se uma LLM externa sabe usar o MCP corretamente.

Nao basta verificar:

- status HTTP
- existencia de payload
- ausencia de `500`

E preciso verificar se a LLM interpreta corretamente:

- `resolution_source`
- `outcome`
- `evidence_level`
- `requires_polling`
- IDs achatados e objetos aninhados
- diferenca entre `requested_cep` e `configured_default_cep`

## Checklist de leitura correta

### Busca direta

Para `search_products`, a LLM deve:

- ler `result.resolution_source`
- ler `result.outcome`
- ler `result.evidence_level`
- ler `result.requires_polling`
- ler `result.results_count`
- ler `result.offers_count`

### Fila

Se `result.requires_polling = true`, a LLM deve:

- capturar `result.search_job_id`
- capturar `result.operation_job_id`
- usar `get_search_job`
- nao responder como se ja tivesse resultado final

### Oferta real

Se `result.evidence_level = "real_offer"`, a LLM pode:

- responder com farmacia e preco
- usar `offers`
- citar frescor e disponibilidade

### Match sem oferta

Se `result.evidence_level = "canonical_only"`, a LLM deve:

- dizer que o produto foi reconhecido
- nao inventar preco

### Fallback por source product

Se `result.resolution_source = "source_product_fallback"`, a LLM deve:

- tratar como resposta util
- nao classificar como falha

### Semantica de CEP

Em payloads operacionais:

- `requested_cep` = escopo real da consulta
- `configured_default_cep` = CEP padrao da aplicacao

A LLM nao deve concluir erro apenas porque esses campos diferem.

## Matriz minima de validacao

### Caso 1: oferta real

- `GET /tool/search-products?query=buscopan%20composto&cep=89254300`

Esperado:

- `outcome = resolved`
- `evidence_level = real_offer`
- `offers_count > 0`

### Caso 2: fallback real

- `GET /tool/search-products?query=dipirona&cep=89254300`

Esperado:

- `resolution_source = source_product_fallback`
- `outcome = resolved`

### Caso 3: fila

- `GET /tool/search-products?query=produto%20raro%20xyz%202026&cep=89252000`

Esperado:

- `outcome = queued`
- `requires_polling = true`
- `search_job_id` preenchido

### Caso 4: match sem oferta

- `GET /tool/search-products?query=buscopan%20composto&cep=89251000`

Esperado:

- `outcome = resolved`
- `evidence_level = canonical_only`
- `offers_count = 0`

### Caso 5: leitura operacional por CEP

- `GET /ops/metrics?cep=89251000`

Esperado:

- `requested_cep = 89251000`
- `configured_default_cep = 89254300`

## Classificacao correta dos resultados

### `sucesso_real`

Quando houver:

- `evidence_level = real_offer`

### `sucesso_parcial_com_fallback`

Quando houver:

- `evidence_level = source_product`
- ou `evidence_level = canonical_only`
- ou `outcome = queued`

### `falha_funcional`

Quando a LLM concluir algo incompatível com o payload.

Exemplos:

- dizer que nao houve jobs quando `search_job_id` existe
- dizer que tudo veio vazio quando `results_count > 0`
- dizer que multi-CEP esta quebrado sem olhar `requested_cep`

### `falha_tecnica`

Quando houver:

- `500`
- traceback
- timeout
- payload inconsistente

## Prompt curto para outra LLM

```text
Teste o MCP sem inferencias fracas.

Para cada resposta, leia obrigatoriamente:
- result.resolution_source
- result.outcome
- result.evidence_level
- result.requires_polling
- result.results_count
- result.offers_count
- result.search_job_id
- result.operation_job_id

Nao trate:
- queued_enrichment como resultado final
- source_product_fallback como erro
- configured_default_cep como escopo da consulta

Valide ao menos estes cenarios:
- oferta real
- source product fallback
- fila
- match sem oferta
- leitura operacional por CEP
```
