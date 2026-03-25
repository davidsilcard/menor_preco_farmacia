# Exemplos de Tools para LLM

## 1. Oferta real imediata

### Request

`GET /tool/search-products?query=buscopan%20composto&cep=89254300`

### Sinais esperados

- `result.outcome = "resolved"`
- `result.evidence_level = "real_offer"`
- `result.resolution_source = "canonical_match"`
- `result.requires_polling = false`
- `result.results_count > 0`
- `result.offers_count > 0`

### Como a LLM deve responder

- citar farmacias e precos
- citar frescor se relevante
- nao falar em fila

## 2. Resolvido por source product fallback

### Request

`GET /tool/search-products?query=dipirona&cep=89254300`

### Sinais esperados

- `result.outcome = "resolved"`
- `result.evidence_level = "real_offer"` ou `source_product`
- `result.resolution_source = "source_product_fallback"`
- `result.requires_polling = false`

### Como a LLM deve responder

- tratar como resposta util
- nao classificar como erro
- nao abrir polling

## 3. Busca enfileirada

### Request

`GET /tool/search-products?query=produto%20raro%20xyz%202026&cep=89252000`

### Sinais esperados

- `result.outcome = "queued"`
- `result.evidence_level = "none"`
- `result.resolution_source = "queued_enrichment"`
- `result.requires_polling = true`
- `result.search_job_id` preenchido
- `result.operation_job_id` preenchido

### Como a LLM deve responder

- informar que a busca entrou em fila
- guardar `search_job_id`
- usar `get_search_job` depois

## 4. Match sem oferta util

### Request

`GET /tool/search-products?query=buscopan%20composto&cep=89251000`

### Sinais esperados

- `result.outcome = "resolved"`
- `result.evidence_level = "canonical_only"`
- `result.resolution_source = "canonical_match"`
- `result.results_count > 0`
- `result.offers_count = 0`

### Como a LLM deve responder

- o produto foi reconhecido
- ainda nao ha oferta util para aquele `cep`
- nao inventar preco

## 5. Leitura operacional por CEP

### Request

`GET /ops/metrics?cep=89251000`

### Sinais esperados

- `requested_cep = "89251000"`
- `configured_default_cep = "89254300"`

### Como a LLM deve interpretar

- `requested_cep` e o escopo real da consulta
- `configured_default_cep` e apenas o CEP padrao da aplicacao
- isso nao significa que o sistema ignorou o CEP informado
