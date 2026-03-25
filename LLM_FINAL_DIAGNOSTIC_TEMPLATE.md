# Template de Diagnóstico Final para LLM

Use este bloco ao final de qualquer bateria de teste do MCP/API.

Preencha apenas com base no payload real recebido.

## Diagnóstico Final

### 1. Casos com oferta real

- Liste aqui os casos com:
  - `evidence_level = real_offer`
  - `offers_count > 0`

### 2. Casos com fallback útil

- Liste aqui os casos com:
  - `resolution_source = source_product_fallback`
  - ou `evidence_level = source_product`
  - ou `evidence_level = canonical_only`

### 3. Casos em fila

- Liste aqui os casos com:
  - `outcome = queued`
  - `requires_polling = true`
  - `search_job_id` preenchido

### 4. Casos sem oferta

- Liste aqui os casos com:
  - `results_count > 0`
  - `offers_count = 0`

### 5. Casos com erro técnico

- Liste aqui:
  - `500`
  - timeout
  - traceback
  - payload inconsistente

### 6. Leitura correta de CEP

- Confirmar explicitamente:
  - `requested_cep`
  - `configured_default_cep`
- Dizer se houve ou nao confusao entre escopo real e configuracao padrao

### 7. Conclusão Final

Responder objetivamente:

1. O MCP foi interpretado corretamente?
2. Existem ofertas reais verificáveis?
3. `source_product_fallback` funcionou?
4. A fila funcionou?
5. O isolamento por `cep` parece correto?
6. Houve algum erro técnico real?
7. Quais achados anteriores estavam errados por leitura incorreta do payload?

## Regras de preenchimento

- Nao tratar `queued_enrichment` como resultado final.
- Nao tratar `source_product_fallback` como erro.
- Se `offers_count > 0`, isso e evidencia de oferta real.
- Se `results_count > 0` e `offers_count = 0`, isso e match sem oferta, nao falha tecnica.
- Se `requested_cep` difere de `configured_default_cep`, isso nao e erro por si so.
