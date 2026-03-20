# Contrato de Entradas Multimodais

Este documento define o que a aplicacao, o OCR e a LLM devem enviar para esta API em cada tipo de entrada.

O objetivo e evitar ambiguidade entre:

- texto livre do usuario
- OCR de nota fiscal
- OCR de caixa de remedio
- lista digitada manualmente

## Regra geral

Esta API nao recebe imagem bruta.

Ela recebe texto e campos estruturados produzidos pela aplicacao e/ou pela LLM.

Preco depende de regiao. Por isso, o CEP deve sempre ser enviado nas consultas.

Fluxo correto:

1. a aplicacao captura imagem, PDF ou texto
2. OCR/parser extrai texto bruto
3. a LLM ou a aplicacao organiza os campos
4. a API recebe apenas payload estruturado

## 1. Lista de compras

Endpoint:

- `POST /tool/compare-shopping-list`

Payload minimo:

```json
{
  "cep": "89254300",
  "items": [
    "dipirona 1g 10 comprimidos",
    "novalgina infantil 100ml"
  ]
}
```

Quando usar:

- usuario digitou nomes manualmente
- usuario colou uma lista
- LLM extraiu itens de uma conversa

## 2. Itens de nota fiscal

Endpoint:

- `POST /tool/compare-invoice-items`

Payload recomendado:

```json
{
  "cep": "89254300",
  "items": [
    {
      "description": "novalgina infantil 100ml",
      "paid_price": 48.44,
      "quantity": 1
    }
  ]
}
```

Campos:

- `description`: obrigatorio
- `paid_price`: opcional, mas recomendado para calcular economia
- `quantity`: opcional, default `1`

Quando usar:

- o OCR da nota extraiu apenas linhas de item
- ainda nao existe contexto completo do cupom

## 3. Nota fiscal inteira

Endpoint:

- `POST /tool/compare-receipt`

Payload recomendado:

```json
{
  "cep": "89254300",
  "merchant_name": "Farmacia Exemplo",
  "captured_at": "2026-03-18T10:00:00-03:00",
  "items": [
    {
      "description": "novalgina infantil 100ml",
      "paid_price": 48.44,
      "quantity": 1
    },
    {
      "description": "dipirona 1g 10 comprimidos",
      "paid_price": 22.72,
      "quantity": 1
    }
  ]
}
```

Campos:

- `merchant_name`: opcional
- `captured_at`: opcional
- `items`: obrigatorio

Quando usar:

- a aplicacao quer comparar a cesta inteira
- a LLM quer responder economia total e melhor farmacia para o conjunto

## 4. Foto da caixa do remedio

Endpoint:

- `POST /tool/search-observed-item`

Payload recomendado:

```json
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

Campos:

- `source_type`: usar `box_photo`
- `observations`: lista de fragmentos OCR ou campos detectados

Quando usar:

- usuario tira foto da embalagem
- OCR captura frente, lateral ou verso da caixa
- a LLM quer localizar o produto mais provavel

Observacao:

Este endpoint tenta ignorar ruido comum de embalagem, como:

- lote
- validade
- fabricacao
- industria brasileira

## 5. Texto livre do usuario

Endpoint:

- `GET /tool/search-products?query=...`

Exemplos:

- `GET /tool/search-products?query=novalg inf 100ml&cep=89254300`
- `GET /tool/search-products?query=dip sod 1g 10 cpr medley&cep=89254300`
- `GET /tool/search-products?query=7891058464073&cep=89254300`

Quando usar:

- pergunta curta
- busca manual
- usuario informa EAN ou nome parcial

## Pipeline recomendado por tipo de entrada

### A. Lista digitada

1. quebrar texto em itens
2. chamar `compare-shopping-list`

### B. Nota fiscal

1. OCR extrai linhas
2. parser identifica descricao, quantidade e preco pago
3. chamar `compare-receipt`

### C. Foto da caixa

1. OCR extrai blocos de texto
2. aplicar limpeza basica
3. mandar blocos em `observations`
4. chamar `search-observed-item`

### D. Pergunta livre

1. LLM identifica se e busca simples ou comparacao
2. se for busca simples: `search-products`
3. se for comparacao de cesta: `compare-shopping-list`
4. se for nota: `compare-receipt`

## Regras para o OCR/aplicacao

### Para nota fiscal

Tentar extrair:

- descricao do item
- quantidade
- preco unitario ou total pago
- nome do estabelecimento
- data/hora

### Para caixa de remedio

Tentar extrair:

- nome comercial
- principio ativo
- dosagem
- apresentacao
- quantidade
- EAN se visivel
- registro MS/ANVISA se visivel

Evitar depender de:

- lote
- validade
- textos promocionais
- frases legais

## Regras para a LLM

### Deve fazer

- escolher o endpoint certo conforme a origem do dado
- sempre enviar o `cep`
- enviar descricoes curtas e relevantes
- preservar `paid_price` quando existir
- incluir `quantity` quando existir
- usar `source_type=box_photo` para OCR de embalagem

### Nao deve fazer

- mandar imagem binaria para esta API
- mandar OCR bruto sem qualquer separacao quando houver estrutura suficiente
- assumir que um match com baixa confianca e definitivo
- omitir CEP em consultas de preco

## Envelope de resposta esperado

Todos os endpoints `/tool/*` devolvem:

```json
{
  "tool_name": "search_observed_item",
  "input": {},
  "confidence": 0.9,
  "warnings": [],
  "result": {}
}
```

Interpretacao:

- `tool_name`: qual tool foi usada
- `input`: payload recebido
- `confidence`: confianca global da resposta
- `warnings`: alertas para a LLM considerar
- `result`: dados estruturados para resposta final

## Resumo de disponibilidade

Os endpoints de tool use podem devolver, dentro de `result`, um campo `availability_summary`.

### Em busca de item unico

Exemplo:

```json
{
  "availability_summary": {
    "state": "only_out_of_stock_offers",
    "offer_counts": {
      "available": 0,
      "unknown": 0,
      "out_of_stock": 2
    },
    "best_offer_availability": null
  }
}
```

Estados atuais:

- `has_available_offers`
- `only_unknown_offers`
- `only_out_of_stock_offers`
- `no_offers`

### Em comparacao de cesta

Exemplo:

```json
{
  "availability_summary": {
    "items_with_available_offers": 1,
    "items_only_unknown_offers": 0,
    "items_only_out_of_stock_offers": 1,
    "items_without_offers": 0
  }
}
```

### Regra para a LLM

- se `state = has_available_offers`, a resposta pode tratar o item como compravel
- se `state = only_unknown_offers`, a resposta deve dizer que o estoque nao foi confirmado
- se `state = only_out_of_stock_offers`, a resposta deve dizer que o produto foi encontrado, mas esta sem estoque
- se a cesta tiver `items_only_out_of_stock_offers > 0`, a LLM nao deve afirmar que alguma farmacia cobre toda a cesta
