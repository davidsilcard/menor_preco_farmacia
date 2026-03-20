# Arquitetura MCP

## Tese do projeto

O usuario final nao conversa com esta API.

O usuario final conversa com uma aplicacao que usa uma LLM.

A LLM usa esta API como ferramenta para responder perguntas sobre economia em farmacias, equivalencia de produtos e comparacao de precos.

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
