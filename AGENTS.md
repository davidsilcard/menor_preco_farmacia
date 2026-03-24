# AGENTS

## Objetivo do Projeto

Este projeto deve continuar sendo um backend MCP orientado a ferramentas.
A interface principal de uso e orquestracao e uma LLM, nao um frontend humano.
A LLM recebe o pedido do cliente, solicita o CEP quando necessario, aciona os servicos e interpreta as respostas.

## Premissas Nao Negociaveis

1. Todo o controle de demanda e coleta e separado por `cep`.
2. Nenhum dado operacional de um `cep` pode vazar ou ser reutilizado como resposta de outro `cep`.
3. Se um cliente pedir um remedio inexistente no tracking daquele `cep`, o item deve nascer para aquele `cep` e entrar em fila imediatamente.
4. Se o item ja existir como ativo no `cep`, ele deve participar da rotina agendada de coletas das `08:00` e `15:00`.
5. Se ninguem solicitar o item naquele `cep` por `90 dias`, ele deve sair da busca recorrente.
6. Dados operacionais nao podem ficar eternamente no banco.
7. Precos, snapshots, pedidos, jobs e demais dados operacionais do remedio nao devem permanecer por mais de `90 dias`.

## Fluxo Esperado

### Nascimento do Dado

1. Cliente pede um remedio, por exemplo `Jardiance 25mg`.
2. A LLM solicita o `cep`.
3. A busca e sempre contextualizada por `cep`.
4. Se nao houver item ativo para aquele `cep`, o sistema deve:
   - registrar a demanda por `cep`
   - criar ou reutilizar o item rastreado por `cep`
   - criar o `search_job`
   - enfileirar o processamento imediatamente
5. A primeira busca deve acontecer via fila/worker, sem depender de uma chamada operacional manual adicional.

### Vida Util

1. Enquanto houver demanda recente, o item permanece ativo no `cep`.
2. Itens ativos daquele `cep` entram na rotina agendada das `08:00` e `15:00`.
3. Leituras de preco, disponibilidade e ultimos resultados devem ser sempre filtradas por `cep`.

### Morte do Dado

1. Se o item ficar `90 dias` sem solicitacao naquele `cep`, ele sai da rotina de busca.
2. Apos o vencimento da retencao, os dados operacionais associados devem ser removidos.
3. Nao basta marcar como inativo; o ciclo de vida precisa prever expiracao e limpeza real.

## Regras de Arquitetura

1. O runtime deve aceitar processamento real por multiplos CEPs.
2. O sistema nao pode depender de um unico `settings.CEP` para decidir o que pode ou nao pode rodar.
3. Toda leitura de oferta, preco, disponibilidade e snapshot deve ser `cep-aware`.
4. Filas, tracking, jobs, requests e rotinas agendadas devem operar com `cep` como chave de contexto.

## Exposicao MCP

1. O MCP principal deve ser orientado a atendimento.
2. Tools administrativas nao devem ser expostas por padrao para a LLM de atendimento.
3. Tools administrativas so podem ser expostas mediante configuracao explicita.
4. A flag atual para isso e `MCP_EXPOSE_ADMIN_TOOLS`.

### Tools de atendimento esperadas

- `search_products`
- `compare_shopping_list`
- `compare_basket`
- `compare_invoice_items`
- `compare_receipt`
- `search_observed_item`
- `compare_canonical_product`
- `get_search_job`

### Tools administrativas que nao devem aparecer por default

- `list_review_matches`
- `list_search_jobs`

## Retencao

As seguintes entidades devem respeitar retencao maxima de `90 dias`, salvo documentacao explicita em contrario:

- `PriceSnapshot`
- `ScrapeRun`
- `SearchJob`
- `CatalogRequest`
- `TrackedItemByCep`
- `OperationJob`

Se alguma entidade precisar de excecao, isso deve ser documentado explicitamente no codigo e na documentacao de arquitetura.

## Critério de Aceite

Qualquer mudanca futura deve preservar estas garantias:

- o dado nasce por `cep`
- o dado e consultado por `cep`
- o dado entra em fila quando necessario
- o dado participa da rotina agendada quando ativo
- o dado expira e morre em ate `90 dias`
