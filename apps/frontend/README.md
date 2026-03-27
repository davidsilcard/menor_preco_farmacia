# Frontend

Aplicacao web voltada ao cliente final.

Responsabilidades:

- capturar mensagem, nota, lista ou consulta
- enviar a solicitacao para o `assistant-service`
- exibir resposta, historico e status da consulta

Restricoes arquiteturais:

- nao conversa diretamente com o `pricing-core`
- nao deve conter regra operacional de coleta, fila ou retencao
