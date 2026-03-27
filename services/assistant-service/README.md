# Assistant Service

Servico de atendimento que recebe chamadas do frontend, Telegram e WhatsApp.

Responsabilidades:

- integrar com a LLM
- manter contexto de conversa
- aplicar autenticacao e limites por canal
- consumir o `pricing-core` por HTTP interno

Restricoes arquiteturais:

- nao deve acessar o banco do `pricing-core` diretamente
- nao deve importar codigo interno do `pricing-core`
- a comunicacao entre servicos deve acontecer por contrato HTTP
