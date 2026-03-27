## Dados de referencia

Este diretorio existe para receber cargas locais e reprocessaveis de:

- `regulatory_products.csv` ou `.json`
- `dcb_aliases.csv` ou `.json`
- `cmed_prices.csv` ou `.json`

O objetivo e permitir:

- recriar o banco do zero
- recarregar referencias regulatorias sem editar o codigo
- atualizar DCB e CMED sempre que sair uma carga nova

### Regulatory products

Campos aceitos:

- `product_name`
- `dcb_name`
- `active_ingredient`
- `concentration`
- `dosage`
- `dosage_form`
- `presentation`
- `route`
- `manufacturer`
- `registration_holder`
- `ean_gtin`
- `anvisa_code`
- `external_id`
- `source_system`
- `source_url`

Sinonimos aceitos:

- `medicamento`, `nome_produto`, `nome_comercial`
- `principio_ativo`
- `concentracao`
- `dosagem`
- `forma_farmaceutica`
- `apresentacao`
- `via_administracao`
- `fabricante`, `laboratorio`
- `detentor_registro`
- `registro_ms`, `registro_anvisa`
- `ean`, `gtin`

### DCB aliases

Campos aceitos:

- `dcb_name`
- `alias`
- `alias_type`
- `source_system`

Sinonimos aceitos:

- `denominacao_comum_brasileira`
- `principio_ativo`
- `nome_comercial`
- `termo`

### CMED prices

Campos aceitos:

- `product_name`
- `presentation`
- `laboratory`
- `dcb_name`
- `ean_gtin`
- `anvisa_code`
- `pmc_price`
- `pf_price`
- `list_price`
- `tax_rate`
- `source_dataset`
- `source_url`

Sinonimos aceitos:

- `medicamento`, `produto`
- `apresentacao`
- `laboratorio`
- `principio_ativo`
- `ean`, `gtin`
- `registro_ms`, `registro_anvisa`
- `pmc`
- `pf`
- `preco_fabrica`, `preco`
- `icms`

### Atualizacao

Com banco novo ou existente:

```bash
uv run python -m src.init_db
uv run python -m src.update_reference_data
```

Para substituir completamente as referencias:

```bash
uv run python -m src.update_reference_data --replace
```

Para pular um dataset:

```bash
uv run python -m src.update_reference_data --skip-cmed
```
