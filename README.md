# Data Araguaia — Pipeline Comercial

Pipeline local Bronze → Silver → Gold para automatizar o relatório mensal de margem por consultor e tornar reproduzível a investigação da queda de margem de Pedro Alves (`V03`) em outubro de 2026.

## Requisitos e execução

- Python 3.13
- DuckDB 2.0
- SQLite 3
- Pytest 9

Instale as dependências e execute, a partir da raiz:

```powershell
python -m pip install -r requirements.txt
python -m src.jobs.processar_comercial
python -m pytest tests/ -v
```

O comando único para reconstruir as camadas e imprimir a apresentação comercial é:

```powershell
python -m src.jobs.processar_comercial
```

## Organização Bronze, Silver e Gold

- `dados/`: cinco CSVs de origem. O pipeline nunca os modifica.
- `bronze/<dominio>/<arquivo>/<run_id>/data/`: cópia versionada em Parquet, com colunas de origem textuais, SHA-256, arquivo, `run_id` e horário de ingestão.
- `silver/faturamento.parquet`: fatos tipados, válidos, deduplicados e com snapshot mensal selecionado.
- `silver/clientes.parquet`: cadastro normalizado e único por cliente.
- `silver/quarentena_*.parquet`: registros inválidos com motivo e valores de origem disponíveis.
- `gold_comercial.sqlite`: tabelas `gold_faturamento_item` e `gold_margem_consultor`.
- `pipeline.duckdb`: auditoria da Bronze e conexão local de transformação.

A Silver lê schemas diferentes com `union_by_name`: `valor_custo_total` tem prioridade e `vlr_custo` é a alternativa. Campos desconhecidos permanecem preservados na Bronze, mas não são propagados silenciosamente para a Silver: a Silver publica somente o contrato comercial documentado nesta entrega. Esse limite evita transformar colunas sem semântica definida.

## Regras de processamento

1. Para cada `source_file`, somente o `run_id` de maior `ingestion_ts` é lido; `run_id` desempata.
2. A competência é o mês de `data_emissao`. A data do snapshot vem dos oito dígitos no nome do arquivo, não da data de execução.
3. Entre `20261103` e `20261105`, ambos de outubro, `20261105` substitui integralmente o anterior.
4. Duplicatas exatas usam somente campos de negócio; metadados de ingestão não participam da identidade.
5. Conflitos com a mesma chave documento/item e conteúdo diferente são enviados à quarentena, sem escolha arbitrária.
6. Valores monetários são `DECIMAL(18,2)` na Silver e centavos inteiros na Gold. Sinais de devoluções e estornos são preservados.
7. O consultor histórico vem da nota (`cod_vendedor`), nunca do cadastro atual do cliente.
8. Margem mensal é `100 × (SUM(receita) − SUM(custo)) / SUM(receita)`. Receita agregada zero produz margem `NULL`.
9. Silver publica por troca atômica de arquivo; Gold substitui as duas tabelas dentro de uma transação SQLite com rollback.

## Resultado de regressão

| Competência | Snapshot | Itens válidos |
|---|---|---:|
| 2026-08 | `faturamento_export_20260902.csv` | 2.440 |
| 2026-09 | `faturamento_export_20261002.csv` | 2.453 |
| 2026-10 | `faturamento_export_20261105.csv` | 2.222 |
| **Total** |  | **7.115** |

O cadastro publicado contém 357 clientes distintos. A Gold contém os mesmos 7.115 itens, sem multiplicação no JOIN.

## Investigação de Pedro Alves

As consultas completas estão em [`docs/consultas_investigacao.sql`](docs/consultas_investigacao.sql).

| Competência | Margem |
|---|---:|
| 2026-08 | 25,0042% |
| 2026-09 | 23,3515% |
| 2026-10 | 16,9668% |

A média aritmética de agosto e setembro é 24,1779%. Outubro ficou 7,2111 pontos percentuais abaixo, uma queda relativa de 29,8252%.

Os dados demonstram uma devolução relevante no documento `404622`, cliente `C00094`, com quatro itens de defensivos: R$ 4.447.530,93 de receita, R$ 3.068.796,33 de custo e R$ 1.378.734,60 de lucro bruto revertidos. Excluir somente esse documento elevaria a margem contrafactual de outubro para aproximadamente 20,2436%; essa não é a margem oficial. As consultas de mix mostram a mudança de participação entre fertilizantes e defensivos e ajudam a decompor o resultado.

Os dados não demonstram o motivo operacional da devolução, as condições comerciais negociadas, a causa definitiva da mudança de mix nem responsabilidade individual. Essas conclusões exigem evidência operacional adicional.

## Consulta direta ao SQLite

```powershell
sqlite3 gold_comercial.sqlite "SELECT * FROM gold_margem_consultor WHERE cod_vendedor='V03' ORDER BY competencia;"
sqlite3 gold_comercial.sqlite ".read docs/consultas_investigacao.sql"
```

## Arquitetura

**Fluxo:** Relatório de Margem Comercial.

**Serviços:** Faturamento e Cadastro de Clientes.

**Competência:** mês de referência do documento, derivado de `data_emissao`.

A implementação local usa Parquet + DuckDB + SQLite. O piloto corporativo usa Parquet na Bronze, Delta Lake na Silver e PostgreSQL na Gold. A lógica de negócio separada da infraestrutura, os formatos abertos e os contratos entre camadas permitem avaliar posteriormente Google Cloud, Microsoft Fabric e Databricks sem reescrever a regra de margem.

Uma decisão futura de plataforma deve considerar integração com sistemas existentes, governança, custo, operação, segurança, desempenho e facilidade de migração; esta entrega não seleciona fornecedor.

## Premissas e limitações

- Nas amostras, emissão e lançamento pertencem ao mesmo mês; competência usa emissão por premissa do case.
- O nome do arquivo deve conter uma data de exportação `AAAAMMDD`.
- O cadastro atual enriquece nome e segmento, mas não reatribui vendas históricas.
- Quarentena identifica problemas; correção de fonte é responsabilidade do sistema produtor.
