import os
from pathlib import Path
from uuid import uuid4

import duckdb


class SilverPipeline:
    """Normaliza a Bronze e publica fatos e quarentenas em Parquet."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, bronze_root: str = "bronze", silver_root: str = "silver"):
        self._con = connection
        self._bronze_root = Path(bronze_root)
        self._silver_root = Path(silver_root)

    @staticmethod
    def _literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _ler_bronze(self, dominio: str, tabela: str) -> None:
        """Lê schemas evolutivos e mantém somente o último run de cada arquivo."""
        arquivos = sorted((self._bronze_root / dominio).glob("*/*/data/*.parquet"))
        if not arquivos:
            raise FileNotFoundError(
                f"Nenhum arquivo Bronze encontrado para o domínio {dominio}: {self._bronze_root / dominio}"
            )

        self._con.execute(
            f"CREATE OR REPLACE TEMP TABLE {tabela} AS SELECT * FROM read_parquet(?, union_by_name = true)",
            [[arquivo.as_posix() for arquivo in arquivos]],
        )
        self._con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {tabela}_atual AS
            WITH execucoes AS (
                SELECT source_file, run_id, MAX(ingestion_ts) AS ingestion_ts
                FROM {tabela}
                GROUP BY source_file, run_id
            ), escolhidas AS (
                SELECT source_file, run_id
                FROM execucoes
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY source_file
                    ORDER BY ingestion_ts DESC, run_id DESC
                ) = 1
            )
            SELECT bronze.*
            FROM {tabela} AS bronze
            INNER JOIN escolhidas USING (source_file, run_id)
        """)

    def _publicar_parquet(self, tabela: str, destino: Path) -> None:
        """Grava em staging e troca o arquivo final de forma atômica."""
        destino.parent.mkdir(parents=True, exist_ok=True)
        temporario = destino.parent / f".{destino.stem}_{uuid4().hex}.parquet"
        try:
            self._con.execute(f"""
                COPY (SELECT * FROM {tabela})
                TO {self._literal(temporario.as_posix())}
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            os.replace(temporario, destino)
        finally:
            temporario.unlink(missing_ok=True)

    def executar(self) -> None:
        print("\n=== INICIANDO PIPELINE SILVER ===")
        self._ler_bronze("faturamento", "bronze_faturamento")
        self._ler_bronze("clientes", "bronze_clientes")

        colunas = {
            linha[1]
            for linha in self._con.execute("PRAGMA table_info('bronze_faturamento_atual')").fetchall()
        }
        if "valor_custo_total" in colunas and "vlr_custo" in colunas:
            custo_origem = "COALESCE(NULLIF(TRIM(valor_custo_total), ''), NULLIF(TRIM(vlr_custo), ''))"
        elif "valor_custo_total" in colunas:
            custo_origem = "NULLIF(TRIM(valor_custo_total), '')"
        elif "vlr_custo" in colunas:
            custo_origem = "NULLIF(TRIM(vlr_custo), '')"
        else:
            raise ValueError("Nenhuma coluna de custo foi encontrada na Bronze.")
        canal = "NULLIF(TRIM(canal), '')" if "canal" in colunas else "CAST(NULL AS VARCHAR)"

        self._con.execute(f"""
            CREATE OR REPLACE TEMP TABLE faturamento_normalizado AS
            WITH origem AS (
                SELECT *,
                    NULLIF(TRIM(data_emissao), '') AS raw_data_emissao,
                    NULLIF(TRIM(data_lancamento), '') AS raw_data_lancamento,
                    NULLIF(TRIM(quantidade), '') AS raw_quantidade,
                    NULLIF(TRIM(vlr_receita_liquida), '') AS raw_receita_liquida,
                    {custo_origem} AS raw_custo
                FROM bronze_faturamento_atual
            )
            SELECT
                NULLIF(TRIM(nr_documento), '') AS nr_documento,
                NULLIF(TRIM(item), '') AS item,
                TRANSLATE(UPPER(NULLIF(TRIM(tipo_documento), '')), 'ÇÃÕÁÉÍÓÚÂÊÔ', 'CAOAEIOUAEO') AS tipo_documento,
                CAST(TRY_STRPTIME(raw_data_emissao, '%d/%m/%Y') AS DATE) AS data_emissao,
                CAST(TRY_STRPTIME(raw_data_lancamento, '%d/%m/%Y') AS DATE) AS data_lancamento,
                NULLIF(TRIM(nf_origem), '') AS nf_origem,
                NULLIF(TRIM(cod_cliente), '') AS cod_cliente,
                NULLIF(TRIM(cod_vendedor), '') AS cod_vendedor,
                NULLIF(TRIM(nome_vendedor), '') AS nome_vendedor,
                NULLIF(TRIM(cod_material), '') AS cod_material,
                NULLIF(TRIM(desc_material), '') AS desc_material,
                NULLIF(TRIM(grupo_material), '') AS grupo_material,
                TRY_CAST(REPLACE(REPLACE(raw_quantidade, '.', ''), ',', '.') AS DECIMAL(18,3)) AS quantidade,
                NULLIF(TRIM(unidade), '') AS unidade,
                TRY_CAST(REPLACE(REPLACE(raw_receita_liquida, '.', ''), ',', '.') AS DECIMAL(18,2)) AS receita_liquida,
                TRY_CAST(REPLACE(REPLACE(raw_custo, '.', ''), ',', '.') AS DECIMAL(18,2)) AS custo,
                NULLIF(TRIM(centro), '') AS centro,
                NULLIF(TRIM(uf), '') AS uf,
                {canal} AS canal,
                STRFTIME(TRY_STRPTIME(raw_data_emissao, '%d/%m/%Y'), '%Y-%m') AS competencia,
                CAST(source_file AS VARCHAR) AS source_file,
                CAST(run_id AS VARCHAR) AS run_id,
                CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
                CAST(source_sha256 AS VARCHAR) AS source_sha256,
                raw_data_emissao, raw_data_lancamento, raw_quantidade,
                raw_receita_liquida, raw_custo,
                CAST(TRY_STRPTIME(REGEXP_EXTRACT(source_file, '(\\d{{8}})', 1), '%Y%m%d') AS DATE) AS data_exportacao
            FROM origem
        """)

        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE faturamento_selecionado AS
            WITH arquivo_competencia AS (
                SELECT source_file, MODE(competencia) AS competencia_arquivo,
                       MAX(data_exportacao) AS data_exportacao
                FROM faturamento_normalizado
                WHERE competencia IS NOT NULL
                GROUP BY source_file
            ), snapshots AS (
                SELECT competencia_arquivo, MAX(data_exportacao) AS data_escolhida
                FROM arquivo_competencia
                GROUP BY competencia_arquivo
            ), arquivos_escolhidos AS (
                SELECT a.source_file
                FROM arquivo_competencia AS a
                INNER JOIN snapshots AS s
                    ON a.competencia_arquivo = s.competencia_arquivo
                   AND a.data_exportacao = s.data_escolhida
            )
            SELECT f.*
            FROM faturamento_normalizado AS f
            INNER JOIN arquivos_escolhidos AS e USING (source_file)
        """)

        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE faturamento_deduplicado AS
            SELECT *
            FROM faturamento_selecionado
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY nr_documento, item, tipo_documento, data_emissao,
                    data_lancamento, nf_origem, cod_cliente, cod_vendedor,
                    nome_vendedor, cod_material, desc_material, grupo_material,
                    quantidade, unidade, receita_liquida, custo, centro, uf,
                    canal, competencia, raw_data_emissao, raw_data_lancamento,
                    raw_quantidade, raw_receita_liquida, raw_custo
                ORDER BY ingestion_ts DESC, run_id DESC, source_file DESC
            ) = 1
        """)
        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE chaves_conflitantes AS
            SELECT source_file, nr_documento, item
            FROM faturamento_deduplicado
            WHERE nr_documento IS NOT NULL AND item IS NOT NULL
            GROUP BY source_file, nr_documento, item
            HAVING COUNT(*) > 1
        """)
        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE faturamento_qualidade AS
            SELECT f.*,
                CASE
                    WHEN f.nr_documento IS NULL OR f.item IS NULL THEN 'DOCUMENTO_OU_ITEM_AUSENTE'
                    WHEN f.tipo_documento IS NULL OR f.tipo_documento NOT IN ('VENDA', 'DEVOLUCAO', 'ESTORNO') THEN 'TIPO_DOCUMENTO_INVALIDO'
                    WHEN f.data_emissao IS NULL OR f.data_lancamento IS NULL THEN 'DATA_INVALIDA'
                    WHEN f.cod_cliente IS NULL OR f.cod_vendedor IS NULL OR f.cod_material IS NULL THEN 'CHAVE_OBRIGATORIA_AUSENTE'
                    WHEN f.quantidade IS NULL OR f.receita_liquida IS NULL OR f.custo IS NULL THEN 'VALOR_INVALIDO_OU_AUSENTE'
                    WHEN f.competencia IS NULL THEN 'COMPETENCIA_INVALIDA'
                    WHEN c.nr_documento IS NOT NULL THEN 'CONFLITO_DOCUMENTO_ITEM'
                    ELSE NULL
                END AS motivo_rejeicao
            FROM faturamento_deduplicado AS f
            LEFT JOIN chaves_conflitantes AS c
                USING (source_file, nr_documento, item)
        """)
        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE silver_faturamento AS
            SELECT * EXCLUDE (
                motivo_rejeicao, raw_data_emissao, raw_data_lancamento,
                raw_quantidade, raw_receita_liquida, raw_custo, data_exportacao
            )
            FROM faturamento_qualidade
            WHERE motivo_rejeicao IS NULL
        """)
        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE quarentena_faturamento AS
            SELECT * EXCLUDE (data_exportacao)
            FROM faturamento_qualidade
            WHERE motivo_rejeicao IS NOT NULL
        """)

        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE clientes_normalizados AS
            SELECT
                NULLIF(TRIM(cod_cliente), '') AS cod_cliente,
                NULLIF(TRIM(nome_cliente), '') AS nome_cliente,
                NULLIF(TRIM(tipo_pessoa), '') AS tipo_pessoa,
                NULLIF(TRIM(cpf_cnpj), '') AS cpf_cnpj,
                NULLIF(TRIM(municipio), '') AS municipio,
                NULLIF(TRIM(uf), '') AS uf,
                NULLIF(TRIM(segmento), '') AS segmento,
                NULLIF(TRIM(cod_vendedor_atual), '') AS cod_vendedor_atual,
                TRY_CAST(NULLIF(TRIM(data_atribuicao_vendedor), '') AS DATE) AS data_atribuicao_vendedor,
                CAST(source_file AS VARCHAR) AS source_file,
                CAST(run_id AS VARCHAR) AS run_id,
                CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
                CAST(source_sha256 AS VARCHAR) AS source_sha256,
                NULLIF(TRIM(data_atribuicao_vendedor), '') AS raw_data_atribuicao_vendedor
            FROM bronze_clientes_atual
        """)
        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE clientes_qualidade AS
            SELECT *,
                CASE
                    WHEN cod_cliente IS NULL THEN 'COD_CLIENTE_AUSENTE'
                    WHEN nome_cliente IS NULL THEN 'NOME_CLIENTE_AUSENTE'
                    WHEN raw_data_atribuicao_vendedor IS NOT NULL AND data_atribuicao_vendedor IS NULL THEN 'DATA_ATRIBUICAO_INVALIDA'
                    ELSE NULL
                END AS motivo_rejeicao
            FROM clientes_normalizados
        """)
        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE quarentena_clientes AS
            SELECT * FROM clientes_qualidade
            WHERE motivo_rejeicao IS NOT NULL
        """)
        self._con.execute("""
            CREATE OR REPLACE TEMP TABLE silver_clientes AS
            SELECT * EXCLUDE (motivo_rejeicao, raw_data_atribuicao_vendedor)
            FROM clientes_qualidade
            WHERE motivo_rejeicao IS NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY cod_cliente
                ORDER BY data_atribuicao_vendedor DESC NULLS LAST,
                         ingestion_ts DESC, run_id DESC, source_sha256 DESC
            ) = 1
        """)

        for tabela, nome in (
            ("silver_faturamento", "faturamento.parquet"),
            ("silver_clientes", "clientes.parquet"),
            ("quarentena_faturamento", "quarentena_faturamento.parquet"),
            ("quarentena_clientes", "quarentena_clientes.parquet"),
        ):
            self._publicar_parquet(tabela, self._silver_root / nome)

        print("\n=== RESULTADO SILVER ===")
        for tabela in (
            "bronze_faturamento_atual", "faturamento_selecionado",
            "faturamento_deduplicado", "silver_faturamento",
            "quarentena_faturamento", "bronze_clientes_atual",
            "silver_clientes", "quarentena_clientes",
        ):
            quantidade = self._con.execute(f"SELECT COUNT(*) FROM {tabela}").fetchone()[0]
            print(f"[SILVER] {tabela}: {quantidade} registros")

        for competencia, arquivo, quantidade in self._con.execute("""
            SELECT competencia, source_file, COUNT(*)
            FROM silver_faturamento
            GROUP BY competencia, source_file
            ORDER BY competencia
        """).fetchall():
            print(f"[SILVER] snapshot {competencia}: {arquivo} ({quantidade} itens)")
        print("[SILVER] Processamento concluído.")
