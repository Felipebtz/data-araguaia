from contextlib import closing
from decimal import Decimal
from pathlib import Path
import sqlite3

import duckdb

from src.domain.margem_domain import MargemDomain


class GoldPipeline:
    """Publica fatos enriquecidos e margens mensais no SQLite."""

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        silver_root: str = "silver",
        gold_db: str = "gold_comercial.sqlite",
    ):
        self._con = connection
        self._silver_root = Path(silver_root)
        self._gold_db = str(gold_db)

    @staticmethod
    def _centavos(valor: Decimal) -> int:
        return int((Decimal(valor) * 100).to_integral_exact())

    def executar(self) -> None:
        faturamento = self._silver_root / "faturamento.parquet"
        clientes = self._silver_root / "clientes.parquet"
        if not faturamento.exists() or not clientes.exists():
            raise FileNotFoundError("Execute a Silver antes da Gold.")

        self._con.execute(
            """
            CREATE OR REPLACE TEMP TABLE gold_fatos AS
            SELECT
                f.competencia, f.nr_documento, f.item, f.tipo_documento,
                f.data_emissao, f.data_lancamento, f.nf_origem,
                f.cod_cliente, c.nome_cliente, c.segmento,
                f.cod_vendedor, f.nome_vendedor,
                f.cod_material, f.desc_material, f.grupo_material,
                f.quantidade, f.unidade, f.receita_liquida, f.custo,
                f.centro, f.uf, f.canal, f.source_file, f.run_id
            FROM read_parquet(?) AS f
            LEFT JOIN read_parquet(?) AS c
                ON f.cod_cliente = c.cod_cliente
            """,
            [faturamento.as_posix(), clientes.as_posix()],
        )
        total_silver = self._con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [faturamento.as_posix()]
        ).fetchone()[0]
        total_gold = self._con.execute(
            "SELECT COUNT(*) FROM gold_fatos"
        ).fetchone()[0]
        if total_silver != total_gold:
            raise ValueError(
                f"JOIN com clientes multiplicou registros: Silver={total_silver}, Gold={total_gold}"
            )

        fatos = self._con.execute("""
            SELECT
                competencia, nr_documento, item, tipo_documento,
                CAST(data_emissao AS VARCHAR), CAST(data_lancamento AS VARCHAR),
                nf_origem, cod_cliente, nome_cliente, segmento,
                cod_vendedor, nome_vendedor, cod_material, desc_material,
                grupo_material, CAST(quantidade AS VARCHAR), unidade,
                receita_liquida, custo, centro, uf, canal, source_file, run_id
            FROM gold_fatos
            ORDER BY competencia, nr_documento, item
        """).fetchall()
        convertidos = [
            (*linha[:17], self._centavos(linha[17]), self._centavos(linha[18]), *linha[19:])
            for linha in fatos
        ]

        Path(self._gold_db).parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self._gold_db)) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                db.execute("DROP TABLE IF EXISTS gold_margem_consultor")
                db.execute("DROP TABLE IF EXISTS gold_faturamento_item")
                db.execute("""
                    CREATE TABLE gold_faturamento_item (
                        competencia TEXT NOT NULL,
                        nr_documento TEXT NOT NULL,
                        item TEXT NOT NULL,
                        tipo_documento TEXT NOT NULL,
                        data_emissao TEXT NOT NULL,
                        data_lancamento TEXT NOT NULL,
                        nf_origem TEXT,
                        cod_cliente TEXT NOT NULL,
                        nome_cliente TEXT,
                        segmento TEXT,
                        cod_vendedor TEXT NOT NULL,
                        nome_vendedor TEXT,
                        cod_material TEXT NOT NULL,
                        desc_material TEXT,
                        grupo_material TEXT,
                        quantidade TEXT NOT NULL,
                        unidade TEXT,
                        receita_centavos INTEGER NOT NULL,
                        custo_centavos INTEGER NOT NULL,
                        centro TEXT,
                        uf TEXT,
                        canal TEXT,
                        source_file TEXT NOT NULL,
                        run_id TEXT NOT NULL
                    )
                """)
                db.execute("""
                    CREATE TABLE gold_margem_consultor (
                        competencia TEXT NOT NULL,
                        cod_vendedor TEXT NOT NULL,
                        nome_vendedor TEXT,
                        receita_centavos INTEGER NOT NULL,
                        custo_centavos INTEGER NOT NULL,
                        lucro_centavos INTEGER NOT NULL,
                        margem_percentual REAL,
                        itens INTEGER NOT NULL,
                        PRIMARY KEY (competencia, cod_vendedor)
                    )
                """)
                db.execute("CREATE INDEX idx_gold_item_competencia ON gold_faturamento_item(competencia)")
                db.execute("CREATE INDEX idx_gold_item_consultor ON gold_faturamento_item(cod_vendedor)")
                db.execute("CREATE INDEX idx_gold_item_comp_consultor ON gold_faturamento_item(competencia, cod_vendedor)")
                db.execute("CREATE INDEX idx_gold_margem_consultor ON gold_margem_consultor(cod_vendedor)")
                db.executemany(
                    "INSERT INTO gold_faturamento_item VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    convertidos,
                )

                totais = db.execute("""
                    SELECT competencia, cod_vendedor, MAX(nome_vendedor),
                           SUM(receita_centavos), SUM(custo_centavos), COUNT(*)
                    FROM gold_faturamento_item
                    GROUP BY competencia, cod_vendedor
                    ORDER BY competencia, cod_vendedor
                """).fetchall()
                indicadores = []
                for competencia, codigo, nome, receita, custo, itens in totais:
                    margem = None
                    if receita != 0:
                        margem = float(
                            MargemDomain.calcular(Decimal(receita), Decimal(custo))
                        )
                    indicadores.append(
                        (competencia, codigo, nome, receita, custo, receita - custo, margem, itens)
                    )
                db.executemany(
                    "INSERT INTO gold_margem_consultor VALUES (?,?,?,?,?,?,?,?)",
                    indicadores,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

        print(f"[GOLD] gold_faturamento_item: {len(fatos)} registros")
        print(f"[GOLD] gold_margem_consultor: {len(indicadores)} registros")
