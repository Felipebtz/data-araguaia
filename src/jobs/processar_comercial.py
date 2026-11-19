from decimal import Decimal
from pathlib import Path
import sqlite3

import duckdb

from src.domain.bronze_config import DomainConfig
from src.pipeline.bronze_pipeline import BronzePipeline
from src.pipeline.gold_pipeline import GoldPipeline
from src.pipeline.silver_pipeline import SilverPipeline


def _moeda(centavos: int, absoluto: bool = False) -> str:
    valor = Decimal(abs(centavos) if absoluto else centavos) / Decimal(100)
    texto = f"{valor:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"R$ {texto}"


def _imprimir_evidencias(gold_db: Path) -> None:
    with sqlite3.connect(gold_db) as db:
        print("\n=== PEDRO ALVES (V03): EVOLUÇÃO MENSAL ===")
        mensais = db.execute("""
            SELECT competencia, receita_centavos, custo_centavos,
                   lucro_centavos, margem_percentual
            FROM gold_margem_consultor
            WHERE cod_vendedor = 'V03'
            ORDER BY competencia
        """).fetchall()
        for competencia, receita, custo, lucro, margem in mensais:
            print(
                f"{competencia}: receita={_moeda(receita)}; custo={_moeda(custo)}; "
                f"lucro={_moeda(lucro)}; margem={margem:.4f}%"
            )

        print("\n=== DEVOLUÇÕES DE PEDRO ===")
        devolucoes = db.execute("""
            SELECT nr_documento, cod_cliente, MAX(nome_cliente), COUNT(*),
                   SUM(receita_centavos), SUM(custo_centavos),
                   SUM(receita_centavos - custo_centavos)
            FROM gold_faturamento_item
            WHERE cod_vendedor = 'V03' AND tipo_documento = 'DEVOLUCAO'
            GROUP BY nr_documento, cod_cliente
            ORDER BY ABS(SUM(receita_centavos)) DESC
        """).fetchall()
        for documento, cliente, nome, itens, receita, custo, lucro in devolucoes:
            print(
                f"doc={documento}; cliente={cliente} ({nome}); itens={itens}; "
                f"receita_revertida={_moeda(receita, True)}; "
                f"custo_revertido={_moeda(custo, True)}; "
                f"lucro_revertido={_moeda(lucro, True)}"
            )

        print("\n=== MIX DE PEDRO: SETEMBRO x OUTUBRO ===")
        mix = db.execute("""
            WITH grupos AS (
                SELECT competencia, grupo_material,
                       SUM(receita_centavos) AS receita,
                       SUM(custo_centavos) AS custo
                FROM gold_faturamento_item
                WHERE cod_vendedor = 'V03'
                  AND competencia IN ('2026-09', '2026-10')
                GROUP BY competencia, grupo_material
            ), totais AS (
                SELECT competencia, SUM(receita) AS receita_total
                FROM grupos GROUP BY competencia
            )
            SELECT g.competencia, g.grupo_material, g.receita, g.custo,
                   g.receita - g.custo AS lucro,
                   CASE WHEN g.receita = 0 THEN NULL
                        ELSE 100.0 * (g.receita - g.custo) / g.receita END,
                   CASE WHEN t.receita_total = 0 THEN NULL
                        ELSE 100.0 * g.receita / t.receita_total END
            FROM grupos AS g
            INNER JOIN totais AS t USING (competencia)
            WHERE g.grupo_material IN ('FERTILIZANTES', 'DEFENSIVOS')
            ORDER BY g.competencia, g.grupo_material
        """).fetchall()
        for competencia, grupo, receita, custo, lucro, margem, participacao in mix:
            print(
                f"{competencia} {grupo}: receita={_moeda(receita)}; "
                f"custo={_moeda(custo)}; lucro={_moeda(lucro)}; "
                f"margem={margem:.4f}%; participação={participacao:.4f}%"
            )

        margem_sem_devolucao = db.execute("""
            SELECT CASE WHEN SUM(receita_centavos) = 0 THEN NULL
                        ELSE 100.0 * (
                            SUM(receita_centavos) - SUM(custo_centavos)
                        ) / SUM(receita_centavos) END
            FROM gold_faturamento_item
            WHERE competencia = '2026-10'
              AND cod_vendedor = 'V03'
              AND nr_documento <> '404622'
        """).fetchone()[0]
        print("\n=== CENÁRIO CONTRAFACTUAL ===")
        print(f"Margem de outubro sem o documento 404622: {margem_sem_devolucao:.4f}%")
        print("A margem oficial permanece a apurada com todos os documentos.")


def executar_pipeline(project_root: Path | None = None, dados_root: Path | None = None) -> Path:
    raiz = Path(project_root) if project_root else Path(__file__).resolve().parents[2]
    dados = Path(dados_root) if dados_root else raiz / "dados"
    if not dados.is_dir():
        raise FileNotFoundError(f"Pasta de dados não encontrada: {dados}")

    db_duck = raiz / "pipeline.duckdb"
    bronze_root = raiz / "bronze"
    silver_root = raiz / "silver"
    gold_db = raiz / "gold_comercial.sqlite"

    with BronzePipeline(str(db_duck), str(bronze_root)) as bronze:
        bronze.run_step(DomainConfig("faturamento", str(dados / "faturamento_export_*.csv")))
        bronze.run_step(DomainConfig("clientes", str(dados / "clientes.csv")))

    with duckdb.connect(str(db_duck)) as connection:
        SilverPipeline(connection, str(bronze_root), str(silver_root)).executar()
        GoldPipeline(connection, str(silver_root), str(gold_db)).executar()

    _imprimir_evidencias(gold_db)
    return gold_db


def main() -> None:
    executar_pipeline()


if __name__ == "__main__":
    main()
