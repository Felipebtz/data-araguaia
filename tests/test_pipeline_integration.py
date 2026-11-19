from pathlib import Path
import sqlite3

import duckdb
import pytest

from src.jobs.processar_comercial import executar_pipeline


DADOS_REAIS = Path(__file__).resolve().parents[1] / "dados"


@pytest.fixture(scope="module")
def pipeline_real(tmp_path_factory):
    raiz = tmp_path_factory.mktemp("pipeline_real")
    gold = executar_pipeline(raiz, DADOS_REAIS)
    return raiz, gold


def test_deve_processar_cinco_csvs_ate_gold(pipeline_real):
    raiz, gold = pipeline_real
    with duckdb.connect(":memory:") as con:
        contagens = con.execute("""
            SELECT competencia, COUNT(*)
            FROM read_parquet(?)
            GROUP BY competencia
            ORDER BY competencia
        """, [str(raiz / "silver" / "faturamento.parquet")]).fetchall()
        clientes = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT cod_cliente) FROM read_parquet(?)",
            [str(raiz / "silver" / "clientes.parquet")],
        ).fetchone()
    with sqlite3.connect(gold) as db:
        itens_gold = db.execute(
            "SELECT COUNT(*) FROM gold_faturamento_item"
        ).fetchone()[0]
    assert contagens == [("2026-08", 2440), ("2026-09", 2453), ("2026-10", 2222)]
    assert clientes == (357, 357)
    assert itens_gold == 7115


def test_deve_reproduzir_margem_pedro_alves(pipeline_real):
    _, gold = pipeline_real
    with sqlite3.connect(gold) as db:
        margens = db.execute("""
            SELECT competencia, margem_percentual
            FROM gold_margem_consultor
            WHERE cod_vendedor = 'V03'
            ORDER BY competencia
        """).fetchall()
    assert [linha[0] for linha in margens] == ["2026-08", "2026-09", "2026-10"]
    assert [linha[1] for linha in margens] == pytest.approx(
        [25.0042, 23.3515, 16.9668], abs=0.0001
    )


def test_deve_manter_resultado_apos_reexecucao(tmp_path):
    gold = executar_pipeline(tmp_path, DADOS_REAIS)
    with sqlite3.connect(gold) as db:
        anterior = db.execute(
            "SELECT * FROM gold_margem_consultor ORDER BY competencia, cod_vendedor"
        ).fetchall()
        itens_anteriores = db.execute(
            "SELECT competencia, COUNT(*) FROM gold_faturamento_item GROUP BY competencia ORDER BY competencia"
        ).fetchall()

    executar_pipeline(tmp_path, DADOS_REAIS)
    with sqlite3.connect(gold) as db:
        posterior = db.execute(
            "SELECT * FROM gold_margem_consultor ORDER BY competencia, cod_vendedor"
        ).fetchall()
        itens_posteriores = db.execute(
            "SELECT competencia, COUNT(*) FROM gold_faturamento_item GROUP BY competencia ORDER BY competencia"
        ).fetchall()
    assert posterior == anterior
    assert itens_posteriores == itens_anteriores
