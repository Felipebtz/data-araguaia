import sqlite3

import pytest

from src.domain.margem_domain import MargemDomain
from src.pipeline.gold_pipeline import GoldPipeline
from tests.conftest import cliente, silver_criar, venda


def gold_criar(tmp_path, con, registros, clientes=None):
    silver_criar(
        tmp_path,
        con,
        {"faturamento_export_20261105.csv": registros},
        clientes,
    )
    path = tmp_path / "gold.sqlite"
    GoldPipeline(con, str(tmp_path / "silver"), str(path)).executar()
    return path


def ler(path, sql):
    with sqlite3.connect(path) as db:
        return db.execute(sql).fetchall()


def test_deve_calcular_margem_sobre_totais(tmp_path, con):
    path = gold_criar(tmp_path, con, [
        venda(nr_documento="1", vlr_receita_liquida="1.000,00", vlr_custo="700,00"),
        venda(nr_documento="2", vlr_receita_liquida="2.000,00", vlr_custo="1.500,00"),
    ])
    row = ler(path, "SELECT receita_centavos,custo_centavos,lucro_centavos,margem_percentual FROM gold_margem_consultor")[0]
    assert row[:3] == (300000, 220000, 80000)
    assert row[3] == pytest.approx(26.6666666667)


def test_deve_considerar_devolucoes_e_estornos(tmp_path, con):
    path = gold_criar(tmp_path, con, [
        venda(nr_documento="1", vlr_receita_liquida="1.000,00", vlr_custo="700,00"),
        venda(nr_documento="2", tipo_documento="DEVOLUCAO", vlr_receita_liquida="-200,00", vlr_custo="-140,00"),
    ])
    assert ler(path, "SELECT receita_centavos,custo_centavos,lucro_centavos,margem_percentual FROM gold_margem_consultor") == [(80000, 56000, 24000, 30.0)]


def test_deve_tratar_receita_zero_sem_divisao_por_zero(tmp_path, con):
    path = gold_criar(tmp_path, con, [
        venda(nr_documento="1", vlr_receita_liquida="100,00", vlr_custo="70,00"),
        venda(nr_documento="2", tipo_documento="ESTORNO", vlr_receita_liquida="-100,00", vlr_custo="-70,00"),
    ])
    assert ler(path, "SELECT receita_centavos, margem_percentual FROM gold_margem_consultor") == [(0, None)]


def test_deve_impedir_multiplicacao_no_join_clientes(tmp_path, con):
    path = gold_criar(tmp_path, con, [
        venda(nr_documento="1"), venda(nr_documento="2")
    ], [
        cliente(nome_cliente="antigo", data_atribuicao_vendedor="2025-01-01"),
        cliente(nome_cliente="novo", data_atribuicao_vendedor="2026-01-01"),
    ])
    assert ler(path, "SELECT COUNT(*) FROM gold_faturamento_item") == [(2,)]


def test_deve_ser_idempotente_no_reprocessamento(tmp_path, con):
    path = gold_criar(tmp_path, con, [venda()])
    anterior = ler(path, "SELECT * FROM gold_margem_consultor")
    GoldPipeline(con, str(tmp_path / "silver"), str(path)).executar()
    assert ler(path, "SELECT * FROM gold_margem_consultor") == anterior
    assert ler(path, "SELECT COUNT(*) FROM gold_faturamento_item") == [(1,)]


def test_deve_preservar_dados_anteriores_em_falha(tmp_path, con, monkeypatch):
    path = gold_criar(tmp_path, con, [venda()])
    anterior = ler(path, "SELECT * FROM gold_margem_consultor")

    def falha(_receita, _custo):
        raise RuntimeError("Erro simulado após DROP/INSERT")

    monkeypatch.setattr(MargemDomain, "calcular", staticmethod(falha))
    with pytest.raises(RuntimeError):
        GoldPipeline(con, str(tmp_path / "silver"), str(path)).executar()
    assert ler(path, "SELECT * FROM gold_margem_consultor") == anterior
    assert ler(path, "SELECT COUNT(*) FROM gold_faturamento_item") == [(1,)]
