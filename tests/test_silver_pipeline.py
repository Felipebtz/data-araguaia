from decimal import Decimal
from pathlib import Path
from src.pipeline.silver_pipeline import SilverPipeline
from tests.conftest import venda, cliente, silver_criar


def consultar(con, caminho, sql="SELECT * FROM read_parquet(?)"):
    return con.execute(sql, [str(caminho)]).fetchall()


def test_deve_ler_bronze_com_schemas_diferentes(tmp_path, con):
    recente = venda(nr_documento="1002", valor_custo_total="800,00")
    recente.pop("vlr_custo")
    silver = silver_criar(tmp_path, con, {
        "faturamento_export_20260902.csv": [venda(data_emissao="15/08/2026", data_lancamento="15/08/2026")],
        "faturamento_export_20261105.csv": [recente],
    })
    rows = consultar(con, silver / "faturamento.parquet", "SELECT nr_documento, custo FROM read_parquet(?) ORDER BY nr_documento")
    assert rows == [("1001", Decimal("700.00")), ("1002", Decimal("800.00"))]


def test_deve_converter_valores_monetarios_brasileiros(tmp_path, con):
    silver = silver_criar(tmp_path, con, {"faturamento_export_20261105.csv": [venda(vlr_receita_liquida="1.234,56", vlr_custo="900,25")]})
    row = consultar(con, silver / "faturamento.parquet", "SELECT receita_liquida, custo FROM read_parquet(?)")[0]
    assert row == (Decimal("1234.56"), Decimal("900.25"))


def test_deve_preservar_valores_negativos(tmp_path, con):
    silver = silver_criar(tmp_path, con, {"faturamento_export_20261105.csv": [venda(tipo_documento="DEVOLUCAO", vlr_receita_liquida="-1.000,00", vlr_custo="-700,00")]})
    row = consultar(con, silver / "faturamento.parquet", "SELECT receita_liquida, custo FROM read_parquet(?)")[0]
    assert row == (Decimal("-1000.00"), Decimal("-700.00"))


def test_deve_normalizar_datas_e_competencia(tmp_path, con):
    silver = silver_criar(tmp_path, con)
    row = consultar(con, silver / "faturamento.parquet", "SELECT CAST(data_emissao AS VARCHAR), competencia FROM read_parquet(?)")[0]
    assert row == ("2026-10-15", "2026-10")


def test_deve_selecionar_ultimo_snapshot_de_outubro(tmp_path, con):
    silver = silver_criar(tmp_path, con, {
        "faturamento_export_20261103.csv": [venda(vlr_receita_liquida="100,00")],
        "faturamento_export_20261105.csv": [venda(vlr_receita_liquida="200,00")]
    })
    rows = consultar(con, silver / "faturamento.parquet", "SELECT receita_liquida FROM read_parquet(?)")
    assert rows == [(Decimal("200.00"),)]


def test_deve_remover_duplicatas_exatas(tmp_path, con):
    row = venda()
    silver = silver_criar(tmp_path, con, {"faturamento_export_20261105.csv": [row, row]})
    assert consultar(con, silver / "faturamento.parquet", "SELECT COUNT(*) FROM read_parquet(?)")[0][0] == 1


def test_deve_enviar_registros_invalidos_para_quarentena(tmp_path, con):
    silver = silver_criar(tmp_path, con, {"faturamento_export_20261105.csv": [
        venda(nr_documento="1", data_emissao="31/02/2026"),
        venda(nr_documento="2", vlr_custo="ERRADO"),
        venda(nr_documento="3", cod_cliente=""),
        venda(nr_documento="4"),
    ]})
    assert consultar(con, silver / "faturamento.parquet", "SELECT COUNT(*) FROM read_parquet(?)")[0][0] == 1
    motivos = {row[0] for row in consultar(con, silver / "quarentena_faturamento.parquet", "SELECT motivo_rejeicao FROM read_parquet(?)")}
    assert motivos == {"DATA_INVALIDA", "VALOR_INVALIDO_OU_AUSENTE", "CHAVE_OBRIGATORIA_AUSENTE"}


def test_deve_deduplicar_clientes_por_codigo(tmp_path, con):
    silver = silver_criar(tmp_path, con, clientes=[cliente(nome_cliente="antigo", data_atribuicao_vendedor="2025-01-01"), cliente(nome_cliente="novo", data_atribuicao_vendedor="2026-01-01")])
    assert consultar(con, silver / "clientes.parquet", "SELECT nome_cliente FROM read_parquet(?)") == [("novo",)]


def test_deve_preservar_consultor_historico(tmp_path, con):
    silver = silver_criar(tmp_path, con, clientes=[cliente(cod_vendedor_atual="V07")])
    assert consultar(con, silver / "faturamento.parquet", "SELECT cod_vendedor FROM read_parquet(?)") == [("V03",)]


def test_deve_substituir_parquet_no_reprocessamento(tmp_path, con):
    silver = silver_criar(tmp_path, con)
    primeira = consultar(con, silver / "faturamento.parquet", "SELECT COUNT(*) FROM read_parquet(?)")[0][0]
    SilverPipeline(con, str(tmp_path / "bronze"), str(silver)).executar()
    segunda = consultar(con, silver / "faturamento.parquet", "SELECT COUNT(*) FROM read_parquet(?)")[0][0]
    assert primeira == segunda == 1
