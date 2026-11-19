import csv
from pathlib import Path

import duckdb
import pytest

from src.domain.bronze_config import DomainConfig
from src.extract.csv_to_parquet import CsvToParquetExtractor
from src.pipeline.silver_pipeline import SilverPipeline


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def venda(**alteracoes):
    registro = {
        "nr_documento": "1001",
        "item": "10",
        "tipo_documento": "VENDA",
        "data_emissao": "15/10/2026",
        "data_lancamento": "15/10/2026",
        "nf_origem": "",
        "cod_cliente": "C00001",
        "cod_vendedor": "V03",
        "nome_vendedor": "Pedro Alves",
        "cod_material": "MAT01",
        "desc_material": "Produto de teste",
        "grupo_material": "DEFENSIVOS",
        "quantidade": "1,000",
        "unidade": "UN",
        "vlr_receita_liquida": "1.000,00",
        "vlr_custo": "700,00",
        "centro": "CD01",
        "uf": "GO",
        "canal": "Representante",
    }
    registro.update(alteracoes)
    return registro


def cliente(**alteracoes):
    registro = {
        "cod_cliente": "C00001",
        "nome_cliente": "Cliente de teste",
        "tipo_pessoa": "PJ",
        "cpf_cnpj": "00.000.000/0001-00",
        "municipio": "Rio Verde",
        "uf": "GO",
        "segmento": "Produtor",
        "cod_vendedor_atual": "V03",
        "data_atribuicao_vendedor": "2026-01-01",
    }
    registro.update(alteracoes)
    return registro


def _gravar_csv(caminho: Path, registros: list[dict], encoding: str) -> None:
    colunas = list(registros[0])
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", newline="", encoding=encoding) as arquivo:
        escritor = csv.DictWriter(
            arquivo, fieldnames=colunas, delimiter=";", lineterminator="\n"
        )
        escritor.writeheader()
        escritor.writerows(registros)


def silver_criar(tmp_path, con, faturamentos=None, clientes=None):
    faturamentos = faturamentos or {
        "faturamento_export_20261105.csv": [venda()]
    }
    clientes = clientes or [cliente()]
    bronze = tmp_path / "bronze"
    extractor = CsvToParquetExtractor(con, bronze)

    for nome, registros in faturamentos.items():
        caminho = tmp_path / "dados" / nome
        _gravar_csv(caminho, registros, "latin-1")
        extractor.extract_file(
            DomainConfig("faturamento", str(caminho)), caminho
        )

    caminho_clientes = tmp_path / "dados" / "clientes.csv"
    _gravar_csv(caminho_clientes, clientes, "utf-8")
    extractor.extract_file(
        DomainConfig("clientes", str(caminho_clientes)), caminho_clientes
    )

    silver = tmp_path / "silver"
    SilverPipeline(con, str(bronze), str(silver)).executar()
    return silver
