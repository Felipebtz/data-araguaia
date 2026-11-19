
from pathlib import Path
import hashlib
import duckdb
import pytest

from src.domain.bronze_config import DomainConfig
from src.extract.csv_to_parquet import CsvToParquetExtractor


@pytest.fixture
def connection():
    con = duckdb.connect(":memory:")
    yield con
    con.close()


@pytest.fixture
def config():
    return DomainConfig(
        name="faturamento",
        input_pattern="*.csv",
        delimiter=";"
    )


@pytest.fixture
def csv_file(tmp_path):
    arquivo = tmp_path / "faturamento_export_20261105.csv"
    arquivo.write_text(
        "nr_documento;item;vlr_receita_liquida;vlr_custo\n"
        "001;1;1000,00;700,00\n"
        "002;1;2000,00;1500,00\n",
        encoding="utf-8"
    )
    return arquivo


@pytest.fixture
def extractor(connection, tmp_path):
    return CsvToParquetExtractor(
        connection=connection,
        bronze_root=tmp_path / "bronze"
    )


def test_deve_gerar_parquet(extractor, config, csv_file):
    rows_ok, rows_rejected, output = extractor.extract_file(config, csv_file)

    assert rows_ok == 2
    assert rows_rejected == 0
    assert Path(output).exists()
    assert list((Path(output) / "data").glob("*.parquet"))


def test_deve_preservar_csv_original(extractor, config, csv_file):
    _, _, output = extractor.extract_file(config, csv_file)
    original = Path(output) / "_source" / csv_file.name

    assert original.exists()
    assert original.read_bytes() == csv_file.read_bytes()


def test_deve_manter_colunas_textuais(extractor, config, csv_file, connection):
    _, _, output = extractor.extract_file(config, csv_file)
    parquet = (Path(output) / "data" / "*.parquet").as_posix()

    resultado = connection.execute(
        """
        SELECT nr_documento, vlr_receita_liquida,
               typeof(nr_documento),
               typeof(vlr_receita_liquida)
        FROM read_parquet(?)
        ORDER BY nr_documento
        """,
        [parquet]
    ).fetchall()

    assert resultado[0] == ("001", "1000,00", "VARCHAR", "VARCHAR")
    assert resultado[1] == ("002", "2000,00", "VARCHAR", "VARCHAR")


def test_deve_registrar_metadados(extractor, config, csv_file, connection):
    _, _, output = extractor.extract_file(config, csv_file)
    parquet = (Path(output) / "data" / "*.parquet").as_posix()

    resultado = connection.execute(
        """
        SELECT source_file, run_id, ingestion_ts, source_sha256
        FROM read_parquet(?)
        """,
        [parquet]
    ).fetchall()

    hash_esperado = hashlib.sha256(csv_file.read_bytes()).hexdigest()

    assert len(resultado) == 2
    assert all(row[0] == csv_file.name for row in resultado)
    assert all(row[1] for row in resultado)
    assert all(row[2] is not None for row in resultado)
    assert all(row[3] == hash_esperado for row in resultado)
    assert len({row[1] for row in resultado}) == 1


def test_deve_preservar_versoes_ao_reprocessar(extractor, config, csv_file, connection):
    _, _, primeira_versao = extractor.extract_file(config, csv_file)

    csv_file.write_text(
        "nr_documento;item;vlr_receita_liquida;vlr_custo\n"
        "001;1;1500,00;700,00\n"
        "002;1;2000,00;1500,00\n",
        encoding="utf-8"
    )

    _, _, segunda_versao = extractor.extract_file(config, csv_file)

    assert primeira_versao != segunda_versao
    assert Path(primeira_versao).exists()
    assert Path(segunda_versao).exists()

    def consultar(path):
        parquet = (Path(path) / "data" / "*.parquet").as_posix()
        return connection.execute(
            """
            SELECT vlr_receita_liquida, run_id, source_sha256
            FROM read_parquet(?)
            WHERE nr_documento = '001'
            """,
            [parquet]
        ).fetchone()

    anterior = consultar(primeira_versao)
    atual = consultar(segunda_versao)

    assert anterior[0] == "1000,00"
    assert atual[0] == "1500,00"
    assert anterior[1] != atual[1]
    assert anterior[2] != atual[2]


def test_deve_limpar_staging_em_caso_de_falha(extractor, config, tmp_path):
    arquivo_inexistente = tmp_path / "inexistente.csv"

    with pytest.raises(Exception):
        extractor.extract_file(config, arquivo_inexistente)

    staging = tmp_path / "bronze" / config.name / "_tmp"

    assert not staging.exists() or not list(staging.rglob("*.parquet"))
