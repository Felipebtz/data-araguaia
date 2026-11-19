
from pathlib import Path
import glob
import duckdb
from src.domain.bronze_config import DomainConfig
from src.extract.csv_to_parquet import CsvToParquetExtractor
from src.repository.bronze_audit import BronzeAudit


class BronzePipeline:
    """Orquestrador da camada Bronze: gerencia conexão, auditoria e execução dos Steps."""

    def __init__(self, db_path: str = "pipeline.duckdb", bronze_root: str = "bronze", memory_limit: str = "4GB"):
        self._db_path = db_path
        self._bronze_root = Path(bronze_root)
        self._memory_limit = memory_limit
        self._con: duckdb.DuckDBPyConnection | None = None
        self._audit_repo: BronzeAudit | None = None
        self._extractor: CsvToParquetExtractor | None = None

    def __enter__(self) -> "BronzePipeline":
        """Abre a conexão e inicializa as dependências da Bronze."""
        self._con = duckdb.connect(self._db_path)

        try:
            self._con.execute(f"SET memory_limit = '{self._memory_limit}';")
            self._audit_repo = BronzeAudit(self._con)
            self._extractor = CsvToParquetExtractor(
                connection=self._con,
                bronze_root=self._bronze_root
            )
            return self

        except Exception:
            self._con.close()
            self._con = None
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Garante o fechamento da conexão ao finalizar o Job."""
        if self._con is not None:
            self._con.close()
            self._con = None
            self._audit_repo = None
            self._extractor = None

    def run_step(self, config: DomainConfig) -> None:
        """Executa a ingestão de todos os arquivos CSV de um domínio."""
        if self._con is None or self._audit_repo is None or self._extractor is None:
            raise RuntimeError("BronzePipeline deve ser executado dentro de um bloco 'with'.")

        csv_files = sorted(glob.glob(config.input_pattern))

        if not csv_files:
            raise FileNotFoundError(
                f"Nenhum arquivo encontrado para o padrão: {config.input_pattern}"
            )

        print(f"=== Iniciando Step Bronze: {config.name.upper()} ({len(csv_files)} arquivos) ===")

        for csv_path in csv_files:
            self._process_single_file(config, Path(csv_path))

    def _process_single_file(self, config: DomainConfig, csv_file: Path) -> None:
        """Coordena auditoria e extração de um único arquivo."""
        if self._audit_repo is None or self._extractor is None:
            raise RuntimeError("Dependências da Bronze não inicializadas.")

        filename = csv_file.name

        if self._audit_repo.is_completed(config.name, filename):
            print(f"  [SKIP] {filename} já processado.")
            return

        self._audit_repo.register_start(config.name, filename)

        try:
            rows_ok, rows_rejected, output_dir = self._extractor.extract_file(
                config=config,
                csv_file=csv_file
            )

            self._audit_repo.register_success(
                config.name,
                filename,
                rows_ok,
                rows_rejected,
                output_dir
            )

            print(
                f"  [OK] {filename} -> "
                f"{rows_ok} linhas | "
                f"{rows_rejected} rejeitadas | "
                f"Destino: {output_dir}"
            )

        except Exception as exc:
            self._audit_repo.register_failure(
                config.name,
                filename,
                str(exc)
            )

            raise RuntimeError(
                f"Falha ao processar {filename} no domínio {config.name}: {exc}"
            ) from exc
