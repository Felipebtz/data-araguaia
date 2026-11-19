
from pathlib import Path
from uuid import uuid4
import hashlib
import shutil
import duckdb
from src.domain.bronze_config import DomainConfig


class CsvToParquetExtractor:
    def __init__(self, connection: duckdb.DuckDBPyConnection, bronze_root: Path):
        self._con = connection
        self._bronze_root = bronze_root

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _file_hash(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def extract_file(self, config: DomainConfig, csv_file: Path) -> tuple[int, int, str]:
        filename = csv_file.name
        stem = csv_file.stem
        run_id = str(uuid4())
        tmp_dir = self._bronze_root / config.name / "_tmp" / stem / run_id
        parquet_dir = tmp_dir / "data"
        final_dir = self._bronze_root / config.name / stem / run_id
        reject_name = f"rej_{config.name}_{stem}_{run_id.replace('-', '_')}"
        reject_scan_name = f"scan_{config.name}_{run_id.replace('-', '_')}"
        reject_table = '"' + reject_name.replace('"', '""') + '"'
        reject_scan_table = '"' + reject_scan_name.replace('"', '""') + '"'
        chunk_size = getattr(config, "chunk_size", "64MB")

        self._cleanup(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        encoding = "latin-1" if config.name == "faturamento" else "utf-8"

        try:
            source_dir = tmp_dir / "_source"
            source_dir.mkdir(parents=True, exist_ok=True)
            source_copy = source_dir / filename
            shutil.copy2(csv_file, source_copy)
            source_hash = self._file_hash(source_copy)

            self._con.execute(f"DROP TABLE IF EXISTS {reject_table}")
            self._con.execute(f"DROP TABLE IF EXISTS {reject_scan_table}")

            self._con.execute(f"""
                COPY (
                    SELECT
                        *,
                        {self._sql_literal(filename)} AS source_file,
                        {self._sql_literal(run_id)} AS run_id,
                        CURRENT_TIMESTAMP AS ingestion_ts,
                        {self._sql_literal(source_hash)} AS source_sha256
                    FROM read_csv(
                        {self._sql_literal(source_copy.as_posix())},
                        delim = {self._sql_literal(config.delimiter)},
                        header = true,
                        all_varchar = true,
                        store_rejects = true,
                        rejects_scan = {self._sql_literal(reject_scan_name)},
                        rejects_table = {self._sql_literal(reject_name)},
                        encoding = {self._sql_literal(encoding)}
                    )
                )
                TO {self._sql_literal(parquet_dir.as_posix() + '/')}
                (
                    FORMAT PARQUET,
                    COMPRESSION 'ZSTD',
                    PER_THREAD_OUTPUT true,
                    FILE_SIZE_BYTES {self._sql_literal(chunk_size)}
                );
            """)

            parquet_files = list(parquet_dir.glob("*.parquet"))
            if not parquet_files:
                raise RuntimeError(f"Nenhum Parquet foi gerado para {filename}")

            result = self._con.execute(f"""
                SELECT COUNT(*)
                FROM read_parquet({self._sql_literal(parquet_dir.as_posix() + '/*.parquet')})
            """).fetchone()
            rows_ok = int(result[0]) if result else 0

            exists = self._con.execute("""
                SELECT COUNT(*)
                FROM duckdb_tables()
                WHERE table_name = ?
            """, [reject_name]).fetchone()

            rows_rejected = 0

            if exists and exists[0] > 0:
                result = self._con.execute(f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT DISTINCT scan_id, file_id, line
                        FROM {reject_table}
                    )
                """).fetchone()
                rows_rejected = int(result[0]) if result else 0

                if rows_rejected > 0:
                    quarantine_dir = tmp_dir / "_quarantine"
                    quarantine_dir.mkdir(parents=True, exist_ok=True)
                    self._con.execute(f"""
                        COPY (
                            SELECT
                                *,
                                {self._sql_literal(run_id)} AS run_id,
                                {self._sql_literal(filename)} AS source_file
                            FROM {reject_table}
                        )
                        TO {self._sql_literal((quarantine_dir / 'rejects.parquet').as_posix())}
                        (FORMAT PARQUET);
                    """)

            final_dir.parent.mkdir(parents=True, exist_ok=True)

            if final_dir.exists():
                raise FileExistsError(f"Versão já existente: {final_dir}")

            tmp_dir.rename(final_dir)
            return int(rows_ok), int(rows_rejected), final_dir.as_posix()

        except Exception:
            self._cleanup(tmp_dir)
            raise

        finally:
            self._con.execute(f"DROP TABLE IF EXISTS {reject_table}")
            self._con.execute(f"DROP TABLE IF EXISTS {reject_scan_table}")

    @staticmethod
    def _cleanup(*dirs: Path) -> None:
        for directory in dirs:
            shutil.rmtree(directory, ignore_errors=True)
