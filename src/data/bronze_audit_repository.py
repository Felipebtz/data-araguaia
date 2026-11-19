import duckdb


class BronzeAudit:
    """Gerenciar estados da tabela"""

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self._con = connection
        self._init_schema()

    def _init_schema(self) -> None:
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS bronze_control (
                dominio VARCHAR,
                arquivo VARCHAR,
                status VARCHAR,
                linhas_parquet BIGINT,
                linhas_rejeitadas BIGINT,
                destino VARCHAR,
                erro VARCHAR,
                atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (dominio, arquivo)
            )
        """)

    def is_completed(self, domain: str, filename: str) -> bool:
        res = self._con.execute(
            "SELECT 1 FROM bronze_control WHERE dominio = ? AND arquivo = ? AND status = 'COMPLETED'",
            [domain, filename]
        ).fetchone()
        return res is not None

    def register_start(self, domain: str, filename: str) -> None:
        self._con.execute("""
            INSERT OR REPLACE INTO bronze_control 
            (dominio, arquivo, status, linhas_parquet, linhas_rejeitadas, destino, erro, atualizado_em)
            VALUES (?, ?, 'STARTED', 0, 0, NULL, NULL, CURRENT_TIMESTAMP)
        """, [domain, filename])

    def register_success(self, domain: str, filename: str, rows_ok: int, rows_rejected: int, output_dir: str) -> None:
        self._con.execute("""
            UPDATE bronze_control
            SET status = 'COMPLETED',
                linhas_parquet = ?,
                linhas_rejeitadas = ?,
                destino = ?,
                atualizado_em = CURRENT_TIMESTAMP
            WHERE dominio = ? AND arquivo = ?
        """, [rows_ok, rows_rejected, output_dir, domain, filename])

    def register_failure(self, domain: str, filename: str, error_msg: str) -> None:
        self._con.execute("""
            UPDATE bronze_control
            SET status = 'FAILED',
                erro = ?,
                atualizado_em = CURRENT_TIMESTAMP
            WHERE dominio = ? AND arquivo = ?
        """, [error_msg, domain, filename])
