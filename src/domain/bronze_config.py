from dataclasses import dataclass

@dataclass(frozen=True)
class DomainConfig:
    """Representa a configuração de um Step/Domínio de ingestão."""
    name: str
    input_pattern: str
    delimiter: str = ";"
    chunk_size: str = "64MB"