from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class MargemConsultor:
    competencia: str
    cod_vendedor: str
    receita_liquida: Decimal
    custo: Decimal
    margem: Decimal