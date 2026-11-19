from decimal import Decimal

from comercial.entity.faturamento_item import FaturamentoItem
from comercial.entity.margem_consultor import MargemConsultor
from comercial.domain.margem_domain import MargemDomain


class ApurarMargemConsultor:

    def executar(
        self,
        competencia: str,
        cod_vendedor: str,
        itens: list[FaturamentoItem]
    ) -> MargemConsultor:

        itens_consultor = [
            item for item in itens
            if item.cod_vendedor == cod_vendedor
        ]

        if not itens_consultor:
            raise ValueError(
                "Nenhum faturamento encontrado para o consultor"
            )

        receita = sum(
            (item.receita_liquida for item in itens_consultor),
            Decimal("0")
        )

        custo = sum(
            (item.custo for item in itens_consultor),
            Decimal("0")
        )

        margem = MargemDomain.calcular(
            receita,
            custo
        )

        return MargemConsultor(
            competencia=competencia,
            cod_vendedor=cod_vendedor,
            receita_liquida=receita,
            custo=custo,
            margem=margem
        )