from decimal import Decimal
class MargemDomain:  
    @staticmethod

    def calcular (receita: Decimal, custo: Decimal) -> Decimal | None:

        if receita == 0 :
            raise ValueError("receita não pode ser 0")

        return (receita - custo) / receita * 100
            
