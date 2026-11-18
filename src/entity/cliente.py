@dataclass
class Cliente:
    cod_cliente: str
    nome_cliente: str
    tipo_pessoa: str
    cpf_cnpj: str
    municipio: str
    uf: str
    segmento: str
    cod_vendedor_atual: str
    data_atribuicao_vendedor: date | None

    