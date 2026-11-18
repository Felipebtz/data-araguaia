@dataclass
class FaturamentoItem:
    nr_documento: str
    item: str
    tipo_documento: str
    data_emissao: date
    data_lancamento: date
    nf_origem: str | None
    cod_cliente: str
    cod_vendedor: str
    nome_vendedor: str
    cod_material: str
    desc_material: str
    grupo_material: str
    quantidade: Decimal
    receita_liquida: Decimal
    custo: Decimal
    unidade: str
    centro: str
    uf: str