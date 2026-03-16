import pandas as pd

from pipeline.curation import adicionar_tipo_pessoa


def _df(**kwargs):
    return pd.DataFrame(kwargs)


def test_cnpj_preenchido_emitente_eh_pj():
    df = _df(cnpj_emitente=[12345678000195], cpf_emitente=[None])
    resultado = adicionar_tipo_pessoa(df)
    assert resultado['tipo_emitente'].iloc[0] == 'PJ'


def test_cpf_preenchido_emitente_eh_pf():
    df = _df(cnpj_emitente=[None], cpf_emitente=['***910688**'])
    resultado = adicionar_tipo_pessoa(df)
    assert resultado['tipo_emitente'].iloc[0] == 'PF'


def test_ambos_nulos_emitente_eh_nulo():
    df = _df(cnpj_emitente=[None], cpf_emitente=[None])
    resultado = adicionar_tipo_pessoa(df)
    assert pd.isna(resultado['tipo_emitente'].iloc[0])


def test_cnpj_preenchido_destinatario_eh_pj():
    df = _df(cnpj_destinatario=[98765432000111], cpf_destinatario=[None])
    resultado = adicionar_tipo_pessoa(df)
    assert resultado['tipo_destinatario'].iloc[0] == 'PJ'


def test_cpf_preenchido_destinatario_eh_pf():
    df = _df(cnpj_destinatario=[None], cpf_destinatario=['***123456**'])
    resultado = adicionar_tipo_pessoa(df)
    assert resultado['tipo_destinatario'].iloc[0] == 'PF'


def test_ambos_nulos_destinatario_eh_nulo():
    df = _df(cnpj_destinatario=[None], cpf_destinatario=[None])
    resultado = adicionar_tipo_pessoa(df)
    assert pd.isna(resultado['tipo_destinatario'].iloc[0])


def test_multiplas_linhas_misto():
    df = _df(
        cnpj_emitente=[12345678000195, None,          None],
        cpf_emitente= [None,           '***910688**', None],
    )
    resultado = adicionar_tipo_pessoa(df)
    assert resultado['tipo_emitente'].tolist() == ['PJ', 'PF', pd.NA]


def test_colunas_ausentes_nao_falha():
    df = pd.DataFrame({'outra_coluna': [1, 2]})
    resultado = adicionar_tipo_pessoa(df)
    assert 'tipo_emitente' not in resultado.columns
    assert 'tipo_destinatario' not in resultado.columns
