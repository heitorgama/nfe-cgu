import duckdb
import pandas as pd
import pytest

from pipeline.silver import (
    converter_colunas_para_snake_case,
    converter_dados,
    separar_cpf_cnpj,
)


@pytest.fixture
def con():
    return duckdb.connect()


def _rel(**kwargs):
    """Helper: cria DuckDBPyRelation a partir de kwargs {col: [valores]}."""
    return duckdb.from_df(pd.DataFrame(kwargs))


def _empty_rel(*cols):
    """Helper: cria DuckDBPyRelation vazia com as colunas especificadas."""
    return duckdb.from_df(pd.DataFrame(columns=list(cols)))


def test_snake_case_espacos_e_acentos(con):
    assert converter_colunas_para_snake_case(_empty_rel("NATUREZA DA OPERAÇÃO"), con).columns[0] == "natureza_da_operacao"


def test_snake_case_caracteres_especiais(con):
    assert converter_colunas_para_snake_case(_empty_rel("DESCRIÇÃO DO PRODUTO/SERVIÇO"), con).columns[0] == "descricao_do_produto_servico"


def test_snake_case_parenteses(con):
    assert converter_colunas_para_snake_case(_empty_rel("NCM/SH (TIPO DE PRODUTO)"), con).columns[0] == "ncm_sh_tipo_de_produto"


def test_snake_case_ja_minusculo(con):
    assert converter_colunas_para_snake_case(_empty_rel("periodo"), con).columns[0] == "periodo"


def test_converter_colunas_para_snake_case_multiplas(con):
    rel = _empty_rel("CHAVE DE ACESSO", "DATA EMISSÃO", "CPF/CNPJ Emitente")
    assert converter_colunas_para_snake_case(rel, con).columns == ["chave_de_acesso", "data_emissao", "cpf_cnpj_emitente"]


def test_converter_dados_str_preserva_asteriscos(con):
    rel = _rel(**{"CPF/CNPJ Emitente": ["***.910.688-**", "12345678000195"]})
    resultado = converter_dados(rel, {"CPF/CNPJ Emitente": "str"}, con).df()
    assert resultado["CPF/CNPJ Emitente"].tolist() == ["***.910.688-**", "12345678000195"]


def test_converter_dados_int64_valor_simples(con):
    rel = _rel(**{"SÉRIE": ["1", "2", "3"]})
    resultado = converter_dados(rel, {"SÉRIE": "int64"}, con).df()
    assert resultado["SÉRIE"].tolist() == [1, 2, 3]


def test_converter_dados_int64_separador_milhar(con):
    rel = _rel(**{"VALOR TOTAL": ["1.234", "56.789"]})
    resultado = converter_dados(rel, {"VALOR TOTAL": "int64"}, con).df()
    assert resultado["VALOR TOTAL"].tolist() == [1234, 56789]


def test_converter_dados_int64_invalido_vira_nulo(con):
    rel = _rel(**{"SÉRIE": ["1", "abc", "3"]})
    resultado = converter_dados(rel, {"SÉRIE": "int64"}, con).df()
    assert pd.isna(resultado["SÉRIE"].iloc[1])


def test_converter_dados_datetime_formato_br(con):
    rel = _rel(**{"DATA EMISSÃO": ["01/03/2026", "15/12/2025"]})
    resultado = converter_dados(rel, {"DATA EMISSÃO": "datetime64[ns]"}, con).df()
    assert resultado["DATA EMISSÃO"].iloc[0] == pd.Timestamp("2026-03-01")
    assert resultado["DATA EMISSÃO"].iloc[1] == pd.Timestamp("2025-12-15")


def test_converter_dados_datetime_invalido_vira_nulo(con):
    rel = _rel(**{"DATA EMISSÃO": ["01/03/2026", "nao_e_data"]})
    resultado = converter_dados(rel, {"DATA EMISSÃO": "datetime64[ns]"}, con).df()
    assert pd.isna(resultado["DATA EMISSÃO"].iloc[1])


def test_converter_dados_cnpj_valor_numerico(con):
    rel = _rel(**{"CNPJ Emitente": ["09094142000130"]})
    resultado = converter_dados(rel, {"CNPJ Emitente": "decimal"}, con).df()
    assert int(resultado["CNPJ Emitente"].iloc[0]) == 9094142000130


def test_converter_dados_cnpj_vazio_vira_nulo(con):
    rel = _rel(**{"CNPJ Emitente": [None, "12345678000195"]})
    resultado = converter_dados(rel, {"CNPJ Emitente": "decimal"}, con).df()
    assert pd.isna(resultado["CNPJ Emitente"].iloc[0])


def test_converter_dados_ignora_coluna_ausente(con):
    rel = _rel(**{"OUTRA COLUNA": ["x"]})
    resultado = converter_dados(rel, {"COLUNA QUE NAO EXISTE": "int64"}, con)
    assert resultado.columns == ["OUTRA COLUNA"]


def test_separar_cpf_cnpj_com_asterisco_vai_para_cpf(con):
    rel = _rel(**{"CPF/CNPJ Emitente": ["***.910.688-**"]})
    resultado = separar_cpf_cnpj(rel, con).df()
    assert resultado["CPF Emitente"].iloc[0] == "***.910.688-**"
    assert pd.isna(resultado["CNPJ Emitente"].iloc[0])


def test_separar_cpf_cnpj_sem_asterisco_vai_para_cnpj(con):
    rel = _rel(**{"CPF/CNPJ Emitente": ["12345678000195"]})
    resultado = separar_cpf_cnpj(rel, con).df()
    assert resultado["CNPJ Emitente"].iloc[0] == "12345678000195"
    assert pd.isna(resultado["CPF Emitente"].iloc[0])


def test_separar_cpf_cnpj_remove_coluna_original(con):
    rel = _rel(**{"CPF/CNPJ Emitente": ["***.910.688-**"]})
    assert "CPF/CNPJ Emitente" not in separar_cpf_cnpj(rel, con).columns


def test_separar_cpf_cnpj_sem_coluna_alvo_nao_falha(con):
    rel = _rel(**{"OUTRA COLUNA": ["x"]})
    assert separar_cpf_cnpj(rel, con).columns == ["OUTRA COLUNA"]


def test_distinct_remove_duplicatas():
    rel = _rel(**{"CHAVE DE ACESSO": ["ABC", "ABC", "DEF"]})
    assert rel.distinct().count("*").fetchone()[0] == 2


def test_distinct_sem_duplicatas_nao_altera():
    rel = _rel(**{"CHAVE DE ACESSO": ["ABC", "DEF"]})
    assert rel.distinct().count("*").fetchone()[0] == 2
