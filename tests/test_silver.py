import os
import duckdb
import pandas as pd
import pytest

from pipeline.silver import (
    converter_colunas_para_snake_case,
    converter_dados,
    exportar_parquets,
    inserir_no_silver,
    periodos_no_silver,
    separar_cpf_cnpj,
)


@pytest.fixture
def con():
    return duckdb.connect()


def _rel(con, **kwargs):
    """Helper: cria DuckDBPyRelation na conexão con a partir de kwargs {col: [valores]}."""
    return con.from_df(pd.DataFrame(kwargs))


def _empty_rel(con, *cols):
    """Helper: cria DuckDBPyRelation vazia na conexão con com as colunas especificadas."""
    return con.from_df(pd.DataFrame(columns=list(cols)))


def test_snake_case_espacos_e_acentos(con):
    assert converter_colunas_para_snake_case(_empty_rel(con, "NATUREZA DA OPERAÇÃO"), con).columns[0] == "natureza_da_operacao"


def test_snake_case_caracteres_especiais(con):
    assert converter_colunas_para_snake_case(_empty_rel(con, "DESCRIÇÃO DO PRODUTO/SERVIÇO"), con).columns[0] == "descricao_do_produto_servico"


def test_snake_case_parenteses(con):
    assert converter_colunas_para_snake_case(_empty_rel(con, "NCM/SH (TIPO DE PRODUTO)"), con).columns[0] == "ncm_sh_tipo_de_produto"


def test_snake_case_ja_minusculo(con):
    assert converter_colunas_para_snake_case(_empty_rel(con, "periodo"), con).columns[0] == "periodo"


def test_converter_colunas_para_snake_case_multiplas(con):
    rel = _empty_rel(con, "CHAVE DE ACESSO", "DATA EMISSÃO", "CPF/CNPJ Emitente")
    assert converter_colunas_para_snake_case(rel, con).columns == ["chave_de_acesso", "data_emissao", "cpf_cnpj_emitente"]


def test_converter_dados_str_preserva_asteriscos(con):
    rel = _rel(con, **{"CPF/CNPJ Emitente": ["***.910.688-**", "12345678000195"]})
    resultado = converter_dados(rel, {"CPF/CNPJ Emitente": "str"}, con).df()
    assert resultado["CPF/CNPJ Emitente"].tolist() == ["***.910.688-**", "12345678000195"]


def test_converter_dados_int64_valor_simples(con):
    rel = _rel(con, **{"SÉRIE": ["1", "2", "3"]})
    resultado = converter_dados(rel, {"SÉRIE": "int64"}, con).df()
    assert resultado["SÉRIE"].tolist() == [1, 2, 3]


def test_converter_dados_int64_separador_milhar(con):
    rel = _rel(con, **{"VALOR TOTAL": ["1.234", "56.789"]})
    resultado = converter_dados(rel, {"VALOR TOTAL": "int64"}, con).df()
    assert resultado["VALOR TOTAL"].tolist() == [1234, 56789]


def test_converter_dados_int64_invalido_vira_nulo(con):
    rel = _rel(con, **{"SÉRIE": ["1", "abc", "3"]})
    resultado = converter_dados(rel, {"SÉRIE": "int64"}, con).df()
    assert pd.isna(resultado["SÉRIE"].iloc[1])


def test_converter_dados_datetime_formato_br(con):
    rel = _rel(con, **{"DATA EMISSÃO": ["01/03/2026", "15/12/2025"]})
    resultado = converter_dados(rel, {"DATA EMISSÃO": "datetime64[ns]"}, con).df()
    assert resultado["DATA EMISSÃO"].iloc[0] == pd.Timestamp("2026-03-01")
    assert resultado["DATA EMISSÃO"].iloc[1] == pd.Timestamp("2025-12-15")


def test_converter_dados_datetime_invalido_vira_nulo(con):
    rel = _rel(con, **{"DATA EMISSÃO": ["01/03/2026", "nao_e_data"]})
    resultado = converter_dados(rel, {"DATA EMISSÃO": "datetime64[ns]"}, con).df()
    assert pd.isna(resultado["DATA EMISSÃO"].iloc[1])


def test_converter_dados_cnpj_valor_numerico(con):
    rel = _rel(con, **{"CNPJ Emitente": ["09094142000130"]})
    resultado = converter_dados(rel, {"CNPJ Emitente": "decimal"}, con).df()
    assert int(resultado["CNPJ Emitente"].iloc[0]) == 9094142000130


def test_converter_dados_cnpj_vazio_vira_nulo(con):
    rel = _rel(con, **{"CNPJ Emitente": [None, "12345678000195"]})
    resultado = converter_dados(rel, {"CNPJ Emitente": "decimal"}, con).df()
    assert pd.isna(resultado["CNPJ Emitente"].iloc[0])


def test_converter_dados_ignora_coluna_ausente(con):
    rel = _rel(con, **{"OUTRA COLUNA": ["x"]})
    resultado = converter_dados(rel, {"COLUNA QUE NAO EXISTE": "int64"}, con)
    assert resultado.columns == ["OUTRA COLUNA"]


def test_separar_cpf_cnpj_com_asterisco_vai_para_cpf(con):
    rel = _rel(con, **{"CPF/CNPJ Emitente": ["***.910.688-**"]})
    resultado = separar_cpf_cnpj(rel, con).df()
    assert resultado["CPF Emitente"].iloc[0] == "***.910.688-**"
    assert pd.isna(resultado["CNPJ Emitente"].iloc[0])


def test_separar_cpf_cnpj_sem_asterisco_vai_para_cnpj(con):
    rel = _rel(con, **{"CPF/CNPJ Emitente": ["12345678000195"]})
    resultado = separar_cpf_cnpj(rel, con).df()
    assert resultado["CNPJ Emitente"].iloc[0] == "12345678000195"
    assert pd.isna(resultado["CPF Emitente"].iloc[0])


def test_separar_cpf_cnpj_remove_coluna_original(con):
    rel = _rel(con, **{"CPF/CNPJ Emitente": ["***.910.688-**"]})
    assert "CPF/CNPJ Emitente" not in separar_cpf_cnpj(rel, con).columns


def test_separar_cpf_cnpj_sem_coluna_alvo_nao_falha(con):
    rel = _rel(con, **{"OUTRA COLUNA": ["x"]})
    assert separar_cpf_cnpj(rel, con).columns == ["OUTRA COLUNA"]


def test_periodos_no_silver_sem_tabela_retorna_vazio(con):
    assert periodos_no_silver(con, 'itens') == set()


def test_periodos_no_silver_retorna_periodos_existentes(con):
    con.execute("CREATE TABLE itens (periodo VARCHAR)")
    con.execute("INSERT INTO itens VALUES ('2024-01'), ('2024-02'), ('2024-01')")
    assert periodos_no_silver(con, 'itens') == {'2024-01', '2024-02'}


def test_inserir_no_silver_cria_tabela_na_primeira_vez(con):
    rel = _rel(con, **{"chave": ["A", "B"], "periodo": ["2024-01", "2024-01"]})
    inserir_no_silver(con, 'itens', rel)
    assert con.execute("SELECT COUNT(*) FROM itens").fetchone()[0] == 2


def test_inserir_no_silver_acumula_em_chamadas_subsequentes(con):
    rel1 = _rel(con, **{"chave": ["A"], "periodo": ["2024-01"]})
    rel2 = _rel(con, **{"chave": ["B"], "periodo": ["2024-02"]})
    inserir_no_silver(con, 'itens', rel1)
    inserir_no_silver(con, 'itens', rel2)
    assert con.execute("SELECT COUNT(*) FROM itens").fetchone()[0] == 2


def test_exportar_parquets_gera_arquivo(con, tmp_path, monkeypatch):
    con.execute("CREATE TABLE itens (chave VARCHAR, periodo VARCHAR)")
    con.execute("INSERT INTO itens VALUES ('A', '2024-01')")
    monkeypatch.chdir(tmp_path)
    os.makedirs('extracoes/silver', exist_ok=True)
    exportar_parquets(con, [('itens', {})])
    assert (tmp_path / 'extracoes/silver/itens.parquet').exists()


def test_exportar_parquets_ignora_tabela_inexistente(con, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs('extracoes/silver', exist_ok=True)
    exportar_parquets(con, [('nao_existe', {})])
    assert not (tmp_path / 'extracoes/silver/nao_existe.parquet').exists()


def test_converter_dados_datetime_com_hora(con):
    rel = _rel(con, **{"DATA EMISSÃO": ["01/03/2026 14:30:00"]})
    resultado = converter_dados(rel, {"DATA EMISSÃO": "datetime64[ns]"}, con).df()
    assert resultado["DATA EMISSÃO"].iloc[0] == pd.Timestamp("2026-03-01 14:30:00")


def test_converter_dados_decimal_mascarado_vira_nulo(con):
    rel = _rel(con, **{"CNPJ Emitente": ["***.***.***/****-**"]})
    resultado = converter_dados(rel, {"CNPJ Emitente": "decimal"}, con).df()
    assert pd.isna(resultado["CNPJ Emitente"].iloc[0])


def test_separar_cpf_cnpj_linhas_mistas(con):
    rel = _rel(con, **{"CPF/CNPJ Emitente": ["***.910.688-**", "12345678000195"]})
    resultado = separar_cpf_cnpj(rel, con).df()
    assert resultado["CPF Emitente"].iloc[0] == "***.910.688-**"
    assert pd.isna(resultado["CPF Emitente"].iloc[1])
    assert pd.isna(resultado["CNPJ Emitente"].iloc[0])
    assert resultado["CNPJ Emitente"].iloc[1] == "12345678000195"
