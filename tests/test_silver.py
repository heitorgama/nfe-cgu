import pandas as pd
import pyarrow as pa
from pipeline.silver import (
    converter_colunas_para_snake_case,
    converter_dados,
    deduplicar_nf,
    TIPO_DECIMAL_CNPJ,
)


def test_snake_case_espacos_e_acentos():
    df = pd.DataFrame(columns=["NATUREZA DA OPERAÇÃO"])
    assert converter_colunas_para_snake_case(df).columns[0] == "natureza_da_operacao"


def test_snake_case_caracteres_especiais():
    df = pd.DataFrame(columns=["DESCRIÇÃO DO PRODUTO/SERVIÇO"])
    assert converter_colunas_para_snake_case(df).columns[0] == "descricao_do_produto_servico"


def test_snake_case_parenteses():
    df = pd.DataFrame(columns=["NCM/SH (TIPO DE PRODUTO)"])
    assert converter_colunas_para_snake_case(df).columns[0] == "ncm_sh_tipo_de_produto"


def test_snake_case_ja_minusculo():
    df = pd.DataFrame(columns=["periodo"])
    assert converter_colunas_para_snake_case(df).columns[0] == "periodo"


def test_converter_colunas_para_snake_case():
    df = pd.DataFrame(columns=["CHAVE DE ACESSO", "DATA EMISSÃO", "CPF/CNPJ Emitente"])
    df = converter_colunas_para_snake_case(df)
    assert df.columns.tolist() == ["chave_de_acesso", "data_emissao", "cpf_cnpj_emitente"]


def test_converter_dados_str_preserva_asteriscos():
    df = pd.DataFrame({"CPF/CNPJ Emitente": ["***.910.688-**", "12345678000195"]})
    resultado = converter_dados(df, {"CPF/CNPJ Emitente": "str"})
    assert resultado["CPF/CNPJ Emitente"].tolist() == ["***.910.688-**", "12345678000195"]


def test_converter_dados_int64_valor_simples():
    df = pd.DataFrame({"SÉRIE": ["1", "2", "3"]})
    resultado = converter_dados(df, {"SÉRIE": "int64"})
    assert resultado["SÉRIE"].tolist() == [1.0, 2.0, 3.0]


def test_converter_dados_int64_separador_milhar():
    df = pd.DataFrame({"VALOR TOTAL": ["1.234", "56.789"]})
    resultado = converter_dados(df, {"VALOR TOTAL": "int64"})
    assert resultado["VALOR TOTAL"].tolist() == [1234.0, 56789.0]


def test_converter_dados_int64_invalido_vira_nan():
    df = pd.DataFrame({"SÉRIE": ["1", "abc", "3"]})
    resultado = converter_dados(df, {"SÉRIE": "int64"})
    assert pd.isna(resultado["SÉRIE"].iloc[1])


def test_converter_dados_datetime_formato_br():
    df = pd.DataFrame({"DATA EMISSÃO": ["01/03/2026", "15/12/2025"]})
    resultado = converter_dados(df, {"DATA EMISSÃO": "datetime64[ns]"})
    assert resultado["DATA EMISSÃO"].iloc[0] == pd.Timestamp("2026-03-01")
    assert resultado["DATA EMISSÃO"].iloc[1] == pd.Timestamp("2025-12-15")


def test_converter_dados_datetime_invalido_vira_nat():
    df = pd.DataFrame({"DATA EMISSÃO": ["01/03/2026", "nao_e_data"]})
    resultado = converter_dados(df, {"DATA EMISSÃO": "datetime64[ns]"})
    assert pd.isna(resultado["DATA EMISSÃO"].iloc[1])


def test_converter_dados_cnpj_dtype_decimal():
    df = pd.DataFrame({"CNPJ Emitente": ["09094142000130"]})
    resultado = converter_dados(df, {"CNPJ Emitente": "decimal"})
    assert resultado["CNPJ Emitente"].dtype == TIPO_DECIMAL_CNPJ


def test_converter_dados_cnpj_valor_numerico():
    df = pd.DataFrame({"CNPJ Emitente": ["09094142000130"]})
    resultado = converter_dados(df, {"CNPJ Emitente": "decimal"})
    assert int(resultado["CNPJ Emitente"].iloc[0]) == 9094142000130


def test_converter_dados_cnpj_vazio_vira_nulo():
    df = pd.DataFrame({"CNPJ Emitente": [None, "12345678000195"]})
    resultado = converter_dados(df, {"CNPJ Emitente": "decimal"})
    assert pd.isna(resultado["CNPJ Emitente"].iloc[0])


def test_converter_dados_ignora_coluna_ausente():
    df = pd.DataFrame({"OUTRA COLUNA": ["x"]})
    resultado = converter_dados(df, {"COLUNA QUE NAO EXISTE": "int64"})
    assert list(resultado.columns) == ["OUTRA COLUNA"]


def test_deduplicar_nf_sem_duplicatas_nao_altera():
    df = pd.DataFrame({
        'CHAVE DE ACESSO': ['ABC', 'DEF'],
        'DATA/HORA EVENTO MAIS RECENTE': [pd.Timestamp('2024-01-01'), pd.Timestamp('2024-02-01')],
    })
    assert len(deduplicar_nf(df)) == 2


def test_deduplicar_nf_sem_colunas_retorna_intacto():
    df = pd.DataFrame({'OUTRA': [1, 2]})
    assert len(deduplicar_nf(df)) == 2
