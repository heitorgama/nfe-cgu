import pandas as pd

from concatenar_nfs import converter_string_para_snake_case
from concatenar_nfs import converter_colunas_para_snake_case

def test_converter_para_snake_case():
    assert converter_string_para_snake_case("Teste de Conversão") == "teste_de_conversao"
    assert converter_string_para_snake_case("NATUREZA DA OPERAÇÃO") == "natureza_da_operacao"
    assert converter_string_para_snake_case("DESCRIÇÃO DO PRODUTO/SERVIÇO") == "descricao_do_produto_servico"
    assert converter_string_para_snake_case("NCM/SH (TIPO DE PRODUTO)") == "ncm_sh_tipo_de_produto"

def test_converter_colunas_para_snake_case():
    df = pd.DataFrame(columns=["Teste de Conversão", "NATUREZA DA OPERAÇÃO", "DESCRIÇÃO DO PRODUTO/SERVIÇO"])
    df = converter_colunas_para_snake_case(df)
    assert df.columns.tolist() == ["teste_de_conversao", "natureza_da_operacao", "descricao_do_produto_servico"]