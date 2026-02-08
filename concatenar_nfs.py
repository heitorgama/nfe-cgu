# Cria arquivos parquet a partir dos arquivos zipados de NFes baixados do Portal da Transparência.
# Localização dos arquivos zipados: `/dados/nfe`
#   (conforme especificado na variável DIRETORIO_DADOS).
# Localização dos arquivos extraídos e concatenados, em formato parquet:
#   `/extracoes` (conforme especificado na variável DIRETORIO_EXTRACAO).
# Arquivos pickle também são gerados para facilitar debugging na fase
#   de desenvolvimento, mas não são usados para consultas.
#   O formato parquet é mais eficiente para consultas com DuckDB.
# Consultas aos arquivos parquet podem ser feitas com DuckDB.
#   Veja o arquivo `consultar_nfs.py`, por exemplo.

# TODO
# Renomear colunas para snake_case (alternativa à função customizada:
#   df.columns = [slugify(col, separator="_") for col in df.columns]).
# Adicionar downloader para arquivos faltantes.
# Criar script que faz carga incremental de um ou mais novos arquivos .zip.
# Usar tipo Decimal no arquivo pickle.
# Excluir duplicatas usando último evento (ano de 2023 e anteriores)?
# Excluir notas em que se manifestou desconhecimento da transação
#   por parte do destinatário?
# Marcar CPF, CNPJ e outros IDs em uma nova coluna.
# NCMs: buscar código a partir da descrição dada.
# Particionar os arquivos por ano e mês:
    # sql.duckdb("""
    #     COPY my_table 
    #     TO 'output_dir' 
    #     (FORMAT PARQUET, PARTITION_BY (year, month))
    # """)

from datetime import datetime
import os
import pandas as pd
import re
import shutil
from tqdm import tqdm
import unicodedata
import zipfile

DIRETORIO_DADOS = 'dados/nfe'
DIRETORIO_EXTRACAO = 'extracoes'
TIPOS_DE_DADOS_ITENS = {
    'CHAVE DE ACESSO':                 'str',
    'MODELO':                          'str',
    'SÉRIE':                           'int64',
    'NÚMERO':                          'int64',
    'NATUREZA DA OPERAÇÃO':            'str',
    'DATA EMISSÃO':                    'datetime64[ns]',
    'CPF/CNPJ Emitente':               'str', # String porque CPFs são exibidos no formato ***.910.688-**
    'RAZÃO SOCIAL EMITENTE':           'str',
    'INSCRIÇÃO ESTADUAL EMITENTE':     'int64',
    'UF EMITENTE':                     'str',
    'MUNICÍPIO EMITENTE':              'str',
    'CNPJ DESTINATÁRIO':               'int64',
    'NOME DESTINATÁRIO':               'str',
    'UF DESTINATÁRIO':                 'str',
    'INDICADOR IE DESTINATÁRIO':       'str',
    'DESTINO DA OPERAÇÃO':             'str',
    'CONSUMIDOR FINAL':                'str',
    'PRESENÇA DO COMPRADOR':           'str',
    'NÚMERO PRODUTO':                  'int64',
    'DESCRIÇÃO DO PRODUTO/SERVIÇO':    'str',
    'CÓDIGO NCM/SH':                   'int64',
    'NCM/SH (TIPO DE PRODUTO)':        'str',
    'CFOP':                            'int64',
    'QUANTIDADE':                      'int64',
    'UNIDADE':                         'str',
    'VALOR UNITÁRIO':                  'int64',
    'VALOR TOTAL':                     'int64',
    'periodo':                         'str',
}

TIPOS_DE_DADOS_NF = {
    'CHAVE DE ACESSO':                  'str',
    'MODELO':                           'str',
    'SÉRIE':                            'int64',
    'NÚMERO':                           'int64',
    'NATUREZA DA OPERAÇÃO':             'str',
    'DATA EMISSÃO':                     'datetime64[ns]',
    'EVENTO MAIS RECENTE':              'str',
    'DATA/HORA EVENTO MAIS RECENTE':    'datetime64[ns]',
    'CPF/CNPJ Emitente':                'str', # String porque CPFs são exibidos no formato ***.910.688-**
    'RAZÃO SOCIAL EMITENTE':            'str',
    'INSCRIÇÃO ESTADUAL EMITENTE':      'int64',
    'UF EMITENTE':                      'str',
    'MUNICÍPIO EMITENTE':               'str',
    'CNPJ DESTINATÁRIO':                'int64',
    'NOME DESTINATÁRIO':                'str',
    'UF DESTINATÁRIO':                  'str',
    'INDICADOR IE DESTINATÁRIO':        'str',
    'DESTINO DA OPERAÇÃO':              'str',
    'CONSUMIDOR FINAL':                 'str',
    'PRESENÇA DO COMPRADOR':            'str',
    'VALOR NOTA FISCAL':                'int64',
    'periodo':                          'str',
}

TIPOS_DE_DADOS_EVENTOS = {
    'CHAVE DE ACESSO':                  'str',
    'MODELO':                           'str',
    'SÉRIE':                            'int64',
    'NÚMERO':                           'int64',
    'NATUREZA DA OPERAÇÃO':             'str',
    'DATA EMISSÃO':                     'datetime64[ns]',
    'EVENTO':                           'str',
    'DATA/HORA EVENTO':                 'datetime64[ns]',
    'DESCRIÇÃO EVENTO':                 'str',
    'MOTIVO EVENTO':                    'str',
    'periodo':                          'str',
}

def converter_string_para_snake_case(string):
    """Converte string com espaços, caracteres especiais e letras acentuadas para snake-case."""
    s = string.lower()
    # Remover caracteres acentuados
    s = unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("utf-8")
    # Substituir caracteres não alfanuméricos por espaço
    s = re.sub(r"[^a-z0-9_]", " ", s)
    # Substituir espaços e múltiplos underscores por um único underscore
    s = re.sub(r'[\s_]+', '_', s)
    # Não deixar underscores no início ou no fim
    s = s.strip('_')
    return s

def converter_colunas_para_snake_case(df: pd.DataFrame):
    df.columns = [converter_string_para_snake_case(col) for col in df.columns]
    return df

def formatar_hora_atual():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def imprimir_mensagem(mensagem):
    hora_atual = formatar_hora_atual()
    print(f"[{hora_atual}] {mensagem}")

def listar_arquivos_DIRETORIO_DADOS(DIRETORIO_DADOS):
    try:
        arquivos = os.listdir(DIRETORIO_DADOS)
        return [arquivo for arquivo in arquivos if os.path.isfile(os.path.join(DIRETORIO_DADOS, arquivo))]
    except FileNotFoundError:
        print(f"Diretório {DIRETORIO_DADOS} não encontrado.")
        return []

def mapear_arquivos_e_periodos(DIRETORIO_DADOS):
    arquivos = listar_arquivos_DIRETORIO_DADOS(DIRETORIO_DADOS)
    mapa = {}

    for nome_do_arquivo in arquivos:
        periodo_regex = re.match(r'^\d{6}', nome_do_arquivo)
        if periodo_regex is not None:
            periodo = periodo_regex.group(0)
            mapa[periodo] = nome_do_arquivo
    
    return mapa

def converter_dados(df, tipos_de_dados: dict):
    for coluna, tipo in tipos_de_dados.items():
        if tipo == 'datetime64[ns]':
            df[coluna] = pd.to_datetime(df[coluna], dayfirst=True)
        elif tipo == 'int64':
            if df[coluna].dtype == 'object':
                df[coluna] = df[coluna].astype(str).str.replace(',' , '') # Remover vírgulas antes de converter
            df[coluna] = pd.to_numeric(df[coluna])
        elif tipo == 'str':
            df[coluna] = df[coluna].astype(str)
    return df

def adicionar_coluna_sinal(df, coluna_cfop):
    if coluna_cfop not in df.columns:
        raise ValueError(f"A coluna '{coluna_cfop}' não existe no DataFrame.")
    
    df['sinal'] = df[coluna_cfop].apply(
        lambda x: 1 if str(x).startswith(('5', '6', '7')) else -1
        )
    return df

def main():
    imprimir_mensagem("Iniciando o processamento dos arquivos de notas fiscais...")

    mapa = mapear_arquivos_e_periodos(DIRETORIO_DADOS)

    imprimir_mensagem(f"Arquivos encontrados: {len(mapa)}")

    dfs_nf = []
    dfs_itens = []
    dfs_eventos = []

    for periodo, arquivo in tqdm(mapa.items()):
        imprimir_mensagem(f"Processando arquivo: {arquivo} para o período: {periodo}")

        with zipfile.ZipFile(os.path.join(DIRETORIO_DADOS, arquivo), 'r') as zip_ref:
            zip_ref.extractall(periodo)

        arquivos = os.listdir(periodo)
        if len(arquivos) != 3:
            print(f"Erro: Esperado 3 arquivos, mas encontrado {len(arquivos)} no período {periodo}.")
            continue
        arquivo_itens = [a for a in arquivos if 'item' in a.lower()][0]
        arquivo_eventos = [a for a in arquivos if 'evento' in a.lower()][0]
        arquivo_nf = [a for a in arquivos if a not in [arquivo_itens, arquivo_eventos]][0]

        df_itens = pd.read_csv(os.path.join(periodo, arquivo_itens), sep=';', encoding='windows-1252')
        df_eventos = pd.read_csv(os.path.join(periodo, arquivo_eventos), sep=';', encoding='windows-1252')
        df_nf = pd.read_csv(os.path.join(periodo, arquivo_nf), sep=';', encoding='windows-1252')

        periodo_formatado = periodo[:4] + '-' + periodo[4:6]
        df_itens['periodo'] = periodo_formatado
        df_eventos['periodo'] = periodo_formatado
        df_nf['periodo'] = periodo_formatado

        dfs_itens.append(df_itens)
        dfs_eventos.append(df_eventos)
        dfs_nf.append(df_nf)

        shutil.rmtree(periodo)

    imprimir_mensagem("Todos os arquivos foram processados. Concatenando DataFrames...")

    df_itens = pd.concat(dfs_itens, ignore_index=True)
    df_eventos = pd.concat(dfs_eventos, ignore_index=True)
    df_nf = pd.concat(dfs_nf, ignore_index=True)

    df_itens_convertido = converter_dados(df_itens, TIPOS_DE_DADOS_ITENS)
    df_eventos = converter_dados(df_eventos, TIPOS_DE_DADOS_EVENTOS)
    df_nf_convertido = converter_dados(df_nf, TIPOS_DE_DADOS_NF)

    df_itens_convertido_com_sinal = adicionar_coluna_sinal(df=df_itens_convertido, coluna_cfop='CFOP')

    df_itens_convertido_com_sinal_snake_case = converter_colunas_para_snake_case(df_itens_convertido_com_sinal)
    df_eventos_snake_case = converter_colunas_para_snake_case(df_eventos)
    df_nf_convertido_snake_case = converter_colunas_para_snake_case(df_nf_convertido)

    imprimir_mensagem(f"DataFrames concatenados. Salvando como arquivos Parquet no diretório `{DIRETORIO_EXTRACAO}`...")

    os.makedirs(DIRETORIO_EXTRACAO, exist_ok=True)

    df_itens_convertido_com_sinal_snake_case.to_parquet(os.path.join(DIRETORIO_EXTRACAO, 'itens.parquet'), compression='snappy')
    df_eventos_snake_case.to_parquet(os.path.join(DIRETORIO_EXTRACAO, 'eventos.parquet'), compression='snappy')
    df_nf_convertido_snake_case.to_parquet(os.path.join(DIRETORIO_EXTRACAO, 'nf.parquet'), compression='snappy')

    df_itens_convertido_com_sinal_snake_case.to_pickle('itens.pickle')
    df_eventos_snake_case.to_pickle('eventos.pickle')
    df_nf_convertido_snake_case.to_pickle('nf.pickle')

    imprimir_mensagem("Arquivos Parquet salvos com sucesso. Processamento concluído.")


if __name__ == "__main__":
    main()
