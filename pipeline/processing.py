import os
from datetime import datetime

import pandas as pd
import pyarrow as pa
from slugify import slugify

DIRETORIO_BRONZE = 'extracoes/bronze'
DIRETORIO_SILVER = 'extracoes/silver'

TIPOS_DE_DADOS_ITENS = {
    'CHAVE DE ACESSO':                 'str',
    'MODELO':                          'str',
    'SÉRIE':                           'int64',
    'NÚMERO':                          'int64',
    'NATUREZA DA OPERAÇÃO':            'str',
    'DATA EMISSÃO':                    'datetime64[ns]',
    'CPF Emitente':                    'str',
    'CNPJ Emitente':                   'decimal',
    'RAZÃO SOCIAL EMITENTE':           'str',
    'INSCRIÇÃO ESTADUAL EMITENTE':     'int64',
    'UF EMITENTE':                     'str',
    'MUNICÍPIO EMITENTE':              'str',
    'CPF Destinatário':                'str',
    'CNPJ Destinatário':               'decimal',
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
    'CPF Emitente':                     'str',
    'CNPJ Emitente':                    'decimal',
    'RAZÃO SOCIAL EMITENTE':            'str',
    'INSCRIÇÃO ESTADUAL EMITENTE':      'int64',
    'UF EMITENTE':                      'str',
    'MUNICÍPIO EMITENTE':               'str',
    'CPF Destinatário':                 'str',
    'CNPJ Destinatário':                'decimal',
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

TIPO_DECIMAL_CNPJ = pd.ArrowDtype(pa.decimal128(14, 0))


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


# Brief: converte colunas do DataFrame para os tipos definidos em tipos_de_dados
def converter_dados(df: pd.DataFrame, tipos_de_dados: dict) -> pd.DataFrame:
    for coluna, tipo in tipos_de_dados.items():
        if coluna not in df.columns:
            continue
        if tipo == 'datetime64[ns]':
            df[coluna] = pd.to_datetime(df[coluna], dayfirst=True, errors='coerce')
        elif tipo == 'int64':
            s = df[coluna].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[coluna] = pd.to_numeric(s, errors='coerce')
        elif tipo == 'decimal':
            s = df[coluna].astype(str).str.replace(r'\D', '', regex=True)
            s = s.where(s != '', other=None)
            df[coluna] = s.astype(TIPO_DECIMAL_CNPJ)
        elif tipo == 'str':
            df[coluna] = df[coluna].astype(str)
    return df


# Brief: renomeia todas as colunas do DataFrame para snake_case
def converter_colunas_para_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [slugify(col, separator='_') for col in df.columns]
    return df


# Brief: remove duplicatas de itens mantendo uma linha por chave de acesso + número do produto
def deduplicar_itens(df: pd.DataFrame) -> pd.DataFrame:
    cols = ['CHAVE DE ACESSO', 'NÚMERO PRODUTO']
    if not all(c in df.columns for c in cols):
        return df
    return df.drop_duplicates(subset=cols, keep='first').reset_index(drop=True)


# Brief: remove duplicatas de nf mantendo a linha com o evento mais recente por chave de acesso
def deduplicar_nf(df: pd.DataFrame) -> pd.DataFrame:
    col_chave = 'CHAVE DE ACESSO'
    col_data = 'DATA/HORA EVENTO MAIS RECENTE'
    if col_chave not in df.columns or col_data not in df.columns:
        return df
    return (
        df.sort_values(col_data, ascending=False)
          .drop_duplicates(subset=col_chave, keep='first')
          .reset_index(drop=True)
    )


# Brief: lê parquet do bronze, converte tipos, deduplica e salva em silver
def main():
    os.makedirs(DIRETORIO_SILVER, exist_ok=True)

    tabelas = [
        ('itens', TIPOS_DE_DADOS_ITENS),
        ('nf', TIPOS_DE_DADOS_NF),
        ('eventos', TIPOS_DE_DADOS_EVENTOS),
    ]
    for nome, tipos in tabelas:
        df = pd.read_parquet(os.path.join(DIRETORIO_BRONZE, f'{nome}.parquet'))
        df = converter_dados(df, tipos)
        if nome == 'itens':
            df = deduplicar_itens(df)
        elif nome == 'nf':
            df = deduplicar_nf(df)
        df = converter_colunas_para_snake_case(df)
        df.to_parquet(os.path.join(DIRETORIO_SILVER, f'{nome}.parquet'), compression='snappy')
        imprimir_mensagem(f"{nome}: {len(df)} linhas.")

    imprimir_mensagem("Silver salvo.")


if __name__ == "__main__":
    main()
