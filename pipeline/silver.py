import os
from datetime import datetime
from pathlib import Path

import duckdb
from slugify import slugify

DIRETORIO_BRONZE = 'extracoes/bronze'
DIRETORIO_SILVER = 'extracoes/silver'
SILVER_DB = 'extracoes/silver/silver.duckdb'

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


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


def separar_cpf_cnpj(rel: duckdb.DuckDBPyRelation, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Separa a coluna CPF/CNPJ em duas colunas distintas com base na presença de ***"""
    pares = [
        ('CPF/CNPJ Emitente', 'CPF Emitente', 'CNPJ Emitente'),
        ('CNPJ DESTINATÁRIO', 'CPF Destinatário', 'CNPJ Destinatário'),
    ]
    for col_orig, col_cpf, col_cnpj in pares:
        if col_orig not in rel.columns:
            continue
        select_parts = []
        for c in rel.columns:
            if c == col_orig:
                select_parts.append(f"CASE WHEN \"{c}\" LIKE '%*%' THEN \"{c}\" END AS \"{col_cpf}\"")
                select_parts.append(f"CASE WHEN \"{c}\" NOT LIKE '%*%' THEN \"{c}\" END AS \"{col_cnpj}\"")
            else:
                select_parts.append(f'"{c}"')
        rel = con.sql(f'SELECT {", ".join(select_parts)} FROM rel')
    return rel


def converter_dados(rel: duckdb.DuckDBPyRelation, tipos_de_dados: dict, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Converte colunas da relation para os tipos definidos em tipos_de_dados"""
    select_parts = []
    for c in rel.columns:
        tipo = tipos_de_dados.get(c)
        if tipo == 'datetime64[ns]':
            expr = (
                f"COALESCE("
                f"TRY_STRPTIME(\"{c}\", '%d/%m/%Y %H:%M:%S'), "
                f"TRY_STRPTIME(\"{c}\", '%d/%m/%Y')"
                f")"
            )
        elif tipo == 'int64':
            expr = f"TRY_CAST(REPLACE(REPLACE(\"{c}\", '.', ''), ',', '.') AS BIGINT)"
        elif tipo == 'decimal':
            expr = f"TRY_CAST(NULLIF(REGEXP_REPLACE(\"{c}\", '\\D', '', 'g'), '') AS DECIMAL(14,0))"
        else:
            expr = f'"{c}"'
        select_parts.append(f'{expr} AS "{c}"')
    return con.sql(f'SELECT {", ".join(select_parts)} FROM rel')


def converter_colunas_para_snake_case(rel: duckdb.DuckDBPyRelation, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Renomeia todas as colunas da relation para snake_case."""
    select_parts = [f'"{c}" AS {slugify(c, separator="_")}' for c in rel.columns]
    return con.sql(f'SELECT {", ".join(select_parts)} FROM rel')


def main():
    """Lê parquet do bronze, converte tipos, deduplica e salva em silver (parquet + duckdb)"""
    os.makedirs(DIRETORIO_SILVER, exist_ok=True)
    con = duckdb.connect(SILVER_DB)

    tabelas = [
        ('itens', TIPOS_DE_DADOS_ITENS),
        ('nf', TIPOS_DE_DADOS_NF),
        ('eventos', TIPOS_DE_DADOS_EVENTOS),
    ]
    for nome, tipos in tabelas:
        caminho_bronze = Path(DIRETORIO_BRONZE, f'{nome}.parquet').as_posix()
        rel = con.sql(f"SELECT * FROM read_parquet('{caminho_bronze}')")
        rel = separar_cpf_cnpj(rel, con)
        rel = converter_dados(rel, tipos, con)
        if nome in ('itens', 'nf'):
            rel = rel.distinct()
        rel = converter_colunas_para_snake_case(rel, con)
        con.sql(f"CREATE OR REPLACE TABLE {nome} AS SELECT * FROM rel")
        caminho_silver = Path(DIRETORIO_SILVER, f'{nome}.parquet').as_posix()
        con.execute(f"COPY {nome} TO '{caminho_silver}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
        count = con.execute(f"SELECT COUNT(*) FROM {nome}").fetchone()[0]
        imprimir_mensagem(f"{nome}: {count} linhas.")

    con.close()
    imprimir_mensagem("Silver salvo.")


if __name__ == "__main__":
    main()
