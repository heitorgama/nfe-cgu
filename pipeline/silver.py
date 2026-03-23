import os
from datetime import datetime
from pathlib import Path

import duckdb
from slugify import slugify
from tqdm import tqdm

BRONZE_DB = 'extracoes/bronze/bronze.duckdb'
SILVER_DB = 'extracoes/silver/silver.duckdb'
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


def separar_cpf_cnpj(rel: duckdb.DuckDBPyRelation, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Separa a coluna CPF/CNPJ em duas colunas distintas com base na presença de ***"""
    pares = [
        ('CPF/CNPJ Emitente', 'CPF Emitente', 'CNPJ Emitente')
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
            expr = f"TRY_CAST(REPLACE(REPLACE(CAST(\"{c}\" AS VARCHAR), '.', ''), ',', '.') AS BIGINT)"
        elif tipo == 'decimal':
            expr = f"TRY_CAST(NULLIF(REGEXP_REPLACE(CAST(\"{c}\" AS VARCHAR), '\\D', '', 'g'), '') AS DECIMAL(14,0))"
        else:
            expr = f'"{c}"'
        select_parts.append(f'{expr} AS "{c}"')
    return con.sql(f'SELECT {", ".join(select_parts)} FROM rel')


def converter_colunas_para_snake_case(rel: duckdb.DuckDBPyRelation, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
    """Renomeia todas as colunas da relation para snake_case."""
    select_parts = [f'"{c}" AS {slugify(c, separator="_")}' for c in rel.columns]
    return con.sql(f'SELECT {", ".join(select_parts)} FROM rel')


def periodos_no_silver(con: duckdb.DuckDBPyConnection, nome: str) -> set[str]:
    """Retorna o conjunto de períodos já presentes na tabela silver"""
    try:
        return {r[0] for r in con.execute(f"SELECT DISTINCT periodo FROM {nome}").fetchall()}
    except duckdb.CatalogException:
        return set()


def inserir_no_silver(con: duckdb.DuckDBPyConnection, nome: str, rel: duckdb.DuckDBPyRelation) -> None:
    """Insere relation na tabela silver, criando se não existir"""
    try:
        con.execute(f"INSERT INTO {nome} SELECT * FROM rel")
    except duckdb.CatalogException:
        con.execute(f"CREATE TABLE {nome} AS SELECT * FROM rel")


def exportar_parquets(con: duckdb.DuckDBPyConnection, tabelas: list, memoria: str = '6GB') -> None:
    """Exporta todas as tabelas silver para parquets individuais"""
    con.execute(f"SET memory_limit='{memoria}'")
    for nome, _ in tabelas:
        caminho = Path(DIRETORIO_SILVER, f'{nome}.parquet').as_posix()
        try:
            con.execute(f"COPY {nome} TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
        except duckdb.CatalogException:
            pass
    con.execute("RESET memory_limit")


def main():
    """Lê bronze por período, converte tipos, deduplica e salva em silver (duckdb + parquet)"""
    os.makedirs(DIRETORIO_SILVER, exist_ok=True)
    con = duckdb.connect(SILVER_DB)
    con.execute(f"ATTACH '{BRONZE_DB}' AS bronze (READ_ONLY)")

    tabelas = [
        ('itens', TIPOS_DE_DADOS_ITENS),
        ('nf', TIPOS_DE_DADOS_NF),
        ('eventos', TIPOS_DE_DADOS_EVENTOS),
    ]

    for nome, tipos in tqdm(tabelas, desc='tabelas', unit='tabela'):
        ja_no_silver = periodos_no_silver(con, nome)
        periodos = [
            r[0] for r in con.execute(
                f"SELECT DISTINCT periodo FROM bronze.{nome} ORDER BY periodo"
            ).fetchall()
            if r[0] not in ja_no_silver
        ]

        if not periodos:
            tqdm.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {nome}: já atualizado.")
            continue

        for periodo in tqdm(periodos, desc=nome, unit='período', leave=False):
            rel = con.sql(f"SELECT * FROM bronze.{nome} WHERE periodo = '{periodo}'")
            rel = separar_cpf_cnpj(rel, con)
            rel = converter_dados(rel, tipos, con)
            if nome in ('itens', 'nf'):
                rel = rel.distinct()
            rel = converter_colunas_para_snake_case(rel, con)
            # Remove item inválido, após consulta manual no portal eletrônico da CGU
            rel = con.sql("SELECT * FROM rel WHERE chave_de_acesso != '12250804034484000140558910000282551122697618'")
            inserir_no_silver(con, nome, rel)

        count = con.execute(f"SELECT COUNT(*) FROM {nome}").fetchone()[0]
        tqdm.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {nome}: {count} linhas.")

    tqdm.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Exportando parquets...")
    exportar_parquets(con, tabelas)
    con.close()
    tqdm.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Silver salvo.")


if __name__ == "__main__":
    main()
