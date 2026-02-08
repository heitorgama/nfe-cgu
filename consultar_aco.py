import duckdb
import pandas as pd

ARQUIVO_FINAL='aco/aco_totais_por_grupo_e_orgao' # Com diretório, sem extensão

# DEMANDA:
# Para os itens mapeados, dados dos últimos 12 meses
# Métricas: valor total, volume
# Dimensões: NCM, grupo, órgão comprador

ncm_4=['7216', '7228', '7214', '7222', '7215', '7217', '7213', '7227', '7229', '7218', '7206', '7224', '7208', '7210', '7219', '7225', '7212', '7209', '7211', '7220', '7226', '7207', '7304', '7307', '7306', '7305']
ncm_6=['830710']
ncm_8=['94062000','94069020','72230000','72210000']

ncms = ncm_4 + ncm_6 + ncm_8
grupos = [
    'Barras e perfis',
    'Barras e perfis',
    'Barras e perfis',
    'Barras e perfis',
    'Barras e perfis',
    'Fio-máquina e fios',
    'Fio-máquina e fios',
    'Fio-máquina e fios',
    'Fio-máquina e fios',
    'Formas primárias',
    'Formas primárias',
    'Formas primárias',
    'Laminados planos',
    'Laminados planos',
    'Laminados planos',
    'Laminados planos',
    'Laminados planos',
    'Laminados planos',
    'Laminados planos',
    'Laminados planos',
    'Laminados planos',
    'Semimanufaturados',
    'Tubos, perfis ocos e acessórios',
    'Tubos, perfis ocos e acessórios',
    'Tubos, perfis ocos e acessórios',
    'Tubos, perfis ocos e acessórios',
    'Tubos, perfis ocos e acessórios',
    'Construções pré-fabricadas (estrutura em ferro/aço)',
    'Construções pré-fabricadas (estrutura em ferro/aço)',
    'Fio-máquina e fios',
    'Fio-máquina e fios',
]

df_aco=pd.DataFrame(data={
    "ncm_cod": ncms,
    "grupo": grupos,
})


query="""
    WITH
        
        totais AS (
            SELECT
                YEAR(data_emissao) AS ano,
                cnpj_destinatario AS cnpj_comprador,
                nome_destinatario AS orgao_comprador,
                codigo_ncm_sh AS ncm_cod,
                SUM(valor_total) AS total,
                COUNT(1) AS qtd_nfs
            FROM 'extracoes/itens_limpos.parquet'
            GROUP BY 1, 2, 3, 4
        )
        
        SELECT
            ano,
            COALESCE(grupo, 'TODOS OS DEMAIS PRODUTOS') AS grupo,
            orgao_comprador,
            LIST_DISTINCT(LIST(IF(df.ncm_cod IS NOT NULL, t.ncm_cod, NULL))) AS ncms_encontradas,
            SUM(total)/100 AS total,
            SUM(qtd_nfs) AS qtd_nfs
        FROM totais AS t
            LEFT JOIN df_aco AS df ON t.ncm_cod = df.ncm_cod OR TRUNC(t.ncm_cod / 100) = df.ncm_cod OR TRUNC(t.ncm_cod / 10000) = df.ncm_cod
        GROUP BY 1, 2, 3
        ORDER BY 1, 5 DESC
"""

duckdb.sql(query)

# Salvar resultado como parquet
duckdb.sql(f"""
    COPY(
        {query}
    ) TO '{ARQUIVO_FINAL}.parquet' (FORMAT parquet)     
""")

# Salvar CSV
duckdb.sql(f"SELECT * FROM '{ARQUIVO_FINAL}.parquet'").df().to_csv(
    f'{ARQUIVO_FINAL}.csv',
    sep=";",
    decimal=",",
    quotechar="\"",
    index=False,
    encoding="windows-1252" # A depender do uso do arquivo final: encoding="utf-8"
)