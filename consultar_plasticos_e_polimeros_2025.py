import duckdb

duckdb.sql("""
    COPY(
        WITH
        
        totais AS (
            SELECT
                YEAR(data_emissao) AS ano,
                codigo_ncm_sh AS ncm_cod,
                ncm_sh_tipo_de_produto AS ncm_descr,
                descricao_do_produto_servico AS descricao,
                SUM(valor_total)/100 AS total,
                -- COUNT(1) AS qtd_nfs,
            FROM 'extracoes/itens_limpos.parquet'
            GROUP BY 1, 2, 3, 4
            ORDER BY 1, 5 DESC
        ),
        
        ranking AS (
            SELECT
                *,
                SUM(total) OVER (PARTITION BY ano, ncm_cod) AS total_do_ncm,
                ROW_NUMBER() OVER (PARTITION BY ano, ncm_cod ORDER BY total DESC) AS ranking_no_ncm,
                ROUND(total / SUM(total) OVER (PARTITION BY ano, ncm_cod), 4) AS pct_no_ncm,
            FROM totais
        ),
        
        pct_total AS (
            SELECT
                *,
                SUM(pct_no_ncm) OVER (PARTITION BY ano, ncm_cod) AS pct_total_das_descricoes_exibidas,
            FROM ranking
            ORDER BY ano, total_do_ncm DESC, ranking_no_ncm
        )
        
        SELECT *
        FROM pct_total
        ORDER BY ano, total_do_ncm DESC
    ) TO 'bioquimicos/totais_com_agregacao_das_descricoes.parquet' (FORMAT parquet)
""")

duckdb.sql("""
COPY(
    SELECT
        REGEXP_MATCHES(LOWER(ncm_descr), 'plastico|plástico|polimero|polímero') AS ncm_contem_palavras_chave,
        REGEXP_MATCHES(LOWER(descricao), 'plastico|plástico|polimero|polímero') AS produto_contem_palavras_chave,
        CASE WHEN ano = 2025 THEN '2025 (até 30/09)' ELSE CAST(ano AS VARCHAR) END AS ano,
        ncm_cod,
        ncm_descr,
        descricao,
        total,
        total_do_ncm,
        ranking_no_ncm,
        pct_no_ncm
    FROM 'bioquimicos/totais_com_agregacao_das_descricoes.parquet'
    WHERE REGEXP_MATCHES(LOWER(ncm_descr), 'plastico|plástico|polimero|polímero') OR 
        REGEXP_MATCHES(LOWER(descricao), 'plastico|plástico|polimero|polímero')
    ORDER BY ano, total_do_ncm DESC, total DESC
) TO 'bioquimicos/totais_palavras_chave.parquet' (FORMAT parquet)
""")

duckdb.sql("""
    SELECT ncm_descr
    FROM 'bioquimicos/totais_com_agregacao_das_descricoes.parquet'
    WHERE ano = 2022 AND ncm_cod = 27132000
        --AND (
        --    REGEXP_MATCHES(LOWER(ncm_descr), 'plastico|plástico|polimero|polímero') OR 
        --    REGEXP_MATCHES(LOWER(descricao), 'plastico|plástico|polimero|polímero')
        --)
    GROUP BY 1
""")

# Salvar CSV
duckdb.sql("""
    SELECT *
    FROM 'bioquimicos/totais_palavras_chave.parquet'
""").df().to_csv(
    'bioquimicos/2025-12-12_totais_palavras_chave.csv',
    sep=";",
    decimal=",",
    quotechar="\"",
    index=False,
    encoding="windows-1252"
)