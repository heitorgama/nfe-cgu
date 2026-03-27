import duckdb

# Busca por NCMs relacionados a plásticos e polímeros

duckdb.sql("""
    COPY(
        WITH
        
        totais AS (
            SELECT
                YEAR(i.data_emissao) AS ano,
                m.tipo AS tipo_de_produto,
                i.codigo_ncm_sh AS ncm_cod,
                i.ncm_sh_tipo_de_produto AS ncm_descr,
                i.descricao_do_produto_servico AS descricao,
                SUM(i.valor_total) AS total,
                -- COUNT(1) AS qtd_nfs,
            FROM 'extracoes/silver/itens.parquet' AS i
                JOIN 'dados/mapeamento_grupo_A.csv' AS m ON i.codigo_ncm_sh = m.ncm
            GROUP BY 1, 2, 3, 4, 5
            ORDER BY 1, 6 DESC
        ),
        
        ranking AS (
            SELECT
                *,
                SUM(total) OVER (PARTITION BY ano, tipo_de_produto) AS total_do_tipo_de_produto,
                ROW_NUMBER() OVER (PARTITION BY ano, tipo_de_produto ORDER BY total DESC) AS ranking_no_tipo_de_produto,
                ROUND(total / SUM(total) OVER (PARTITION BY ano, tipo_de_produto), 4) AS pct_no_tipo_de_produto,
            FROM totais
        )
        
        SELECT *
        FROM ranking
        ORDER BY ano, total_do_tipo_de_produto DESC
    ) TO 'bioquimicos_2026/totais_com_agregacao_das_descricoes.parquet' (FORMAT parquet)
""")

# Busca por palavras-chave e abreviações

duckdb.sql("""
    COPY (
        WITH
    
        itens_normalizado AS (
            SELECT *,
                REGEXP_REPLACE(
                    REPLACE(LOWER(descricao_do_produto_servico), '-', ' '),
                    '\\s+', ' '
                ) AS descricao_normalizada
            FROM 'extracoes/silver/itens.parquet'
        ),
    
        chaves_normalizado AS (
            SELECT *,
                REGEXP_REPLACE(
                    REPLACE(LOWER(TRIM("Palavras Chaves")), '-', ' '),
                    '\\s+', ' '
                ) AS chave_normalizada
            FROM 'dados/mapeamento_grupo_B.csv'
        ),
    
        totais AS (
            SELECT
                YEAR(i.data_emissao)  AS ano,
                TRIM(b."Palavras Chaves") AS tipo_de_produto,
                i.descricao_normalizada LIKE '%' || b.chave_normalizada || '%' AS chave_encontrada,
                REGEXP_MATCHES(LOWER(i.descricao_do_produto_servico), '\bpe\b') AS pe_encontrado,
                REGEXP_MATCHES(LOWER(i.descricao_do_produto_servico), '\bpvc\b') AS pvc_encontrado,
                REGEXP_MATCHES(LOWER(i.descricao_do_produto_servico), '\bps\b') AS ps_encontrado,
                REGEXP_MATCHES(LOWER(i.descricao_do_produto_servico), '\bbr\b') AS br_encontrado,
                REGEXP_MATCHES(LOWER(i.descricao_do_produto_servico), '\bsbr\b') AS sbr_encontrado,
                i.descricao_do_produto_servico AS descricao,
                SUM(i.valor_total) AS total
            FROM itens_normalizado AS i
            LEFT JOIN chaves_normalizado AS b
                ON i.descricao_normalizada LIKE '%' || b.chave_normalizada || '%'
            GROUP BY ALL
            ORDER BY 1, 10 DESC
        ),
    
        ranking AS (
            SELECT
                *,
                SUM(total) OVER (PARTITION BY ano, tipo_de_produto)                          AS total_do_tipo_de_produto,
                ROW_NUMBER() OVER (PARTITION BY ano, tipo_de_produto ORDER BY total DESC)    AS ranking_no_tipo_de_produto,
                ROUND(total / SUM(total) OVER (PARTITION BY ano, tipo_de_produto), 4)        AS pct_no_grupo
            FROM totais
            -- WHERE chave_encontrada OR pe_encontrado OR pvc_encontrado OR ps_encontrado OR br_encontrado OR sbr_encontrado
        )
    
        SELECT *
        FROM ranking
        ORDER BY ano, total_do_tipo_de_produto DESC
    
    ) TO 'bioquimicos_2026/totais_palavras_chave.parquet' (FORMAT parquet)
""")

import ipdb; ipdb.set_trace()



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