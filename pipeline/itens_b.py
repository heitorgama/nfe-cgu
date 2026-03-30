import duckdb

duckdb.sql(r"""
    WITH itens_normalizado AS (
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
                YEAR(i.data_emissao)                                            AS ano,
                TRIM(b."Palavras Chaves")                                       AS tipo_de_produto,
                i.codigo_ncm_sh                                                 AS ncm,
                TRIM(a.tipo)                                                    AS tipo_ncm,
                a.tipo IS NOT NULL                                              AS ncm_mapeada,
                i.descricao_normalizada LIKE '%' || b.chave_normalizada || '%'  AS chave_encontrada,
                ' ' || i.descricao_normalizada || ' ' LIKE '% pe %'             AS pe_encontrado,
                ' ' || i.descricao_normalizada || ' ' LIKE '% pvc %'            AS pvc_encontrado,
                ' ' || i.descricao_normalizada || ' ' LIKE '% ps %'             AS ps_encontrado,
                ' ' || i.descricao_normalizada || ' ' LIKE '% br %'             AS br_encontrado,
                ' ' || i.descricao_normalizada || ' ' LIKE '% sbr %'            AS sbr_encontrado,
                i.descricao_do_produto_servico                                  AS descricao,
                SUM(i.valor_total)                                              AS total
            FROM itens_normalizado AS i
            LEFT JOIN chaves_normalizado AS b
                ON i.descricao_normalizada LIKE '%' || b.chave_normalizada || '%'
                OR i.descricao_normalizada LIKE '% ' || LOWER(b."Abreviação") || ' %'
                OR i.descricao_normalizada LIKE '% ' || LOWER(b."Abreviação") || '%'
                OR i.descricao_normalizada LIKE '%' || LOWER(b."Abreviação") || ' %'
            LEFT JOIN 'dados\mapeamento_grupo_A.csv' AS a
                ON CAST(i.codigo_ncm_sh AS VARCHAR) = CAST(a.ncm AS VARCHAR)
            GROUP BY ALL
            ORDER BY ano, total DESC
        )
        SELECT * FROM totais
        WHERE ano = 2025
""").df().to_csv(
    'extracoes/gold/itens_grupo_b.csv',
    sep=";",
    decimal=",",
    quotechar='"',
    index=False,
    encoding="windows-1252"
)