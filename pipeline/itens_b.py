import duckdb
import os

# A consulta SQL foi convertida em raw string para evitar o alerta:
# SyntaxWarning: "\s" is an invalid escape sequence. Such sequences will not work in the future. Did you mean "\\s"? A raw string is also an option.

df = duckdb.sql(r"""
    WITH

    itens_normalizado AS (
        SELECT *,
            REGEXP_REPLACE(
                REPLACE(LOWER(descricao_do_produto_servico), '-', ' '),
                '\s+', ' '
            ) AS descricao_normalizada
        FROM 'extracoes/silver/itens.parquet'
    ),

    chaves_normalizado AS (
        SELECT *,
            REGEXP_REPLACE(
                REPLACE(LOWER(TRIM("Palavras Chaves")), '-', ' '),
                '\s+', ' '
            ) AS chave_normalizada
        FROM 'dados/mapeamento_grupo_B.csv'
    ),

    totais AS (
        SELECT
            YEAR(i.data_emissao)                                                                        AS ano,
            i.codigo_ncm_sh                                                                             AS ncm,
            TRIM(a.tipo)                                                                                AS tipo_ncm,
            COALESCE(a.tipo IS NOT NULL, false)                                                         AS ncm_mapeada,
            CASE
                WHEN REGEXP_MATCHES(i.descricao_normalizada, '\b' || b.chave_normalizada || '\b')
                THEN TRIM(b."Palavras Chaves") END                                                      AS palavra_chave,
            REGEXP_MATCHES(i.descricao_normalizada, '\bpolietileno\b')                            AS chave_encontrada,
            REGEXP_MATCHES(i.descricao_normalizada, '\bpe\b')                                           AS pe_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bpvc\b')                                          AS pvc_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bps\b')                                           AS ps_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bbr\b')                                           AS br_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bsbr\b')                                          AS sbr_encontrado,
            i.descricao_do_produto_servico                                                              AS descricao,
            SUM(i.valor_total)                                                                          AS total
        FROM itens_normalizado AS i
            LEFT JOIN 'dados/mapeamento_grupo_A.csv' AS a
                ON CAST(i.codigo_ncm_sh AS VARCHAR) = CAST(a.ncm AS VARCHAR)
        GROUP BY ALL
        ORDER BY YEAR(i.data_emissao), SUM(i.valor_total) DESC
    )

    SELECT
        ano,
        ncm,
        tipo_ncm,
        ncm_mapeada,
        ANY_VALUE(palavra_chave) AS palavra_chave,
        chave_encontrada,
        pe_encontrado,
        pvc_encontrado,
        ps_encontrado,
        br_encontrado,
        sbr_encontrado,
        descricao,
        total        
    FROM totais
    GROUP BY
        ano,
        ncm,
        tipo_ncm,
        ncm_mapeada,
        chave_encontrada,
        pe_encontrado,
        pvc_encontrado,
        ps_encontrado,
        br_encontrado,
        sbr_encontrado,
        descricao,
        total
""").df()

os.makedirs("extracoes/gold", exist_ok=True)

df.to_csv(
    'extracoes/gold/itens_grupo_b_regex.csv',
    sep=";",
    decimal=",",
    quotechar='"',
    index=False,
    encoding="windows-1252"
)

df.to_csv(
    'extracoes/gold/itens_grupo_b_regex_utf8.csv',
    index=False,
)