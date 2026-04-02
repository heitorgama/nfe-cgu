import duckdb
import os

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
            REGEXP_MATCHES(i.descricao_normalizada, '\bpolietileno\b')                                  AS polietileno_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bpolicloreto de vinila\b')                        AS "policloreto de vinila_encontrado",
            REGEXP_MATCHES(i.descricao_normalizada, '\bpoliestireno\b')                                 AS poliestireno_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bpolibutadieno\b')                                AS polibutadieno_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bestireno butadieno\b')                           AS estireno_butadieno_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bpneus?\b')                                       AS pneus_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bembalagem plástica\b')                           AS embalagem_plastica_encontrada,
            REGEXP_MATCHES(i.descricao_normalizada, '\btubo pvc\b')                                     AS tubo_pvc_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bembalagem pet\b')                                AS embalagem_pet_encontrada,
            REGEXP_MATCHES(i.descricao_normalizada, '\bpoliéster\b')                                    AS poliester_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\bplástica\b')                                     AS plastica_encontrada,
            REGEXP_MATCHES(i.descricao_normalizada, '\bplástico\b')                                     AS plastico_encontrada,
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

    SELECT *
    FROM totais
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