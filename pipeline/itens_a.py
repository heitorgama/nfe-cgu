import duckdb

duckdb.sql(r"""
    with 
  totais AS (
    SELECT
      YEAR(i.data_emissao)                                            AS ano,
      i.codigo_ncm_sh                                                 AS ncm,
      TRIM(a.tipo)                                                    AS tipo_ncm,
      a.tipo IS NOT NULL                                              AS ncm_mapeada,
      i.descricao_do_produto_servico                                  AS descricao,
      SUM(i.valor_total)                                              AS total
      FROM 'extracoes\silver\itens.parquet' i
      LEFT JOIN 'dados\mapeamento_grupo_A.csv' a
      ON i.codigo_ncm_sh = a.ncm
    GROUP BY ALL
    ORDER BY ano, total DESC
  )

SELECT *
FROM totais
WHERE ano = 2025
""").df().to_csv(
    'extracoes/gold/itens_grupo_a.csv',
    sep=";",
    decimal=",",
    quotechar='"',
    index=False,
    encoding="windows-1252"
)