import duckdb

duckdb.sql("""
    COPY (        
        SELECT DISTINCT *
        FROM 'extracoes/itens.parquet'
    ) TO 'extracoes/itens_limpos.parquet' (FORMAT PARQUET)
""")

duckdb.sql("""
    COPY (
        WITH
        
        repeticoes_numeradas AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY chave_de_acesso ORDER BY data_emissao, valor_nota_fiscal DESC) AS row_num
            FROM 'extracoes/nf.parquet'
        )
        
        SELECT *
        FROM repeticoes_numeradas
        WHERE row_num = 1
    ) TO 'extracoes/nf_limpas.parquet' (FORMAT PARQUET)
""")

# duckdb.sql("""SELECT COUNT(DISTINCT chave_de_acesso) FROM 'extracoes/itens.parquet'""") # 6.322.632
# duckdb.sql("""SELECT COUNT(1) FROM 'extracoes/itens.parquet'""") # 23.250.046
# duckdb.sql("""SELECT COUNT(1) FROM 'extracoes/itens_limpos.parquet'""") # 22.959.646

# duckdb.sql("""SELECT COUNT(DISTINCT chave_de_acesso) FROM 'extracoes/nf.parquet'""") # 6.322.632
# duckdb.sql("""SELECT COUNT(1) FROM 'extracoes/nf.parquet'""") # 6.322.632
# duckdb.sql("""SELECT COUNT(1) FROM 'extracoes/nf_limpas.parquet'""") # 645.5313
# duckdb.sql("""SELECT YEAR(data_emissao), COUNT(1) FROM 'extracoes/nf_limpas.parquet' GROUP BY 1 ORDER BY 1""")
# │                   2022 │  1.706.425 │ 1.706.425 OK
# │                   2023 │  1.761.910 │ 1.761.910 OK
# │                   2024 │  1.631.722 │ 1.631.490 --> 1.631.722
# │                   2025 │    763.982 │   861.182 --> 1.222.575

# duckdb.sql("""SELECT YEAR(data_emissao), SUM(valor_nota_fiscal)/100 FROM 'extracoes/nf_limpas.parquet' GROUP BY 1 ORDER BY 1""")
# │                   2022 │            86.350.403.860,21 │ OK --> Valor idêntico ao do Portal: https://portaldatransparencia.gov.br/notas-fiscais?ano=2022
# │                   2023 │            76.230.829.120,90 │ OK R$ 76,62 bilhões no Portal --> R$ 76,23 bilhões no Portal
# │                   2024 │            69.648.853.440,58 │ OK R$ 72,54 bilhões --> R$ 69,65 bilhões
# │                   2025 │            56.461.270.066,82 │ R$ 57,62 bilhões No Portal (inclui 1 a 8/10/2025)

# duckdb.sql("""SELECT YEAR(data_emissao), SUM(valor_total)/100 FROM 'extracoes/itens.parquet' GROUP BY 1 ORDER BY 1""")
# │                   2022 │             90.536.105.091.48 │ OK
# │                   2023 │             87.365.944.745.96 │ OK
# │                   2024 │             80.915.699.072.35 │ OK
# │                   2025 │             70.956.170.661,66 │ 

# duckdb.sql("""SELECT YEAR(data_emissao), SUM(valor_total)/100 FROM 'extracoes/itens_limpos.parquet' GROUP BY 1 ORDER BY 1""")
# │                   2022 │             90.536.105.091.48 │ OK
# │                   2023 │             86.909.229.737.13 │ OK
# │                   2024 │             80.339.396.463.37 │ OK
# │                   2025 │             68.053.042.405,65 │

# duckdb.sql("""SELECT * FROM 'extracoes_backup_2025_08/eventos.parquet' """).df().columns
# duckdb.sql("""
#     SELECT
#         YEAR(CAST(data_emissao AS datetime)),
#         COUNT(1),
#         COUNT(DISTINCT chave_de_acesso)
#     FROM 'extracoes/eventos.parquet'
#     GROUP BY 1
# """)