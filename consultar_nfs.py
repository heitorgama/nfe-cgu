import duckdb
import pandas as pd

duckdb.sql("SELECT YEAR('DATA EMISSÃO'), SUM('VALOR TOTAL') FROM 'modelos/nf.parquet' GROUP BY 1")

duckdb.sql("""
    SELECT "UF EMITENTE", SUM("VALOR NOTA FISCAL")/100
    FROM 'extracoes/nf.parquet'
    WHERE YEAR("DATA EMISSÃO") = 2024
    GROUP BY "UF EMITENTE"
    ORDER BY 2
""")

duckdb.sql("""
    SELECT "UF DESTINATÁRIO", SUM("VALOR NOTA FISCAL")/100
    FROM 'extracoes/nf.parquet'
    WHERE YEAR("DATA EMISSÃO") = 2024
    GROUP BY "UF DESTINATÁRIO"
    ORDER BY 2
""")

# Valor total 2024, considerando o sinal dado pelo CFOP: R$ 64,4 bilhões (CGU:72,5, CNI: 58,6, Fred: 39,2)
# Valor total 2024, sem considerar sinal: R$ 80,9 bilhões
# Itens comprados: 5,6 milhões
# Itens: 6,6 milhões
# Notas fiscais: 1,7 milhões, sendo que 88 mil possuem mais de um registro, conforme mostrado abaixo (CGU: 1,6 milhões de notas fiscais)
# Notas fiscais distintas: 1.631.722, exatamente o número exibido no portal da CGU! :)
duckdb.sql("""
    SELECT SUM("VALOR TOTAL")/100 AS valor_total,
           SUM("VALOR TOTAL" * sinal)/100 AS valor_total_com_descontos,
           SUM(sinal) qtd_itens_comprados,
           COUNT(1) AS qtd_itens
    FROM 'extracoes/itens.parquet'
    WHERE YEAR("DATA EMISSÃO") = 2024
""")
duckdb.sql("""
    SELECT COUNT(1) AS qtd_notas
    FROM 'extracoes/nf.parquet'
    WHERE YEAR("DATA EMISSÃO") = 2024
""")
duckdb.sql("""
    SELECT COUNT(DISTINCT "CHAVE DE ACESSO") AS qtd_notas_distintas
    FROM 'extracoes/nf.parquet'
    WHERE YEAR("DATA EMISSÃO") = 2024
""")

# Todas as colunas
duckdb.sql("""
    SELECT *
    FROM 'extracoes/itens.parquet'
    -- WHERE YEAR("DATA EMISSÃO") = 2024
    LIMIT 1
""")

# NCMs mais frequentes
duckdb.sql("""
    SELECT
        "NCM/SH (TIPO DE PRODUTO)" AS ncm,
        COUNT(1) AS qtd_itens,
        SUM("QUANTIDADE" * sinal) AS qtd_com_sinal,
        SUM("VALOR TOTAL" * sinal)/100 AS total_com_sinal_brl
    FROM 'extracoes/itens.parquet'
    WHERE YEAR("DATA EMISSÃO") = 2024
    GROUP BY 1
    ORDER BY 3 DESC
""")

# Contar dígitos do NCM
duckdb.sql("""
    SELECT
        "NCM/SH (TIPO DE PRODUTO)" AS ncm,
        COUNT(1) AS qtd_itens,
        SUM("QUANTIDADE" * sinal) AS qtd_com_sinal,
        SUM("VALOR TOTAL" * sinal)/100 AS total_com_sinal_brl
    FROM 'extracoes/itens.parquet'
    WHERE YEAR("DATA EMISSÃO") = 2024
    GROUP BY 1
    ORDER BY 3 DESC
""")

# Chaves repetidas na base de itens: algumas chaves com mais de 700 itens
# 750 mil notas têm múltiplos itens
duckdb.sql("""
    WITH x AS (
        SELECT "CHAVE DE ACESSO", COUNT(1)
        FROM 'extracoes/itens.parquet'
        GROUP BY 1
        HAVING COUNT(1) > 1
        ORDER BY 2 DESC
    )
    SELECT COUNT(1) FROM x
""")

# Chaves repetidas na base de NFs: chaves aparecem no máximo 3 vezes
# 88 mil notas possuem duplicidade
duckdb.sql("""
    WITH x AS (
        SELECT "CHAVE DE ACESSO", COUNT(1)
        FROM 'extracoes/nf.parquet'
        GROUP BY 1
        HAVING COUNT(1) > 1
        ORDER BY 2 DESC
    )
    -- SELECT COUNT(1) FROM x
    SELECT * FROM x
""")

# Conferência: valor unitário * unidades = total
# 6.6 milhões de itens inconsistentes!
# 24.8 mil consistentes
duckdb.sql("""
    SELECT "QUANTIDADE" * "VALOR UNITÁRIO" = "VALOR TOTAL", COUNT(1)
    FROM 'extracoes/itens.parquet'
    GROUP BY 1
""")

duckdb.sql("""
    SELECT
           "DESCRIÇÃO DO PRODUTO/SERVIÇO",
           "UNIDADE",
           "QUANTIDADE",
           "VALOR TOTAL" / "VALOR UNITÁRIO" AS qtd_inferida,
           "VALOR UNITÁRIO",
           "QUANTIDADE" * "VALOR UNITÁRIO",
           "VALOR TOTAL"
    FROM 'extracoes/itens.parquet'
    WHERE  "QUANTIDADE" * "VALOR UNITÁRIO" <> "VALOR TOTAL"
    LIMIT 100
""")

df_itens = pd.read_parquet('extracoes/itens.parquet')

