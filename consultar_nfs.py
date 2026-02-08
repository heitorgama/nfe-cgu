import duckdb
import pandas as pd

duckdb.sql("""
    SELECT YEAR("DATA EMISSÃO"), SUM("VALOR NOTA FISCAL")/100
    FROM 'extracoes/nf.parquet'
    GROUP BY 1
""")

# Valor total de notas fiscais em que a natureza da operação contém 'importação', 'exterior' ou 'internacional'
duckdb.sql("""
    WITH total AS (SELECT SUM("VALOR NOTA FISCAL") AS total FROM 'extracoes/nf.parquet')
    
    SELECT
        YEAR("DATA EMISSÃO"),
        "NATUREZA DA OPERAÇÃO",
        SUM("VALOR NOTA FISCAL")/100 AS valor,
        total,
        SUM("VALOR NOTA FISCAL")/total/100 AS valor_pct,
        COUNT(1) AS qtd_notas
    FROM 'extracoes/nf.parquet', total
    WHERE YEAR("DATA EMISSÃO") = 2024
        AND (
            LOWER("NATUREZA DA OPERAÇÃO") LIKE '%import%' -- Natureza da operação que menciona 'importação'
            OR LOWER("NATUREZA DA OPERAÇÃO") LIKE '%exterior%' -- Natureza da operação que menciona 'exterior': 1 caso
            OR LOWER("NATUREZA DA OPERAÇÃO") LIKE '%internacional%' -- Natureza da operação que menciona 'internacional'
        )
    GROUP BY 1, 2, 4
    ORDER BY 1, 6 DESC, 3 DESC, 2
""")


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
        SELECT "CHAVE DE ACESSO", COUNT(1), SUM("VALOR NOTA FISCAL")/100 AS total
        FROM 'extracoes/nf.parquet'
        GROUP BY 1
        HAVING COUNT(1) > 1
        ORDER BY 2 DESC, 3 DESC
    )
    -- SELECT COUNT(1) FROM x
    SELECT * FROM x
""")

# Compra de 57M, com três notas fiscais de mesmo valor (R$ 19.291.508,29) e mesma chave: 52250633009945000204550010004536511595251094
# No Portal da RFB a nota aparece ativa, com 1 item, no valor de R$ 19.291.508,29
duckdb.sql("""
    SELECT * FROM 'extracoes/itens.parquet' WHERE "CHAVE DE ACESSO" = '52250633009945000204550010004536511595251094'
""").df()

df = duckdb.sql("""
    SELECT * FROM 'extracoes/nf.parquet' WHERE "CHAVE DE ACESSO" = '52250633009945000204550010004536511595251094'
""").df()

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

# CFOPs que indicam importação (primeiro dígito 3 ou 7)
# Vão de 1101 a 7667 em todos os anos de 2022 a 2025
# Há apenas 4 notas com CFOP iniciando em '3' e 18 notas em '7' no ano de 2022
# Iniciando com '7': 4 em 2023, 30 em 2024, 120 em 2025
duckdb.sql("""
    SELECT
        YEAR("DATA EMISSÃO"),
        CFOP//1000 AS cfop_primeiro_digito,
        COUNT(1) AS qtd_itens
    FROM 'extracoes/itens.parquet'
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

# A investigação das notas mostrou que tratam-se de abastecimentos, sobretudo pelo Ministério da Defesa, em aeroportos
duckdb.sql("""
    SELECT
        YEAR("DATA EMISSÃO") AS ano,
        CFOP,
        -- CFOP//1000 AS cfop_primeiro_digito,
        "DESTINO DA OPERAÇÃO",
        "CÓDIGO NCM/SH",
        COUNT(1) AS qtd_itens,
        SUM("VALOR TOTAL")/100 AS valor_total_brl,
        ANY_VALUE("CHAVE DE ACESSO") AS chave_acesso_exemplo
    FROM 'extracoes/itens.parquet'
    WHERE CFOP//1000 IN (3, 7)
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
""").show(max_width=1000)

# Tentativa
duckdb.sql("""
    WHERE "DESTINO DA OPERAÇÃO" = "3 - OPERAÇÃO COM EXTERIOR"
""")

# Buscar 10 produtos estrangeiros mais comprados em 2025, por todas os entes federativos de todos Poderes
# 'vacina', 'eculizumabe', 'dolutegravir', 'insulina', 'elexacaftor', 'adalimumabe', 'imunoglobulina', 'lamivudina', 'pertuzumabe', 'burosumabe'
# A Imunoglobulina é humana, mas evitou-se ser muito específico para que não se omitissem produtos
duckdb.sql("""
    WITH
           
    produtos_identificados AS (
        SELECT
            "CHAVE DE ACESSO",
            YEAR("DATA EMISSÃO") AS ano,
            "DESCRIÇÃO DO PRODUTO/SERVIÇO" AS produto,
            CASE
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\b'||'vacina'||'\\b') THEN 'vacina'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\beculizumabe\\b') THEN 'eculizumabe'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\bdolutegravir\\b') THEN 'dolutegravir'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\binsulina\\b') THEN 'insulina'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\belexacaftor\\b') THEN 'elexacaftor'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\badalimumabe\\b') THEN 'adalimumabe'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\bimunoglobulina\\b') THEN 'imunoglobulina'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\blamivudina\\b') THEN 'lamivudina'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\bpertuzumabe\\b') THEN 'pertuzumabe'
                WHEN REGEXP_MATCHES(LOWER("DESCRIÇÃO DO PRODUTO/SERVIÇO"), '\\bburosumabe\\b') THEN 'burosumabe'
                ELSE 'Todos os demais produtos'
                END AS produto_identificado,
            "VALOR TOTAL" AS valor_total
        FROM 'extracoes/itens.parquet'
    )
           
    SELECT
        ano,
        produto_identificado,
        COUNT(1) AS qtd_itens,
        SUM(valor_total)/100 AS valor_total_brl,
        ANY_VALUE("CHAVE DE ACESSO") AS chave_exemplo,
        ANY_VALUE(produto) AS produto_exemplo
    FROM produtos_identificados
    GROUP BY 1, 2
    ORDER BY 1, 4 DESC
""")

# Buscar valores fornecidos por empresas com presença no exterior, a partir de consulta ao CNPJ
duckdb.sql("""
        SELECT
            YEAR(nf."DATA EMISSÃO") AS ano_emissao,
           pi.cnpj_basico IS NOT NULL AS cnpj_disponivel,
            pi.tipo_matriz,
            pi.presenca_internacional,
            pi.presenca_nacional,
            COUNT(1) AS qtd_notas,
           SUM(nf."VALOR NOTA FISCAL")/100//10**9 AS total_pago_brl,
           ANY_VALUE(nf."CPF/CNPJ Emitente") AS cnpj_exemplo,
           ANY_VALUE(nf."CHAVE DE ACESSO") AS chave_acesso_exemplo
        FROM 'extracoes/nf_limpas.parquet' AS nf
            LEFT JOIN '../receita-federal/presenca_internacional.parquet' AS pi
            ON LEFT(nf."CPF/CNPJ Emitente", 8) = pi.cnpj_basico       
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 1, 2, 3, 4, 5
""")

# Buscar identificação de empresas internacionais (inciadas com 'EX' no PNCP)
duckdb.sql("""
    SELECT "CPF/CNPJ Emitente", COUNT(1)
    FROM 'extracoes/nf.parquet'
    WHERE "CPF/CNPJ Emitente"
    GROUP BY 1
""")

# Buscar notas da empresa de , que tem nome fantasia Glaxosmithkline no CNPJ
# UF=EX no CNPJ: cnpj_base = 21788148 (não encontradas notas fiscais)
# GLAXOSMITHKLINE - GSK - LABORATORIOS STIEFEL - STIEFEL - UF=RJ: cnpj_base= 33247743
duckdb.sql("""
    SELECT *
    FROM 'extracoes/nf.parquet'
    WHERE LEFT("CPF/CNPJ Emitente", 8) = '33247743'
""")

duckdb.sql("""
    SELECT
        YEAR("DATA EMISSÃO"),
        -- "NATUREZA DA OPERAÇÃO",
        "RAZÃO SOCIAL EMITENTE",
        "UF EMITENTE",
        "UF DESTINATÁRIO",
        COUNT(1) AS qtd_notas,
        COUNT(DISTINCT "CHAVE DE ACESSO") AS qtd_notas_distintas,
        SUM("VALOR NOTA FISCAL")/100 AS valor_total_brl,
        ANY_VALUE("CHAVE DE ACESSO") AS chave_acesso_exemplo
    FROM 'extracoes/nf.parquet'
    WHERE LEFT("CPF/CNPJ Emitente", 8) = '33247743'
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 7 DESC
""")

duckdb.sql("""
    SELECT
        --YEAR("DATA EMISSÃO"),
        COUNT(DISTINCT "CHAVE DE ACESSO") AS qtd_notas_distintas,
        SUM("VALOR NOTA FISCAL")/100 AS valor_total_brl,
        ANY_VALUE("CHAVE DE ACESSO") AS chave_acesso_exemplo
    FROM 'extracoes/nf.parquet'
    WHERE LEFT("CPF/CNPJ Emitente", 8) = '33247743'
    --GROUP BY 1
    --ORDER BY 1, 4 DESC
""")


# Cálculo do valor total por UF destinatária
duckdb.sql("""
    WITH totais_por_ano_e_uf AS (
        SELECT
            YEAR("DATA EMISSÃO") AS ano,
            "UF DESTINATÁRIO" AS uf_destino,
            SUM("VALOR TOTAL")/100000000 AS valor_total_brl_milhoes
        FROM 'extracoes/itens_limpos.parquet'
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    )
    
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ano ORDER BY valor_total_brl_milhoes DESC) AS ranking_no_anp
    FROM totais_por_ano_e_uf
    ORDER BY ano, valor_total_brl_milhoes DESC
""").show(max_rows=1000)

duckdb.sql("""SELECT COUNT(1) FROM 'extracoes/itens_limpos.parquet'  WHERE YEAR("DATA EMISSÃO") IN (2022, 2023, 2024)""")