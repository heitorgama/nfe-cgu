import duckdb
import os

df = duckdb.sql(r"""
    WITH
  tabela_expandida AS (
    SELECT
      *
    FROM
      'dados\mapeamento_EcoAdvance.csv' AS e
      JOIN 'dados/ncm.csv' AS n ON e.NCM = n.prefixo
  )
    SELECT
        chave_de_acesso,
        YEAR(data_emissao) as ano,
        codigo_ncm_sh as "NCM subitem",
        TRIM("PRODUTO PESPP") as ecoadvance_produto_pespp,
        "PE" as ecoadvance_pe,
        uf_emitente,
        uf_destinatario,
        orgao_superior_destinatario,
        codigo_orgao_superior_destinatario,
        cnpj_destinatario,
        nome_destinatario,
        descricao_do_produto_servico as "Descrição do Item",
        cnpj_emitente,
        cpf_emitente,
        valor_total as "Valor Total"
    FROM
        'extracoes/silver/itens.parquet' as s
    LEFT JOIN tabela_expandida as t ON t.codigo = s.codigo_ncm_sh
    WHERE
        ano = 2024
        OR ano = 2025
""").df()

os.makedirs("extracoes/gold", exist_ok=True)

df.to_csv(
    'extracoes/gold/itens_giz.csv',
    sep=";",
    decimal=",",
    quotechar='"',
    index=False,
    encoding="windows-1252"
)

df.to_csv(
    'extracoes/gold/itens_giz_utf8.csv',
    index=False,
    encoding="utf-8"
)