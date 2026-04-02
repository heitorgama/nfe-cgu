import os
import tempfile
from datetime import datetime

import duckdb
import pandas as pd

DIRETORIO_GOLD = 'extracoes/gold'
GOLD_DB = os.path.join(DIRETORIO_GOLD, 'gold.duckdb')
MAPEAMENTO_ECOADVANCE = 'dados/mapeamento_EcoAdvance.csv'
NCM_CSV = 'dados/ncm.csv'
DIRETORIO_ENTREGA = os.path.join(DIRETORIO_GOLD, 'cruzamento_ncm')

EMITENTE = "COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente)"


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


def criar_itens_giz(con: duckdb.DuckDBPyConnection) -> None:
    """Itens silver cruzados com mapeamento EcoAdvance via ncm.csv (join exato por NCM), 2024-2025."""
    con.execute(f"""
        CREATE OR REPLACE TABLE itens_giz AS
        WITH tabela_expandida AS (
            SELECT *
            FROM '{MAPEAMENTO_ECOADVANCE}' AS e
            JOIN '{NCM_CSV}' AS n ON e.NCM = n.prefixo
        )
        SELECT
            chave_de_acesso,
            YEAR(data_emissao) AS ano,
            codigo_ncm_sh AS "NCM subitem",
            TRIM("PRODUTO PESPP") AS ecoadvance_produto_pespp,
            "PE" AS ecoadvance_pe,
            uf_emitente,
            uf_destinatario,
            orgao_superior_destinatario,
            codigo_orgao_superior_destinatario,
            cnpj_destinatario,
            nome_destinatario,
            descricao_do_produto_servico AS "Descrição do Item",
            cnpj_emitente,
            cpf_emitente,
            valor_total AS "Valor Total"
        FROM 'extracoes/silver/itens.parquet' AS s
        LEFT JOIN tabela_expandida AS t ON t.codigo = s.codigo_ncm_sh
        WHERE YEAR(data_emissao) IN (2024, 2025)
    """)
    count = con.execute("SELECT COUNT(*) FROM itens_giz").fetchone()[0]
    imprimir_mensagem(f"itens_giz: {count} linhas.")

    df = con.execute("SELECT * FROM itens_giz").df()
    df.to_csv(
        'extracoes/gold/itens_giz.csv',
        sep=";", decimal=",", quotechar='"', index=False, encoding="windows-1252"
    )
    df.to_csv(
        'extracoes/gold/itens_giz_utf8.csv',
        index=False, encoding="utf-8"
    )
    imprimir_mensagem("CSVs itens_giz exportados.")


def criar_resumo_ecoadvance(con: duckdb.DuckDBPyConnection) -> None:
    """Resumo EcoAdvance por produto/PE a partir de itens_giz, 2024-2025."""
    con.execute(f"""
        CREATE OR REPLACE TABLE resumo_ecoadvance AS
        SELECT
            ecoadvance_produto_pespp,
            ecoadvance_pe,
            SUM(CASE WHEN ano = 2024 THEN "Valor Total" ELSE 0 END)                              AS valor_2024,
            SUM(CASE WHEN ano = 2025 THEN "Valor Total" ELSE 0 END)                              AS valor_2025,
            COUNT(DISTINCT CASE WHEN ano = 2024 THEN {EMITENTE} END)                             AS qtd_emitentes_2024,
            COUNT(DISTINCT CASE WHEN ano = 2025 THEN {EMITENTE} END)                             AS qtd_emitentes_2025,
            COUNT(DISTINCT CASE WHEN ano = 2024 THEN chave_de_acesso END)                        AS qtd_notas_fiscais_2024,
            COUNT(DISTINCT CASE WHEN ano = 2025 THEN chave_de_acesso END)                        AS qtd_notas_fiscais_2025
        FROM itens_giz
        GROUP BY ALL
    """)
    count = con.execute("SELECT COUNT(*) FROM resumo_ecoadvance").fetchone()[0]
    imprimir_mensagem(f"resumo_ecoadvance: {count} linhas.")


def exportar_parquets(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta tabelas gold como parquet."""
    for tabela in ('resumo_ecoadvance',):
        caminho = os.path.join(DIRETORIO_GOLD, f'{tabela}.parquet')
        con.execute(f"COPY {tabela} TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    imprimir_mensagem("Parquets gold exportados.")


def exportar_html_interativo() -> None:
    """Gera HTML standalone com os parquets embutidos em base64."""
    import base64

    TABELAS = ('resumo_ecoadvance',)

    def parquet_base64(tabela):
        with open(os.path.join(DIRETORIO_GOLD, f'{tabela}.parquet'), 'rb') as f:
            return base64.b64encode(f.read()).decode('ascii')

    template_path = os.path.join(os.path.dirname(__file__), 'template', 'dashboard.html')
    with open(template_path, encoding='utf-8') as f:
        html = f.read()

    data_js = '\n'.join(
        f"const _DATA_{t} = '{parquet_base64(t)}';"
        for t in TABELAS
    )
    parquet_map = '{\n' + '\n'.join(
        f"  '{t}.parquet': _DATA_{t},"
        for t in TABELAS
    ) + '\n}'
    inline_register = f"""{data_js}

const _PARQUET_DATA = {parquet_map};

function base64ToUint8Array(b64) {{
  const bin = atob(b64);
  const arr = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
  return arr;
}}

async function registerParquet(file) {{
  const buf = base64ToUint8Array(_PARQUET_DATA[file]);
  await db.registerFileBuffer(file, buf);
}}

  await registerParquet('resumo_ecoadvance.parquet');"""

    old_register = (
        "  async function registerParquet(file) {\n"
        "    const resp = await fetch(file);\n"
        "    const buf = new Uint8Array(await resp.arrayBuffer());\n"
        "    await db.registerFileBuffer(file, buf);\n"
        "  }\n"
        "\n"
        "  await registerParquet('resumo_ecoadvance.parquet');"
    )
    html = html.replace(old_register, inline_register)

    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    destino = os.path.join(DIRETORIO_ENTREGA, 'preview.html')
    with open(destino, 'w', encoding='utf-8') as f:
        f.write(html)

    tamanho_mb = os.path.getsize(destino) / 1024 / 1024
    imprimir_mensagem(f"HTML standalone gerado: {destino} ({tamanho_mb:.1f} MB)")


def exportar_csvs_entrega(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta CSVs prontos pra entrega"""
    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    for tabela in ('resumo_ecoadvance',):
        caminho = os.path.join(DIRETORIO_ENTREGA, f'{tabela}.csv')
        con.execute(f"COPY {tabela} TO '{caminho}' (HEADER, DELIMITER ',')")
    imprimir_mensagem("CSVs de entrega exportados.")


def main():
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)
    con = duckdb.connect(GOLD_DB, config={'temp_directory': tempfile.gettempdir()})
    con.execute("SET preserve_insertion_order=false")

    imprimir_mensagem("Gerando itens GIZ (EcoAdvance x ncm.csv)...")
    criar_itens_giz(con)
    imprimir_mensagem("Gerando resumo EcoAdvance...")
    criar_resumo_ecoadvance(con)
    imprimir_mensagem("Exportando parquets gold...")
    exportar_parquets(con)
    imprimir_mensagem("Exportando CSVs de entrega...")
    exportar_csvs_entrega(con)

    template = os.path.join(os.path.dirname(__file__), 'template', 'dashboard.html')
    if os.path.exists(template):
        imprimir_mensagem("Exportando HTML interativo...")
        exportar_html_interativo()
    else:
        imprimir_mensagem("Template dashboard.html não encontrado, pulando HTML interativo.")

    con.close()
    imprimir_mensagem("Gold salvo.")


if __name__ == "__main__":
    main()
