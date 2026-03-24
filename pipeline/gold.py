import os
import tempfile
from datetime import datetime

import duckdb

DIRETORIO_GOLD = 'extracoes/gold'
GOLD_DB = os.path.join(DIRETORIO_GOLD, 'gold.duckdb')
MAPEAMENTO_ECOADVANCE = 'dados/mapeamento_EcoAdvance.csv'
DIRETORIO_ENTREGA = os.path.join(DIRETORIO_GOLD, 'cruzamento_ncm')

EMITENTE = "COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente)"


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


def criar_resumo_ecoadvance(con: duckdb.DuckDBPyConnection) -> None:
    """Cruzamento dos itens silver com o mapeamento EcoAdvance (join por prefixo NCM 4 dígitos)."""
    con.execute(f"""
        CREATE OR REPLACE TABLE resumo_ecoadvance AS
        WITH cruzamento AS (
            SELECT *
            FROM 'extracoes/silver/itens.parquet' AS i
            JOIN '{MAPEAMENTO_ECOADVANCE}' AS m
                ON LEFT(CAST(m.CODIGO AS VARCHAR), 4) = LEFT(i.codigo_ncm_sh, 4)
        )
        SELECT
            "PRODUTO PESPP", PE,
            CODIGO, "DESCRIÇÃO",
            SUM(valor_total)                                                        AS "VALOR TOTAL",
            COUNT(DISTINCT {EMITENTE})                                              AS "FORNECEDORES DISTINTOS",
            SUM(CASE WHEN YEAR(data_emissao) = 2022 THEN valor_total ELSE 0 END)   AS valor_2022,
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END)   AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END)   AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END)   AS valor_2025
        FROM cruzamento
        GROUP BY ALL
        ORDER BY "VALOR TOTAL" DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM resumo_ecoadvance").fetchone()[0]
    imprimir_mensagem(f"resumo_ecoadvance: {count} linhas.")


def criar_resumo_por_pespp(con: duckdb.DuckDBPyConnection) -> None:
    """Agrega o cruzamento EcoAdvance por Produto PESPP."""
    con.execute(f"""
        CREATE OR REPLACE TABLE resumo_por_pespp AS
        WITH cruzamento AS (
            SELECT *
            FROM 'extracoes/silver/itens.parquet' AS i
            JOIN '{MAPEAMENTO_ECOADVANCE}' AS m
                ON LEFT(CAST(m.CODIGO AS VARCHAR), 4) = LEFT(i.codigo_ncm_sh, 4)
        )
        SELECT
            "PRODUTO PESPP",
            SUM(valor_total)                                                        AS "VALOR TOTAL",
            COUNT(DISTINCT {EMITENTE})                                              AS "FORNECEDORES DISTINTOS",
            SUM(CASE WHEN YEAR(data_emissao) = 2022 THEN valor_total ELSE 0 END)   AS valor_2022,
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END)   AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END)   AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END)   AS valor_2025
        FROM cruzamento
        GROUP BY ALL
        ORDER BY "VALOR TOTAL" DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM resumo_por_pespp").fetchone()[0]
    imprimir_mensagem(f"resumo_por_pespp: {count} linhas.")


def criar_totais_globais_ecoadvance(con: duckdb.DuckDBPyConnection) -> None:
    """Totais globais do cruzamento EcoAdvance, deduplicados por item físico."""
    con.execute(f"""
        CREATE OR REPLACE TABLE totais_globais_ecoadvance AS
        SELECT
            SUM(valor_total)            AS "VALOR TOTAL",
            COUNT(DISTINCT emitente)    AS "FORNECEDORES DISTINTOS",
            COUNT(*)                    AS "ITENS"
        FROM (
            SELECT
                chave_de_acesso, numero, numero_produto,
                ANY_VALUE(valor_total)      AS valor_total,
                ANY_VALUE({EMITENTE})       AS emitente
            FROM (
                SELECT *
                FROM 'extracoes/silver/itens.parquet' AS i
                JOIN '{MAPEAMENTO_ECOADVANCE}' AS m
                    ON LEFT(CAST(m.CODIGO AS VARCHAR), 4) = LEFT(i.codigo_ncm_sh, 4)
            )
            GROUP BY chave_de_acesso, numero, numero_produto
        )
    """)
    imprimir_mensagem("totais_globais_ecoadvance: calculado.")


def exportar_parquets(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta tabelas gold como parquet."""
    for tabela in ('resumo_ecoadvance', 'resumo_por_pespp', 'totais_globais_ecoadvance'):
        caminho = os.path.join(DIRETORIO_GOLD, f'{tabela}.parquet')
        con.execute(f"COPY {tabela} TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    imprimir_mensagem("Parquets gold exportados.")


def exportar_html_interativo() -> None:
    """Gera HTML standalone com os parquets embutidos em base64."""
    import base64

    TABELAS = ('resumo_ecoadvance', 'resumo_por_pespp', 'totais_globais_ecoadvance')

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

  await registerParquet('resumo_ecoadvance.parquet');
  await registerParquet('resumo_por_pespp.parquet');
  await registerParquet('totais_globais_ecoadvance.parquet');"""

    old_register = (
        "  async function registerParquet(file) {\n"
        "    const resp = await fetch(file);\n"
        "    const buf = new Uint8Array(await resp.arrayBuffer());\n"
        "    await db.registerFileBuffer(file, buf);\n"
        "  }\n"
        "\n"
        "  await registerParquet('resumo_ecoadvance.parquet');\n"
        "  await registerParquet('resumo_por_pespp.parquet');\n"
        "  await registerParquet('totais_globais_ecoadvance.parquet');"
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
    for tabela in ('resumo_ecoadvance', 'resumo_por_pespp', 'totais_globais_ecoadvance'):
        caminho = os.path.join(DIRETORIO_ENTREGA, f'{tabela}.csv')
        con.execute(f"COPY {tabela} TO '{caminho}' (HEADER, DELIMITER ',')")
    imprimir_mensagem("CSVs de entrega exportados.")


def main():
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)
    con = duckdb.connect(GOLD_DB, config={'temp_directory': tempfile.gettempdir()})
    con.execute("SET preserve_insertion_order=false")

    imprimir_mensagem("Gerando cruzamento EcoAdvance...")
    criar_resumo_ecoadvance(con)
    imprimir_mensagem("Agregando por Produto PESPP...")
    criar_resumo_por_pespp(con)
    imprimir_mensagem("Calculando totais globais EcoAdvance...")
    criar_totais_globais_ecoadvance(con)
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
