import os
import tempfile
from datetime import datetime

import duckdb

DIRETORIO_SILVER = 'extracoes/silver'
DIRETORIO_GOLD = 'extracoes/gold'
GOLD_DB = os.path.join(DIRETORIO_GOLD, 'gold.duckdb')
MAPEAMENTO_GRUPO_A = 'dados/mapeamento_grupo_A.csv'
MAPEAMENTO_GRUPO_B = 'dados/mapeamento_grupo_B.csv'
DIRETORIO_ENTREGA = os.path.join(DIRETORIO_GOLD, 'cruzamento_ncm')

EMITENTE = "COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente)"


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


def _anos_disponiveis(con: duckdb.DuckDBPyConnection, tabela: str) -> list:
    return [r[0] for r in con.execute(f"SELECT DISTINCT ano FROM {tabela} ORDER BY ano").fetchall()]


def _col_defs_pivot(anos: list, col_valor: str, col_forn: str) -> str:
    partes = []
    for a in anos:
        partes.append(f"SUM(CASE WHEN ano = '{a}' THEN {col_valor} END)::DOUBLE AS \"Valor Total {a}\"")
        partes.append(f"SUM(CASE WHEN ano = '{a}' THEN {col_forn} END)::BIGINT AS \"Fornecedores {a}\"")
    return ', '.join(partes)


def criar_resumo_grupo_a(con: duckdb.DuckDBPyConnection) -> None:
    """Valor total e fornecedores distintos por tipo — anos como colunas (Grupo A, NCM exato)."""
    con.execute(f"""
        CREATE OR REPLACE TABLE _tmp_grupo_a AS
        WITH itens_cruzados AS (
            SELECT s.*, m.tipo
            FROM 'extracoes/silver/itens.parquet' s
            JOIN '{MAPEAMENTO_GRUPO_A}' m ON m.ncm = s.codigo_ncm_sh
        )
        SELECT
            tipo,
            LEFT(periodo, 4)           AS ano,
            SUM(valor_total)           AS valor_total,
            COUNT(DISTINCT {EMITENTE}) AS fornecedores_distintos
        FROM itens_cruzados
        GROUP BY ALL
    """)

    anos = _anos_disponiveis(con, '_tmp_grupo_a')
    col_defs = _col_defs_pivot(anos, 'valor_total', 'fornecedores_distintos')

    con.execute(f"""
        CREATE OR REPLACE TABLE resumo_grupo_a AS
        SELECT tipo, {col_defs}
        FROM _tmp_grupo_a
        GROUP BY tipo
        ORDER BY SUM(valor_total) DESC
    """)
    con.execute("DROP TABLE _tmp_grupo_a")

    count = con.execute("SELECT COUNT(*) FROM resumo_grupo_a").fetchone()[0]
    imprimir_mensagem(f"resumo_grupo_a: {count} linhas.")


def criar_resumo_grupo_b(con: duckdb.DuckDBPyConnection) -> None:
    """Valor total e fornecedores distintos por palavra-chave — anos como colunas (Grupo B)."""
    con.execute(f"""
        CREATE OR REPLACE TABLE _tmp_grupo_b AS
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
            FROM '{MAPEAMENTO_GRUPO_B}'
        )
        SELECT
            LEFT(s.periodo, 4)               AS ano,
            TRIM(b."Palavras Chaves")         AS grupo_b,
            b."Abreviação"                    AS abreviacao,
            SUM(s.valor_total)               AS valor_total_adquirido,
            COUNT(DISTINCT {EMITENTE})        AS fornecedores_distintos
        FROM chaves_normalizado AS b
        JOIN itens_normalizado AS s
            ON s.descricao_normalizada LIKE '%' || b.chave_normalizada || '%'
        GROUP BY ano, TRIM(b."Palavras Chaves"), b."Abreviação"
    """)

    anos = _anos_disponiveis(con, '_tmp_grupo_b')
    col_defs = _col_defs_pivot(anos, 'valor_total_adquirido', 'fornecedores_distintos')

    con.execute(f"""
        CREATE OR REPLACE TABLE resumo_grupo_b AS
        SELECT grupo_b, abreviacao, {col_defs}
        FROM _tmp_grupo_b
        GROUP BY grupo_b, abreviacao
        ORDER BY SUM(valor_total_adquirido) DESC
    """)
    con.execute("DROP TABLE _tmp_grupo_b")

    count = con.execute("SELECT COUNT(*) FROM resumo_grupo_b").fetchone()[0]
    imprimir_mensagem(f"resumo_grupo_b: {count} linhas.")


def exportar_parquets(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta tabelas gold como parquet."""
    for tabela in ('resumo_grupo_a', 'resumo_grupo_b'):
        caminho = os.path.join(DIRETORIO_GOLD, f'{tabela}.parquet')
        con.execute(f"COPY {tabela} TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    imprimir_mensagem("Parquets gold exportados.")


def exportar_csvs_entrega(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta CSVs prontos para entrega."""
    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    for tabela in ('resumo_grupo_a', 'resumo_grupo_b'):
        caminho = os.path.join(DIRETORIO_ENTREGA, f'{tabela}.csv')
        con.execute(f"COPY {tabela} TO '{caminho}' (HEADER, DELIMITER ',')")
    imprimir_mensagem("CSVs de entrega exportados.")


def exportar_html_interativo() -> None:
    """Gera HTML standalone com os parquets embutidos em base64."""
    import base64

    TABELAS = ('resumo_grupo_a', 'resumo_grupo_b')

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

const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
const workerUrl = URL.createObjectURL(
  new Blob([`importScripts("${{bundle.mainWorker}}");`], {{ type: 'text/javascript' }})
);
const logger = new duckdb.ConsoleLogger();
const worker = new Worker(workerUrl);
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule);
URL.revokeObjectURL(workerUrl);
const conn = await db.connect();

async function registerParquet(file) {{
  const buf = base64ToUint8Array(_PARQUET_DATA[file]);
  await db.registerFileBuffer(file, buf);
}}"""

    old_register = (
        "  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());\n"
        "  const workerUrl = URL.createObjectURL(\n"
        "    new Blob([`importScripts(\"${bundle.mainWorker}\");`], { type: 'text/javascript' })\n"
        "  );\n"
        "  const logger = new duckdb.ConsoleLogger();\n"
        "  const worker = new Worker(workerUrl);\n"
        "  const db = new duckdb.AsyncDuckDB(logger, worker);\n"
        "  await db.instantiate(bundle.mainModule);\n"
        "  URL.revokeObjectURL(workerUrl);\n"
        "  const conn = await db.connect();\n"
        "\n"
        "  async function registerParquet(file) {\n"
        "    const resp = await fetch(file);\n"
        "    const buf = new Uint8Array(await resp.arrayBuffer());\n"
        "    await db.registerFileBuffer(file, buf);\n"
        "  }"
    )
    html = html.replace(old_register, inline_register)

    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    destino = os.path.join(DIRETORIO_ENTREGA, 'index.html')
    with open(destino, 'w', encoding='utf-8') as f:
        f.write(html)

    tamanho_mb = os.path.getsize(destino) / 1024 / 1024
    imprimir_mensagem(f"HTML standalone gerado: {destino} ({tamanho_mb:.1f} MB)")


def main():
    """Gera as tabelas gold para Grupo A (tipo) e Grupo B (palavras-chave) e exporta entregáveis."""
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)
    con = duckdb.connect(GOLD_DB, config={'temp_directory': tempfile.gettempdir()})
    con.execute("SET preserve_insertion_order=false")

    imprimir_mensagem("Calculando resumo Grupo A (por tipo)...")
    criar_resumo_grupo_a(con)

    imprimir_mensagem("Calculando resumo Grupo B (por palavras-chave)...")
    criar_resumo_grupo_b(con)

    imprimir_mensagem("Exportando parquets gold...")
    exportar_parquets(con)

    imprimir_mensagem("Exportando CSVs de entrega...")
    exportar_csvs_entrega(con)

    template = os.path.join(os.path.dirname(__file__), 'template', 'dashboard.html')
    if os.path.exists(template):
        imprimir_mensagem("Exportando HTML interativo standalone...")
        exportar_html_interativo()
    else:
        imprimir_mensagem("Template dashboard.html não encontrado, pulando HTML interativo.")

    con.close()
    imprimir_mensagem("Gold salvo.")


if __name__ == "__main__":
    main()
