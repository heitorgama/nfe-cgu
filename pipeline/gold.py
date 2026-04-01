import os
import tempfile
from datetime import datetime

import duckdb

DIRETORIO_SILVER = 'extracoes/silver'
DIRETORIO_GOLD = 'extracoes/gold'
GOLD_DB = os.path.join(DIRETORIO_GOLD, 'gold.duckdb')
MAPEAMENTO_GRUPO_A = 'dados/mapeamento_grupo_A.csv'
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
    """Valor total e fornecedores distintos por tipo - anos como colunas (Grupo A, NCM exato)"""
    con.execute(f"""
        CREATE OR REPLACE TABLE _tmp_grupo_a AS
        WITH itens_cruzados AS (
            SELECT s.*, m.tipo
            FROM 'extracoes/silver/itens.parquet' s
            JOIN '{MAPEAMENTO_GRUPO_A}' m ON m.ncm = s.codigo_ncm_sh
        )
        SELECT
            tipo,
            LEFT(periodo, 4)                  AS ano,
            SUM(valor_total)                  AS valor_total,
            COUNT(DISTINCT {EMITENTE})         AS fornecedores_distintos,
            COUNT(DISTINCT chave_de_acesso)    AS notas_distintas
        FROM itens_cruzados
        GROUP BY ALL
    """)

    anos = _anos_disponiveis(con, '_tmp_grupo_a')
    col_defs = _col_defs_pivot(anos, 'valor_total', 'fornecedores_distintos')
    col_notas = ', '.join(
        f"SUM(CASE WHEN ano = '{a}' THEN notas_distintas END)::BIGINT AS \"Notas Distintas {a}\""
        for a in anos
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE resumo_grupo_a AS
        SELECT tipo, {col_defs}, {col_notas}
        FROM _tmp_grupo_a
        GROUP BY tipo
        ORDER BY SUM(valor_total) DESC
    """)
    con.execute("DROP TABLE _tmp_grupo_a")

    count = con.execute("SELECT COUNT(*) FROM resumo_grupo_a").fetchone()[0]
    imprimir_mensagem(f"resumo_grupo_a: {count} linhas.")


def criar_totais_grupo_a(con: duckdb.DuckDBPyConnection) -> None:
    """Totais distintos por ano para Grupo A (NCM exato)"""
    con.execute(f"""
        CREATE OR REPLACE TABLE totais_grupo_a AS
        WITH itens_cruzados AS (
            SELECT s.*, m.tipo
            FROM 'extracoes/silver/itens.parquet' s
            JOIN '{MAPEAMENTO_GRUPO_A}' m ON m.ncm = s.codigo_ncm_sh
        )
        SELECT
            LEFT(periodo, 4)                                AS ano,
            SUM(valor_total)::DOUBLE                        AS valor_total,
            COUNT(DISTINCT {EMITENTE})                      AS fornecedores_distintos,
            COUNT(DISTINCT chave_de_acesso)                 AS notas_distintas
        FROM itens_cruzados
        GROUP BY LEFT(periodo, 4)
        ORDER BY ano
    """)
    imprimir_mensagem("totais_grupo_a calculado.")


def criar_itens_grupo_b_regex(con: duckdb.DuckDBPyConnection) -> None:
    """Detecta presença de palavras-chave e abreviações plásticas por regex nos itens (Grupo B)"""
    con.execute(f"""
        CREATE OR REPLACE TABLE itens_grupo_b_regex AS
        WITH itens_normalizado AS (
            SELECT *,
                REGEXP_REPLACE(
                    REPLACE(LOWER(descricao_do_produto_servico), '-', ' '),
                    '\\s+', ' '
                ) AS descricao_normalizada
            FROM 'extracoes/silver/itens.parquet'
        )
        SELECT
            YEAR(i.data_emissao)                                                        AS ano,
            i.codigo_ncm_sh                                                             AS ncm,
            TRIM(a.tipo)                                                                AS tipo_ncm,
            COALESCE(a.tipo IS NOT NULL, false)                                         AS ncm_mapeada,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bpolietileno\\b')                AS polietileno_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bpolicloreto de vinila\\b')      AS "policloreto de vinila_encontrado",
            REGEXP_MATCHES(i.descricao_normalizada, '\\bpoliestireno\\b')               AS poliestireno_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bpolibutadieno\\b')              AS polibutadieno_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bestireno butadieno\\b')         AS estireno_butadieno_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bpneus?\\b')                     AS pneus_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bembalagem plástica\\b')         AS embalagem_plastica_encontrada,
            REGEXP_MATCHES(i.descricao_normalizada, '\\btubo pvc\\b')                   AS tubo_pvc_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bembalagem pet\\b')              AS embalagem_pet_encontrada,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bpoliéster\\b')                  AS poliester_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bplástica\\b')                   AS plastica_encontrada,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bplástico\\b')                   AS plastico_encontrada,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bpe\\b')                         AS pe_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bpvc\\b')                        AS pvc_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bps\\b')                         AS ps_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bbr\\b')                         AS br_encontrado,
            REGEXP_MATCHES(i.descricao_normalizada, '\\bsbr\\b')                        AS sbr_encontrado,
            i.descricao_do_produto_servico                                              AS descricao,
            SUM(i.valor_total)                                                          AS total
        FROM itens_normalizado AS i
            LEFT JOIN '{MAPEAMENTO_GRUPO_A}' AS a
                ON CAST(i.codigo_ncm_sh AS VARCHAR) = CAST(a.ncm AS VARCHAR)
        GROUP BY ALL
        ORDER BY ano, total DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM itens_grupo_b_regex").fetchone()[0]
    imprimir_mensagem(f"itens_grupo_b_regex: {count} linhas.")


def exportar_parquets(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta tabelas gold como parquet"""
    for tabela in ('resumo_grupo_a', 'totais_grupo_a', 'itens_grupo_b_regex'):
        caminho = os.path.join(DIRETORIO_GOLD, f'{tabela}.parquet')
        con.execute(f"COPY {tabela} TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    imprimir_mensagem("Parquets gold exportados.")


def exportar_csvs_entrega(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta CSVs prontos para entrega"""
    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    for tabela in ('resumo_grupo_a',):
        caminho = os.path.join(DIRETORIO_ENTREGA, f'{tabela}.csv')
        con.execute(f"COPY {tabela} TO '{caminho}' (HEADER, DELIMITER ',')")

    df_b = con.execute("SELECT * FROM itens_grupo_b_regex").df()
    df_b.to_csv(
        os.path.join(DIRETORIO_GOLD, 'itens_grupo_b_regex.csv'),
        sep=";",
        decimal=",",
        quotechar='"',
        index=False,
        encoding="windows-1252",
        errors="replace",
    )
    df_b.to_csv(
        os.path.join(DIRETORIO_GOLD, 'itens_grupo_b_regex_utf8.csv'),
        index=False,
    )
    imprimir_mensagem("CSVs de entrega exportados.")


def exportar_html_interativo() -> None:
    """Gera HTML standalone com os parquets embutidos em base64"""
    import base64

    TABELAS = ('resumo_grupo_a', 'totais_grupo_a', 'itens_grupo_b_regex')

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
    """Gera as tabelas gold para Grupo A (por tipo de NCM) e exporta entregáveis."""
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)
    con = duckdb.connect(GOLD_DB, config={'temp_directory': tempfile.gettempdir()})
    con.execute("SET preserve_insertion_order=false")

    imprimir_mensagem("Calculando resumo Grupo A (por tipo)...")
    criar_resumo_grupo_a(con)

    imprimir_mensagem("Calculando totais distintos Grupo A...")
    criar_totais_grupo_a(con)

    imprimir_mensagem("Calculando itens Grupo B (regex keywords)...")
    criar_itens_grupo_b_regex(con)

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
