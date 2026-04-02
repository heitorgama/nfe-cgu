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
            codigo_cadeia,
            cadeia,
            missao,
            nome_missao,
            SUM(valor_total)                                                      AS valor_total_adquirido,
            COUNT(DISTINCT {EMITENTE})                                            AS fornecedores_distintos,
            COUNT(*)                                                              AS quantidade_itens,
            SUM(CASE WHEN YEAR(data_emissao) = 2022 THEN valor_total ELSE 0 END) AS valor_2022,
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END) AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END) AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END) AS valor_2025
        FROM itens_nib
        GROUP BY codigo_cadeia, cadeia, missao, nome_missao
        ORDER BY valor_total_adquirido DESC
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
            codigo_cadeia, cadeia, missao, nome_missao,
            cnae, desc_cnae, criterio, icp,
            codigo_ncm_sh, ncm_sh_tipo_de_produto, descricao_do_produto_servico,
            SUM(valor_total)                                                      AS valor_total,
            COUNT(DISTINCT {EMITENTE})                                            AS fornecedores_distintos,
            SUM(quantidade)                                                       AS quantidade_total,
            COUNT(*)                                                              AS registros,
            SUM(CASE WHEN YEAR(data_emissao) = 2022 THEN valor_total ELSE 0 END) AS valor_2022,
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END) AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END) AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END) AS valor_2025
        FROM itens_nib
        GROUP BY ALL
        ORDER BY valor_total DESC
        LIMIT 2000
    """)
    count = con.execute("SELECT COUNT(*) FROM detalhado_cadeia").fetchone()[0]
    imprimir_mensagem(f"detalhado_cadeia: {count} linhas (top 2000 por valor).")


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
            missao            AS "Número da Missão",
            nome_missao       AS "Nome da Missão",
            codigo_cadeia     AS "Código da Cadeia",
            cadeia            AS "Cadeia Prioritária",
            cnae              AS "CNAE",
            desc_cnae         AS "Descrição da CNAE",
            criterio          AS "Critério",
            missoes           AS "Missões",
            icp               AS "ICP",
            secao_cnae        AS "Seção CNAE",
            sh_correspondente AS "SH Correspondente",
            SUM(valor_total)  AS "Valor Total",
            COUNT(DISTINCT {EMITENTE})
                              AS "Número de Fornecedores",
            COUNT(DISTINCT chave_de_acesso)
                              AS "Número de Notas Fiscais",
            COUNT(DISTINCT chave_de_acesso || '-' || CAST(numero AS VARCHAR) || '-' || CAST(numero_produto AS VARCHAR))
                              AS "Número de Itens",
            SUM(CASE WHEN YEAR(data_emissao) = 2022 THEN valor_total ELSE 0 END) AS valor_2022,
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END) AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END) AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END) AS valor_2025
        FROM '{parquet_itens}'
        GROUP BY
            missao, nome_missao, codigo_cadeia, cadeia,
            cnae, desc_cnae, criterio, missoes,
            icp, secao_cnae, sh_correspondente
        ORDER BY "Código da Cadeia" ASC
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
}}

  await registerParquet('resumo_cadeia.parquet');
  await registerParquet('detalhado_cadeia.parquet');
  await registerParquet('totais_globais.parquet');
  await registerParquet(TABELA_NIB);"""

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
        "  }\n"
        "\n"
        "  await registerParquet('resumo_cadeia.parquet');\n"
        "  await registerParquet('detalhado_cadeia.parquet');\n"
        "  await registerParquet('totais_globais.parquet');\n"
        "  await registerParquet(TABELA_NIB);"
    )
    html = html.replace(old_register, inline_register)

    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    destino = os.path.join(DIRETORIO_ENTREGA, 'preview.html')
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
