import os
import tempfile
from datetime import datetime

import duckdb

DIRETORIO_SILVER = 'extracoes/silver'
DIRETORIO_GOLD = 'extracoes/gold'
GOLD_DB = os.path.join(DIRETORIO_GOLD, 'gold.duckdb')
DIRETORIO_ENTREGA = os.path.join(DIRETORIO_GOLD, 'cruzamento_ncm')

EMITENTE = "COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente)"


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


def criar_itens_nib(con: duckdb.DuckDBPyConnection) -> None:
    """Itens NIB: notas fiscais cruzadas com mapeamento NCM por prefixo"""
    con.execute("""
        CREATE OR REPLACE TABLE itens_nib AS
        WITH mapeamento AS (
            SELECT m.*, n.codigo AS codigo_ncm_sh_mapeado
            FROM 'dados/mapeamento_ncm.csv' AS m
            LEFT JOIN 'dados/ncm.csv' AS n ON m.codigo_ncm = n.prefixo
                OR RPAD(m.codigo_ncm, 6, '0') = n.prefixo
                OR RPAD(m.codigo_ncm, 8, '0') = n.prefixo
                OR (m.codigo_ncm = '560390' AND n.prefixo = '56039')
                OR (m.codigo_ncm = '401511' AND n.prefixo = '40151')
                OR (m.codigo_ncm = '270760' AND n.prefixo = '27074000')
        )
        SELECT
            s.chave_de_acesso,
            YEAR(s.data_emissao)                            AS ano,
            ARRAY_AGG(DISTINCT m.codigo_cadeia)             AS cadeias_nib,
            s.uf_emitente,
            s.uf_destinatario,
            s.codigo_orgao_superior_destinatario,
            s.codigo_orgao_destinatario,
            s.cnpj_destinatario,
            s.nome_destinatario,
            s.descricao_do_produto_servico                  AS descricao,
            s.cnpj_emitente,
            s.cpf_emitente,
            s.valor_total                                   AS valor
        FROM 'extracoes/silver/itens.parquet' AS s
            LEFT JOIN mapeamento AS m
                ON s.codigo_ncm_sh = m.codigo_ncm_sh_mapeado
        WHERE YEAR(s.data_emissao) > 2022
        GROUP BY ALL
    """)
    count = con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0]
    imprimir_mensagem(f"itens_nib: {count} linhas.")


def criar_resumo_nib(con: duckdb.DuckDBPyConnection) -> None:
    """Resumo NIB por cadeia: valores e contagens por ano"""
    con.execute(r"""
        CREATE OR REPLACE TABLE resumo_nib AS
        WITH mapeamento_raw AS (
            SELECT m.*, n.codigo AS codigo_ncm_sh_mapeado
            FROM 'dados/mapeamento_ncm.csv' AS m
                LEFT JOIN 'dados/ncm.csv' AS n
                    ON  m.codigo_ncm = n.prefixo
                    OR  RPAD(m.codigo_ncm, 6, '0') = n.prefixo
                    OR  RPAD(m.codigo_ncm, 8, '0') = n.prefixo
                    OR  (m.codigo_ncm = '560390' AND n.prefixo = '56039')
                    OR  (m.codigo_ncm = '401511' AND n.prefixo = '40151')
                    OR  (m.codigo_ncm = '270760' AND n.prefixo = '27074000')
        ),
        -- deduplica por (cadeia, cnae, ncm): mesmo NCM pode estar em múltiplas linhas do CSV com critérios diferentes
        mapeamento AS (
            SELECT
                codigo_cadeia,
                "CNAE"                                                    AS cnae,
                codigo_ncm_sh_mapeado,
                ANY_VALUE("Cadeia Prioritária")                           AS cadeia,
                ANY_VALUE("Número da Missão")                             AS missao,
                ANY_VALUE("Nome da Missão")                               AS nome_missao,
                ANY_VALUE("Descrição da CNAE")                            AS desc_cnae,
                ANY_VALUE("Critério")                                     AS criterio,
                ANY_VALUE("ICP")                                          AS icp,
                ANY_VALUE("Missões")                                      AS missoes,
                ANY_VALUE("Seção CNAE")                                   AS secao_cnae,
                ANY_VALUE("SH Correspondente")                            AS sh_correspondente
            FROM mapeamento_raw
            WHERE codigo_ncm_sh_mapeado IS NOT NULL
            GROUP BY codigo_cadeia, "CNAE", codigo_ncm_sh_mapeado
        )
        SELECT
            m.codigo_cadeia,
            m.cadeia,
            m.missao,
            m.nome_missao,
            m.cnae,
            m.desc_cnae,
            m.criterio,
            m.icp,
            m.missoes,
            m.secao_cnae,
            m.sh_correspondente,
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END)::DOUBLE                                                                     AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END)::DOUBLE                                                                     AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END)::DOUBLE                                                                     AS valor_2025,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2023 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)                              AS qtd_emitentes_2023,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2024 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)                              AS qtd_emitentes_2024,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2025 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)                              AS qtd_emitentes_2025,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2023 THEN chave_de_acesso END)                                                                     AS qtd_notas_fiscais_2023,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2024 THEN chave_de_acesso END)                                                                     AS qtd_notas_fiscais_2024,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2025 THEN chave_de_acesso END)                                                                     AS qtd_notas_fiscais_2025
        FROM 'extracoes/silver/itens.parquet' AS s
            JOIN mapeamento AS m ON s.codigo_ncm_sh = m.codigo_ncm_sh_mapeado
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    """)
    count = con.execute("SELECT COUNT(*) FROM resumo_nib").fetchone()[0]
    imprimir_mensagem(f"resumo_nib: {count} linhas.")


def criar_resumo_cadeia_nib(con: duckdb.DuckDBPyConnection) -> None:
    """Resumo NIB agregado por cadeia: valores e contagens por ano"""
    con.execute(r"""
        CREATE OR REPLACE TABLE resumo_cadeia_nib AS
        WITH mapeamento_raw AS (
            SELECT m.*, n.codigo AS codigo_ncm_sh_mapeado
            FROM 'dados/mapeamento_ncm.csv' AS m
                LEFT JOIN 'dados/ncm.csv' AS n
                    ON  m.codigo_ncm = n.prefixo
                    OR  RPAD(m.codigo_ncm, 6, '0') = n.prefixo
                    OR  RPAD(m.codigo_ncm, 8, '0') = n.prefixo
                    OR  (m.codigo_ncm = '560390' AND n.prefixo = '56039')
                    OR  (m.codigo_ncm = '401511' AND n.prefixo = '40151')
                    OR  (m.codigo_ncm = '270760' AND n.prefixo = '27074000')
        ),
        -- deduplica: um único registro por (cadeia, ncm), evitando que o mesmo item seja somado múltiplas vezes
        mapeamento AS (
            SELECT DISTINCT
                codigo_cadeia,
                ANY_VALUE("Cadeia Prioritária") AS cadeia,
                codigo_ncm_sh_mapeado
            FROM mapeamento_raw
            WHERE codigo_ncm_sh_mapeado IS NOT NULL
            GROUP BY codigo_cadeia, codigo_ncm_sh_mapeado
        )
        SELECT
            m.codigo_cadeia,
            ANY_VALUE(m.cadeia)                                                                                                          AS cadeia,
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END)::DOUBLE                                                AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END)::DOUBLE                                                AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END)::DOUBLE                                                AS valor_2025,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2023 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)         AS qtd_emitentes_2023,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2024 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)         AS qtd_emitentes_2024,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2025 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)         AS qtd_emitentes_2025,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2023 THEN chave_de_acesso END)                                                AS qtd_notas_fiscais_2023,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2024 THEN chave_de_acesso END)                                                AS qtd_notas_fiscais_2024,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2025 THEN chave_de_acesso END)                                                AS qtd_notas_fiscais_2025
        FROM mapeamento AS m
            JOIN 'extracoes/silver/itens.parquet' AS s
                ON s.codigo_ncm_sh = m.codigo_ncm_sh_mapeado
        GROUP BY 1
    """)
    count = con.execute("SELECT COUNT(*) FROM resumo_cadeia_nib").fetchone()[0]
    imprimir_mensagem(f"resumo_cadeia_nib: {count} linhas.")


def criar_resumo_uf_cnae_nib(con: duckdb.DuckDBPyConnection) -> None:
    """Resumo NIB agregado por UF do emitente, cadeia e CNAE: valores por ano"""
    con.execute(r"""
        CREATE OR REPLACE TABLE resumo_uf_cnae_nib AS
        WITH mapeamento_raw AS (
            SELECT m.*, n.codigo AS codigo_ncm_sh_mapeado
            FROM 'dados/mapeamento_ncm.csv' AS m
                LEFT JOIN 'dados/ncm.csv' AS n
                    ON  m.codigo_ncm = n.prefixo
                    OR  RPAD(m.codigo_ncm, 6, '0') = n.prefixo
                    OR  RPAD(m.codigo_ncm, 8, '0') = n.prefixo
                    OR  (m.codigo_ncm = '560390' AND n.prefixo = '56039')
                    OR  (m.codigo_ncm = '401511' AND n.prefixo = '40151')
                    OR  (m.codigo_ncm = '270760' AND n.prefixo = '27074000')
        ),
        mapeamento AS (
            SELECT DISTINCT codigo_cadeia, codigo_ncm_sh_mapeado
            FROM mapeamento_raw
            WHERE codigo_ncm_sh_mapeado IS NOT NULL
        )
        SELECT
            m.codigo_cadeia,
            s.codigo_ncm_sh                                                                                                                                   AS ncm,
            s.uf_emitente                                                                                                                                     AS uf,
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END)::DOUBLE                                                                     AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END)::DOUBLE                                                                     AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END)::DOUBLE                                                                     AS valor_2025
        FROM mapeamento AS m
            JOIN 'extracoes/silver/itens.parquet' AS s
                ON s.codigo_ncm_sh = m.codigo_ncm_sh_mapeado
        GROUP BY 1, 2, 3
        ORDER BY valor_2025 DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM resumo_uf_cnae_nib").fetchone()[0]
    imprimir_mensagem(f"resumo_uf_cnae_nib: {count} linhas.")


def criar_totais_nib(con: duckdb.DuckDBPyConnection) -> None:
    """Total geral NIB: soma direto do silver filtrando pelos NCMs do mapeamento"""
    con.execute("""
        CREATE OR REPLACE TABLE totais_nib AS
        WITH mapeamento_raw AS (
            SELECT m.*, n.codigo AS codigo_ncm_sh_mapeado
            FROM 'dados/mapeamento_ncm.csv' AS m
                LEFT JOIN 'dados/ncm.csv' AS n
                    ON  m.codigo_ncm = n.prefixo
                    OR  RPAD(m.codigo_ncm, 6, '0') = n.prefixo
                    OR  RPAD(m.codigo_ncm, 8, '0') = n.prefixo
                    OR  (m.codigo_ncm = '560390' AND n.prefixo = '56039')
                    OR  (m.codigo_ncm = '401511' AND n.prefixo = '40151')
                    OR  (m.codigo_ncm = '270760' AND n.prefixo = '27074000')
        ),
        ncms AS (
            SELECT DISTINCT codigo_ncm_sh_mapeado
            FROM mapeamento_raw
            WHERE codigo_ncm_sh_mapeado IS NOT NULL
        )
        SELECT
            SUM(CASE WHEN YEAR(data_emissao) = 2023 THEN valor_total ELSE 0 END)::DOUBLE                                              AS valor_2023,
            SUM(CASE WHEN YEAR(data_emissao) = 2024 THEN valor_total ELSE 0 END)::DOUBLE                                              AS valor_2024,
            SUM(CASE WHEN YEAR(data_emissao) = 2025 THEN valor_total ELSE 0 END)::DOUBLE                                              AS valor_2025,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2023 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)       AS qtd_emitentes_2023,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2024 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)       AS qtd_emitentes_2024,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2025 THEN COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) END)       AS qtd_emitentes_2025,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2023 THEN chave_de_acesso END)                                              AS qtd_notas_fiscais_2023,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2024 THEN chave_de_acesso END)                                              AS qtd_notas_fiscais_2024,
            COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = 2025 THEN chave_de_acesso END)                                              AS qtd_notas_fiscais_2025
        FROM 'extracoes/silver/itens.parquet' AS s
        JOIN ncms ON ncms.codigo_ncm_sh_mapeado = s.codigo_ncm_sh
    """)
    count = con.execute("SELECT COUNT(*) FROM totais_nib").fetchone()[0]
    imprimir_mensagem(f"totais_nib: {count} linhas.")


def exportar_parquets(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta tabelas gold como parquet"""
    for tabela in ('itens_nib', 'resumo_nib', 'resumo_cadeia_nib', 'resumo_uf_cnae_nib', 'totais_nib'):
        caminho = os.path.join(DIRETORIO_GOLD, f'{tabela}.parquet')
        con.execute(f"COPY {tabela} TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    imprimir_mensagem("Parquets gold exportados.")


def exportar_csvs_entrega(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta CSVs prontos para entrega"""
    for tabela in ('itens_nib', 'resumo_nib', 'resumo_cadeia_nib', 'resumo_uf_cnae_nib', 'totais_nib'):
        df = con.execute(f"SELECT * FROM {tabela}").df()
        df.to_csv(
            os.path.join(DIRETORIO_GOLD, f'{tabela}.csv'),
            sep=";",
            decimal=",",
            quotechar='"',
            index=False,
            encoding="windows-1252",
            errors="replace",
        )
        df.to_csv(
            os.path.join(DIRETORIO_GOLD, f'{tabela}_utf8.csv'),
            index=False,
        )
    imprimir_mensagem("CSVs de entrega exportados.")


def exportar_html_interativo() -> None:
    """Gera HTML standalone com os parquets embutidos em base64"""
    import base64

    TABELAS = ('resumo_cadeia_nib', 'resumo_nib', 'resumo_uf_cnae_nib', 'totais_nib')

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

  await registerParquet('resumo_cadeia_nib.parquet');
  await registerParquet('resumo_nib.parquet');
  await registerParquet('resumo_uf_cnae_nib.parquet');
  await registerParquet('totais_nib.parquet');"""

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
        "  await registerParquet('resumo_cadeia_nib.parquet');\n"
        "  await registerParquet('resumo_nib.parquet');"
    )
    html = html.replace(old_register, inline_register)

    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    destino = os.path.join(DIRETORIO_ENTREGA, 'preview_nib.html')
    with open(destino, 'w', encoding='utf-8') as f:
        f.write(html)

    tamanho_mb = os.path.getsize(destino) / 1024 / 1024
    imprimir_mensagem(f"HTML standalone gerado: {destino} ({tamanho_mb:.1f} MB)")


def main():
    """Gera a tabela itens_nib e exporta entregáveis."""
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)
    con = duckdb.connect(GOLD_DB, config={'temp_directory': tempfile.gettempdir()})
    con.execute("SET preserve_insertion_order=false")

    imprimir_mensagem("Calculando itens NIB...")
    criar_itens_nib(con)

    imprimir_mensagem("Calculando resumo NIB...")
    criar_resumo_nib(con)

    imprimir_mensagem("Calculando resumo por cadeia NIB...")
    criar_resumo_cadeia_nib(con)

    imprimir_mensagem("Calculando resumo por UF NIB...")
    criar_resumo_uf_cnae_nib(con)

    imprimir_mensagem("Calculando totais NIB sem duplicação...")
    criar_totais_nib(con)

    imprimir_mensagem("Exportando parquets gold...")
    exportar_parquets(con)

    #imprimir_mensagem("Exportando CSVs de entrega...")
    #exportar_csvs_entrega(con)

    template = os.path.join(os.path.dirname(__file__), 'template', 'dashboard.html')
    if os.path.exists(template):
        imprimir_mensagem("Exportando HTML interativo standalone...")
        exportar_html_interativo()
    else:
        imprimir_mensagem("Template dashboard.html não encontrado, pulando HTML interativo.")

    #con.close()
    imprimir_mensagem("Gold salvo.")


if __name__ == "__main__":
    main()
