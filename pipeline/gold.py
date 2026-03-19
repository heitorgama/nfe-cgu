import os
import shutil
import tempfile
from datetime import datetime

import duckdb

DIRETORIO_SILVER = 'extracoes/silver'
DIRETORIO_GOLD = 'extracoes/gold'
GOLD_DB = os.path.join(DIRETORIO_GOLD, 'gold.duckdb')
MAPEAMENTO_NCM = 'dados/mapeamento_ncm.csv'
DIRETORIO_ENTREGA = os.path.join(DIRETORIO_GOLD, 'cruzamento_ncm')

EMITENTE = "COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente)"


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


def carregar_mapeamento(con: duckdb.DuckDBPyConnection) -> None:
    """Carrega o CSV de mapeamento NCM->Cadeia NIB no DuckDB"""
    con.execute(f"""
        CREATE OR REPLACE TABLE mapeamento_ncm AS
        SELECT
            codigo_cadeia, cadeia, missao, nome_missao,
            prefixo_sh, tipo_match, cnae, desc_cnae, criterio, icp,
            missoes, secao_cnae, sh_correspondente,
            CASE tipo_match
                WHEN 'ncm8'     THEN 8
                WHEN 'prefixo6' THEN 6
                WHEN 'prefixo5' THEN 5
                WHEN 'prefixo4' THEN 4
            END AS tamanho_prefixo
        FROM read_csv_auto('{MAPEAMENTO_NCM}', header=true, all_varchar=true)
    """)


def criar_itens_nib(con: duckdb.DuckDBPyConnection) -> None:
    """Faz o join dos itens do silver com o mapeamento NCM por prefixo fixo, deduplicado"""
    con.execute("""
        CREATE OR REPLACE TABLE itens_nib AS
        WITH silver AS (
            SELECT *, CAST(codigo_ncm_sh AS VARCHAR) AS ncm_str
            FROM read_parquet('extracoes/silver/itens.parquet')
            WHERE YEAR(data_emissao) BETWEEN 2022 AND 2025
        )
        SELECT DISTINCT *
        FROM (
            SELECT s.*, m.codigo_cadeia, m.cadeia, m.missao, m.nome_missao,
                         m.cnae, m.desc_cnae, m.criterio, m.icp, m.missoes, m.secao_cnae, m.sh_correspondente
            FROM silver s JOIN mapeamento_ncm m ON s.ncm_str = m.prefixo_sh AND m.tipo_match = 'ncm8'
            UNION ALL
            SELECT s.*, m.codigo_cadeia, m.cadeia, m.missao, m.nome_missao,
                         m.cnae, m.desc_cnae, m.criterio, m.icp, m.missoes, m.secao_cnae, m.sh_correspondente
            FROM silver s JOIN mapeamento_ncm m ON LEFT(s.ncm_str, 6) = m.prefixo_sh AND m.tipo_match = 'prefixo6'
            UNION ALL
            SELECT s.*, m.codigo_cadeia, m.cadeia, m.missao, m.nome_missao,
                         m.cnae, m.desc_cnae, m.criterio, m.icp, m.missoes, m.secao_cnae, m.sh_correspondente
            FROM silver s JOIN mapeamento_ncm m ON LEFT(s.ncm_str, 5) = m.prefixo_sh AND m.tipo_match = 'prefixo5'
            UNION ALL
            SELECT s.*, m.codigo_cadeia, m.cadeia, m.missao, m.nome_missao,
                         m.cnae, m.desc_cnae, m.criterio, m.icp, m.missoes, m.secao_cnae, m.sh_correspondente
            FROM silver s JOIN mapeamento_ncm m ON LEFT(s.ncm_str, 4) = m.prefixo_sh AND m.tipo_match = 'prefixo4'
        ) sub
    """)
    count = con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0]
    imprimir_mensagem(f"itens_nib: {count} linhas (2022-2025).")


def criar_resumo_por_cadeia(con: duckdb.DuckDBPyConnection) -> None:
    """Valor total e fornecedores distintos por cadeia NIB."""
    con.execute(f"""
        CREATE OR REPLACE TABLE resumo_cadeia AS
        SELECT
            codigo_cadeia,
            cadeia,
            missao,
            nome_missao,
            SUM(valor_total)           AS valor_total_adquirido,
            COUNT(DISTINCT {EMITENTE}) AS fornecedores_distintos,
            COUNT(*)                   AS quantidade_itens
        FROM itens_nib
        GROUP BY codigo_cadeia, cadeia, missao, nome_missao
        ORDER BY valor_total_adquirido DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM resumo_cadeia").fetchone()[0]
    imprimir_mensagem(f"resumo_cadeia: {count} cadeias.")


def criar_totais_globais(con: duckdb.DuckDBPyConnection) -> None:
    """Total global deduplicado por item físico — sem dupla contagem entre cadeias.
    Um item que pertence a N cadeias conta apenas uma vez."""
    con.execute(f"""
        CREATE OR REPLACE TABLE totais_globais AS
        SELECT
            SUM(valor_total)         AS valor_total_global,
            COUNT(DISTINCT emitente) AS fornecedores_global,
            COUNT(*)                 AS itens_global
        FROM (
            SELECT
                chave_de_acesso, numero, numero_produto,
                ANY_VALUE(valor_total) AS valor_total,
                ANY_VALUE({EMITENTE})  AS emitente
            FROM itens_nib
            GROUP BY chave_de_acesso, numero, numero_produto
        )
    """)
    imprimir_mensagem("totais_globais: calculado.")


def criar_detalhado_por_cadeia(con: duckdb.DuckDBPyConnection) -> None:
    """Tabela detalhada com descrição dos produtos agrupadas por cadeia e NCM"""
    con.execute(f"""
        CREATE OR REPLACE TABLE detalhado_cadeia AS
        SELECT
            codigo_cadeia, cadeia, missao, nome_missao,
            cnae, desc_cnae, criterio, icp,
            codigo_ncm_sh, ncm_sh_tipo_de_produto, descricao_do_produto_servico,
            SUM(valor_total)                AS valor_total,
            COUNT(DISTINCT {EMITENTE})      AS fornecedores_distintos,
            SUM(quantidade)                 AS quantidade_total,
            COUNT(*)                        AS registros
        FROM itens_nib
        GROUP BY ALL
        ORDER BY codigo_cadeia, valor_total DESC
    """)
    count = con.execute("SELECT COUNT(*) FROM detalhado_cadeia").fetchone()[0]
    imprimir_mensagem(f"detalhado_cadeia: {count} linhas.")


def criar_cnae_cadeia(con: duckdb.DuckDBPyConnection) -> None:
    """Agrega no nível cadeia x CNAE, lendo do parquet para eficiência de memória."""
    parquet_itens = os.path.join(DIRETORIO_GOLD, 'itens_nib.parquet')
    con.execute(f"""
        CREATE OR REPLACE TABLE cnae_cadeia AS
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
                              AS "Número de Itens"
        FROM '{parquet_itens}'
        GROUP BY
            missao, nome_missao, codigo_cadeia, cadeia,
            cnae, desc_cnae, criterio, missoes,
            icp, secao_cnae, sh_correspondente
        ORDER BY "Código da Cadeia" ASC
    """)
    count = con.execute("SELECT COUNT(*) FROM cnae_cadeia").fetchone()[0]
    imprimir_mensagem(f"cnae_cadeia: {count} linhas.")


def exportar_parquets(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta tabelas gold como parquet."""
    for tabela in ('itens_nib', 'resumo_cadeia', 'detalhado_cadeia', 'cnae_cadeia', 'totais_globais'):
        caminho = os.path.join(DIRETORIO_GOLD, f'{tabela}.parquet')
        con.execute(f"COPY {tabela} TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    imprimir_mensagem("Parquets gold exportados.")


def exportar_csvs_entrega(con: duckdb.DuckDBPyConnection) -> None:
    """Exporta CSVs prontos pra entrega ao MDIC."""
    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    for tabela in ('resumo_cadeia', 'detalhado_cadeia', 'cnae_cadeia'):
        caminho = os.path.join(DIRETORIO_ENTREGA, f'{tabela}.csv')
        con.execute(f"COPY {tabela} TO '{caminho}' (HEADER, DELIMITER ',')")
    imprimir_mensagem("CSVs de entrega exportados.")


def exportar_html_interativo() -> None:
    """Gera HTML standalone com os parquets embutidos em base64."""
    import base64

    TABELAS = ('resumo_cadeia', 'detalhado_cadeia', 'cnae_cadeia', 'totais_globais')

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
}}"""

    old_register = (
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
    """Gera as tabelas gold com cruzamento NCM→Cadeias NIB e exporta entregáveis"""
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)
    con = duckdb.connect(GOLD_DB, config={'temp_directory': tempfile.gettempdir()})
    con.execute("SET preserve_insertion_order=false")

    imprimir_mensagem("Carregando mapeamento NCM...")
    carregar_mapeamento(con)
    imprimir_mensagem("Cruzando itens com cadeias NIB (JOIN por prefixo NCM)...")
    criar_itens_nib(con)
    imprimir_mensagem("Calculando resumo por cadeia...")
    criar_resumo_por_cadeia(con)
    imprimir_mensagem("Calculando totais globais deduplicados...")
    criar_totais_globais(con)
    imprimir_mensagem("Gerando tabela detalhada por cadeia...")
    criar_detalhado_por_cadeia(con)
    imprimir_mensagem("Exportando itens_nib.parquet para leitura colunar...")
    parquet_itens = os.path.join(DIRETORIO_GOLD, 'itens_nib.parquet')
    con.execute(f"COPY itens_nib TO '{parquet_itens}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    imprimir_mensagem("Gerando agregação por CNAE e cadeia...")
    criar_cnae_cadeia(con)
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
