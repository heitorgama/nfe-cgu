import os
import tempfile
from datetime import datetime

import duckdb

DIRETORIO_SILVER = 'extracoes/silver'
DIRETORIO_GOLD = 'extracoes/gold'
GOLD_GERAL_DB = os.path.join(DIRETORIO_GOLD, 'gold_geral.duckdb')
NCM_CSV = 'dados/ncm.csv'

# Metadados da entrega: preencha para cada demanda
CONFIG = {
    'titulo':    'Compras Públicas — [Demanda]',
    'subtitulo': 'Notas Fiscais Eletrônicas do Executivo Federal · 2022-2025',
    'orgao':     'MGI · Ministério da Gestão e da Inovação em Serviços Públicos',
    'badge1':    'Portal da Transparência · CGU',
    'badge2':    '[Demanda]',
    # parquets a embutir: {nome_arquivo: caminho_local}
    'parquets': {
        'itens_ncm.parquet': os.path.join(DIRETORIO_GOLD, 'itens_ncm.parquet'),
    },
    # parquet que a tabela principal do HTML vai exibir
    'tabela_principal': 'itens_ncm.parquet',
    # colunas com formato monetário (R$) e numérico (inteiro)
    'colunas_monetarias': ['valor_total', 'valor_unitario'],
    'colunas_numericas':  ['quantidade', 'numero', 'numero_produto'],
    'diretorio_entrega':  os.path.join(DIRETORIO_GOLD, 'entrega'),
}


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


def carregar_ncm(con: duckdb.DuckDBPyConnection) -> None:
    """Carrega o CSV NCM filtrando apenas códigos de 8 dígitos"""
    con.execute(f"""
        CREATE OR REPLACE TABLE ncm AS
        SELECT
            Codigo  AS codigo_ncm,
            Descricao AS descricao_ncm
        FROM read_csv_auto('{NCM_CSV}', header=true, all_varchar=true)
        WHERE LENGTH(REGEXP_REPLACE(Codigo, '[^0-9]', '', 'g')) = 8
    """)
    count = con.execute("SELECT COUNT(*) FROM ncm").fetchone()[0]
    imprimir_mensagem(f"ncm: {count} códigos de 8 dígitos carregados.")


def criar_itens_com_ncm(con: duckdb.DuckDBPyConnection) -> None:
    """Join dos itens do silver com a tabela NCM para enriquecer com descrição oficial"""
    con.execute("""
        CREATE OR REPLACE TABLE itens_ncm AS
        SELECT
            s.*,
            n.descricao_ncm
        FROM read_parquet('extracoes/silver/itens.parquet') s
        LEFT JOIN ncm n
            ON LPAD(CAST(s.codigo_ncm_sh AS VARCHAR), 8, '0') = n.codigo_ncm
    """)
    count = con.execute("SELECT COUNT(*) FROM itens_ncm").fetchone()[0]
    imprimir_mensagem(f"itens_ncm: {count} linhas.")


def aplicar_mapeamento_demanda(con: duckdb.DuckDBPyConnection) -> None:
    """Placeholder: aplica mapeamento específico de uma demanda sobre itens_ncm.

    Implemente esta função no gold de cada demanda. O padrão esperado é criar
    uma tabela `itens_demanda` com as colunas adicionais necessárias.

    Exemplo:
        con.execute('''
            CREATE OR REPLACE TABLE itens_demanda AS
            SELECT i.*, m.categoria, m.flag_interesse
            FROM itens_ncm i
            JOIN read_csv_auto('dados/mapeamento_minha_demanda.csv', ...) m
                ON i.codigo_ncm_sh = m.ncm
        ''')
    """
    imprimir_mensagem("aplicar_mapeamento_demanda: nenhum mapeamento de demanda configurado (placeholder).")


def exportar_parquet(con: duckdb.DuckDBPyConnection) -> None:
    caminho = os.path.join(DIRETORIO_GOLD, 'itens_ncm.parquet')
    con.execute(f"COPY itens_ncm TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    imprimir_mensagem(f"Exportado: {caminho}")


def exportar_html_standalone(cfg: dict = CONFIG) -> None:
    """Gera HTML standalone com os parquets embutidos em base64

    Usa template/dashboard.html e substitui placeholders pelo CONFIG.
    Edite CONFIG no topo do arquivo para adaptar à entrega da demanda.
    """
    import base64
    import json

    template_path = os.path.join(os.path.dirname(__file__), 'template', 'dashboard.html')
    with open(template_path, encoding='utf-8') as f:
        html = f.read()

    # Embutir parquets como base64
    parquet_js_entries = []
    for nome, caminho in cfg['parquets'].items():
        with open(caminho, 'rb') as f:
            b64 = base64.b64encode(f.read()).decode('ascii')
        parquet_js_entries.append(f"  {json.dumps(nome)}: {json.dumps(b64)}")
    parquet_data_js = '{\n' + ',\n'.join(parquet_js_entries) + '\n}'

    html = (html
        .replace('__TITULO__',               cfg['titulo'])
        .replace('__SUBTITULO__',            cfg['subtitulo'])
        .replace('__ORGAO__',                cfg['orgao'])
        .replace('__BADGE1__',               cfg['badge1'])
        .replace('__BADGE2__',               cfg['badge2'])
        .replace('__TABELA_PRINCIPAL__',     cfg['tabela_principal'])
        .replace('__PARQUET_DATA_JS__',      parquet_data_js)
        .replace('__COLUNAS_MONETARIAS_JS__', json.dumps(cfg['colunas_monetarias']))
        .replace('__COLUNAS_NUMERICAS_JS__',  json.dumps(cfg['colunas_numericas']))
    )

    os.makedirs(cfg['diretorio_entrega'], exist_ok=True)
    destino = os.path.join(cfg['diretorio_entrega'], 'preview.html')
    with open(destino, 'w', encoding='utf-8') as f:
        f.write(html)

    tamanho_mb = os.path.getsize(destino) / 1024 / 1024
    imprimir_mensagem(f"HTML standalone gerado: {destino} ({tamanho_mb:.1f} MB)")


def main():
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)
    con = duckdb.connect(GOLD_GERAL_DB, config={'temp_directory': tempfile.gettempdir()})
    con.execute("SET preserve_insertion_order=false")

    imprimir_mensagem("Carregando tabela NCM...")
    carregar_ncm(con)
    imprimir_mensagem("Cruzando itens do silver com NCM...")
    criar_itens_com_ncm(con)
    imprimir_mensagem("Aplicando mapeamento de demanda...")
    aplicar_mapeamento_demanda(con)
    imprimir_mensagem("Exportando parquet...")
    exportar_parquet(con)
    con.close()

    template = os.path.join(os.path.dirname(__file__), 'template', 'dashboard.html')
    if os.path.exists(template):
        imprimir_mensagem("Gerando HTML standalone...")
        exportar_html_standalone()
    else:
        imprimir_mensagem("Template dashboard.html não encontrado, pulando HTML.")

    imprimir_mensagem("Gold geral salvo.")


if __name__ == "__main__":
    main()
