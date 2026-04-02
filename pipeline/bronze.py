import os
import re
import requests
import tempfile
import zipfile
from datetime import datetime
import duckdb
from tqdm import tqdm

DIRETORIO_DADOS = 'dados/nfe'
DIRETORIO_EXTRACAO = 'extracoes/bronze'
URL_BASE = 'https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/nfe'
PERIODO_INICIO = '202201'
PERIODO_FIM = '202512'
CAMINHO_DUCKDB = os.path.join(DIRETORIO_EXTRACAO, 'bronze.duckdb')

TABELAS = ['itens', 'eventos', 'nf']


def periodo_anterior() -> str:
    """Retorna 'YYYYMM' do mês anterior ao atual"""
    hoje = datetime.today()
    if hoje.month == 1:
        return f"{hoje.year - 1}12"
    return f"{hoje.year}{hoje.month - 1:02d}"


def gerar_periodos(inicio: str, fim: str) -> list[str]:
    """Gera lista de períodos 'YYYYMM' de inicio até fim"""
    atual = datetime.strptime(inicio, '%Y%m')
    fim_dt = datetime.strptime(fim, '%Y%m')
    periodos = []
    while atual <= fim_dt:
        periodos.append(atual.strftime('%Y%m'))
        if atual.month == 12:
            atual = atual.replace(year=atual.year + 1, month=1)
        else:
            atual = atual.replace(month=atual.month + 1)
    return periodos


def gerar_url(periodo: str) -> str:
    """Retorna a URL de download do zip para um período 'YYYYMM'"""
    return f"{URL_BASE}/{periodo}_NFe.zip"


def identificar_periodos_faltantes(diretorio: str, periodos: list[str]) -> list[str]:
    """Retorna os períodos da lista que ainda não existem como arquivo em diretorio"""
    existentes = {
        re.match(r'^\d{6}', a).group(0)
        for a in listar_arquivos_DIRETORIO_DADOS(diretorio)
        if re.match(r'^\d{6}', a)
    }
    return [p for p in periodos if p not in existentes]


def baixar_zip(url: str, destino: str) -> None:
    """Baixa um único arquivo zip de url e salva em destino"""
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(destino, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def baixar_zips_faltantes(diretorio: str = DIRETORIO_DADOS) -> None:
    """Baixa todos os zips faltantes de PERIODO_INICIO até o mês anterior ao atual"""
    periodos = gerar_periodos(PERIODO_INICIO, PERIODO_FIM)
    faltantes = identificar_periodos_faltantes(diretorio, periodos)
    imprimir_mensagem(f"{len(faltantes)} períodos para baixar.")
    for periodo in (pbar := tqdm(faltantes)):
        url = gerar_url(periodo)
        destino = os.path.join(diretorio, f"{periodo}_NFe.zip")
        pbar.set_description(f"[{formatar_hora_atual()}] {periodo}")
        try:
            baixar_zip(url, destino)
        except requests.HTTPError as e:
            tqdm.write(f"Erro ao baixar {url}: {e}")


def formatar_hora_atual() -> str:
    """Retorna a hora atual formatada como 'YYYY-MM-DD HH:MM:SS'"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def imprimir_mensagem(mensagem: str) -> None:
    """Imprime mensagem com timestamp no formato '[YYYY-MM-DD HH:MM:SS] mensagem'"""
    print(f"[{formatar_hora_atual()}] {mensagem}")


def listar_arquivos_DIRETORIO_DADOS(diretorio: str) -> list[str]:
    """Retorna lista de arquivos em diretorio; retorna [] se não encontrado"""
    try:
        arquivos = os.listdir(diretorio)
        return [arquivo for arquivo in arquivos if os.path.isfile(os.path.join(diretorio, arquivo))]
    except FileNotFoundError:
        print(f"Diretório {diretorio} não encontrado.")
        return []


def mapear_arquivos_e_periodos(diretorio: str) -> dict[str, str]:
    """Retorna dict {periodo: nome_arquivo} para arquivos cujo nome começa com 6 dígitos"""
    arquivos = listar_arquivos_DIRETORIO_DADOS(diretorio)
    mapa = {}
    for nome_do_arquivo in arquivos:
        periodo_regex = re.match(r'^\d{6}', nome_do_arquivo)
        if periodo_regex is not None:
            mapa[periodo_regex.group(0)] = nome_do_arquivo
    return mapa


def formatar_periodo(periodo_raw: str) -> str:
    """Converte 'YYYYMM' para 'YYYY-MM'"""
    return periodo_raw[:4] + '-' + periodo_raw[4:6]


def identificar_arquivos_zip(arquivos: list[str]) -> tuple[str, str, str]:
    """Identifica itens, eventos e nf numa lista de 3 arquivos extraídos do zip"""
    itens   = [a for a in arquivos if 'item' in a.lower()][0]
    eventos = [a for a in arquivos if 'evento' in a.lower()][0]
    nf      = [a for a in arquivos if a not in [itens, eventos]][0]
    return itens, eventos, nf


def periodos_no_bronze(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Retorna o conjunto de períodos 'YYYY-MM' já presentes no bronze via DuckDB"""
    try:
        return {r[0] for r in con.execute("SELECT DISTINCT periodo FROM itens").fetchall()}
    except duckdb.CatalogException:
        return set()


def inserir_csv(con: duckdb.DuckDBPyConnection, nome: str, caminho: str, periodo: str) -> None:
    """Insere CSV diretamente no DuckDB sem passar por pandas. Todas as colunas como VARCHAR."""
    read = f"SELECT *, '{periodo}' AS periodo FROM read_csv('{caminho}', sep=';', encoding='latin-1', all_varchar=true)"
    try:
        con.execute(f"INSERT INTO {nome} {read}")
    except duckdb.CatalogException:
        con.execute(f"CREATE TABLE {nome} AS {read}")


def exportar_parquets(con: duckdb.DuckDBPyConnection, memoria: str = '6GB') -> None:
    """Exporta todas as tabelas do DuckDB para parquets individuais"""
    con.execute(f"SET memory_limit='{memoria}'")
    for nome in TABELAS:
        caminho = os.path.join(DIRETORIO_EXTRACAO, f'{nome}.parquet')
        con.execute(f"COPY {nome} TO '{caminho}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    con.execute("RESET memory_limit")


def main():
    """Baixa zips faltantes, extrai CSVs dos períodos novos e salva no bronze"""
    imprimir_mensagem("Iniciando extração bronze...")
    baixar_zips_faltantes()
    mapa = mapear_arquivos_e_periodos(DIRETORIO_DADOS)
    imprimir_mensagem(f"Arquivos encontrados: {len(mapa)}")

    os.makedirs(DIRETORIO_EXTRACAO, exist_ok=True)
    con = duckdb.connect(CAMINHO_DUCKDB)

    ja_no_bronze = periodos_no_bronze(con)
    mapa_novo = {p: a for p, a in mapa.items() if formatar_periodo(p) not in ja_no_bronze}

    if not mapa_novo:
        imprimir_mensagem("Bronze já está atualizado.")
        con.close()
        return

    imprimir_mensagem(f"{len(mapa_novo)} períodos novos para processar.")

    for periodo_raw, arquivo in (pbar := tqdm(mapa_novo.items())):
        pbar.set_description(f"Extraindo {periodo_raw}")
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(os.path.join(DIRETORIO_DADOS, arquivo), 'r') as z:
                z.extractall(tmp)

            arquivos = os.listdir(tmp)
            if len(arquivos) != 3:
                print(f"Erro: esperado 3 arquivos, encontrado {len(arquivos)} em {periodo_raw}.")
                continue

            arq_itens, arq_eventos, arq_nf = identificar_arquivos_zip(arquivos)
            periodo = formatar_periodo(periodo_raw)
            for nome, arq in zip(TABELAS, [arq_itens, arq_eventos, arq_nf]):
                inserir_csv(con, nome, os.path.join(tmp, arq), periodo)

    imprimir_mensagem("Exportando parquets...")
    exportar_parquets(con)
    con.close()

    imprimir_mensagem("Bronze salvo.")


if __name__ == "__main__":
    main()