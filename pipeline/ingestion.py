import os
import re
import requests
import tempfile
import zipfile
from datetime import datetime
import pandas as pd
from tqdm import tqdm

DIRETORIO_DADOS = 'dados/nfe'
DIRETORIO_EXTRACAO = 'extracoes/bronze'
URL_BASE = 'https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/nfe'
PERIODO_INICIO = '202601' # 01/2022


# Brief: retorna 'YYYYMM' do mês anterior ao atual
def periodo_anterior() -> str:
    hoje = datetime.today()
    if hoje.month == 1:
        return f"{hoje.year - 1}12"
    return f"{hoje.year}{hoje.month - 1:02d}"


# Brief: gera lista de períodos 'YYYYMM' de inicio até fim
def gerar_periodos(inicio: str, fim: str) -> list[str]:
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


# Brief: retorna a URL de download do zip para um período 'YYYYMM'
def gerar_url(periodo: str) -> str:
    return f"{URL_BASE}/{periodo}_NFe.zip"


# Brief: baixa um único arquivo zip de url e salva em destino
def baixar_zip(url: str, destino: str) -> None:
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(destino, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


# Brief: baixa todos os zips de PERIODO_INICIO até o mês anterior ao atual
def baixar_zips(diretorio: str = DIRETORIO_DADOS) -> None:
    periodos = gerar_periodos(PERIODO_INICIO, periodo_anterior())
    imprimir_mensagem(f"{len(periodos)} períodos para baixar.")
    for periodo in (pbar := tqdm(periodos)):
        url = gerar_url(periodo)
        destino = os.path.join(diretorio, f"{periodo}_NFe.zip")
        pbar.set_description(f"[{formatar_hora_atual()}] {periodo}")
        try:
            baixar_zip(url, destino)
        except requests.HTTPError as e:
            tqdm.write(f"Erro ao baixar {url}: {e}")


# Brief: retorna a hora atual formatada como 'YYYY-MM-DD HH:MM:SS'
def formatar_hora_atual() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# Brief: imprime mensagem com timestamp no formato '[YYYY-MM-DD HH:MM:SS] mensagem'
def imprimir_mensagem(mensagem: str) -> None:
    print(f"[{formatar_hora_atual()}] {mensagem}")


# Brief: retorna lista de arquivos em diretorio; retorna [] se não encontrado
def listar_arquivos_DIRETORIO_DADOS(diretorio: str) -> list[str]:
    try:
        arquivos = os.listdir(diretorio)
        return [arquivo for arquivo in arquivos if os.path.isfile(os.path.join(diretorio, arquivo))]
    except FileNotFoundError:
        print(f"Diretório {diretorio} não encontrado.")
        return []


# Brief: retorna dict {periodo: nome_arquivo} para arquivos cujo nome começa com 6 dígitos
def mapear_arquivos_e_periodos(diretorio: str) -> dict[str, str]:
    arquivos = listar_arquivos_DIRETORIO_DADOS(diretorio)
    mapa = {}
    for nome_do_arquivo in arquivos:
        periodo_regex = re.match(r'^\d{6}', nome_do_arquivo)
        if periodo_regex is not None:
            mapa[periodo_regex.group(0)] = nome_do_arquivo
    return mapa


# Brief: identifica itens, eventos e nf numa lista de 3 arquivos extraídos do zip
def identificar_arquivos_zip(arquivos: list[str]) -> tuple[str, str, str]:
    itens   = [a for a in arquivos if 'item' in a.lower()][0]
    eventos = [a for a in arquivos if 'evento' in a.lower()][0]
    nf      = [a for a in arquivos if a not in [itens, eventos]][0]
    return itens, eventos, nf


# Brief: lê os 3 CSVs de um período extraído e retorna os DataFrames (itens, eventos, nf) com todas as colunas como string
def ler_csvs(diretorio: str, arq_itens: str, arq_eventos: str, arq_nf: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    kwargs = dict(sep=';', encoding='windows-1252', dtype=str)
    return (
        pd.read_csv(os.path.join(diretorio, arq_itens),   **kwargs),
        pd.read_csv(os.path.join(diretorio, arq_eventos), **kwargs),
        pd.read_csv(os.path.join(diretorio, arq_nf),      **kwargs),
    )


# Brief: baixa todos os zips, extrai CSVs e salva o bronze
def main():
    imprimir_mensagem("Iniciando extração bronze...")
    baixar_zips()
    mapa = mapear_arquivos_e_periodos(DIRETORIO_DADOS)
    imprimir_mensagem(f"Arquivos encontrados: {len(mapa)}")

    dfs_itens, dfs_eventos, dfs_nf = [], [], []

    for periodo, arquivo in (pbar := tqdm(mapa.items())):
        pbar.set_description(f"Extraindo {periodo}")
        with tempfile.TemporaryDirectory() as tmp:
            with zipfile.ZipFile(os.path.join(DIRETORIO_DADOS, arquivo), 'r') as z:
                z.extractall(tmp)

            arquivos = os.listdir(tmp)
            if len(arquivos) != 3:
                print(f"Erro: esperado 3 arquivos, encontrado {len(arquivos)} em {periodo}.")
                continue

            arq_itens, arq_eventos, arq_nf = identificar_arquivos_zip(arquivos)
            df_itens, df_eventos, df_nf = ler_csvs(tmp, arq_itens, arq_eventos, arq_nf)

        for df in (df_itens, df_eventos, df_nf):
            df['periodo'] = periodo

        dfs_itens.append(df_itens)
        dfs_eventos.append(df_eventos)
        dfs_nf.append(df_nf)

    imprimir_mensagem("Concatenando e salvando bronze...")
    os.makedirs(DIRETORIO_EXTRACAO, exist_ok=True)

    for nome, dfs in [('itens', dfs_itens), ('eventos', dfs_eventos), ('nf', dfs_nf)]:
        caminho = os.path.join(DIRETORIO_EXTRACAO, f'{nome}.parquet')
        pd.concat(dfs, ignore_index=True).to_parquet(caminho, compression='snappy')

    imprimir_mensagem("Bronze salvo.")


if __name__ == "__main__":
    main()
