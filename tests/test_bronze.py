import re
import duckdb
import pandas as pd
import pytest
import pipeline.bronze as bronze
from pipeline.bronze import (
    formatar_periodo,
    identificar_arquivos_zip,
    exportar_parquets,
    listar_arquivos_DIRETORIO_DADOS,
    mapear_arquivos_e_periodos,
    gerar_periodos,
    gerar_url,
    identificar_periodos_faltantes,
    periodo_anterior,
    periodos_no_bronze,
)


def test_formatar_periodo():
    assert formatar_periodo("202501") == "2025-01"
    assert formatar_periodo("202312") == "2023-12"


def test_identificar_arquivos_zip_ordem_qualquer():
    arquivos = ["202501_NF.csv", "202501_Item.csv", "202501_Eventos.csv"]
    itens, eventos, nf = identificar_arquivos_zip(arquivos)
    assert "item" in itens.lower()
    assert "evento" in eventos.lower()
    assert nf == "202501_NF.csv"


def test_identificar_arquivos_zip_levanta_se_faltando_itens():
    with pytest.raises(IndexError):
        identificar_arquivos_zip(["202501_NF.csv", "202501_Eventos.csv"])


def test_mapear_arquivos_e_periodos_ignora_sem_periodo(tmp_path):
    (tmp_path / "202501_dados.zip").touch()
    (tmp_path / "readme.txt").touch()
    mapa = mapear_arquivos_e_periodos(str(tmp_path))
    assert list(mapa.keys()) == ["202501"]
    assert "readme.txt" not in mapa.values()


def test_mapear_arquivos_e_periodos_diretorio_inexistente():
    assert mapear_arquivos_e_periodos("/nao/existe") == {}


def test_gerar_periodos_intervalo_simples():
    assert gerar_periodos("202201", "202203") == ["202201", "202202", "202203"]


def test_gerar_periodos_virada_de_ano():
    assert gerar_periodos("202211", "202302") == ["202211", "202212", "202301", "202302"]


def test_gerar_periodos_mesmo_mes():
    assert gerar_periodos("202601", "202601") == ["202601"]


def test_gerar_periodos_fim_antes_inicio_retorna_vazio():
    assert gerar_periodos("202603", "202601") == []


def test_gerar_url():
    assert gerar_url("202601") == (
        "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/nfe/202601_NFe.zip"
    )


def test_periodo_anterior_formato():
    p = periodo_anterior()
    assert re.match(r'^\d{6}$', p)


def test_identificar_periodos_faltantes(tmp_path):
    (tmp_path / "202201_NFe.zip").touch()
    faltantes = identificar_periodos_faltantes(str(tmp_path), ["202201", "202202", "202203"])
    assert faltantes == ["202202", "202203"]


def test_identificar_periodos_faltantes_todos_presentes(tmp_path):
    (tmp_path / "202201_NFe.zip").touch()
    assert identificar_periodos_faltantes(str(tmp_path), ["202201"]) == []


def test_identificar_periodos_faltantes_diretorio_vazio(tmp_path):
    faltantes = identificar_periodos_faltantes(str(tmp_path), ["202201", "202202"])
    assert faltantes == ["202201", "202202"]


def test_periodos_no_bronze_sem_tabela_retorna_vazio():
    con = duckdb.connect()
    assert periodos_no_bronze(con) == set()


def test_periodos_no_bronze_retorna_periodos_existentes():
    con = duckdb.connect()
    con.execute("CREATE TABLE itens (periodo VARCHAR)")
    con.execute("INSERT INTO itens VALUES ('2022-01'), ('2022-02'), ('2022-01')")
    assert periodos_no_bronze(con) == {'2022-01', '2022-02'}


def test_listar_arquivos_retorna_somente_arquivos(tmp_path):
    (tmp_path / "a.zip").touch()
    (tmp_path / "b.csv").touch()
    (tmp_path / "subdir").mkdir()
    resultado = listar_arquivos_DIRETORIO_DADOS(str(tmp_path))
    assert set(resultado) == {"a.zip", "b.csv"}


def test_listar_arquivos_diretorio_inexistente():
    assert listar_arquivos_DIRETORIO_DADOS("/caminho/que/nao/existe") == []


def test_exportar_parquets_cria_arquivos(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DIRETORIO_EXTRACAO", str(tmp_path))
    monkeypatch.setattr(bronze, "TABELAS", ["itens"])
    con = duckdb.connect()
    con.execute("CREATE TABLE itens (periodo VARCHAR)")
    con.execute("INSERT INTO itens VALUES ('2025-01')")
    exportar_parquets(con)
    assert (tmp_path / "itens.parquet").exists()
