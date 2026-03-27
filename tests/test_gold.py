import os
import duckdb
import pandas as pd
import pytest
import pipeline.gold as gold
from pipeline.gold import carregar_ncm, criar_itens_com_ncm, exportar_parquet, exportar_html_standalone

NCM_COLS = ['Codigo', 'Descricao', 'Data_Inicio', 'Data_Fim', 'Tipo_Ato_Ini', 'Numero_Ato_Ini', 'Ano_Ato_Ini']
TEMPLATE_MINIMO = (
    "<title>__TITULO__</title>__ORGAO__ __SUBTITULO__ __BADGE1__ __BADGE2__\n"
    "<script>const P=__PARQUET_DATA_JS__;const M=__COLUNAS_MONETARIAS_JS__;"
    "const N=__COLUNAS_NUMERICAS_JS__;const T='__TABELA_PRINCIPAL__';</script>"
)


@pytest.fixture
def con(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs('extracoes/silver', exist_ok=True)
    os.makedirs('extracoes/gold', exist_ok=True)
    os.makedirs('dados', exist_ok=True)
    return duckdb.connect()


def _ncm_csv(rows):
    pd.DataFrame(
        [(c, d, '', '', '', '', '') for c, d in rows],
        columns=NCM_COLS,
    ).to_csv('dados/ncm.csv', index=False)


def _silver(ncms, valores=None):
    n = len(ncms)
    pd.DataFrame({
        'chave_de_acesso':             [f'CH{i}' for i in range(n)],
        'codigo_ncm_sh':               ncms,
        'data_emissao':                [pd.Timestamp('2024-01-01')] * n,
        'valor_total':                 valores or [100.0] * n,
        'cnpj_emitente':               [111] * n,
        'cpf_emitente':                [None] * n,
        'numero':                      list(range(n)),
        'numero_produto':              [1] * n,
        'ncm_sh_tipo_de_produto':      [None] * n,
        'descricao_do_produto_servico': [None] * n,
        'quantidade':                  [1] * n,
    }).to_parquet('extracoes/silver/itens.parquet')


# ── carregar_ncm ──────────────────────────────────────────────────────────

def test_carregar_ncm_aceita_8_digitos_e_rejeita_menores(con):
    _ncm_csv([('88', 'Cap'), ('8806', 'Pos'), ('880621', 'Sub'), ('88062100', 'Drones')])
    carregar_ncm(con)
    rows = con.execute("SELECT codigo_ncm, descricao_ncm FROM ncm").fetchall()
    assert rows == [('88062100', 'Drones')]


def test_carregar_ncm_multiplos_codigos(con):
    _ncm_csv([('88062100', 'Drones'), ('85420000', 'Chips')])
    carregar_ncm(con)
    assert con.execute("SELECT COUNT(*) FROM ncm").fetchone()[0] == 2


# ── criar_itens_com_ncm ───────────────────────────────────────────────────

def test_criar_itens_com_ncm_enriquece_descricao(con):
    _ncm_csv([('88062100', 'Drones')])
    carregar_ncm(con)
    _silver([88062100])
    criar_itens_com_ncm(con)
    assert con.execute("SELECT descricao_ncm FROM itens_ncm").fetchone()[0] == 'Drones'


def test_criar_itens_com_ncm_left_join_sem_match_retorna_null(con):
    _ncm_csv([])
    carregar_ncm(con)
    _silver([88062100])
    criar_itens_com_ncm(con)
    assert con.execute("SELECT COUNT(*) FROM itens_ncm").fetchone()[0] == 1
    assert con.execute("SELECT descricao_ncm FROM itens_ncm").fetchone()[0] is None


def test_criar_itens_com_ncm_lpad_zeros(con):
    _ncm_csv([('01012100', 'Cavalos')])
    carregar_ncm(con)
    _silver([1012100])  # armazenado como inteiro, sem o zero à esquerda
    criar_itens_com_ncm(con)
    assert con.execute("SELECT descricao_ncm FROM itens_ncm").fetchone()[0] == 'Cavalos'


# ── exportar_parquet ──────────────────────────────────────────────────────

def test_exportar_parquet_cria_arquivo(con, tmp_path, monkeypatch):
    monkeypatch.setattr(gold, 'DIRETORIO_GOLD', str(tmp_path))
    con.execute("CREATE TABLE itens_ncm (id INTEGER)")
    exportar_parquet(con)
    assert (tmp_path / 'itens_ncm.parquet').exists()


# ── exportar_html_standalone ──────────────────────────────────────────────

@pytest.fixture
def cfg_html(tmp_path, monkeypatch):
    monkeypatch.setattr(gold, '__file__', str(tmp_path / 'gold.py'))
    (tmp_path / 'template').mkdir()
    (tmp_path / 'template' / 'dashboard.html').write_text(TEMPLATE_MINIMO, encoding='utf-8')
    parquet = tmp_path / 'itens_ncm.parquet'
    pd.DataFrame({'id': [1]}).to_parquet(parquet)
    return {
        'titulo': 'Título Teste', 'subtitulo': 'Sub', 'orgao': 'Org',
        'badge1': 'B1', 'badge2': 'B2',
        'parquets': {'itens_ncm.parquet': str(parquet)},
        'tabela_principal': 'itens_ncm.parquet',
        'colunas_monetarias': ['valor_total'], 'colunas_numericas': ['quantidade'],
        'diretorio_entrega': str(tmp_path / 'entrega'),
    }


def test_exportar_html_standalone(cfg_html):
    exportar_html_standalone(cfg_html)
    html = open(cfg_html['diretorio_entrega'] + '/preview.html', encoding='utf-8').read()
    assert 'Título Teste' in html
    for p in ('__TITULO__', '__SUBTITULO__', '__ORGAO__', '__BADGE1__', '__BADGE2__',
              '__TABELA_PRINCIPAL__', '__PARQUET_DATA_JS__',
              '__COLUNAS_MONETARIAS_JS__', '__COLUNAS_NUMERICAS_JS__'):
        assert p not in html
