import os
import duckdb
import pandas as pd
import pytest
import pipeline.gold as gold
from pipeline.gold import (
    criar_resumo_ecoadvance,
    criar_resumo_por_pespp,
    criar_totais_globais_ecoadvance,
    exportar_csvs_entrega,
    exportar_parquets,
)


@pytest.fixture
def con(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs('extracoes/silver', exist_ok=True)
    os.makedirs('dados', exist_ok=True)
    return duckdb.connect()


def _setup(itens_rows, mapa_rows):
    """Salva itens.parquet e mapeamento_EcoAdvance.csv no diretório corrente."""
    pd.DataFrame(itens_rows).to_parquet('extracoes/silver/itens.parquet')
    pd.DataFrame(mapa_rows, columns=['CODIGO', 'PRODUTO PESPP', 'PE', 'DESCRIÇÃO']).to_csv(
        'dados/mapeamento_EcoAdvance.csv', index=False
    )


def _item(**kwargs):
    base = dict(
        chave_de_acesso='CHAVE001',
        codigo_ncm_sh='88062100',
        data_emissao=pd.Timestamp('2024-06-15'),
        valor_total=1000,
        cnpj_emitente=11111111000100,
        cpf_emitente=None,
        numero=1,
        numero_produto=1,
    )
    base.update(kwargs)
    return base


def _mapa(codigo, produto, pe='PE1', descricao='Desc'):
    return [codigo, produto, pe, descricao]


# ─── criar_resumo_ecoadvance ──────────────────────────────────────────────────

def test_resumo_ecoadvance_match_por_prefixo4(con):
    _setup([_item(codigo_ncm_sh='88062100')], [_mapa('8806', 'P1')])
    criar_resumo_ecoadvance(con)
    assert con.execute("SELECT COUNT(*) FROM resumo_ecoadvance").fetchone()[0] == 1


def test_resumo_ecoadvance_sem_match_nao_aparece(con):
    _setup([_item(codigo_ncm_sh='99999999')], [_mapa('8806', 'P1')])
    criar_resumo_ecoadvance(con)
    assert con.execute("SELECT COUNT(*) FROM resumo_ecoadvance").fetchone()[0] == 0


def test_resumo_ecoadvance_agrega_valor_total(con):
    _setup(
        [_item(chave_de_acesso='A', valor_total=100), _item(chave_de_acesso='B', valor_total=200)],
        [_mapa('8806', 'P1')],
    )
    criar_resumo_ecoadvance(con)
    assert con.execute('SELECT "VALOR TOTAL" FROM resumo_ecoadvance').fetchone()[0] == 300


def test_resumo_ecoadvance_conta_fornecedores_distintos(con):
    _setup(
        [
            _item(chave_de_acesso='A', cnpj_emitente=111),
            _item(chave_de_acesso='B', cnpj_emitente=111),
            _item(chave_de_acesso='C', cnpj_emitente=222),
        ],
        [_mapa('8806', 'P1')],
    )
    criar_resumo_ecoadvance(con)
    assert con.execute('SELECT "FORNECEDORES DISTINTOS" FROM resumo_ecoadvance').fetchone()[0] == 2


def test_resumo_ecoadvance_valor_por_ano(con):
    _setup(
        [
            _item(chave_de_acesso='A', data_emissao=pd.Timestamp('2022-01-01'), valor_total=100),
            _item(chave_de_acesso='B', data_emissao=pd.Timestamp('2023-06-01'), valor_total=200),
            _item(chave_de_acesso='C', data_emissao=pd.Timestamp('2024-09-01'), valor_total=400),
        ],
        [_mapa('8806', 'P1')],
    )
    criar_resumo_ecoadvance(con)
    row = con.execute("SELECT valor_2022, valor_2023, valor_2024, valor_2025 FROM resumo_ecoadvance").fetchone()
    assert row == (100, 200, 400, 0)


def test_resumo_ecoadvance_dois_produtos_distintos(con):
    _setup(
        [
            _item(chave_de_acesso='A', codigo_ncm_sh='88062100'),
            _item(chave_de_acesso='B', codigo_ncm_sh='85420000'),
        ],
        [_mapa('8806', 'P1'), _mapa('8542', 'P2')],
    )
    criar_resumo_ecoadvance(con)
    assert con.execute("SELECT COUNT(*) FROM resumo_ecoadvance").fetchone()[0] == 2


# ─── criar_resumo_por_pespp ───────────────────────────────────────────────────

def test_resumo_por_pespp_agrupa_por_produto(con):
    """NCMs distintos do mesmo PRODUTO PESPP somam numa só linha."""
    _setup(
        [
            _item(chave_de_acesso='A', codigo_ncm_sh='88062100', valor_total=100),
            _item(chave_de_acesso='B', codigo_ncm_sh='88061100', valor_total=200),
        ],
        [_mapa('8806', 'P1')],
    )
    criar_resumo_por_pespp(con)
    assert con.execute("SELECT COUNT(*) FROM resumo_por_pespp").fetchone()[0] == 1
    assert con.execute('SELECT "VALOR TOTAL" FROM resumo_por_pespp').fetchone()[0] == 300


def test_resumo_por_pespp_fornecedores_distintos_nao_somados(con):
    """Mesmo fornecedor em dois NCMs do mesmo produto → conta 1, não 2."""
    _setup(
        [
            _item(chave_de_acesso='A', codigo_ncm_sh='88062100', cnpj_emitente=111),
            _item(chave_de_acesso='B', codigo_ncm_sh='88061100', cnpj_emitente=111),
        ],
        [_mapa('8806', 'P1')],
    )
    criar_resumo_por_pespp(con)
    assert con.execute('SELECT "FORNECEDORES DISTINTOS" FROM resumo_por_pespp').fetchone()[0] == 1


def test_resumo_por_pespp_dois_produtos(con):
    _setup(
        [
            _item(chave_de_acesso='A', codigo_ncm_sh='88062100', valor_total=500),
            _item(chave_de_acesso='B', codigo_ncm_sh='85420000', valor_total=300),
        ],
        [_mapa('8806', 'P1'), _mapa('8542', 'P2')],
    )
    criar_resumo_por_pespp(con)
    assert con.execute("SELECT COUNT(*) FROM resumo_por_pespp").fetchone()[0] == 2


# ─── criar_totais_globais_ecoadvance ─────────────────────────────────────────

def test_totais_globais_deduplica_item_que_casa_com_dois_produtos(con):
    """Mesmo item (chave+numero+numero_produto) casando com dois PRODUTO PESPP → conta uma vez."""
    _setup(
        [_item(chave_de_acesso='CH1', valor_total=500)],
        [_mapa('8806', 'P1'), _mapa('8806', 'P2')],
    )
    criar_totais_globais_ecoadvance(con)
    valor, fornecedores, itens = con.execute(
        'SELECT "VALOR TOTAL", "FORNECEDORES DISTINTOS", "ITENS" FROM totais_globais_ecoadvance'
    ).fetchone()
    assert itens == 1
    assert valor == 500


def test_totais_globais_dois_itens_distintos(con):
    _setup(
        [
            _item(chave_de_acesso='CH1', cnpj_emitente=111, valor_total=100),
            _item(chave_de_acesso='CH2', numero_produto=2, cnpj_emitente=222, valor_total=200),
        ],
        [_mapa('8806', 'P1')],
    )
    criar_totais_globais_ecoadvance(con)
    valor, fornecedores, itens = con.execute(
        'SELECT "VALOR TOTAL", "FORNECEDORES DISTINTOS", "ITENS" FROM totais_globais_ecoadvance'
    ).fetchone()
    assert valor == 300
    assert itens == 2
    assert fornecedores == 2


def test_totais_globais_sem_match_retorna_zero(con):
    _setup([_item(codigo_ncm_sh='99999999')], [_mapa('8806', 'P1')])
    criar_totais_globais_ecoadvance(con)
    itens = con.execute('SELECT "ITENS" FROM totais_globais_ecoadvance').fetchone()[0]
    assert itens == 0


# ─── exportar_parquets / exportar_csvs_entrega ───────────────────────────────

def test_exportar_parquets_gold_cria_arquivos(con, tmp_path, monkeypatch):
    monkeypatch.setattr(gold, 'DIRETORIO_GOLD', str(tmp_path))
    for tabela in ('resumo_ecoadvance', 'resumo_por_pespp', 'totais_globais_ecoadvance'):
        con.execute(f"CREATE TABLE {tabela} (id INTEGER)")
    exportar_parquets(con)
    for tabela in ('resumo_ecoadvance', 'resumo_por_pespp', 'totais_globais_ecoadvance'):
        assert (tmp_path / f'{tabela}.parquet').exists()


def test_exportar_csvs_entrega_cria_csvs(con, tmp_path, monkeypatch):
    monkeypatch.setattr(gold, 'DIRETORIO_ENTREGA', str(tmp_path / 'entrega'))
    for tabela in ('resumo_ecoadvance', 'resumo_por_pespp', 'totais_globais_ecoadvance'):
        con.execute(f"CREATE TABLE {tabela} (id INTEGER)")
    exportar_csvs_entrega(con)
    for tabela in ('resumo_ecoadvance', 'resumo_por_pespp', 'totais_globais_ecoadvance'):
        assert (tmp_path / 'entrega' / f'{tabela}.csv').exists()
