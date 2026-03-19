import os
import duckdb
import pandas as pd
import pytest
import pipeline.gold as gold
from pipeline.gold import (
    criar_detalhado_por_cadeia,
    criar_itens_nib,
    criar_resumo_por_cadeia,
    criar_totais_globais,
    exportar_csvs_entrega,
    exportar_parquets,
)


@pytest.fixture
def con(tmp_path, monkeypatch):
    """Cria conexão DuckDB em diretório temporário."""
    monkeypatch.chdir(tmp_path)
    os.makedirs('extracoes/silver', exist_ok=True)
    return duckdb.connect()


def _setup(con, itens_df, mapeamento_rows):
    """Helper: salva o parquet silver e cria a tabela mapeamento_ncm."""
    itens_df.to_parquet('extracoes/silver/itens.parquet')
    mapa_df = pd.DataFrame(mapeamento_rows, columns=[
        'codigo_cadeia', 'cadeia', 'missao', 'nome_missao',
        'prefixo_sh', 'tipo_match', 'tamanho_prefixo',
        'cnae', 'desc_cnae', 'criterio', 'icp', 'missoes', 'secao_cnae', 'sh_correspondente',
    ])
    con.execute("CREATE TABLE mapeamento_ncm AS SELECT * FROM mapa_df")


def _itens(**kwargs):
    """Helper: gera DataFrame com colunas padrão de itens silver."""
    base = dict(
        chave_de_acesso=['CHAVE001'],
        codigo_ncm_sh=[88062100],
        data_emissao=[pd.Timestamp('2024-06-15')],
        valor_total=[1000],
        cnpj_emitente=[11111111000100],
        cpf_emitente=[None],
        numero=[1],
        numero_produto=[1],
        ncm_sh_tipo_de_produto=[None],
        descricao_do_produto_servico=[None],
        quantidade=[1],
    )
    base.update(kwargs)
    return pd.DataFrame(base)


def _mapa(codigo_cadeia, prefixo_sh, tipo_match):
    """Helper: gera uma linha de mapeamento com todas as colunas esperadas."""
    tam = {'ncm8': 8, 'prefixo6': 6, 'prefixo5': 5, 'prefixo4': 4}
    return [codigo_cadeia, f'Cadeia {codigo_cadeia}', 'M1', 'Missao 1',
            str(prefixo_sh), tipo_match, tam[tipo_match],
            None, None, None, None, None, None, None]


def _criar_itens_nib(con, df: pd.DataFrame):
    """Cria diretamente a tabela itens_nib para testes de funções downstream.
    Converte colunas object para StringDtype para que DuckDB as leia como VARCHAR."""
    df = df.copy()
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(pd.StringDtype())
    con.execute("CREATE OR REPLACE TABLE itens_nib AS SELECT * FROM df")


def test_match_ncm8_exato(con):
    _setup(con,
        _itens(codigo_ncm_sh=[88062100]),
        [_mapa('C1', '88062100', 'ncm8')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 1


def test_match_prefixo4(con):
    _setup(con,
        _itens(codigo_ncm_sh=[85010000]),
        [_mapa('C1', '8501', 'prefixo4')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 1


def test_match_prefixo5(con):
    _setup(con,
        _itens(codigo_ncm_sh=[84244100]),
        [_mapa('C1', '84244', 'prefixo5')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 1


def test_match_prefixo6(con):
    _setup(con,
        _itens(codigo_ncm_sh=[85176100]),
        [_mapa('C1', '851761', 'prefixo6')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 1


def test_sem_match_nao_aparece(con):
    _setup(con,
        _itens(codigo_ncm_sh=[99999999]),
        [_mapa('C1', '88062100', 'ncm8')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 0


def test_exclui_anos_fora_do_intervalo(con):
    """Itens de 2021 são excluídos; itens de 2022 e 2025 são incluídos."""
    _setup(con,
        _itens(
            chave_de_acesso=['A', 'B', 'C'],
            codigo_ncm_sh=[88062100, 88062100, 88062100],
            data_emissao=[
                pd.Timestamp('2021-12-31'),
                pd.Timestamp('2022-01-01'),
                pd.Timestamp('2025-06-15'),
            ],
            valor_total=[100, 200, 300],
            cnpj_emitente=[111, 222, 333],
            cpf_emitente=[None, None, None],
            numero=[1, 1, 1],
            numero_produto=[1, 1, 1],
            ncm_sh_tipo_de_produto=[None, None, None],
            descricao_do_produto_servico=[None, None, None],
            quantidade=[1, 1, 1],
        ),
        [_mapa('C1', '88062100', 'ncm8')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 2
    assert con.execute("SELECT SUM(valor_total) FROM itens_nib").fetchone()[0] == 500


def test_exclui_2026(con):
    _setup(con,
        _itens(data_emissao=[pd.Timestamp('2026-03-01')]),
        [_mapa('C1', '88062100', 'ncm8')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 0


def test_mesmo_item_mesma_cadeia_dois_prefixos_aparece_uma_vez(con):
    """NCM 84244100 casa com prefixo5 '84244' e ncm8 '84244100' na mesma cadeia."""
    _setup(con,
        _itens(codigo_ncm_sh=[84244100]),
        [
            _mapa('C1', '84244100', 'ncm8'),
            _mapa('C1', '84244', 'prefixo5'),
        ],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 1


def test_mesmo_item_mesma_cadeia_tres_prefixos_aparece_uma_vez(con):
    """NCM 84244100 casa com prefixo4, prefixo5 e ncm8 na mesma cadeia."""
    _setup(con,
        _itens(codigo_ncm_sh=[84244100]),
        [
            _mapa('C1', '84244100', 'ncm8'),
            _mapa('C1', '84244', 'prefixo5'),
            _mapa('C1', '8424', 'prefixo4'),
        ],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 1


def test_mesmo_item_cadeias_diferentes_aparece_em_cada_cadeia(con):
    """NCM 85420000 pertence a Semicondutores e Robôs → 2 linhas."""
    _setup(con,
        _itens(codigo_ncm_sh=[85420000]),
        [
            _mapa('C11', '8542', 'prefixo4'),
            _mapa('C12', '8542', 'prefixo4'),
        ],
    )
    criar_itens_nib(con)
    result = con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0]
    assert result == 2
    cadeias = con.execute("SELECT DISTINCT codigo_cadeia FROM itens_nib ORDER BY codigo_cadeia").fetchall()
    assert [r[0] for r in cadeias] == ['C11', 'C12']


def test_mesmo_item_mesma_cadeia_e_outra_cadeia_deduplicado_corretamente(con):
    """NCM 84244100 casa com C1 por 3 prefixos e com C2 por 1 → deve dar 2 linhas (uma por cadeia)."""
    _setup(con,
        _itens(codigo_ncm_sh=[84244100]),
        [
            _mapa('C1', '84244100', 'ncm8'),
            _mapa('C1', '84244', 'prefixo5'),
            _mapa('C1', '8424', 'prefixo4'),
            _mapa('C2', '84244100', 'ncm8'),
        ],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 2


def test_valor_total_preservado_apos_join(con):
    _setup(con,
        _itens(valor_total=[42000]),
        [_mapa('C1', '88062100', 'ncm8')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT valor_total FROM itens_nib").fetchone()[0] == 42000


def test_codigo_cadeia_correto_apos_join(con):
    _setup(con,
        _itens(codigo_ncm_sh=[85010000]),
        [_mapa('M3-C10', '8501', 'prefixo4')],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT codigo_cadeia FROM itens_nib").fetchone()[0] == 'M3-C10'


def test_multiplos_itens_diferentes_ncms(con):
    _setup(con,
        _itens(
            chave_de_acesso=['A', 'B'],
            codigo_ncm_sh=[88062100, 85420000],
            data_emissao=[pd.Timestamp('2024-01-01'), pd.Timestamp('2024-06-01')],
            valor_total=[100, 200],
            cnpj_emitente=[111, 222],
            cpf_emitente=[None, None],
            numero=[1, 1],
            numero_produto=[1, 1],
            ncm_sh_tipo_de_produto=[None, None],
            descricao_do_produto_servico=[None, None],
            quantidade=[1, 1],
        ),
        [
            _mapa('C1', '88062100', 'ncm8'),
            _mapa('C2', '8542', 'prefixo4'),
        ],
    )
    criar_itens_nib(con)
    assert con.execute("SELECT COUNT(*) FROM itens_nib").fetchone()[0] == 2
    assert con.execute("SELECT SUM(valor_total) FROM itens_nib").fetchone()[0] == 300


def test_resumo_cadeia_agrega_por_cadeia(con):
    _criar_itens_nib(con, pd.DataFrame({
        'codigo_cadeia': ['C1', 'C1', 'C2'],
        'cadeia':        ['X',  'X',  'Y'],
        'missao':        ['M1', 'M1', 'M1'],
        'nome_missao':   ['N',  'N',  'N'],
        'valor_total':   [100,  200,  500],
        'cnpj_emitente': [111,  222,  111],
        'cpf_emitente':  [None, None, None],
    }))
    criar_resumo_por_cadeia(con)
    df = con.execute("SELECT * FROM resumo_cadeia ORDER BY codigo_cadeia").fetchdf()
    assert len(df) == 2
    assert df.loc[df['codigo_cadeia'] == 'C1', 'valor_total_adquirido'].iloc[0] == 300
    assert df.loc[df['codigo_cadeia'] == 'C1', 'fornecedores_distintos'].iloc[0] == 2


def test_resumo_cadeia_mesmo_fornecedor_conta_uma_vez(con):
    _criar_itens_nib(con, pd.DataFrame({
        'codigo_cadeia': ['C1', 'C1'],
        'cadeia':        ['X',  'X'],
        'missao':        ['M',  'M'],
        'nome_missao':   ['N',  'N'],
        'valor_total':   [100,  200],
        'cnpj_emitente': [999,  999],
        'cpf_emitente':  [None, None],
    }))
    criar_resumo_por_cadeia(con)
    assert con.execute("SELECT fornecedores_distintos FROM resumo_cadeia").fetchone()[0] == 1


def test_resumo_cadeia_ordenado_por_valor_desc(con):
    _criar_itens_nib(con, pd.DataFrame({
        'codigo_cadeia': ['C1', 'C2'],
        'cadeia':        ['X',  'Y'],
        'missao':        ['M',  'M'],
        'nome_missao':   ['N',  'N'],
        'valor_total':   [100, 9000],
        'cnpj_emitente': [111,  222],
        'cpf_emitente':  [None, None],
    }))
    criar_resumo_por_cadeia(con)
    primeira = con.execute("SELECT codigo_cadeia FROM resumo_cadeia LIMIT 1").fetchone()[0]
    assert primeira == 'C2'


def test_totais_globais_deduplica_mesmo_item_em_duas_cadeias(con):
    """Mesmo item (chave+numero+numero_produto) duplicado entre cadeias → conta uma vez."""
    _criar_itens_nib(con, pd.DataFrame({
        'chave_de_acesso': ['CH1', 'CH1'],
        'numero':          [1,     1],
        'numero_produto':  [1,     1],
        'valor_total':     [500,   500],
        'cnpj_emitente':   [111,   111],
        'cpf_emitente':    [None,  None],
        'codigo_cadeia':   ['C1',  'C2'],
        'cadeia':          ['X',   'Y'],
        'missao':          ['M',   'M'],
        'nome_missao':     ['N',   'N'],
    }))
    criar_totais_globais(con)
    valor, fornecedores, itens = con.execute("SELECT valor_total_global, fornecedores_global, itens_global FROM totais_globais").fetchone()
    assert valor == 500
    assert itens == 1
    assert fornecedores == 1


def test_totais_globais_dois_itens_distintos(con):
    _criar_itens_nib(con, pd.DataFrame({
        'chave_de_acesso': ['CH1', 'CH2'],
        'numero':          [1,     1],
        'numero_produto':  [1,     1],
        'valor_total':     [100,   200],
        'cnpj_emitente':   [111,   222],
        'cpf_emitente':    [None,  None],
        'codigo_cadeia':   ['C1',  'C1'],
        'cadeia':          ['X',   'X'],
        'missao':          ['M',   'M'],
        'nome_missao':     ['N',   'N'],
    }))
    criar_totais_globais(con)
    valor, fornecedores, itens = con.execute("SELECT valor_total_global, fornecedores_global, itens_global FROM totais_globais").fetchone()
    assert valor == 300
    assert itens == 2
    assert fornecedores == 2


def test_detalhado_cadeia_agrega_por_ncm_e_descricao(con):
    _criar_itens_nib(con, pd.DataFrame({
        'codigo_cadeia':              ['C1',        'C1'],
        'cadeia':                     ['X',         'X'],
        'missao':                     ['M',         'M'],
        'nome_missao':                ['N',         'N'],
        'cnae':                       [None,        None],
        'desc_cnae':                  [None,        None],
        'criterio':                   [None,        None],
        'icp':                        [None,        None],
        'codigo_ncm_sh':              [88062100,    88062100],
        'ncm_sh_tipo_de_produto':     ['Drones',    'Drones'],
        'descricao_do_produto_servico': ['Drone',   'Drone'],
        'valor_total':                [1000,        500],
        'cnpj_emitente':              [111,         222],
        'cpf_emitente':               [None,        None],
        'quantidade':                 [2,           1],
        'chave_de_acesso':            ['CH1',       'CH2'],
    }))
    criar_detalhado_por_cadeia(con)
    row = con.execute("SELECT valor_total, quantidade_total, registros, fornecedores_distintos FROM detalhado_cadeia").fetchone()
    assert row == (1500, 3, 2, 2)


def test_detalhado_cadeia_separa_ncms_distintos(con):
    _criar_itens_nib(con, pd.DataFrame({
        'codigo_cadeia':              ['C1',     'C1'],
        'cadeia':                     ['X',      'X'],
        'missao':                     ['M',      'M'],
        'nome_missao':                ['N',      'N'],
        'cnae':                       [None,     None],
        'desc_cnae':                  [None,     None],
        'criterio':                   [None,     None],
        'icp':                        [None,     None],
        'codigo_ncm_sh':              [88062100, 85420000],
        'ncm_sh_tipo_de_produto':     ['A',      'B'],
        'descricao_do_produto_servico': ['P1',   'P2'],
        'valor_total':                [100,      200],
        'cnpj_emitente':              [111,      111],
        'cpf_emitente':               [None,     None],
        'quantidade':                 [1,        1],
        'chave_de_acesso':            ['CH1',    'CH2'],
    }))
    criar_detalhado_por_cadeia(con)
    assert con.execute("SELECT COUNT(*) FROM detalhado_cadeia").fetchone()[0] == 2


def test_exportar_parquets_gold_cria_arquivos(con, tmp_path, monkeypatch):
    monkeypatch.setattr(gold, 'DIRETORIO_GOLD', str(tmp_path))
    tabelas = ('itens_nib', 'resumo_cadeia', 'detalhado_cadeia', 'cnae_cadeia', 'totais_globais')
    for tabela in tabelas:
        con.execute(f"CREATE TABLE {tabela} (id INTEGER)")
    exportar_parquets(con)
    for tabela in tabelas:
        assert (tmp_path / f'{tabela}.parquet').exists()


def test_exportar_csvs_entrega_cria_csvs(con, tmp_path, monkeypatch):
    monkeypatch.setattr(gold, 'DIRETORIO_ENTREGA', str(tmp_path / 'entrega'))
    for tabela in ('resumo_cadeia', 'detalhado_cadeia', 'cnae_cadeia'):
        con.execute(f"CREATE TABLE {tabela} (id INTEGER)")
    exportar_csvs_entrega(con)
    for tabela in ('resumo_cadeia', 'detalhado_cadeia', 'cnae_cadeia'):
        assert (tmp_path / 'entrega' / f'{tabela}.csv').exists()
