import os
import json
import re
import tempfile
from datetime import datetime

import duckdb
import pandas as pd

DIRETORIO_SILVER = 'extracoes/silver'
DIRETORIO_GOLD = 'extracoes/gold'
DIRETORIO_ENTREGA = os.path.join(DIRETORIO_GOLD, 'cruzamento_ncm')

ANOS = [2023, 2024, 2025]

# (ncm_col, em_prefix, notas_prefix, desc_col, nivel_csv, lpad)
NIVEIS = [
    ('ncm2', 'emissores_cap',  'notas_cap',  'desc_ncm2', 'Capítulo',      2),
    ('ncm4', 'emissores_ncm4', 'notas_ncm4', 'desc_ncm4', 'Posição',       4),
    ('ncm5', 'emissores_ncm5', 'notas_ncm5', 'desc_ncm5', 'Subposição 1',  5),
    ('ncm6', 'emissores_ncm6', 'notas_ncm6', 'desc_ncm6', 'Subposição 2',  6),
    ('ncm7', 'emissores_ncm7', 'notas_ncm7', 'desc_ncm7', 'Item',          7),
    ('ncm8', 'emissores_item', 'notas_item', 'desc_ncm8', 'Subitem',        8),
]


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


def _strip_html(text: str) -> str:
    return re.sub(r'<[^>]+>', '', text or '').strip()


def _case_ano(col: str, ano: int, alias: str) -> str:
    return f"COUNT(DISTINCT CASE WHEN YEAR(data_emissao) = {ano} THEN {col} END) AS {alias}"


def agregar_ncm(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, dict]:
    """
    Agrega itens.parquet por NCM (6 níveis) para todos os anos em ANOS.
    Retorna (df_por_ncm, totais_globais_por_ano).
    """
    def em_cte(ncm_col, em_prefix, notas_prefix):
        cols = ',\n               '.join(
            f"{_case_ano('emitente', ano, f'{em_prefix}_{ano}')},\n               "
            f"{_case_ano('chave_de_acesso', ano, f'{notas_prefix}_{ano}')}"
            for ano in ANOS
        )
        return f"em_{ncm_col} AS (SELECT {ncm_col}, {cols} FROM base GROUP BY 1)"

    em_ctes = ',\n        '.join(
        em_cte(ncm_col, em_prefix, notas_prefix)
        for ncm_col, em_prefix, notas_prefix, *_ in NIVEIS
    )

    desc_ctes = ',\n'.join(
        f"""        desc_{ncm_col} AS (
            SELECT LPAD(CAST(CAST(prefixo AS BIGINT) AS VARCHAR), {lpad}, '0') AS {ncm_col},
                   ANY_VALUE(descricao) AS {desc_col}
            FROM 'dados/ncm.csv'
            WHERE nivel = '{nivel_csv}'
            GROUP BY 1
        )"""
        for ncm_col, _, _, desc_col, nivel_csv, lpad in NIVEIS
    )

    valor_cols = ',\n                   '.join(
        f"SUM(CASE WHEN YEAR(data_emissao) = {ano} THEN valor_total ELSE 0 END) AS valor_{ano}"
        for ano in ANOS
    )
    em_item_cols = ',\n                   '.join(
        f"{_case_ano('emitente', ano, f'emissores_item_{ano}')},\n                   "
        f"{_case_ano('chave_de_acesso', ano, f'notas_item_{ano}')}"
        for ano in ANOS
    )

    em_select = ',\n            '.join(
        f"e_{ncm_col}.{em_prefix}_{ano}, e_{ncm_col}.{notas_prefix}_{ano}"
        for ncm_col, em_prefix, notas_prefix, *_ in NIVEIS
        for ano in ANOS
    )

    em_joins = '\n        '.join(
        f"LEFT JOIN em_{ncm_col} e_{ncm_col} ON e_{ncm_col}.{ncm_col} = a.{ncm_col}"
        for ncm_col, *_ in NIVEIS
    )

    desc_select = ',\n            '.join(
        f"d_{ncm_col}.{desc_col}" for ncm_col, _, _, desc_col, *_ in NIVEIS
    )
    desc_joins = '\n        '.join(
        f"LEFT JOIN desc_{ncm_col} d_{ncm_col} ON d_{ncm_col}.{ncm_col} = a.{ncm_col}"
        for ncm_col, *_ in NIVEIS
    )

    query = f"""
        WITH base AS (
            SELECT
                LPAD(CAST(CAST(codigo_ncm_sh AS BIGINT) AS VARCHAR), 8, '0') AS ncm8,
                LEFT(LPAD(CAST(CAST(codigo_ncm_sh AS BIGINT) AS VARCHAR), 8, '0'), 7) AS ncm7,
                LEFT(LPAD(CAST(CAST(codigo_ncm_sh AS BIGINT) AS VARCHAR), 8, '0'), 6) AS ncm6,
                LEFT(LPAD(CAST(CAST(codigo_ncm_sh AS BIGINT) AS VARCHAR), 8, '0'), 5) AS ncm5,
                LEFT(LPAD(CAST(CAST(codigo_ncm_sh AS BIGINT) AS VARCHAR), 8, '0'), 4) AS ncm4,
                LEFT(LPAD(CAST(CAST(codigo_ncm_sh AS BIGINT) AS VARCHAR), 8, '0'), 2) AS ncm2,
                data_emissao,
                valor_total,
                chave_de_acesso,
                COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente) AS emitente
            FROM 'extracoes/silver/itens.parquet'
            WHERE YEAR(data_emissao) BETWEEN {ANOS[0]} AND {ANOS[-1]}
              AND codigo_ncm_sh IS NOT NULL
        ),
        agg_item AS (
            SELECT ncm2, ncm4, ncm5, ncm6, ncm7, ncm8,
                   {valor_cols},
                   {em_item_cols}
            FROM base
            GROUP BY 1, 2, 3, 4, 5, 6
        ),
        {em_ctes},
{desc_ctes}
        ,margem_ativa AS (
            SELECT
                LPAD(CAST(codigo AS VARCHAR), 8, '0') AS ncm8,
                BOOL_OR(margem_adicional_pct IS NOT NULL AND margem_adicional_pct > 0) AS tem_adicional
            FROM 'dados/dim_margem_ncm_utf8.csv'
            WHERE ativa = true
            GROUP BY 1
        )
        SELECT
            a.ncm2, a.ncm4, a.ncm5, a.ncm6, a.ncm7, a.ncm8,
            {', '.join(f'a.valor_{ano}' for ano in ANOS)},
            {', '.join(f'a.emissores_item_{ano}, a.notas_item_{ano}' for ano in ANOS)},
            {em_select},
            {desc_select},
            CASE
                WHEN m.tem_adicional = true THEN 'adicional'
                WHEN m.ncm8 IS NOT NULL     THEN 'normal'
                ELSE NULL
            END AS margem
        FROM agg_item a
        {em_joins}
        {desc_joins}
        LEFT JOIN margem_ativa m ON m.ncm8 = a.ncm8
        ORDER BY a.ncm2, a.valor_{ANOS[-1]} DESC
    """

    df = con.execute(query).fetchdf()

    # Totais globais por ano
    tot_cols = ',\n            '.join(
        f"SUM(CASE WHEN YEAR(data_emissao) = {ano} THEN valor_total ELSE 0 END) AS total_{ano},\n            "
        f"{_case_ano('COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente)', ano, f'emissores_{ano}')},\n            "
        f"{_case_ano('chave_de_acesso', ano, f'notas_{ano}')}"
        for ano in ANOS
    )
    tot_row = con.execute(f"""
        SELECT {tot_cols}
        FROM 'extracoes/silver/itens.parquet'
        WHERE YEAR(data_emissao) BETWEEN {ANOS[0]} AND {ANOS[-1]}
    """).fetchdf().iloc[0]

    totais = {
        str(ano): {
            'valor':     float(tot_row[f'total_{ano}'] or 0),
            'emissores': int(tot_row[f'emissores_{ano}'] or 0),
            'notas':     int(tot_row[f'notas_{ano}'] or 0),
        }
        for ano in ANOS
    }

    return df, totais


def agregar_por_uf(con: duckdb.DuckDBPyConnection, uf_col: str, include_nodes: bool = True) -> tuple[list, dict]:
    """
    Agrega valor por (dimensão, NCM em todos os 6 níveis, ano).
    include_nodes=False: armazena apenas totais (para dimensões com muitos valores).
    """
    node_query = f"""
        WITH base AS (
            SELECT
                LPAD(CAST(CAST(codigo_ncm_sh AS BIGINT) AS VARCHAR), 8, '0') AS ncm8,
                {uf_col} AS uf,
                YEAR(data_emissao) AS ano,
                valor_total AS valor
            FROM 'extracoes/silver/itens.parquet'
            WHERE YEAR(data_emissao) BETWEEN {ANOS[0]} AND {ANOS[-1]}
              AND codigo_ncm_sh IS NOT NULL
              AND {uf_col} IS NOT NULL
        ),
        by_ncm8 AS (
            SELECT ncm8, uf, ano, SUM(valor) AS valor FROM base GROUP BY 1, 2, 3
        ),
        all_levels AS (
            SELECT ncm8 AS cod, uf, ano, valor FROM by_ncm8
            UNION ALL SELECT LEFT(ncm8,7), uf, ano, valor FROM by_ncm8
            UNION ALL SELECT LEFT(ncm8,6), uf, ano, valor FROM by_ncm8
            UNION ALL SELECT LEFT(ncm8,5), uf, ano, valor FROM by_ncm8
            UNION ALL SELECT LEFT(ncm8,4), uf, ano, valor FROM by_ncm8
            UNION ALL SELECT LEFT(ncm8,2), uf, ano, valor FROM by_ncm8
        )
        SELECT cod, uf, ano, SUM(valor) AS valor
        FROM all_levels
        GROUP BY 1, 2, 3
    """

    tot_query = f"""
        SELECT
            {uf_col} AS uf,
            YEAR(data_emissao) AS ano,
            SUM(valor_total) AS valor,
            COUNT(DISTINCT COALESCE(CAST(cnpj_emitente AS VARCHAR), cpf_emitente)) AS emissores,
            COUNT(DISTINCT chave_de_acesso) AS notas
        FROM 'extracoes/silver/itens.parquet'
        WHERE YEAR(data_emissao) BETWEEN {ANOS[0]} AND {ANOS[-1]}
          AND {uf_col} IS NOT NULL
        GROUP BY 1, 2
    """

    anos_idx = {ano: i for i, ano in enumerate(ANOS)}
    uf_vals: dict = {}

    if include_nodes:
        df_nodes = con.execute(node_query).fetchdf()
        for row in df_nodes.itertuples(index=False):
            uf, cod, ano, val = str(row.uf), str(row.cod), int(row.ano), float(row.valor)
            if ano not in anos_idx:
                continue
            if uf not in uf_vals:
                uf_vals[uf] = {'totais': {}, 'nodes': {}}
            nodes = uf_vals[uf]['nodes']
            if cod not in nodes:
                nodes[cod] = [0.0] * len(ANOS)
            nodes[cod][anos_idx[ano]] = val

    df_tot = con.execute(tot_query).fetchdf()
    for row in df_tot.itertuples(index=False):
        uf, ano = str(row.uf), int(row.ano)
        if uf not in uf_vals:
            uf_vals[uf] = {'totais': {}}
        uf_vals[uf]['totais'][str(ano)] = {
            'valor':     float(row.valor or 0),
            'emissores': int(row.emissores or 0),
            'notas':     int(row.notas or 0),
        }

    return sorted(uf_vals.keys()), uf_vals


def exportar_parquet(df: pd.DataFrame) -> None:
    os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
    destino = os.path.join(DIRETORIO_ENTREGA, 'ncm_2025.parquet')
    destino_fwd = destino.replace('\\', '/')
    con = duckdb.connect()
    con.register('_df', df)
    con.execute(f"COPY _df TO '{destino_fwd}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    con.close()
    tamanho_mb = os.path.getsize(destino) / 1024 / 1024
    imprimir_mensagem(f"Parquet salvo: {destino} ({tamanho_mb:.1f} MB)")


_MARGEM_PRIO = {None: 0, 'normal': 1, 'adicional': 2}


def construir_hierarquia_ncm(df: pd.DataFrame, totais: dict) -> dict:
    """
    Hierarquia aninhada de 6 níveis com valor, emissores, notas e margem por ano.
    'margem' é propagada para cima: adicional > normal > null.
    """
    root: dict = {}
    seen_em: dict[str, set] = {
        f'{prefix}_{ano}': set()
        for _, em_prefix, notas_prefix, *_ in NIVEIS
        for prefix in (em_prefix, notas_prefix)
        for ano in ANOS
    }

    for _, row in df.iterrows():
        valores = {ano: float(row.get(f'valor_{ano}') or 0) for ano in ANOS}
        margem_item = row.get('margem') if pd.notna(row.get('margem')) else None

        # Capítulos sem entrada na taxonomia (ex: 00, 98, 99) → agrupa em "Outros"
        if not row.get('desc_ncm2'):
            if 'outros' not in root:
                root['outros'] = {
                    'cod': 'outros', 'desc': 'Outros',
                    **{f'valor_{ano}':     0.0 for ano in ANOS},
                    **{f'emissores_{ano}': 0   for ano in ANOS},
                    **{f'notas_{ano}':     0   for ano in ANOS},
                    'margem': None, 'children': None,
                }
            node = root['outros']
            for ano, v in valores.items():
                node[f'valor_{ano}'] += v
            if _MARGEM_PRIO.get(margem_item, 0) > _MARGEM_PRIO.get(node['margem'], 0):
                node['margem'] = margem_item
            continue

        active = [
            (ncm_col, em_prefix, notas_prefix, desc_col)
            for ncm_col, em_prefix, notas_prefix, desc_col, *_ in NIVEIS
            if row.get(desc_col)
        ]

        current = root
        for depth, (ncm_col, em_prefix, notas_prefix, desc_col) in enumerate(active):
            cod = str(row[ncm_col])
            is_leaf = depth == len(active) - 1

            if cod not in current:
                em_by_year, nt_by_year = {}, {}
                for ano in ANOS:
                    em_col = f'{em_prefix}_{ano}'
                    nt_col = f'{notas_prefix}_{ano}'
                    em_by_year[ano] = int(row.get(em_col) or 0) if cod not in seen_em.setdefault(em_col, set()) else 0
                    nt_by_year[ano] = int(row.get(nt_col) or 0) if cod not in seen_em.setdefault(nt_col, set()) else 0
                    seen_em[em_col].add(cod)
                    seen_em[nt_col].add(cod)

                current[cod] = {
                    'cod':  cod,
                    'desc': _strip_html(str(row[desc_col] or '')),
                    **{f'valor_{ano}':     0.0 for ano in ANOS},
                    **{f'emissores_{ano}': em_by_year[ano] for ano in ANOS},
                    **{f'notas_{ano}':     nt_by_year[ano] for ano in ANOS},
                    'margem':   None,
                    'children': None if is_leaf else {},
                }
            elif not is_leaf and current[cod]['children'] is None:
                current[cod]['children'] = {}

            # Propaga margem: o nó herda a prioridade mais alta dos seus filhos
            if _MARGEM_PRIO.get(margem_item, 0) > _MARGEM_PRIO.get(current[cod]['margem'], 0):
                current[cod]['margem'] = margem_item

            for ano, v in valores.items():
                current[cod][f'valor_{ano}'] += v

            if not is_leaf:
                current = current[cod]['children']

    def to_list(d: dict | None) -> list:
        if not d:
            return []
        return sorted(
            [{**node, 'children': to_list(node['children'])} for node in d.values()],
            key=lambda x: -x[f'valor_{ANOS[-1]}'],
        )

    return {'anos': ANOS, 'totais': totais, 'capitulos': to_list(root)}


TEMPLATES = [
    ('dashboard.html', 'preview.html'),
]


def exportar_htmls(data: dict) -> None:
    try:
        data_json = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        os.makedirs(DIRETORIO_ENTREGA, exist_ok=True)
        tpl_dir = os.path.join(os.path.dirname(__file__), 'template')

        for template_file, output_file in TEMPLATES:
            path_tpl = os.path.join(tpl_dir, template_file)
            
            if not os.path.exists(path_tpl):
                imprimir_mensagem(f"ERRO: Template não encontrado em {path_tpl}")
                continue

            with open(path_tpl, encoding='utf-8') as f:
                html = f.read()

            if not html:
                imprimir_mensagem(f"AVISO: O template {template_file} está vazio!")
                
            # Verifica se o placeholder existe antes de tentar o replace
            if '/*DATA_PLACEHOLDER*/' not in html:
                imprimir_mensagem(f"AVISO: Placeholder não encontrado no template {template_file}")

            html = html.replace('/*DATA_PLACEHOLDER*/', data_json)

            destino = os.path.join(DIRETORIO_ENTREGA, output_file)
            
            # Só abre para escrita se tiver conteúdo
            if html:
                with open(destino, 'w', encoding='utf-8') as f:
                    f.write(html)
                tamanho_mb = os.path.getsize(destino) / 1024 / 1024
                imprimir_mensagem(f"  {output_file} ({tamanho_mb:.1f} MB)")
            else:
                imprimir_mensagem(f"ERRO: Conteúdo final do HTML está vazio para {output_file}")

    except Exception as e:
        imprimir_mensagem(f"Erro crítico ao exportar HTML: {e}")


def main():
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)
    con = duckdb.connect(config={'temp_directory': tempfile.gettempdir()})
    con.execute("SET preserve_insertion_order=false")

    imprimir_mensagem(f"Agregando NCM {ANOS[0]}-{ANOS[-1]}...")
    df, totais = agregar_ncm(con)
    imprimir_mensagem(f"{len(df)} NCMs encontrados. Total {ANOS[-1]}: R$ {totais[str(ANOS[-1])]['valor']:,.2f}")

    imprimir_mensagem("Exportando parquet...")
    exportar_parquet(df)

    imprimir_mensagem("Construindo hierarquia e exportando HTML...")
    data = construir_hierarquia_ncm(df, totais)

    imprimir_mensagem("Agregando por UF emitente...")
    ufs_emit, uf_emit_vals = agregar_por_uf(con, 'uf_emitente')
    data['ufs_emit'] = ufs_emit
    data['uf_emit_vals'] = uf_emit_vals
    imprimir_mensagem(f"  {len(ufs_emit)} UFs emitentes encontradas.")

    imprimir_mensagem("Agregando por UF destinatária...")
    ufs_dest, uf_dest_vals = agregar_por_uf(con, 'uf_destinatario')
    data['ufs_dest'] = ufs_dest
    data['uf_dest_vals'] = uf_dest_vals
    imprimir_mensagem(f"  {len(ufs_dest)} UFs destinatárias encontradas.")

    imprimir_mensagem("Agregando por Órgão Superior Destinatário...")
    orgaos_sup, orgao_sup_vals = agregar_por_uf(con, 'orgao_superior_destinatario')
    data['orgaos_sup'] = orgaos_sup
    data['orgao_sup_vals'] = orgao_sup_vals
    imprimir_mensagem(f"  {len(orgaos_sup)} órgãos superiores encontrados.")

    imprimir_mensagem("Agregando por Órgão Destinatário...")
    orgaos_org, orgao_org_vals = agregar_por_uf(con, 'orgao_destinatario', include_nodes=False)
    data['orgaos_org'] = orgaos_org
    data['orgao_org_vals'] = orgao_org_vals
    imprimir_mensagem(f"  {len(orgaos_org)} órgãos encontrados.")

    imprimir_mensagem(f"{len(data['capitulos'])} capítulos. Gerando {len(TEMPLATES)} HTMLs...")
    exportar_htmls(data)

    imprimir_mensagem("Gold salvo.")


if __name__ == "__main__":
    main()
