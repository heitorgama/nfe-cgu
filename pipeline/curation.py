import os
from datetime import datetime

import pandas as pd

DIRETORIO_SILVER = 'extracoes/silver'
DIRETORIO_GOLD = 'extracoes/gold'


def imprimir_mensagem(mensagem: str) -> None:
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {mensagem}")


# Brief: adiciona colunas 'tipo_emitente' e 'tipo_destinatario' ('PF' ou 'PJ') com base em qual coluna está preenchida
def adicionar_tipo_pessoa(df: pd.DataFrame) -> pd.DataFrame:
    pares = [
        ('cnpj_emitente', 'cpf_emitente', 'tipo_emitente'),
        ('cnpj_destinatario', 'cpf_destinatario', 'tipo_destinatario'),
    ]
    for col_cnpj, col_cpf, col_tipo in pares:
        if col_cnpj not in df.columns or col_cpf not in df.columns:
            continue
        tem_cnpj = df[col_cnpj].notna()
        tem_cpf  = df[col_cpf].notna() & (df[col_cpf].astype(str).str.strip() != '') & (df[col_cpf].astype(str) != 'None')
        df[col_tipo] = pd.NA
        df.loc[tem_cnpj, col_tipo] = 'PJ'
        df.loc[~tem_cnpj & tem_cpf,  col_tipo] = 'PF'
    return df


# Brief: lê parquet do silver, adiciona colunas de tipo de pessoa e salva em gold
def main():
    os.makedirs(DIRETORIO_GOLD, exist_ok=True)

    for nome in ('itens', 'nf'):
        df = pd.read_parquet(os.path.join(DIRETORIO_SILVER, f'{nome}.parquet'))
        df = adicionar_tipo_pessoa(df)
        df.to_parquet(os.path.join(DIRETORIO_GOLD, f'{nome}.parquet'), compression='snappy')
        imprimir_mensagem(f"{nome}: {len(df)} linhas.")

    imprimir_mensagem("Gold salvo.")


if __name__ == "__main__":
    main()
