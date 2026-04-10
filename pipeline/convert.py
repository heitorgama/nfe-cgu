import pandas as pd

 

ARQUIVO_EXCEL = r"C:\Users\arthur.barbabella\Documents\vscode\nfe-cgu\dados\2026-02-25 MDIC-SDIC CNAE_NIB-COM_SDIC versão 05_06_2025 (limpa) (1)(1).xlsx"

 

df = pd.read_excel(ARQUIVO_EXCEL, sheet_name="Tabela 2", skiprows=1)

df = df.drop(columns=df.columns[0])

df = df.drop(columns=df.columns[-3:])

df = df.rename(columns={"Código da Cadeia":"codigo_cadeia"})

df["codigo_cadeia"] = df["codigo_cadeia"].ffill() # Em células mescladas no Excel, o pandas mantém o valor apenas na primeira linha do intervalo; o ffill replica esse valor nas linhas abaixo
df["Número da Missão"] = df["Número da Missão"].ffill() # Em células mescladas no Excel, o pandas mantém o valor apenas na primeira linha do intervalo; o ffill replica esse valor nas linhas abaixo
df["Nome da Missão"] = df["Nome da Missão"].ffill() # Em células mescladas no Excel, o pandas mantém o valor apenas na primeira linha do intervalo; o ffill replica esse valor nas linhas abaixo

df["codigos_separados_por_virgulas"] = df["SH Correspondente"].astype(str).str.replace(r"[\n\s|]", ",", regex=True)

df["codigos_separados_por_virgula"] = (
    df["codigos_separados_por_virgulas"]
    .astype(str)
    .str.replace(r",{2,}", ",", regex=True)
)

df["codigos_sem_espacos"] = df["codigos_separados_por_virgula"].astype(str).str.replace(r"[\n\s|]", "", regex=True)

# df.loc[df["codigo_cadeia"] == 'M1-C3', ["SH Correspondente", "codigo_cadeia", "codigos_sem_espacos"]]

df["codigos_sem_pontos"] = df["codigos_sem_espacos"].astype(str).str.replace(r"\.", "", regex=True)

df["codigos_sem_parenteses"] = df["codigos_sem_pontos"].astype(str).str.replace(r"\d{4,},*\(|\)", "", regex=True)

# df.loc[df["codigo_cadeia"] == 'M1-C2', ["SH Correspondente", "codigo_cadeia", "codigos_sem_pontos", "codigos_sem_parenteses"]]

df["codigos_separados_por_virgula"] = df["codigos_sem_parenteses"].astype(str).str.replace(";", ",", regex=False)

df["lista_de_codigos"] = df["codigos_separados_por_virgula"].astype(str).str.split(",").to_list()

df["codigo_ncm"] = df["lista_de_codigos"]

resultado = df.explode("codigo_ncm", ignore_index=True)
resultado.to_csv(r"C:\Users\arthur.barbabella\Documents\vscode\nfe-cgu\dados\mapeamento_ncm.csv", index=False)