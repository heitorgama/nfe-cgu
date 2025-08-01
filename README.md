# Como concatenar os arquivos de Notas Fiscais do [Portal da Transparência](https://portaldatransparencia.gov.br/)


Clonar este repositório na sua máquina, executando:

```bash
git clone https://github.com/heitorgama/nfe-cgu
```

Criar e ativar ambiente virtual, executando, a partir da raiz do repositório clonado:

```bash
 python3 -m venv env
 source myenv/bin/activate
 ```

Os arquivos de notas fiscais eletrônicas são disponibilizados mensalmente em: https://portaldatransparencia.gov.br/download-de-dados/notas-fiscais

Baixar os arquivos `.zip` referentes aos meses de interesse no diretório `dados/nfe`.

Instalar o gerenciador de pacotes `pip` caso não esteja instalado, executando:

```bash
python3 -m venv my_env
```

Instalar as bibliotecas necessárias, executando:

```bash
pip install -r requirements.txt
```

Executar o script `concatenar_nfs.py` para concatenar os arquivos `.zip` baixados, executando:

```bash
python concatenar_nfs.py
```

O script irá extrair os arquivos `.zip` e concatenar os dados em um arquivo Parquet chamado `nfe.parquet` no diretório `extracoes`.
Os **itens** das notas fiscais serão extraídos para um arquivo separado chamado `itens.parquet`.
Os **eventos** de notas fiscais serão extraídos para um arquivo chamado `eventos.parquet`.

O arquivo `consultar_nfs.py` contém consultas SQL para analisar os dados extraídos. Você pode executar essas consultas usando o DuckDB, que é uma biblioteca de banco de dados SQL leve e rápida.