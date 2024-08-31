import requests
import pandas as pd
import duckdb
from time import sleep

# Lista de URLs, descritores e descrições
series = [
    {'url': 'https://apisidra.ibge.gov.br/values/t/5932/n1/all/v/6564/p/all/c11255/90707/d/v6564%201', 'tabela': 'pib_variacao_trimestral'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6784/n1/all/v/9810/p/all/d/v9810%201', 'tabela': 'pib_anual'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6784/n1/all/v/9812/p/all/d/v9812%202', 'tabela': 'pib_anual_pc'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6022/n1/all/v/606/p/all', 'tabela': 'populacao_trimestral'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6415/n1/all/v/606/p/all', 'tabela': 'populacao_anual'}
]

# Conectar ao banco de dados DuckDB
duckdb_conn = duckdb.connect('ibge_data.duckdb')

# Loop pelas séries para fazer as requisições e salvar no banco de dados DuckDB
for serie in series:
    params = {'formato': 'json'}
    sucesso = False
    tentativas = 0
    max_tentativas = 5
    
    while not sucesso and tentativas < max_tentativas:
        try:
            response = requests.get(serie['url'], params=params)
            response.raise_for_status()  # Lança a exceção

            data = response.json()
            df = pd.DataFrame(data)
                
            # Define a primeira linha como nomes das colunas
            colunas = df.iloc[0]
            df.columns = colunas

            # Remove a primeira linha, que agora são os nomes das colunas
            df = df[1:]

            # Reseta o índice do DataFrame
            df = df.reset_index(drop=True)

            # Inserir dados no DuckDB
            duckdb_conn.execute(f"CREATE OR REPLACE TABLE {serie['tabela']} AS SELECT * FROM df")
            print(f"Dados da série {serie['tabela']} salvos com sucesso no banco de dados DuckDB.")
                
            sucesso = True

        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a requisição para a série {serie['tabela']}: {e}")
            tentativas += 1
            sleep(10)

    if not sucesso:
        print(f"Falha ao obter dados da série {serie['tabela']} após {max_tentativas} tentativas.")
        
duckdb_conn.close()
print("Fim das importações de dados do IBGE.")
