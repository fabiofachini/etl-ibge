import requests
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from time import sleep
import urllib

# Lista de URLs, descritores e descrições
series = [
    {'url': 'https://apisidra.ibge.gov.br/values/t/5932/n1/all/v/6564/p/all/c11255/90707/d/v6564%201', 'tabela': 'pib_variacao_trimestral'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6784/n1/all/v/9810/p/all/d/v9810%201', 'tabela': 'pib_anual'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6784/n1/all/v/9812/p/all/d/v9812%202', 'tabela': 'pib_anual_pc'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6022/n1/all/v/606/p/all', 'tabela': 'populacao_trimestral'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6415/n1/all/v/606/p/all', 'tabela': 'populacao_anual'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6318/n1/all/v/1641/p/all/c629/all', 'tabela': 'populacao_economicamente_ativa'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6380/n1/all/v/4098/p/all/d/v4098%201', 'tabela': 'nivel_de_desocupacao'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6381/n1/all/v/4099/p/all/d/v4099%201', 'tabela': 'taxa_de_desocupacao'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6785/n1/all/v/9819/p/all/d/v9819%201', 'tabela': 'taxa_de_subocupacao'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6807/n1/all/v/9869/p/all/d/v9869%201', 'tabela': 'taxa_de_desalentados'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/8513/n1/all/v/12466/p/all/d/v12466%201', 'tabela': 'taxa_de_informalidade'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/5944/n1/all/v/4096/p/all/d/v4096%201', 'tabela': 'taxa_de_part_forca_de_trabalho'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6392/n1/all/v/6293/p/all', 'tabela': 'massa_salarial_habitualmente'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6391/n1/all/v/5932/p/all/c888/all', 'tabela': 'rendimento_mensal_atividade'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6389/n1/all/v/5932/p/all/c11913/31727,96165,96166,96170,96171', 'tabela': 'rendimento_mensal_posicao'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6388/n1/all/v/5934/p/all', 'tabela': 'rendimento_trabalho_principal'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/6387/n1/all/v/5935/p/all', 'tabela': 'rendimento_todos_os_trabalhos'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/7453/n1/all/v/10806/p/all/d/v10806%203', 'tabela': 'indice_de_gini'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/7521/n1/all/v/606/p/all/c1019/49244,49245,49247,49248,49249,49250,49251,49252,49253,49254,49256,49257,49258', 'tabela': 'populacao_classe_social'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/7527/n1/all/v/10826/p/all/c1019/49244,49245,49247,49248,49249,49250,49251,49252,49253,49254,49256,49257,49258/d/v10826%201', 'tabela': 'massa_salarial_por_classe_social'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/7531/n1/all/v/10824/p/all/c1019/49244,49245,49247,49248,49249,49250,49251,49252,49253,49254,49256,49257,49258', 'tabela': 'rendimento_classe_social'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/7113/n1/all/v/10267/p/all/c2/all/c58/2795/d/v10267%201', 'tabela': 'taxa_de_analfabetismo'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/7267/n1/all/v/1641/p/all/c2/6794/c1568/allxt', 'tabela': 'nivel_de_instrucao'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/1419/n1/all/v/63,69,2265/p/all/c315/7169/d/v63%202,v69%202,v2265%202', 'tabela': 'ipca_ate_2019'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/7060/n1/all/v/63,69,2265/p/all/c315/7169/d/v63%202,v69%202,v2265%202', 'tabela': 'ipca_depois_2019'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/1100/n1/all/v/44,68,2292/p/all/c315/7169/d/v44%202,v68%202,v2292%202', 'tabela': 'inpc_ate_2019'},
    {'url': 'https://apisidra.ibge.gov.br/values/t/7063/n1/all/v/44,68,2292/p/all/c315/7169/d/v44%202,v68%202,v2292%202', 'tabela': 'inpc_depois_2019'}
]


# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_DATABASE")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

# Criar a conexão com o banco de dados
params = urllib.parse.quote_plus(f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}")
conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
engine = create_engine(conn_str)

# Loop pelas séries para fazer as requisições e salvar no banco de dados
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

            # Inserir dados no banco de dados
            df.to_sql(serie['tabela'], engine, if_exists='replace', index=False, schema='ibge')
            print(f"Dados da série {serie['tabela']} salvos com sucesso no banco de dados.")
                
            sucesso = True

        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer a requisição para a série {serie['tabela']}: {e}")
            tentativas += 1
            sleep(10)

    if not sucesso:
        print(f"Falha ao obter dados da série {serie['tabela']} após {max_tentativas} tentativas.")
        
    
print("Fim das importações de dados do IBGE.")