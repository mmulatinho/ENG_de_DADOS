import os
import json
import time
import requests
import pandas as pd

link_site = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data/"
token = "--"  #Token da API
raw_pull = "dataset/raw" #caminho dos arquivos brutos em json
bronze_conv = "dataset/bronze" #caminho dos arquivos processados parquet
pull_delay = 1.2  #intervalo entre requisições (tava dando limite de requisições ent pensei num delay)
limitador = 1000 #limitador de paginas

#função para baixar os dados
def baixar_dados():
    os.makedirs(raw_pull, exist_ok=True) #Garante que a pasta exista
    headers = {"Authorization": f"Token {token}"}  #autenticação do token

    #loop para puxar as páginas
    for page in range(1, limitador + 1):
        print(f"baixando página {page}...") #aparece no terminal cada pull
        r = requests.get(link_site, headers=headers, params={"page": page})#faz a requisição

        if r.status_code != 200: #verifica se o pull deu certo
            print(f"Erro {r.status_code}. Encerrando a extração.") #Mensagem no terminal
            break
        
        data = r.json() #converter os arquivos para json

        with open(f"{raw_pull}/gastos_page_{page}.json", "w", encoding="utf-8") as f: #salva os json na pasta raw
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        if not data.get("next"): # Se não existir próxima página, encerra o loop
            print("Última página da API atingida.") #Mensagem no terminal
            break

        time.sleep(pull_delay) #delay entre as chamadas
    print("Extração concluída!")

#função conversao para parquet
def converter_para_parquet():
    
    os.makedirs(raw_pull, exist_ok=True)  #Garante que a pasta exista
    arquivos = [f for f in os.listdir(raw_pull) if f.endswith(".json")]  #lista de arquivos JSON

    if not arquivos: #se nao achar os arquivos json interrompe a execução
        print("Nenhum arquivo JSON encontrado para conversão.")
        return

    df = pd.concat(  #lê os JSONs em um único DataFrame
        [
            pd.json_normalize(
                json.load(open(os.path.join(raw_pull, f), encoding="utf-8"))["results"]
            )
            for f in arquivos
        ],
        ignore_index=True
    )
    
    # Salva em formato Parquet (particionado por ano/mês)
    df.to_parquet(bronze_conv,
        engine = "pyarrow", index = True,
        partition_cols=["ano", "mes"],)
    print("Conversão para Parquet realizada")
    
#execução
if __name__ == "__main__":
    baixar_dados() #executa função para baixar os dados
    converter_para_parquet() #executa função da convesão json/parquet
    print("Pipeline finalizado com sucesso!")

