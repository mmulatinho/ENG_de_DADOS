import os
import pandas as pd
from datetime import datetime

bronze_conv = "dataset/bronze"
silver_kafka = "dataset/silver"

def transformar_silver():
    print("Iniciando transformação para camada Silver...")

    #leitura dos arquivos parquet particionados
    df = pd.read_parquet(bronze_conv)

    #limpeza e padronização de dados
    #Remover espaços, ajustar maiúsculas, converter tipos
    df.columns = [col.strip().lower() for col in df.columns]

    #conversão de tipos
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data'], errors='coerce')
    if 'valor' in df.columns:
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

    #padronização de strings
    if 'favorecido' in df.columns:
        df['favorecido'] = df['favorecido'].str.strip().str.title()

    #tratamento de valores nulos
    df = df.dropna(subset=['data', 'valor'], how='any')

    #testes simples de qualidade
    if df['valor'].isnull().any():
        print("Valores nulos encontrados na coluna 'valor'")
    else:
        print("Nenhum valor nulo em 'valor'")

    #Análise exploratória inicial
    print("\nAnálise de colunas:")
    print(df.describe(include='all', datetime_is_numeric=True))

    #Salvamento particionado (ano/mês)
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month

    os.makedirs(silver_kafka, exist_ok=True)
    df.to_parquet(
        silver_kafka,
        engine="pyarrow",
        index=True,
        partition_cols=["ano", "mes"]
    )

    print("Transformação Silver concluída com sucesso!")

if __name__ == "__main__":
    transformar_silver()
