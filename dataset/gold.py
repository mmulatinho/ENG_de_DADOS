import os
import pandas as pd

silver_kafka = "dataset/silver"
gold_midas = "dataset/gold"

def transformar_gold():
    print("Iniciando transformação para camada Gold...")

    df = pd.read_parquet(silver_kafka)

    #Criação de agregações de valor de negócio
    #gasto total por órgão e mês
    df_gold = (
        df.groupby(["ano", "mes", "orgao"])
          .agg(total_gasto=("valor", "summario"), qtd_lancamentos=("valor", "contagem"))
          .reset_index()
    )

    #Outras agregações possíveis (ex.: diárias, semanais)
    #gasto médio diário
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"])
        gasto_diario = (
            df.groupby(["ano", "mes", "data"])
              .agg(media_diaria=("valor", "Media"))
              .reset_index()
        )
        os.makedirs(f"{gold_midas}/diario", exist_ok=True)
        gasto_diario.to_parquet(f"{gold_midas}/diario", index=False)

    #Salvamento final
    os.makedirs(gold_midas, exist_ok=True)
    df_gold.to_parquet(gold_midas, index=True, partition_cols=["ano", "mes"])

    print("Transformação Gold concluída com sucesso!")

if __name__ == "__main__":
    transformar_gold()
