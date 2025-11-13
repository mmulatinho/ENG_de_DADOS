from silver import transformar_silver
from gold import transformar_gold
from bronze import baixar_dados, converter_para_parquet

if __name__ == "__main__":
    baixar_dados()             #Bronze: pull dos dados da API
    converter_para_parquet()   #Bronze: Conversão JSON → Parquet
    transformar_silver()       #Silver: Limpeza e padronização
    transformar_gold()         #Gold  : Agregação e artefatos analíticos
    print("Pipeline Bronze → Silver → Gold concluída")
