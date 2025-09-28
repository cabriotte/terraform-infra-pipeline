import os
import sys

# 🔹 Garante que a raiz do projeto esteja no PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scraper import baixar_csv_com_selenium, processar_csv_para_parquet, upload_to_s3

def main():
    destino = "./dados_b3_local"
    os.makedirs(destino, exist_ok=True)

    print("➡️ Baixando CSV...")
    caminho_csv = baixar_csv_com_selenium(destino_dir=destino)
    print(f"CSV salvo em: {caminho_csv}")

    print("➡️ Convertendo para Parquet...")
    caminho_parquet = processar_csv_para_parquet(caminho_csv, destino_dir=destino)
    print(f"Parquet salvo em: {caminho_parquet}")

    print("➡️ Listando arquivos finais locais:")
    for root, dirs, files in os.walk(destino):
        for file in files:
            print(os.path.join(root, file))

    # 🔹 Nome do bucket (ajuste aqui se quiser outro)
    bucket_name = "dev-challenge2-files"
    s3_prefix = "b3/pregao"

    print(f"➡️ Fazendo upload para s3://{bucket_name}/{s3_prefix}")
    upload_to_s3(caminho_parquet, bucket_name=bucket_name, s3_prefix=s3_prefix)

if __name__ == "__main__":
    main()