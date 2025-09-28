import os
import sys

# 🔹 Garante que a raiz do projeto esteja no PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aws_scrap_codebuild import (
    baixar_csv_com_selenium,
    processar_csv_para_parquet,
    upload_to_s3,
)

def main():
    destino = "./dados_b3_local"
    os.makedirs(destino, exist_ok=True)

    print("➡️ Iniciando download do CSV via Selenium...")
    caminho_csv = baixar_csv_com_selenium(destino_dir=destino)
    print("✅ CSV baixado em:", caminho_csv)

    print("➡️ Convertendo CSV para Parquet...")
    caminho_parquet = processar_csv_para_parquet(caminho_csv, destino_dir=destino)
    print("✅ Parquet gerado em:", caminho_parquet)

    print("➡️ Fazendo upload para o S3...")
    bucket_name = "dev-challenge2-files"   # ajuste para o seu bucket
    s3_prefix = "b3/pregao"                # ajuste se quiser organizar em pastas
    caminho_s3 = upload_to_s3(caminho_parquet, bucket_name, s3_prefix)
    print("📤 Arquivo disponível em:", caminho_s3)

if __name__ == "__main__":
    main()