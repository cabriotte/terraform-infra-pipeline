import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def baixar_csv_com_selenium(destino_dir="/tmp/dados_b3"):
    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    os.makedirs(destino_dir, exist_ok=True)

    options = Options()
    options.binary_location = os.getenv("CHROME_BIN", "/usr/bin/google-chrome")
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    prefs = {"download.default_directory": os.path.abspath(destino_dir)}
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)
    driver.get(url)

    time.sleep(5)  # aguarda carregamento da página

    # 🔹 Item 1: log antes e depois do clique
    print("Tentando localizar botão de download...")
    botao = driver.find_element(By.XPATH, "//a[contains(text(), 'Download')]")
    print("Botão localizado, clicando...")
    botao.click()

    # 🔹 Item 2: log do diretório de download
    time.sleep(10)  # tempo extra para garantir download
    print("Arquivos no diretório de download:")
    print(os.listdir(destino_dir))

    driver.quit()

    for file in os.listdir(destino_dir):
        if file.endswith(".csv"):
            caminho_csv = os.path.join(destino_dir, file)
            print(f"CSV baixado: {caminho_csv}")
            return caminho_csv

    raise FileNotFoundError("CSV não foi baixado.")

def processar_csv_para_parquet(caminho_csv, destino_base="/tmp/dados_b3"):
    df = pd.read_csv(
        caminho_csv,
        sep=";",
        encoding="latin1",
        skiprows=1,
        names=["Código", "Ação", "Tipo", "Qtde Teórica", "Part (%)"],
        usecols=["Código", "Ação", "Tipo", "Qtde Teórica", "Part (%)"],
        on_bad_lines="skip"
    )

    df = df[df["Código"].notna() & ~df["Código"].str.contains("Quantidade|Redutor", na=False)]
    df["data_extracao"] = datetime.today().strftime("%Y-%m-%d")

    destino_particao = os.path.join(destino_base, f"data_extracao={df['data_extracao'].iloc[0]}")
    os.makedirs(destino_particao, exist_ok=True)
    caminho_parquet = os.path.join(destino_particao, "pregao.parquet")
    pq.write_table(pa.Table.from_pandas(df), caminho_parquet)

    print(f"Parquet salvo em: {caminho_parquet}")
    return destino_particao

def upload_to_s3(local_path, bucket_name, s3_prefix):
    s3 = boto3.client('s3', region_name='sa-east-1')

    for root, dirs, files in os.walk(local_path):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, local_path)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            s3.upload_file(full_path, bucket_name, s3_key)
            print(f"Upload concluído: s3://{bucket_name}/{s3_key}")

def run_pipeline():
    print("Iniciando pipeline B3...")
    bucket_name = os.getenv("BUCKET_NAME", "dev-challenge2-files")
    caminho_csv = baixar_csv_com_selenium()
    caminho_parquet = processar_csv_para_parquet(caminho_csv)
    upload_to_s3(caminho_parquet, bucket_name=bucket_name, s3_prefix="b3/pregao")
    print("Pipeline finalizado com sucesso.")

if __name__ == "__main__":
    run_pipeline()
