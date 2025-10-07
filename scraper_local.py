import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

def baixar_csv_com_selenium(destino_dir="./dados_b3_local"):
    # 🔹 Configurações do Chrome
    os.makedirs(destino_dir, exist_ok=True)
    prefs = {"download.default_directory": os.path.abspath(destino_dir)}
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--headless=new")  # comente se quiser ver o navegador
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    try:
        # 1. Abre a página da B3
        driver.get("https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br")

        # 2. Espera o botão de download ficar disponível
        # ⚠️ Ajuste o XPATH conforme o botão real da página
        botao_download = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'Download')]"))
        )
        botao_download.click()

        # 3. Espera o arquivo aparecer na pasta
        arquivo_final = None
        for _ in range(60):  # tenta por até 60 segundos
            arquivos = [f for f in os.listdir(destino_dir) if f.endswith(".csv")]
            if arquivos:
                arquivo_final = os.path.join(destino_dir, arquivos[0])
                break
            time.sleep(1)

        if not arquivo_final:
            raise RuntimeError("❌ Download não foi concluído a tempo.")

        return arquivo_final

    finally:
        driver.quit()

def processar_csv_para_parquet(caminho_csv, destino_dir="./dados_b3_local"):
    os.makedirs(destino_dir, exist_ok=True)

    # pula a primeira linha (título), usa ; como separador
    df = pd.read_csv(caminho_csv, sep=";", encoding="latin1", skiprows=1, header=0)

    # remove colunas vazias, se existirem
    df = df.dropna(axis=1, how="all")

    nome_base = os.path.splitext(os.path.basename(caminho_csv))[0]
    caminho_parquet = os.path.join(destino_dir, f"{nome_base}.parquet")

    df.to_parquet(caminho_parquet, engine="pyarrow", index=False)
    return caminho_parquet

def upload_to_s3(caminho_arquivo, bucket_name, s3_prefix=""):
    """Faz upload de um arquivo local para um bucket S3."""
    s3 = boto3.client("s3")

    nome_arquivo = os.path.basename(caminho_arquivo)
    chave_s3 = f"{s3_prefix}/{nome_arquivo}" if s3_prefix else nome_arquivo

    s3.upload_file(caminho_arquivo, bucket_name, chave_s3)

    print(f"✅ Upload concluído: s3://{bucket_name}/{chave_s3}")
    return f"s3://{bucket_name}/{chave_s3}"