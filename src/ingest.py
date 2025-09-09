import os
import pandas as pd
import kagglehub

# Dataset do Kaggle
KAGGLE_DATASET = "gregoryoliveira/brazil-weather-information-by-inmet"
KAGGLE_FILE = "weather_sum_all.csv"

# Diretório base (raiz do projeto)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Diretórios do pipeline
DATA_DIR = os.path.join(BASE_DIR, "data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")
BRONZE_FILE = os.path.join(BRONZE_DIR, KAGGLE_FILE)

def ensure_directories():
    for layer in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        os.makedirs(layer, exist_ok=True)

def download_from_kaggle():
    print("[INFO] Downloading dataset from Kaggle...")
    path = kagglehub.dataset_download(KAGGLE_DATASET)

    for root, _, files in os.walk(path):
        if KAGGLE_FILE in files:
            return os.path.join(root, KAGGLE_FILE)

    raise FileNotFoundError(f"{KAGGLE_FILE} not found in {path}")

def main():
    ensure_directories()

    if not os.path.exists(BRONZE_FILE):
        print("[INFO] File not found in data/bronze. Starting ingestion...")
        src_file = download_from_kaggle()
        os.system(f"cp '{src_file}' '{BRONZE_FILE}'")
        print(f"[INFO] Ingestion complete! File saved to {BRONZE_FILE}.")
    else:
        print("[INFO] File already exists in data. Skipping download.")

    df = pd.read_csv(BRONZE_FILE)
    print(df.head())

if __name__ == "__main__":
    main()