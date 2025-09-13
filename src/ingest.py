import os
import pandas as pd
from pathlib import Path
import kagglehub
import shutil

KAGGLE_DATASET = "gregoryoliveira/brazil-weather-information-by-inmet"
KAGGLE_FILE = "weather_sum_all.csv"

def download_from_kaggle():
    print("[INFO] Downloading dataset from Kaggle...")
    path = kagglehub.dataset_download(KAGGLE_DATASET)

    for root, _, files in os.walk(path):
        if KAGGLE_FILE in files:
            return os.path.join(root, KAGGLE_FILE)

    raise FileNotFoundError(f"{KAGGLE_FILE} not found in {path}")

def main():
    ROOT_PATH = Path(__file__).parent.parent
    BRONZE_DIR = ROOT_PATH / "data" / "bronze"

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    BRONZE_FILE = BRONZE_DIR / KAGGLE_FILE

    if not BRONZE_FILE.exists():
        print(f"[INFO] File not found in {BRONZE_DIR}. Starting ingestion...")
        src_file = download_from_kaggle()

        shutil.copy(src_file, BRONZE_FILE)

        print(f"[INFO] Ingestion complete! File saved to {BRONZE_FILE}.")
    else:
        print("[INFO] File already exists in data. Skipping download.")

    print("\n[INFO] Reading file...")
    df = pd.read_csv(BRONZE_FILE)
    print(df.head())

if __name__ == "__main__":
    main()