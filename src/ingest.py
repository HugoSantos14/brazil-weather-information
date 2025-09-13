import os
import shutil
from pathlib import Path
import kagglehub

KAGGLE_DATASET = "gregoryoliveira/brazil-weather-information-by-inmet"
YEARS = range(2000, 2026) # Anos de 2000 a 2025
FILE_PATTERN = "weather_{}.csv"

def download_kaggle_dataset():
    print("[INFO] Downloading dataset from Kaggle... this may take a moment.")
    try:
        download_path = kagglehub.dataset_download(KAGGLE_DATASET)
        print("[INFO] Download complete.")
        return download_path
    except Exception as e:
        print(f"[ERROR] Failed to download dataset from Kaggle: {e}")
        exit()

def find_all_dataset_files(download_path):
    file_map = {}
    print(f"[INFO] Searching for files in '{download_path}'...")
    for root, _, files in os.walk(download_path):
        for file in files:
            if file.endswith(".csv"):
                file_map[file] = os.path.join(root, file)

    return file_map

def main():
    root_path = Path(__file__).parent.parent
    bronze_dir = root_path / "data" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    print(f"[SETUP] Program root: {root_path}")
    print(f"[SETUP] Bronze Layer: {bronze_dir}")

    target_files = [FILE_PATTERN.format(year) for year in YEARS]
    existing_files = {f.name for f in bronze_dir.glob("*.csv")}
    missing_files = [f for f in target_files if f not in existing_files]

    if not missing_files:
        print("\n[SUCCESS] All raw data files already exist in the bronze layer. No download needed.")
        return

    print(f"\n[INFO] Missing files: {len(missing_files)}. Starting ingestion...")

    download_path = download_kaggle_dataset()
    source_files_map = find_all_dataset_files(download_path)
    files_copied_count = 0

    for file_name in missing_files:
        source_path = source_files_map.get(file_name)

        if source_path:
            destination_path = bronze_dir / file_name
            print(f"  -> Copying '{file_name}' to bronze layer...")
            shutil.copy(source_path, destination_path)
            files_copied_count += 1
        else:
            print(f"  -> [WARNING] Could not find '{file_name}' in the downloaded dataset.")

    print(f"\n[INFO] Ingestion complete! Copied {files_copied_count} new file(s) to {bronze_dir}.")

if __name__ == "__main__":
    main()