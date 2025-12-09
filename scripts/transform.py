import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import requests

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
STAGED_DIR = BASE_DIR / "data" / "staged"
IMAGES_DIR = BASE_DIR / "data" / "images"

STAGED_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

def download_image(url, save_dir, filename=None):
    """Download an image and return local path."""
    if url is None:
        return None
    try:
        response = requests.get(url)
        response.raise_for_status()
        if filename is None:
            filename = url.split("/")[-1].split("?")[0]
        filepath = save_dir / filename
        with open(filepath, "wb") as f:
            f.write(response.content)
        return str(filepath)
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def transform_nasa_apod(json_file):
    """Transform JSON to CSV and download images."""
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame([data])

    # Keep only columns needed for Supabase
    df = df[["date", "title", "explanation", "media_type", "url"]].copy()
    df.rename(columns={"url": "image_url"}, inplace=True)

    # Download images and store local path
    df["image_local_path"] = df["image_url"].apply(lambda x: download_image(x, IMAGES_DIR))

    # Save staged CSV
    filename = STAGED_DIR / f"nasa_apod_staged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"Transformed data saved to: {filename}")
    return df

if __name__ == "__main__":
    json_files = sorted(RAW_DIR.glob("nasa_apod_*.json"), reverse=True)
    if not json_files:
        raise FileNotFoundError("No raw JSON files found. Run extract.py first.")
    latest_json = json_files[0]
    transform_nasa_apod(latest_json)
