import json
from pathlib import Path
from datetime import datetime
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
NASA_API_KEY = os.getenv("NASA_API_KEY")

# Directories
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

def extract_nasa_apod(start_date=None, end_date=None):
    """
    Extract NASA APOD data.
    """
    url = "https://api.nasa.gov/planetary/apod"
    params = {"api_key": NASA_API_KEY}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    filename = RAW_DIR / f"nasa_apod_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(data, indent=2))
    print(f"Extracted NASA data saved to: {filename}")
    return data

if __name__ == "__main__":
    extract_nasa_apod()
