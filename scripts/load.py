import os
import pandas as pd
from supabase import create_client
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BASE_DIR = Path(__file__).resolve().parents[1]
STAGED_DIR = BASE_DIR / "data" / "staged"

TABLE_NAME = "nasa_apod"

# Supabase table columns
TABLE_COLUMNS = ["date", "title", "explanation", "media_type", "image_url"]

def load_to_supabase(csv_file):
    """Load staged CSV into Supabase."""
    df = pd.read_csv(csv_file)
    df = df[TABLE_COLUMNS]
    records = df.to_dict(orient="records")

    response = supabase.table(TABLE_NAME).insert(records).execute()

    # Check insert success
    if response.data is not None:
        print(f"Loaded {len(records)} records into Supabase table '{TABLE_NAME}'.")
    else:
        print(f"Error loading data: {response}")

if __name__ == "__main__":
    csv_files = sorted(STAGED_DIR.glob("nasa_apod_staged_*.csv"), reverse=True)
    if not csv_files:
        raise FileNotFoundError("No staged CSV files found. Run transform.py first.")
    latest_csv = csv_files[0]
    load_to_supabase(latest_csv)
