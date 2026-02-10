import os
import requests
import gzip
import io
import pandas as pd
import numpy as np
from supabase import create_client, Client

# --- CONFIGURATION ---
TARGET_TABLE_NAME = "Trivago Hotels" 
BATCH_SIZE = 1000

# LIST OF COLUMNS THAT MUST BE NUMBERS
# We will force-clean these to remove spaces and bad characters
NUMERIC_COLS = [
    "aw_product_id", 
    "merchant_product_id", 
    "search_price", 
    "merchant_id", 
    "data_feed_id", 
    "rating", 
    "Travel:hotel_stars", 
    "Travel:longitude", 
    "Travel:latitude", 
    "Travel:destination_zipcode"
]

# Setup Connection
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
awin_url: str = os.environ.get("AWIN_FEED_URL")
supabase: Client = create_client(url, key)

def clean_numeric_column(df, col_name):
    """Removes spaces and converts to number. Turns bad data into NULL."""
    if col_name in df.columns:
        # 1. Convert to string
        # 2. Remove spaces
        # 3. Force convert to number (errors become NaN)
        df[col_name] = pd.to_numeric(
            df[col_name].astype(str).str.replace(" ", ""), 
            errors='coerce'
        )
    return df

def sync_data():
    print(f"--- STARTING SYNC FOR: {TARGET_TABLE_NAME} ---")
    
    print("1. Downloading Data...")
    response = requests.get(awin_url)
    if response.status_code != 200:
        print(f"Error: Download failed code {response.status_code}")
        exit(1)

    print("2. Processing & Cleaning Data...")
    try:
        with io.BytesIO(response.content) as compressed_file:
            with gzip.open(compressed_file, 'rt', encoding='utf-8', errors='ignore') as f:
                
                # Read CSV as Strings first so we can clean them
                for chunk in pd.read_csv(f, chunksize=BATCH_SIZE, dtype=str):
                    
                    # A. MAP ID (Crucial for updates)
                    if 'aw_product_id' in chunk.columns:
                        chunk['id'] = chunk['aw_product_id']
                    
                    # B. CLEAN NUMERIC COLUMNS
                    for col in NUMERIC_COLS:
                        chunk = clean_numeric_column(chunk, col)

                    # C. HANDLE NULLS (NaN -> None)
                    chunk = chunk.replace({np.nan: None})
                    
                    # D. UPLOAD
                    data = chunk.to_dict(orient='records')
                    
                    print(f"   Upserting batch of {len(data)} rows...")
                    try:
                        supabase.table(TARGET_TABLE_NAME).upsert(data, on_conflict='id').execute()
                    except Exception as e:
                        print(f"   !!! BATCH ERROR: {e}")
                        # We print the error but continue to the next batch 
                        # so one bad row doesn't kill the whole job.

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        exit(1)

if __name__ == "__main__":
    sync_data()
