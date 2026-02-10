import os
import requests
import gzip
import io
import pandas as pd
from supabase import create_client, Client

# --- CONFIGURATION ---
# IMPORTANT: Since your table name has a space, we keep it as is.
TABLE_NAME = "Trivago Hotels"  
BATCH_SIZE = 1000

# Setup Connection
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
awin_url: str = os.environ.get("AWIN_FEED_URL")
supabase: Client = create_client(url, key)

def sync_data():
    print("1. Downloading AWIN feed...")
    response = requests.get(awin_url)
    
    if response.status_code != 200:
        print(f"Error: Download failed with code {response.status_code}")
        return

    print("2. Processing GZIP file...")
    try:
        with io.BytesIO(response.content) as compressed_file:
            with gzip.open(compressed_file, 'rt', encoding='utf-8', errors='ignore') as f:
                
                # Read CSV
                # We read as strings (dtype=str) to prevent data loss (like leading zeros)
                for chunk in pd.read_csv(f, chunksize=BATCH_SIZE, dtype=str):
                    
                    # --- CRITICAL STEP: MAP ID ---
                    # We copy 'aw_product_id' into 'id' so Supabase knows which row is which.
                    if 'aw_product_id' in chunk.columns:
                        chunk['id'] = chunk['aw_product_id']
                    
                    # Clean data: Replace NaN (empty) with None (NULL for SQL)
                    chunk = chunk.where(pd.notnull(chunk), None)
                    
                    # Convert to dictionary
                    data = chunk.to_dict(orient='records')
                    
                    try:
                        # Upsert to Supabase
                        # on_conflict='id' uses the ID we just mapped to update existing rows
                        supabase.table(TABLE_NAME).upsert(data, on_conflict='id').execute()
                        print(f"   Synced batch of {len(data)} rows.")
                    except Exception as e:
                        print(f"   Error syncing batch: {e}")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    sync_data()
