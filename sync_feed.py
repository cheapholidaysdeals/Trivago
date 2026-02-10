import os
import requests
import gzip
import io
import pandas as pd
from supabase import create_client, Client

# 1. Setup Connection
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
awin_url: str = os.environ.get("AWIN_FEED_URL")
supabase: Client = create_client(url, key)

TABLE_NAME = "Trivago Hotels" # <--- IMPORTANT: CHANGE THIS TO YOUR TABLE NAME
BATCH_SIZE = 1000

def sync_data():
    print("1. Downloading AWIN feed...")
    response = requests.get(awin_url)
    
    if response.status_code != 200:
        print(f"Error: Download failed with status code {response.status_code}")
        return

    print("2. Decompressing GZIP file...")
    try:
        # Wrap the downloaded content in a BytesIO buffer
        with io.BytesIO(response.content) as compressed_file:
            # Open it as a Gzip file in text mode ('rt')
            with gzip.open(compressed_file, 'rt', encoding='utf-8', errors='ignore') as f:
                
                print("3. Processing and Syncing...")
                # Read the CSV directly from the decompressed stream
                for chunk in pd.read_csv(f, chunksize=BATCH_SIZE, dtype=str):
                    
                    # Clean data: Replace NaN with None
                    chunk = chunk.where(pd.notnull(chunk), None)
                    data = chunk.to_dict(orient='records')
                    
                    try:
                        # Upsert to Supabase
                        # Ensure 'id' matches your table's primary key
                        supabase.table(TABLE_NAME).upsert(data, on_conflict='id').execute()
                        print(f"   Synced batch of {len(data)} rows.")
                    except Exception as e:
                        print(f"   Error syncing batch: {e}")

    except Exception as e:
        print(f"CRITICAL ERROR: Could not process the file. Details: {e}")

if __name__ == "__main__":
    sync_data()
