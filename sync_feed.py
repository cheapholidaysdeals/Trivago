import os
import requests
import gzip
import io
import pandas as pd
from supabase import create_client, Client

# --- CONFIGURATION ---
# Try these variations if the first doesn't work: "Trivago Hotels", "trivago_hotels", "trivago hotels"
TARGET_TABLE_NAME = "Trivago Hotels" 
BATCH_SIZE = 1000

# Setup Connection
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
awin_url: str = os.environ.get("AWIN_FEED_URL")
supabase: Client = create_client(url, key)

def sync_data():
    print("--- STEP 1: CHECKING TABLE NAME ---")
    # This is a 'hack' to force an error if the table name is wrong
    try:
        # Try to read 1 row just to see if the table exists
        supabase.table(TARGET_TABLE_NAME).select("id").limit(1).execute()
        print(f"SUCCESS: Table '{TARGET_TABLE_NAME}' found!")
    except Exception as e:
        print(f"CRITICAL ERROR: Table '{TARGET_TABLE_NAME}' does not exist in Supabase.")
        print("Detailed Error:", e)
        print("TIP: Check your Supabase Dashboard. Is the table name actually 'trivago_hotels' (all lowercase)?")
        # Stop the script immediately so you see a RED X
        exit(1) 

    print("\n--- STEP 2: DOWNLOADING DATA ---")
    response = requests.get(awin_url)
    if response.status_code != 200:
        print(f"Error: Download failed with code {response.status_code}")
        exit(1)

    print("--- STEP 3: PROCESSING & UPLOADING ---")
    try:
        with io.BytesIO(response.content) as compressed_file:
            with gzip.open(compressed_file, 'rt', encoding='utf-8', errors='ignore') as f:
                
                for chunk in pd.read_csv(f, chunksize=BATCH_SIZE, dtype=str):
                    
                    # Map ID column
                    if 'aw_product_id' in chunk.columns:
                        chunk['id'] = chunk['aw_product_id']
                    
                    # Clean data
                    chunk = chunk.where(pd.notnull(chunk), None)
                    data = chunk.to_dict(orient='records')
                    
                    # UPSERT - NO TRY/EXCEPT BLOCK
                    # If this fails, the script will crash and tell us exactly why
                    print(f"Attempting to upload batch of {len(data)} rows...")
                    supabase.table(TARGET_TABLE_NAME).upsert(data, on_conflict='id').execute()
                    print("   Success!")

    except Exception as e:
        print("\n!!!!!!!! UPLOAD FAILED !!!!!!!!")
        print("Here is exactly why Supabase rejected the data:")
        print(e)
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        exit(1)

if __name__ == "__main__":
    sync_data()
