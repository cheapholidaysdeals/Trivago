import os
import requests
import gzip
import io
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from supabase import create_client, Client

# --- CONFIGURATION ---
TARGET_TABLE_NAME = "Trivago Hotels" 
BATCH_SIZE = 1000

# LIST OF COLUMNS THAT MUST BE NUMBERS
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
        df[col_name] = pd.to_numeric(
            df[col_name].astype(str).str.replace(" ", ""), 
            errors='coerce'
        )
    return df

def sync_data():
    print(f"--- STARTING SYNC FOR: {TARGET_TABLE_NAME} ---")
    
    # 1. Capture the start time of this specific run
    # We will use this to identify which rows were NOT updated later.
    sync_start_time = datetime.now(timezone.utc).isoformat()
    print(f"Timestamp for this run: {sync_start_time}")

    print("2. Downloading Data...")
    response = requests.get(awin_url)
    if response.status_code != 200:
        print(f"Error: Download failed code {response.status_code}")
        exit(1)

    print("3. Processing & Cleaning Data...")
    try:
        with io.BytesIO(response.content) as compressed_file:
            with gzip.open(compressed_file, 'rt', encoding='utf-8', errors='ignore') as f:
                
                # Read CSV in chunks
                for chunk in pd.read_csv(f, chunksize=BATCH_SIZE, dtype=str):
                    
                    # A. MAP ID & TIMESTAMP
                    if 'aw_product_id' in chunk.columns:
                        chunk['id'] = chunk['aw_product_id']
                    
                    # Add the 'last_synced_at' column with current time
                    chunk['last_synced_at'] = sync_start_time
                    
                    # B. CLEAN NUMERIC COLUMNS
                    for col in NUMERIC_COLS:
                        chunk = clean_numeric_column(chunk, col)

                    # C. HANDLE NULLS (NaN -> None)
                    chunk = chunk.replace({np.nan: None})
                    
                    # D. UPLOAD
                    data = chunk.to_dict(orient='records')
                    
                    print(f"   Upserting batch of {len(data)} rows...")
                    try:
                        # Upsert data. This updates existing rows with the new price 
                        # AND the new 'last_synced_at' timestamp.
                        supabase.table(TARGET_TABLE_NAME).upsert(data, on_conflict='id').execute()
                    except Exception as e:
                        print(f"   !!! BATCH ERROR: {e}")

        # 4. CLEANUP: Delete Old Data
        print("4. Removing expired deals (Ghost Data)...")
        
        # Logic: If 'last_synced_at' is NOT equal to our current 'sync_start_time',
        # it means the hotel was not in the file we just processed. Delete it.
        try:
            response = supabase.table(TARGET_TABLE_NAME)\
                .delete()\
                .neq("last_synced_at", sync_start_time)\
                .execute()
            
            # Note: Depending on your Supabase version/client, the response might vary,
            # but the .neq() filter is the key here.
            print("   Expired deals deleted successfully.")
            
        except Exception as e:
            print(f"   !!! CLEANUP ERROR: {e}")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        exit(1)

    print("--- SYNC COMPLETE ---")

if __name__ == "__main__":
    sync_data()
