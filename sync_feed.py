import os
import sys
import pandas as pd
import requests
import numpy as np
from datetime import datetime, timezone
from io import BytesIO
from supabase import create_client, Client

# --- CONFIGURATION ---
BATCH_SIZE = 1000       
CHUNK_SIZE = 5000       
TABLE_NAME = "Trivago Hotels"
UNIQUE_KEY = "aw_product_id" 

def clean_dataframe(df, sync_time):
    """
    Cleans the dataframe and adds the current sync timestamp.
    """
    # 1. Handle Zipcodes
    if 'Travel:destination_zipcode' in df.columns:
        df['Travel:destination_zipcode'] = pd.to_numeric(df['Travel:destination_zipcode'], errors='coerce')

    # 2. Replace Infinity
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # 3. Add the Sync Timestamp to every row in this batch
    df['last_synced_at'] = sync_time

    # 4. Cast to Object and Replace NaN with None for JSON compliance
    df_clean = df.astype(object).where(pd.notnull(df), None)
    
    return df_clean

def sync_data():
    # Capture the exact start time of this run in UTC
    current_run_time = datetime.now(timezone.utc).isoformat()
    
    print(f"--- STARTING SYNC AT {current_run_time} ---")
    
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    feed_url: str = os.environ.get("AWIN_FEED_URL")

    if not all([url, key, feed_url]):
        print("CRITICAL ERROR: Missing environment variables.")
        sys.exit(1)

    try:
        supabase: Client = create_client(url, key)
    except Exception as e:
        print(f"CRITICAL ERROR initializing Supabase: {e}")
        sys.exit(1)

    print(f"Fetching feed from: {feed_url}")
    
    try:
        # Added a 60s timeout to the request to prevent hanging
        with requests.get(feed_url, headers={'Accept-Encoding': 'gzip'}, stream=True, timeout=60) as response:
            if response.status_code != 200:
                print(f"CRITICAL ERROR: Feed URL returned status code {response.status_code}")
                sys.exit(1)

            print("Download connection established. Processing chunks...")

            reader = pd.read_csv(
                response.raw, 
                sep=',',
                compression='gzip',
                chunksize=CHUNK_SIZE,
                low_memory=False,
                on_bad_lines='skip' 
            )

            total_uploaded = 0
            has_errors = False
            
            for chunk_idx, df in enumerate(reader):
                # Clean and tag with timestamp
                df_clean = clean_dataframe(df, current_run_time)
                records = df_clean.to_dict(orient='records')
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i + BATCH_SIZE]
                    try:
                        supabase.table(TABLE_NAME).upsert(
                            batch, 
                            on_conflict=UNIQUE_KEY
                        ).execute()
                    except Exception as e:
                        print(f"   > ERROR uploading batch in chunk {chunk_idx}: {e}")
                        has_errors = True
                
                total_uploaded += len(records)
                if chunk_idx % 10 == 0: # Reduce log noise
                    print(f"Rows processed so far: {total_uploaded}")

            print(f"--- FINISHED UPLOADING {total_uploaded} ROWS ---")

            # --- THE PURGE STEP ---
            # Anything that wasn't updated during this specific run is now obsolete
            print("Cleaning up old 'zombie' records...")
            try:
                # We delete rows where last_synced_at is older than our start time
                purge_query = supabase.table(TABLE_NAME).delete().lt("last_synced_at", current_run_time).execute()
                print("Purge complete. Database now matches the feed exactly.")
            except Exception as e:
                print(f"Warning: Purge step failed: {e}")

            if has_errors:
                 print("Script finished with some batch errors. Check logs.")
                 sys.exit(1)

    except Exception as e:
        print(f"CRITICAL ERROR during processing: {e}")
        sys.exit(1)

    print("--- SYNC COMPLETE SUCCESS ---")

if __name__ == "__main__":
    sync_data()
