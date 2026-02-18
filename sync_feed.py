import os
import sys
import pandas as pd
import requests
import numpy as np
from io import BytesIO
from supabase import create_client, Client

# --- CONFIGURATION ---
BATCH_SIZE = 1000       
CHUNK_SIZE = 5000       
TABLE_NAME = "Trivago Hotels"
UNIQUE_KEY = "aw_product_id" 

def clean_dataframe(df):
    """
    Aggressively cleans the dataframe to ensure JSON compliance.
    """
    
    # 1. Handle Zipcodes: Force errors to NaN
    # (If the CSV has "SW1A" but DB expects Numeric, this prevents a crash by making it Null)
    if 'Travel:destination_zipcode' in df.columns:
        df['Travel:destination_zipcode'] = pd.to_numeric(df['Travel:destination_zipcode'], errors='coerce')

    # 2. Replace Infinity with NaN
    # JSON cannot handle 'Infinity', so we turn it into 'NaN' first
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # 3. THE FIX: Cast to Object and Replace NaN with None
    # Pandas Float columns usually refuse to hold 'None'. 
    # By casting to 'object' first, we allow 'None' (which sends as SQL NULL) to exist.
    df_clean = df.astype(object).where(pd.notnull(df), None)
    
    return df_clean

def sync_data():
    print("--- STARTING SYNC ---")
    
    # --- 1. SETUP ---
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

    # --- 2. DOWNLOAD ---
    print(f"Fetching feed from: {feed_url}")
    
    try:
        with requests.get(feed_url, headers={'Accept-Encoding': 'gzip'}, stream=True) as response:
            if response.status_code != 200:
                print(f"CRITICAL ERROR: Feed URL returned status code {response.status_code}")
                sys.exit(1)

            print("Download connection established. Processing chunks...")

            # --- 3. PROCESS & UPLOAD ---
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
                print(f"--- Processing Chunk {chunk_idx + 1} (Rows: {len(df)}) ---")
                
                # Clean the data
                df_clean = clean_dataframe(df)
                
                # Convert to records
                records = df_clean.to_dict(orient='records')
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i + BATCH_SIZE]
                    try:
                        # Perform Upsert
                        supabase.table(TABLE_NAME).upsert(
                            batch, 
                            on_conflict=UNIQUE_KEY
                        ).execute()
                        
                        # Log success sparingly to save log space
                        if i == 0:
                            print(f"   > Batch {i//BATCH_SIZE + 1}: Success (Sample).")
                        
                    except Exception as e:
                        print(f"   > ERROR uploading batch {i}: {e}")
                        has_errors = True
                
                total_uploaded += len(records)
                print(f"Total rows processed: {total_uploaded}")

            if has_errors:
                 print("Script finished with some batch errors. Check logs above.")
                 sys.exit(1)

    except Exception as e:
        print(f"CRITICAL ERROR during processing: {e}")
        sys.exit(1)

    print("--- SYNC COMPLETE SUCCESS ---")

if __name__ == "__main__":
    sync_data()
