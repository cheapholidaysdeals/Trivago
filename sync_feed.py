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
    """Cleans the dataframe to ensure compatibility with Supabase types."""
    
    # 1. Handle Infinite and NaN values
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # 2. Force Zipcodes to strings/numeric safely
    if 'Travel:destination_zipcode' in df.columns:
        # Force to numeric, turn errors (letters) into NaN
        df['Travel:destination_zipcode'] = pd.to_numeric(df['Travel:destination_zipcode'], errors='coerce')

    # 3. Replace all NaNs with None (SQL NULL)
    df = df.where(pd.notnull(df), None)
    
    # 4. Strip whitespace from column headers
    df.columns = df.columns.str.strip()
    
    return df

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
                print(f"Response: {response.text[:200]}") # Print first 200 chars of error
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
                
                # DEBUG: Print first row keys to verify column mapping matches DB
                if chunk_idx == 0:
                    print(f"Detected Columns: {list(df.columns)}")

                df_clean = clean_dataframe(df)
                records = df_clean.to_dict(orient='records')
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i + BATCH_SIZE]
                    try:
                        # Perform Upsert
                        data = supabase.table(TABLE_NAME).upsert(
                            batch, 
                            on_conflict=UNIQUE_KEY
                        ).execute()
                        
                        # Check if Supabase returned data (successful write)
                        # Note: Depending on library version, data might be in data.data or just data
                        print(f"   > Batch {i//BATCH_SIZE + 1}: Success.")
                        
                    except Exception as e:
                        print(f"   > ERROR uploading batch {i}: {e}")
                        has_errors = True
                
                total_uploaded += len(records)
                print(f"Total rows processed: {total_uploaded}")

            if total_uploaded == 0:
                print("WARNING: Script finished but 0 rows were found in the CSV.")
            
            if has_errors:
                 print("Script finished with some batch errors. Check logs above.")
                 # exit with error so GitHub knows something went wrong
                 sys.exit(1)

    except Exception as e:
        print(f"CRITICAL ERROR during processing: {e}")
        sys.exit(1)

    print("--- SYNC COMPLETE SUCCESS ---")

if __name__ == "__main__":
    sync_data()
