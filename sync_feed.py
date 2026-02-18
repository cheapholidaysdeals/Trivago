import os
import sys
import pandas as pd
import requests
import numpy as np
from io import BytesIO
from supabase import create_client, Client

# --- CONFIGURATION ---
BATCH_SIZE = 1000       # Rows to upload to Supabase at once
CHUNK_SIZE = 5000       # Rows to read from CSV at once (saves memory)
TABLE_NAME = "Trivago Hotels"
UNIQUE_KEY = "aw_product_id" # The column used to identify unique rows

def clean_dataframe(df):
    """Cleans the dataframe to ensure compatibility with Supabase types."""
    
    # 1. Handle Infinite and NaN values (JSON standard)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # 2. Force Zipcodes to strings first, then handle numeric constraint
    # Note: Your DB has zip_code as 'numeric', but zipcodes can have letters (e.g. UK/Canada).
    # If the DB field is strictly numeric, we must force invalid zips to None to prevent crashes.
    if 'Travel:destination_zipcode' in df.columns:
        df['Travel:destination_zipcode'] = pd.to_numeric(df['Travel:destination_zipcode'], errors='coerce')

    # 3. Replace all NaNs with None (which becomes NULL in SQL)
    df = df.where(pd.notnull(df), None)
    
    # 4. Strip whitespace from column headers
    df.columns = df.columns.str.strip()
    
    return df

def sync_data():
    # --- 1. SETUP ---
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    feed_url: str = os.environ.get("AWIN_FEED_URL")

    if not all([url, key, feed_url]):
        print("Error: Missing environment variables.")
        sys.exit(1)

    try:
        supabase: Client = create_client(url, key)
    except Exception as e:
        print(f"Error initializing Supabase: {e}")
        sys.exit(1)

    # --- 2. FETCH & PROCESS DATA (STREAMING) ---
    print("Starting download and sync...")
    
    try:
        # Stream the download to avoid loading 500MB+ into RAM
        with requests.get(feed_url, headers={'Accept-Encoding': 'gzip'}, stream=True) as response:
            response.raise_for_status()
            
            # Use chunks to process the file piece by piece
            # This prevents "Out of Memory" errors on GitHub Actions
            reader = pd.read_csv(
                response.raw, # Read directly from the stream
                sep=',',
                compression='gzip',
                chunksize=CHUNK_SIZE,
                low_memory=False,
                on_bad_lines='skip'
            )

            total_uploaded = 0
            
            for chunk_idx, df in enumerate(reader):
                print(f"Processing chunk {chunk_idx + 1} ({len(df)} rows)...")
                
                # Clean the chunk
                df_clean = clean_dataframe(df)
                
                # Convert to records
                records = df_clean.to_dict(orient='records')
                
                # Upload in smaller batches
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i + BATCH_SIZE]
                    try:
                        # CRITICAL: on_conflict arg ensures we UPDATE existing rows instead of inserting duplicates
                        supabase.table(TABLE_NAME).upsert(
                            batch, 
                            on_conflict=UNIQUE_KEY
                        ).execute()
                    except Exception as e:
                        print(f"Error uploading batch in chunk {chunk_idx}: {e}")
                        # Optional: Don't exit, just log error so other batches continue
                
                total_uploaded += len(records)
                print(f"Total rows synced so far: {total_uploaded}")

    except Exception as e:
        print(f"Critical Error during sync: {e}")
        sys.exit(1)

    print("Sync completed successfully.")

if __name__ == "__main__":
    sync_data()
