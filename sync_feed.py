import os
import sys
import pandas as pd
import requests
import csv
from io import BytesIO  # REQUIRED for handling binary/gzip data
from supabase import create_client, Client

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

    # --- 2. FETCH DATA ---
    print("Fetching GZIP feed...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Encoding': 'gzip'
        }
        # We access .content (raw bytes) instead of .text
        response = requests.get(feed_url, headers=headers)
        response.raise_for_status()
        print(f"Download complete. Size: {len(response.content)} bytes")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download feed: {e}")
        sys.exit(1)

    # --- 3. PARSE DATA ---
    print("Parsing CSV data...")
    try:
        # compression='gzip' decompresses the stream on the fly
        # quoting=csv.QUOTE_NONE prevents "Buffer Overflow" errors from unescaped quotes in descriptions
        df = pd.read_csv(
            BytesIO(response.content), 
            sep='\t', 
            compression='gzip',
            quoting=csv.QUOTE_NONE,
            on_bad_lines='skip',
            low_memory=False
        )
        
        # Verify columns were parsed correctly
        print(f"Columns detected ({len(df.columns)}): {df.columns.tolist()[:3]} ...")
        
        if len(df.columns) < 2:
            print("Error: Decompressed successfully but found < 2 columns. Separator might be wrong.")
            sys.exit(1)

    except Exception as e:
        print(f"Failed to parse CSV: {e}")
        sys.exit(1)

    # --- 4. CLEAN DATA ---
    print("Cleaning data...")
    
    # Supabase JSON cannot handle NaN. Replace with None.
    df = df.where(pd.notnull(df), None)

    # Clean Column Names (remove whitespace)
    df.columns = df.columns.str.strip()

    # OPTIONAL: Handle numeric fields that might be empty strings
    # If your DB expects a number but gets "", Supabase will error.
    # Convert specific numeric columns if needed here.

    # --- 5. UPLOAD TO SUPABASE ---
    records = df.to_dict(orient='records')
    total_records = len(records)
    print(f"Prepared {total_records} records.")

    if total_records == 0:
        print("No records found.")
        sys.exit(0)

    # Batching to avoid timeouts
    batch_size = 1000
    table_name = "Trivago Hotels"

    print(f"Starting upload to table: '{table_name}'")
    
    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        try:
            # Upsert using Primary Key (likely 'aw_product_id')
            supabase.table(table_name).upsert(batch).execute()
            
            # Print progress every 10 batches
            if (i // batch_size) % 10 == 0:
                print(f"Batch {i // batch_size + 1} uploaded...")
                
        except Exception as e:
            print(f"Error uploading batch at row {i}: {e}")
            sys.exit(1)

    print("Sync completed successfully.")

if __name__ == "__main__":
    sync_data()
