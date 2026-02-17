import os
import sys
import pandas as pd
import requests
import csv
from io import BytesIO
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
        response = requests.get(feed_url, headers=headers)
        response.raise_for_status()
        print(f"Download complete. Size: {len(response.content)} bytes")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download feed: {e}")
        sys.exit(1)

    # --- 3. PARSE DATA ---
    print("Parsing CSV data...")
    try:
        # FIX: sep=',' because your error log showed comma-separated headers
        # engine='python' is slower but more robust against "Buffer Overflow"
        # on_bad_lines='skip' ensures the script doesn't crash if 1 row out of 200k is bad
        df = pd.read_csv(
            BytesIO(response.content), 
            sep=',', 
            compression='gzip',
            engine='python', 
            on_bad_lines='skip' 
        )
        
        # Verify columns
        print(f"Columns detected ({len(df.columns)}): {df.columns.tolist()[:3]} ...")
        
        if len(df.columns) < 2:
            print("Error: Parsed < 2 columns. Please check if the separator is actually '|' or ';'.")
            sys.exit(1)

    except Exception as e:
        print(f"Failed to parse CSV: {e}")
        sys.exit(1)

    # --- 4. CLEAN DATA ---
    print("Cleaning data...")
    
    # Replace NaN with None (JSON null)
    df = df.where(pd.notnull(df), None)

    # Clean Column Names
    df.columns = df.columns.str.strip()

    # Fix Zipcodes (force to string)
    if 'Travel:destination_zipcode' in df.columns:
        df['Travel:destination_zipcode'] = df['Travel:destination_zipcode'].astype(str).replace('nan', None)

    # --- 5. UPLOAD TO SUPABASE ---
    records = df.to_dict(orient='records')
    total_records = len(records)
    print(f"Prepared {total_records} records.")

    if total_records == 0:
        print("No records found.")
        sys.exit(0)

    # Batching (Use 1000 for speed, reduce to 500 if you get timeouts)
    batch_size = 1000
    table_name = "Trivago Hotels"

    print(f"Starting upload to table: '{table_name}'")
    
    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table(table_name).upsert(batch).execute()
            
            # Log progress
            if (i // batch_size) % 10 == 0:
                print(f"Batch {i // batch_size + 1} uploaded...")
                
        except Exception as e:
            print(f"Error uploading batch at row {i}: {e}")
            # If a batch fails, we exit so GitHub Action marks as failed
            sys.exit(1)

    print("Sync completed successfully.")

if __name__ == "__main__":
    sync_data()
