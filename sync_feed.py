import os
import sys
import pandas as pd
import requests
import csv
from io import StringIO
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
    print("Fetching CSV feed...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(feed_url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to download feed: {e}")
        sys.exit(1)

    # --- 3. PARSE DATA (ROBUST MODE) ---
    print("Parsing CSV data...")
    try:
        # csv.QUOTE_NONE tells pandas: "Ignore all quotation marks, just split by tab."
        # engine='python' is slower but prevents the "Buffer Overflow" C-engine error.
        df = pd.read_csv(
            StringIO(response.text), 
            sep='\t', 
            engine='python',
            quoting=csv.QUOTE_NONE,
            on_bad_lines='skip'
        )
        
        # Check if we actually got data
        if df.empty or len(df.columns) < 2:
            print(f"Warning: Data parsed but looks wrong. Columns found: {len(df.columns)}")
            print("First 500 chars of raw data for debugging:")
            print(response.text[:500])
            sys.exit(1)

    except Exception as e:
        print(f"Failed to parse CSV: {e}")
        # Print a snippet of the data to help debug if it fails again
        print("--- RAW DATA SNIPPET ---")
        print(response.text[:200]) 
        sys.exit(1)

    # --- 4. CLEAN DATA ---
    print("Cleaning data...")
    
    # 1. Replace NaN with None (JSON null)
    df = df.where(pd.notnull(df), None)

    # 2. Ensure column names match Supabase exactly (Strip whitespace)
    df.columns = df.columns.str.strip()

    # 3. Clean Zipcodes (Handle mixed types)
    if 'Travel:destination_zipcode' in df.columns:
        df['Travel:destination_zipcode'] = df['Travel:destination_zipcode'].astype(str).replace('nan', None)

    # --- 5. UPLOAD TO SUPABASE ---
    records = df.to_dict(orient='records')
    total_records = len(records)
    print(f"Prepared {total_records} records.")

    if total_records == 0:
        print("No records found.")
        sys.exit(0)

    # Batches of 500 are safer for large text fields
    batch_size = 500
    table_name = "Trivago Hotels" 

    print(f"Starting upload to table: {table_name}")
    
    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        try:
            # upsert requires a primary key to be defined in Supabase
            supabase.table(table_name).upsert(batch).execute()
            
            # Print progress every 5 batches to avoid cluttering logs
            if (i // batch_size) % 5 == 0:
                print(f"Processed row {i}...")
                
        except Exception as e:
            print(f"Error uploading batch at row {i}: {e}")
            # If one batch fails, we exit so you know about it
            sys.exit(1)

    print("Sync completed successfully.")

if __name__ == "__main__":
    sync_data()
