import os
import sys
import pandas as pd
import requests
from io import StringIO
from supabase import create_client, Client

def sync_data():
    # --- 1. SETUP ---
    # Load environment variables
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    feed_url: str = os.environ.get("AWIN_FEED_URL")

    # Validate env vars
    if not all([url, key, feed_url]):
        print("Error: Missing one or more environment variables (SUPABASE_URL, SUPABASE_KEY, AWIN_FEED_URL).")
        sys.exit(1)

    # Initialize Supabase
    try:
        supabase: Client = create_client(url, key)
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
        sys.exit(1)

    # --- 2. FETCH DATA ---
    print("Fetching CSV feed...")
    try:
        # standard headers to prevent 403 forbidden errors from some servers
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(feed_url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to download feed: {e}")
        sys.exit(1)

    # --- 3. PARSE DATA ---
    print("Parsing CSV data...")
    try:
        # FIX: explicitly using tab separator ('\t')
        # on_bad_lines='skip' prevents the script from crashing if one row is malformed
        df = pd.read_csv(
            StringIO(response.text), 
            sep='\t', 
            on_bad_lines='skip',
            low_memory=False
        )
        
        # Verify we actually got columns
        if len(df.columns) < 2:
            print("Warning: Parsed fewer than 2 columns. The separator might be wrong (try '|' if '\t' fails).")
            print(f"Columns found: {df.columns.tolist()}")

    except Exception as e:
        print(f"Failed to parse CSV: {e}")
        sys.exit(1)

    # --- 4. CLEAN DATA ---
    print("Cleaning data...")
    
    # Supabase (JSON) cannot handle NaN (Not a Number) values. 
    # We must convert them to None (which becomes null in JSON).
    df = df.where(pd.notnull(df), None)

    # OPTIONAL: Force Zipcode to string if your DB expects text
    # (Comment this out if your DB column is strictly numeric and you want it to fail on letters)
    if 'Travel:destination_zipcode' in df.columns:
         df['Travel:destination_zipcode'] = df['Travel:destination_zipcode'].astype(str)

    # --- 5. UPLOAD TO SUPABASE ---
    records = df.to_dict(orient='records')
    total_records = len(records)
    print(f"Prepared {total_records} records for upload.")

    if total_records == 0:
        print("No records found to upload.")
        sys.exit(0)

    # Batching to avoid timeouts or payload limits
    batch_size = 1000
    table_name = "Trivago Hotels" # Make sure this matches your Supabase table name exactly

    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        try:
            # upsert checks for primary key conflicts and updates them
            response = supabase.table(table_name).upsert(batch).execute()
            print(f"Batch {i // batch_size + 1}: Uploaded rows {i} to {i + len(batch)}")
        except Exception as e:
            print(f"Error uploading batch {i // batch_size + 1}: {e}")
            # We exit on error so GitHub Actions marks the job as Failed
            sys.exit(1)

    print("Sync completed successfully.")

if __name__ == "__main__":
    sync_data()
