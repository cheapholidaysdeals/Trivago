import os
import sys
import pandas as pd
import requests
from io import StringIO
from supabase import create_client, Client

def sync_data():
    # 1. Load Environment Variables
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    feed_url: str = os.environ.get("AWIN_FEED_URL")

    if not all([url, key, feed_url]):
        print("Error: Missing environment variables.")
        sys.exit(1)

    supabase: Client = create_client(url, key)

    # 2. Fetch and Parse CSV
    print("Fetching CSV feed...")
    try:
        response = requests.get(feed_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Failed to download feed: {e}")
        sys.exit(1)

    print("Parsing CSV data...")
    try:
        # Use StringIO to handle the raw string content as a file
        df = pd.read_csv(StringIO(response.text), sep=',') # Ensure sep matches your feed (comma or tab)
    except Exception as e:
        print(f"Failed to parse CSV: {e}")
        sys.exit(1)

    # 3. Clean Data for Supabase
    # Supabase (JSON) cannot handle NaN/Info; replace with None
    df = df.where(pd.notnull(df), None)

    # OPTIONAL: Drop columns in the CSV that don't exist in your DB Schema
    # to prevent "Column not found" errors.
    # df = df[['aw_product_id', 'product_name', ... keep only valid columns ...]]

    # 4. Upsert to Supabase in Batches
    # Batching is crucial for large feeds to avoid timeouts or payload limits
    batch_size = 1000
    records = df.to_dict(orient='records')
    total_records = len(records)
    print(f"Processing {total_records} records...")

    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        try:
            # Assuming 'aw_product_id' is a unique identifier you can use for upserting
            # If you want to just append, use .insert() instead of .upsert()
            data = supabase.table("Trivago Hotels").upsert(batch).execute()
            print(f"Batch {i // batch_size + 1} uploaded successfully.")
        except Exception as e:
            print(f"Error uploading batch starting at index {i}: {e}")
            # IMPORTANT: Exit with error code so GitHub Action knows it failed
            sys.exit(1)

    print("Sync completed successfully.")

if __name__ == "__main__":
    sync_data()
