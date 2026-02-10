import os
import requests
import zipfile
import io
import pandas as pd
from supabase import create_client, Client

# 1. Setup Connection
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
awin_url: str = os.environ.get("AWIN_FEED_URL")
supabase: Client = create_client(url, key)

TABLE_NAME = "Trivago Hotels" # <--- REPLACE THIS WITH YOUR TABLE NAME
BATCH_SIZE = 1000 # Upsert in batches to prevent timeouts

def sync_data():
    print("1. Downloading AWIN feed...")
    response = requests.get(awin_url)
    response.raise_for_status()

    # 2. Unzip in memory
    print("2. Unzipping...")
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Assume the first file in the zip is the data feed
        csv_filename = z.namelist()[0]
        print(f"   Found file: {csv_filename}")
        
        with z.open(csv_filename) as f:
            # 3. Process CSV in chunks using Pandas
            print("3. Processing and Syncing...")
            
            # Adjust 'sep' if your CSV uses semicolons or tabs
            # dtype=str ensures phone numbers/IDs don't lose leading zeros
            for chunk in pd.read_csv(f, chunksize=BATCH_SIZE, dtype=str):
                
                # Clean data: Replace NaN with None (NULL in SQL)
                chunk = chunk.where(pd.notnull(chunk), None)
                
                # Convert to list of dicts
                data = chunk.to_dict(orient='records')
                
                try:
                    # 4. Upsert to Supabase
                    # on_conflict='id' ensures we update existing rows instead of failing
                    # Replace 'id' with your table's Primary Key column name
                    supabase.table(TABLE_NAME).upsert(data, on_conflict='id').execute()
                    print(f"   Synced batch of {len(data)} rows.")
                except Exception as e:
                    print(f"   Error syncing batch: {e}")

if __name__ == "__main__":
    sync_data()
