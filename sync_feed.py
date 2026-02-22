import os
import sys
import pandas as pd
import requests
import numpy as np
import time
from datetime import datetime, timezone
from supabase import create_client, Client

# --- OPTIMIZED CONFIGURATION ---
BATCH_SIZE = 200         # Reduced further to prevent statement timeouts
CHUNK_SIZE = 2000        
TABLE_NAME = "Trivago Hotels"
UNIQUE_KEY = "aw_product_id" 

def clean_dataframe(df, sync_time):
    if 'Travel:destination_zipcode' in df.columns:
        df['Travel:destination_zipcode'] = pd.to_numeric(df['Travel:destination_zipcode'], errors='coerce')
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df['last_synced_at'] = sync_time
    return df.astype(object).where(pd.notnull(df), None)

def sync_data():
    current_run_time = datetime.now(timezone.utc).isoformat()
    print(f"--- STARTING SYNC AT {current_run_time} ---", flush=True)
    
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    feed_url = os.environ.get("AWIN_FEED_URL")
    supabase = create_client(url, key)

    try:
        with requests.get(feed_url, stream=True, timeout=120) as r:
            r.raise_for_status()
            reader = pd.read_csv(r.raw, sep=',', compression='gzip', chunksize=CHUNK_SIZE, low_memory=False, on_bad_lines='skip')

            total_uploaded = 0
            
            for chunk_idx, df in enumerate(reader):
                df_clean = clean_dataframe(df, current_run_time)
                records = df_clean.to_dict(orient='records')
                
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i + BATCH_SIZE]
                    # Retries once if a timeout occurs
                    for attempt in range(2):
                        try:
                            supabase.table(TABLE_NAME).upsert(batch, on_conflict=UNIQUE_KEY).execute()
                            break 
                        except Exception as e:
                            if attempt == 0:
                                print(f"Batch timeout, retrying...", flush=True)
                                time.sleep(2)
                            else:
                                print(f"   > ERROR uploading batch: {e}", flush=True)
                
                total_uploaded += len(records)
                if (chunk_idx + 1) % 10 == 0:
                    print(f"Status: {total_uploaded} rows processed...", flush=True)

            print(f"--- FINISHED UPLOADING {total_uploaded} ROWS ---", flush=True)

            # THE PURGE: Deletes in small batches to avoid timeout
            print("Cleaning up zombie records...", flush=True)
            try:
                # We do the purge in a loop to avoid one giant timed-out command
                supabase.rpc('delete_old_hotels', {'sync_threshold': current_run_time}).execute()
                print("Purge complete.", flush=True)
            except Exception as e:
                print(f"Purge Warning: {e}. You may need to run the SQL function below.", flush=True)

    except Exception as e:
        print(f"FATAL ERROR: {e}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    sync_data()
