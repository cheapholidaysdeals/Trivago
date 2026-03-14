import os
import sys
import pandas as pd
import requests
import numpy as np
import psycopg2
import io

# Load Config
feed_url = os.environ.get("AWIN_FEED_URL")
db_url = os.environ.get("DATABASE_URL")

def run_sync():
    print("--- STARTING HIGH-SPEED DIRECT SYNC ---", flush=True)
    
    # 1. Download the massive CSV
    print("Downloading 500k+ rows from Awin...", flush=True)
    try:
        response = requests.get(feed_url, stream=True, timeout=120)
        response.raise_for_status()
        with open("trivago_raw.csv.gz", "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        print(f"FATAL ERROR downloading feed: {e}", flush=True)
        sys.exit(1)

    # 2. Process with Pandas
    print("Processing CSV data into memory...", flush=True)
    try:
        df = pd.read_csv("trivago_raw.csv.gz", compression='gzip', low_memory=False, on_bad_lines='skip')
        
        # Apply your specific data cleaning rules
        if 'Travel:destination_zipcode' in df.columns:
            df['Travel:destination_zipcode'] = pd.to_numeric(df['Travel:destination_zipcode'], errors='coerce')
        
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.where(pd.notnull(df), None)

        # Drop the last_synced_at column if it exists in your schema, 
        # as the whole table is refreshed daily now anyway.
        if 'last_synced_at' in df.columns:
             df = df.drop(columns=['last_synced_at'])

        # Save the cleaned data to an in-memory CSV buffer for the COPY command
        print("Formatting data for Postgres COPY...", flush=True)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False, sep='\t')
        csv_buffer.seek(0)
    except Exception as e:
        print(f"FATAL ERROR processing CSV: {e}", flush=True)
        sys.exit(1)

    # 3. Connect directly to PostgreSQL for the Swap
    print("Connecting directly to PostgreSQL...", flush=True)
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = False # Handled manually to ensure zero-downtime
        cursor = conn.cursor()
    except Exception as e:
         print(f"FATAL ERROR connecting to database: {e}", flush=True)
         sys.exit(1)

    try:
        # Step A: Empty the staging table
        print("Emptying 'trivago_staging' table...", flush=True)
        cursor.execute('TRUNCATE TABLE "trivago_staging";')

        # Step B: Bulk copy the data into the staging table (Extremely fast)
        print(f"Bulk copying {len(df)} rows into staging...", flush=True)
        columns = tuple(df.columns)
        copy_sql = f"""
            COPY "trivago_staging" ({','.join([f'"{c}"' for c in columns])}) 
            FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '')
        """
        cursor.copy_expert(copy_sql, csv_buffer)

        # Step C: The Zero-Downtime Swap
        print("Swapping staging data into live 'Trivago Hotels' table...", flush=True)
        swap_sql = """
            BEGIN;
            TRUNCATE TABLE "Trivago Hotels";
            INSERT INTO "Trivago Hotels" SELECT * FROM "trivago_staging";
            COMMIT;
        """
        cursor.execute(swap_sql)
        conn.commit()
        
        print(f"--- SUCCESS: {len(df)} ROWS UPDATED WITH ZERO DOWNTIME ---", flush=True)

    except Exception as e:
        conn.rollback()
        print(f"❌ DATABASE TRANSACTION FAILED: {e}", flush=True)
        print("Live data was protected and has not been altered.", flush=True)
        sys.exit(1)
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    run_sync()
