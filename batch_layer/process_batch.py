import duckdb
import os
import pandas as pd
from datetime import datetime

# Path Setup
env_base = os.getenv("LAMBDA_BASE_DIR")
if env_base:
    BASE_DIR = env_base
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
DATA_DIR = os.path.join(BASE_DIR, "data")

def process_batch():
    print("Starting Batch Layer Processing (via DuckDB)...")
    
    # Paths
    raw_history_glob = os.path.join(DATA_DIR, "raw", "batch", "*.json").replace('\\', '/')
    users_path = os.path.join(DATA_DIR, "master", "users.csv").replace('\\', '/')
    output_dir = os.path.join(DATA_DIR, "processed", "batch_views")
    output_file = os.path.join(output_dir, "batch_data.parquet").replace('\\', '/')
    
    os.makedirs(output_dir, exist_ok=True)

    con = duckdb.connect()

    try:
        # Step-by-step for better debugging
        print(f"Reading batch data from {raw_history_glob}")
        con.execute(f"CREATE OR REPLACE VIEW raw_history AS SELECT * FROM read_json_auto('{raw_history_glob}')")
        
        print(f"Reading users from {users_path}")
        con.execute(f"CREATE OR REPLACE VIEW users_master AS SELECT * FROM read_csv_auto('{users_path}')")
        
        print("Transforming and writing to Parquet...")
        query = f"""
            COPY (
                WITH deduplicated AS (
                    SELECT 
                        transaction_id, 
                        user_id, 
                        product, 
                        CAST(amount AS DOUBLE) as amount, 
                        CAST(timestamp AS TIMESTAMP) as timestamp, 
                        status,
                        ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY timestamp DESC) as rn
                    FROM raw_history
                )
                SELECT 
                    d.transaction_id, 
                    d.user_id, 
                    d.product, 
                    d.amount, 
                    d.timestamp, 
                    d.status,
                    u.name as user_name,
                    u.region,
                    now() as processed_at
                FROM deduplicated d
                LEFT JOIN users_master u ON d.user_id = u.user_id
                WHERE d.rn = 1
            ) TO '{output_file}' (FORMAT PARQUET);
        """
        con.execute(query)
        
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_file}')").fetchone()[0]
        print(f"Batch Processing Complete. Processed {count} records into {output_file}")

    except Exception as e:
        print(f"Error in Batch Layer: {e}")
        # Re-raise to let orchestrator/tests know
        raise e

if __name__ == "__main__":
    process_batch()
