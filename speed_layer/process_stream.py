import duckdb
import os
import time
import json
from datetime import datetime

# Path Setup
env_base = os.getenv("LAMBDA_BASE_DIR")
if env_base:
    BASE_DIR = env_base
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_DIR, "data")
STREAM_INPUT = os.path.join(DATA_DIR, "raw", "stream")
SPEED_OUTPUT = os.path.join(DATA_DIR, "processed", "speed_views")

def process_stream_micro_batch():
    """Simulates a single micro-batch of structured streaming using DuckDB."""
    os.makedirs(SPEED_OUTPUT, exist_ok=True)
    
    # Simple file-based "checkpointer": track processed files
    checkpoint_file = os.path.join(DATA_DIR, "speed_checkpoint.txt")
    processed_files = set()
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            processed_files = set(line.strip() for line in f)

    # List new files
    all_files = [f for f in os.listdir(STREAM_INPUT) if f.endswith('.json')]
    new_files = [f for f in all_files if f not in processed_files]

    if not new_files:
        return 0

    con = duckdb.connect()
    
    batch_count = 0
    for file_name in new_files:
        file_path = os.path.join(STREAM_INPUT, file_name)
        output_file = f"speed_{file_name.replace('.json', '')}.parquet"
        output_path = os.path.join(SPEED_OUTPUT, output_file)
        
        try:
            query = f"""
                COPY (
                    SELECT 
                        *,
                        CAST(timestamp AS TIMESTAMP) as event_time,
                        now() as processed_at
                    FROM read_json_auto('{file_path.replace('\\', '/')}')
                ) TO '{output_path.replace('\\', '/')}' (FORMAT PARQUET);
            """
            con.execute(query)
            processed_files.add(file_name)
            batch_count += 1
        except Exception as e:
            print(f"Error processing {file_name}: {e}")

    # Update checkpoint
    with open(checkpoint_file, 'w') as f:
        for f_name in processed_files:
            f.write(f_name + "\n")
            
    return batch_count

def process_stream():
    print("Starting Speed Layer (Micro-batch simulation via DuckDB)...")
    try:
        while True:
            count = process_stream_micro_batch()
            if count > 0:
                print(f"Processed {count} new stream files.")
            time.sleep(5) # Trigger every 5 seconds
    except KeyboardInterrupt:
        print("Stopping Speed Layer.")

if __name__ == "__main__":
    process_stream()
