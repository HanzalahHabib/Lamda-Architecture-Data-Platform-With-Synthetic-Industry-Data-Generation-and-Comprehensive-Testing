import os
import time
import random
import json
import pandas as pd
from datetime import datetime, timedelta
import shutil

# Configuration
env_base = os.getenv("LAMBDA_BASE_DIR")
if env_base:
    DATA_DIR = os.path.join(env_base, "data")
else:
    DATA_DIR = "data"

BATCH_DIR = os.path.join(DATA_DIR, "raw", "batch")
STREAM_DIR = os.path.join(DATA_DIR, "raw", "stream")
MASTER_DIR = os.path.join(DATA_DIR, "master")

def ensure_dirs():
    os.makedirs(BATCH_DIR, exist_ok=True)
    os.makedirs(STREAM_DIR, exist_ok=True)
    os.makedirs(MASTER_DIR, exist_ok=True)

# Constants
NUM_USERS = 1000
PRODUCTS = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headset', 'Webcam']
REGIONS = ['US', 'EU', 'APAC', 'LATAM']

def generate_users():
    ensure_dirs()
    print("Generating Users Master Data...")
    users = []
    for i in range(1, NUM_USERS + 1):
        users.append({
            "user_id": i,
            "name": f"User_{i}",
            "region": random.choice(REGIONS),
            "signup_date": (datetime.now() - timedelta(days=random.randint(100, 1000))).strftime("%Y-%m-%d")
        })
    df = pd.DataFrame(users)
    df.to_csv(os.path.join(MASTER_DIR, "users.csv"), index=False)
    print("Users Generated.")

def generate_batch_history(num_records=10000):
    ensure_dirs()
    print(f"Generating {num_records} historical records for Batch Layer...")
    data = []
    end_date = datetime.now() - timedelta(days=1) # History ends yesterday
    start_date = end_date - timedelta(days=30)
    
    for _ in range(num_records):
        tx_time = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))
        data.append({
            "transaction_id": f"tx_{random.randint(100000, 999999)}_{random.randint(100000, 999999)}",
            "user_id": random.randint(1, NUM_USERS),
            "product": random.choice(PRODUCTS),
            "amount": round(random.uniform(50, 2000), 2),
            "timestamp": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "COMPLETED"
        })
    
    df = pd.DataFrame(data)
    # Save as JSON for "raw" feel, or CSV. Let's use JSON per line for big data feel (simulating dump)
    df.to_json(os.path.join(BATCH_DIR, "history.json"), orient="records", lines=True)
    print("Batch History Generated.")

def generate_stream_event():
    """Generates a single event representing real-time data"""
    tx_time = datetime.now()
    event = {
        "transaction_id": f"stream_{random.randint(100000, 999999)}",
        "user_id": random.randint(1, NUM_USERS),
        "product": random.choice(PRODUCTS),
        "amount": round(random.uniform(50, 2000), 2),
        "timestamp": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "PENDING"
    }
    return event

def simulate_streaming(interval_sec=1, duration_sec=10):
    ensure_dirs()
    print(f"Simulating streaming for {duration_sec} seconds...")
    start_time = time.time()
    batch_id = 0
    while time.time() - start_time < duration_sec:
        # Generate a micro-batch of events
        events = [generate_stream_event() for _ in range(random.randint(1, 5))]
        
        # Write to stream directory as a file (simulating file-drop or Kafka topic partition dump)
        filename = f"events_{batch_id}_{int(time.time())}.json"
        filepath = os.path.join(STREAM_DIR, filename)
        
        with open(filepath, 'w') as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
        
        print(f"Streamed {len(events)} events to {filename}")
        batch_id += 1
        time.sleep(interval_sec)

if __name__ == "__main__":
    generate_users()
    generate_batch_history()
    # Streaming is usually called separately or via a flag, but for setup we might just init headers
