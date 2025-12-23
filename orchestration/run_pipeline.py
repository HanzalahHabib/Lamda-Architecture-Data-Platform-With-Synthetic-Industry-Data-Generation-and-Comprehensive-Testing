import argparse
import subprocess
import sys
import os
import time
import threading

# Add parent dir
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generator.generate_data import generate_batch_history, generate_users, simulate_streaming

def run_setup():
    print("[Orchestrator] Initializing Data Platform...")
    
    # 1. Generate Metadata
    generate_users()
    
    # 2. Generate Historical Data
    generate_batch_history()
    
    print("[Orchestrator] Raw Data Generation Complete.")

def run_batch_layer():
    print("[Orchestrator] Triggering Batch Layer Job...")
    batch_script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "batch_layer", "process_batch.py")
    subprocess.run([sys.executable, batch_script], check=True)
    print("[Orchestrator] Batch Layer Job Complete.")

def run_speed_layer():
    print("[Orchestrator] Starting Speed Layer (Streaming Job)...")
    speed_script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "speed_layer", "process_stream.py")
    # Run in separate non-blocking process
    sp = subprocess.Popen([sys.executable, speed_script])
    return sp

def run_stream_simulation():
    print("[Orchestrator] Starting Real-time Event Simulation...")
    # Simulate for 60 seconds loop, or forever. Let's do a loop.
    try:
        while True:
            simulate_streaming(interval_sec=2, duration_sec=5) # burst extract
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping simulation")

def main():
    parser = argparse.ArgumentParser(description="Multi-Agent Lambda Platform Orchestrator")
    parser.add_argument("--mode", choices=['full', 'batch-only', 'stream-only'], default='full')
    args = parser.parse_args()

    if args.mode in ['full', 'batch-only']:
        run_setup()
        run_batch_layer()
        
    if args.mode in ['full', 'stream-only']:
        print("[Orchestrator] Launching Speed Layer & Stream Simulation in parallel...")
        
        # Start Spark Streaming Job
        speed_process = run_speed_layer()
        
        # Give Spark a moment to initialize
        time.sleep(10)
        
        # Start Data Generator (Blocking loop)
        try:
            run_stream_simulation()
        except KeyboardInterrupt:
            print("Stopping...")
            speed_process.terminate()

if __name__ == "__main__":
    main()
