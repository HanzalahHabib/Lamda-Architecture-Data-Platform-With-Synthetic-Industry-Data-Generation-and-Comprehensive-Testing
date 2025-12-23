import duckdb
import os

env_base = os.getenv("LAMBDA_BASE_DIR")
if env_base:
    BASE_DIR = env_base
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_DIR, "data")
BATCH_PATH = os.path.join(DATA_DIR, "processed", "batch_views", "*.parquet").replace('\\', '/')
SPEED_PATH = os.path.join(DATA_DIR, "processed", "speed_views", "*.parquet").replace('\\', '/')

class ServingLayer:
    def __init__(self):
        self.con = duckdb.connect(database=':memory:')
        
    def _check_files_exist(self, path_pattern):
        import glob
        # Normalize for glob
        return len(glob.glob(path_pattern.replace('/', os.sep))) > 0

    def get_unified_view(self):
        """
        Constructs the Lambda Architecture View.
        Uses DuckDB to perform a robust UNION across potentially different schemas.
        """
        has_batch = self._check_files_exist(BATCH_PATH)
        has_speed = self._check_files_exist(SPEED_PATH)
        
        if not has_batch and not has_speed:
            return None
            
        # Use DuckDB to handle the union. We'll explicitly select columns to ensure alignment.
        # Speed layer might miss joined columns (name, region), we fill with NULL.
        
        batch_part = f"SELECT transaction_id, user_id, product, amount, timestamp, status, user_name, region, processed_at FROM read_parquet('{BATCH_PATH}')"
        speed_part = f"SELECT transaction_id, user_id, product, amount, event_time as timestamp, status, NULL as user_name, NULL as region, processed_at FROM read_parquet('{SPEED_PATH}')"
        
        query = ""
        if has_batch and has_speed:
            query = f"{batch_part} UNION ALL {speed_part}"
        elif has_batch:
            query = batch_part
        elif has_speed:
            speed_part_only = f"SELECT transaction_id, user_id, product, amount, event_time as timestamp, status, processed_at FROM read_parquet('{SPEED_PATH}')"
            query = speed_part_only
            
        try:
            return self.con.query(query).to_df()
        except Exception as e:
            print(f"Serving Layer Query Error: {e}")
            return None

    def get_kpis(self):
        df = self.get_unified_view()
        if df is None or df.empty:
            return {"total_sales": 0, "transaction_count": 0, "avg_order_value": 0}
            
        self.con.register('unified_view', df)
        kpis = self.con.query("""
            SELECT 
                CAST(SUM(amount) AS DOUBLE) as total_sales,
                COUNT(*) as transaction_count,
                CAST(AVG(amount) AS DOUBLE) as avg_order_value
            FROM unified_view
        """).fetchone()
        
        return {
            "total_sales": kpis[0] if kpis[0] is not None else 0,
            "transaction_count": kpis[1] if kpis[1] is not None else 0,
            "avg_order_value": kpis[2] if kpis[2] is not None else 0
        }

    def get_recent_transactions(self, limit=10):
        df = self.get_unified_view()
        if df is None or df.empty:
            import pandas as pd
            return pd.DataFrame()
        self.con.register('unified_view', df)
        return self.con.query(f"SELECT * FROM unified_view ORDER BY timestamp DESC LIMIT {limit}").to_df()
