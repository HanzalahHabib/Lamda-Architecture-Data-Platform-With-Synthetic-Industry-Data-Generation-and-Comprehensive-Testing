# System Architecture Design

## 1. High-Level Data Flow

```mermaid
graph TD
    A[Data Sources] -->|Historical Dump| B(Raw Data Lake - Batch)
    A -->|Real-time Events| C(Event Bus / Stream)
    
    subgraph "Batch Layer"
    B --> D[Spark Batch Job]
    D --> E[Batch Views (Parquet/Delta)]
    end
    
    subgraph "Speed Layer"
    C --> F[Spark Streaming Job]
    F --> G[Real-time Views (Memory/Parquet)]
    end
    
    subgraph "Serving Layer"
    E --> H[Unified Query Engine (DuckDB)]
    G --> H
    H --> I[Dashboard / BI Tool]
    end
```

## 2. Component Detail

### Batch Layer
- **Ingestion**: Reads raw CSV/JSON dumps.
- **Processing**: Deduplication, Cleaning, Aggregation (Daily/Hourly).
- **Output**: Partitioned Parquet files `data/batch_views/`.

### Speed Layer
- **Ingestion**: Reads stream (Kafka or File Watcher).
- **Processing**: Windowed aggregations, handling late data with watermarks.
- **Output**: Low-latency micro-batch updates to `data/speed_views/` or checkpointed state.

### Serving Layer
- **Logic**: `SELECT * FROM batch_view UNION ALL SELECT * FROM speed_view WHERE timestamp > max_batch_timestamp`.
- **Technology**: DuckDB allows querying Parquet files directly with SQL, providing extremely fast response times for the dashboard.
