# NEBULA | Lambda Architecture Data Platform

![Status](https://img.shields.io/badge/Status-Production--Ready-blueviolet)
![Architecture](https://img.shields.io/badge/Architecture-Lambda-00f3ff)
![UI](https://img.shields.io/badge/UI-HUD--Modern-00ff9f)

NEBULA is a 25th-century, production-grade **Lambda Architecture** implementation designed for high-velocity data ingestion, historical accuracy, and real-time analytical serving.

## 🏗️ Architecture Overview

The platform implements the classic Lambda design:
1.  **Batch Layer**: Manages the master dataset (Parquet) and pre-computes historical views using DuckDB.
2.  **Speed Layer**: Processes real-time micro-batches of event streams for low-latency insights.
3.  **Serving Layer**: Provides a unified SQL interface via DuckDB that merges Batch and Speed layers on-the-fly.

## Getting Started

### Prerequisites
- Python 3.9 or higher

### Installation
1.  **Clone the Repository**:
    ```bash
    git clone https://github.com/your-username/lambda-data-platform.git
    cd lambda-data-platform
    ```
2.  **Set Up Environment**:
    ```bash
    python -m venv .venv
    # Windows
    .venv\Scripts\activate
    # Linux/Mac
    source .venv/bin/activate
    ```
3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

### Running the Platform
The project includes a master orchestrator to simplify operations.

1.  **Initial Setup & Run**:
    This generates demo data, initializes the batch views, and starts the real-time stream simulation.
    ```bash
    python orchestration/run_pipeline.py --mode full
    ```
2.  **Launch Analytics Dashboard**:
    In a separate terminal (with the virtual env activated):
    ```bash
    streamlit run dashboard/app.py
    ```

## 📂 Project Structure
- `batch_layer/`: Historical processing logic.
- `speed_layer/`: Real-time micro-batch processing.
- `serving_layer/`: Unified query engine (DuckDB wrapper).
- `data_generator/`: Enterprise-scale data simulator.
- `orchestration/`: Pipeline control and scheduling logic.
- `dashboard/`: Streamlit-based UI.
- `tests/`: Automated validation suite.

## 🧪 Testing
Run the comprehensive test suite to verify the platform integrity:
```bash
python tests/test_suite.py
```

## 📜 License
MIT License
