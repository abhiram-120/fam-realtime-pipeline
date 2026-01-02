# Real-Time Market Data Pipeline

A proof-of-concept data pipeline designed to handle high-velocity market data with low-latency reads and scalable historical storage.

## Architecture

This project implements a Lambda-style architecture to separate "Hot" and "Cold" data paths:

1.  **Ingestion:** Python producer simulating high-frequency ticks.
2.  **Hot Storage (Real-Time):** **Redis** is used as a Key-Value store to maintain the latest state (O(1) access complexity) for the live dashboard.
3.  **Cold Storage (OLAP):** **DuckDB** is used as an embedded columnar database to store historical time-series data efficiently.
4.  **Visualization:** Streamlit dashboard polling Redis for sub-second updates and DuckDB for trend analysis.

## Project Structure

*   `producer.py`: Simulates data generation and dual-write logic (Redis + DuckDB).
*   `dashboard.py`: Frontend visualization.
*   `config.py`: Centralized configuration.

## How to Run

1.  Start Redis (locally or via Docker).
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Start the Producer:
    ```bash
    python producer.py
    ```
4.  Start the Dashboard:
    ```bash
    streamlit run dashboard.py
    ```

    demo video walktrough 90sec-https://drive.google.com/file/d/1El0Y8YSbTtk4tyfU_pjo0kHwDGfYMj9w/view?usp=sharing
