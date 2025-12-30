import duckdb
import config
import time
import logging
import os
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

OUTPUT_DIR = "partitioned_data"

def setup_mock_data(con):
    """Generates a larger dataset by duplicating existing data to simulate scale."""
    logging.info("Generating mock dataset for benchmarking...")
    con.execute("CREATE OR REPLACE TABLE big_data AS SELECT * FROM ticks")
    # Duplicate data 5 times to increase row count
    for i in range(5):
        con.execute("INSERT INTO big_data SELECT * FROM big_data")
    
    row_count = con.execute("SELECT COUNT(*) FROM big_data").fetchone()[0]
    logging.info(f"Mock dataset created: {row_count} rows.")

def benchmark_full_scan(con):
    """Benchmarks a standard query without partitioning (Full Table Scan)."""
    logging.info("Running non-partitioned query...")
    start_time = time.time()
    
    # Force a full scan filter
    con.execute(f"SELECT COUNT(*) FROM big_data WHERE symbol = '{config.TICKER_SYMBOL}'").fetchall()
    
    duration = time.time() - start_time
    logging.info(f"Non-Partitioned Query Duration: {duration:.4f} seconds")

def partition_and_benchmark(con):
    """Partitions data by symbol and benchmarks the pruned query."""
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)

    logging.info(f"Writing partitioned Parquet files to ./{OUTPUT_DIR}...")
    
    # Write data using Hive-style partitioning
    con.execute(f"""
        COPY (SELECT * FROM big_data) 
        TO '{OUTPUT_DIR}' 
        (FORMAT PARQUET, PARTITION_BY symbol, OVERWRITE_OR_IGNORE 1)
    """)

    logging.info("Running partitioned query (Partition Pruning)...")
    start_time = time.time()
    
    # FIX: Use read_parquet with hive_partitioning=True
    # This lets DuckDB find the correct folder automatically, even with special characters like "/"
    query = f"""
        SELECT COUNT(*) 
        FROM read_parquet('{OUTPUT_DIR}/*/*.parquet', hive_partitioning=true) 
        WHERE symbol = '{config.TICKER_SYMBOL}'
    """
    con.execute(query).fetchall()
    
    duration = time.time() - start_time
    logging.info(f"Partitioned Query Duration:     {duration:.4f} seconds")

if __name__ == "__main__":
    try:
        con = duckdb.connect(config.DUCKDB_PATH)
        setup_mock_data(con)
        benchmark_full_scan(con)
        partition_and_benchmark(con)
        con.close()
    except Exception as e:
        logging.error(f"Benchmark failed: {e}")