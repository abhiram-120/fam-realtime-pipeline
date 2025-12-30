import redis
import duckdb
import time
import random
import json
import logging
from datetime import datetime
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_redis_connection():
    try:
        return redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
    except Exception as e:
        logging.error(f"Failed to connect to Redis: {e}")
        raise e

def init_olap_db():
    """Initialize the DuckDB schema if it does not exist."""
    try:
        con = duckdb.connect(config.DUCKDB_PATH)
        con.execute("""
            CREATE TABLE IF NOT EXISTS ticks (
                symbol VARCHAR,
                price DOUBLE,
                timestamp TIMESTAMP
            )
        """)
        con.close()
        logging.info("OLAP storage initialized.")
    except Exception as e:
        logging.error(f"Database initialization failed: {e}")

def simulate_market_data():
    """Generates a random price walk for the ticker."""
    redis_client = get_redis_connection()
    
    # Main Ingestion Loop
    current_price = config.BASE_PRICE
    
    logging.info(f"Starting ingestion pipeline for {config.TICKER_SYMBOL}...")

    while True:
        try:
            # Simulate price movement (Random Walk)
            fluctuation = random.uniform(-50, 50)
            current_price = round(current_price + fluctuation, 2)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            payload = {
                "symbol": config.TICKER_SYMBOL,
                "price": current_price,
                "timestamp": timestamp
            }

            # 1. Real-time Layer (Redis) - Overwrite key for latest state
            redis_client.set(config.TICKER_SYMBOL, json.dumps(payload))

            # 2. Historical Layer (DuckDB) - Append for time-series analysis
            # We open/close connection per write to ensure file safety in this simple demo
            with duckdb.connect(config.DUCKDB_PATH) as con:
                con.execute("INSERT INTO ticks VALUES (?, ?, ?)", 
                           (config.TICKER_SYMBOL, current_price, timestamp))

            logging.info(f"Processed tick: {current_price}")
            time.sleep(config.UPDATE_INTERVAL_SEC)

        except Exception as e:
            logging.error(f"Pipeline error: {e}")
            time.sleep(5) # Backoff on error

if __name__ == "__main__":
    init_olap_db()
    simulate_market_data()