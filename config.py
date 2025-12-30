import os

# Database Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DUCKDB_PATH = "market_data.duckdb"

# Simulation Settings
TICKER_SYMBOL = "BTC/USD"
BASE_PRICE = 50000.00
UPDATE_INTERVAL_SEC = 0.5