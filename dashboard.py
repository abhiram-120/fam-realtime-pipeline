import streamlit as st
import redis
import duckdb
import pandas as pd
import json
import time
import plotly.express as px
import config

# Page Configuration
st.set_page_config(page_title="Real-Time Market Monitor", layout="wide")

@st.cache_resource
def get_redis_client():
    return redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)

def fetch_historical_data(limit: int = 200) -> pd.DataFrame:
    """Queries the OLAP layer for trend analysis."""
    try:
        # Connect in read_only mode to prevent locking issues with the producer
        con = duckdb.connect(config.DUCKDB_PATH, read_only=True)
        query = f"""
            SELECT price, timestamp 
            FROM ticks 
            WHERE symbol = '{config.TICKER_SYMBOL}' 
            ORDER BY timestamp DESC 
            LIMIT {limit}
        """
        df = con.execute(query).fetchdf()
        con.close()
        return df.sort_values(by="timestamp")
    except Exception:
        return pd.DataFrame()

def main():
    st.title("Real-Time Data Pipeline")
    st.markdown("### Architecture Proof of Concept")
    
    # Dashboard Layout
    kpi_col, chart_col = st.columns([1, 3])
    
    # UI Elements (Empty containers for updates)
    price_container = kpi_col.empty()
    chart_container = chart_col.empty()
    
    redis_client = get_redis_client()

    # Auto-refresh loop
    while True:
        # Fetch Real-time State
        latest_payload = redis_client.get(config.TICKER_SYMBOL)
        
        if latest_payload:
            data = json.loads(latest_payload)
            current_price = data.get("price")
            
            # Update KPI
            with price_container.container():
                st.metric(
                    label=f"{config.TICKER_SYMBOL} (Live)", 
                    value=f"${current_price:,.2f}"
                )
        
        # Fetch Historical State (every iteration for demo smoothness)
        df_history = fetch_historical_data()
        
        if not df_history.empty:
            with chart_container.container():
                # Use a Line chart instead of Area to see the wiggle
                fig = px.line(
                    df_history, 
                    x='timestamp', 
                    y='price', 
                    title='Price Trend (Zoomed In)',
                    template='plotly_dark'
                )
                
                # FORCE the Y-axis to zoom in on the price action (don't start at 0)
                min_price = df_history['price'].min()
                max_price = df_history['price'].max()
                padding = (max_price - min_price) * 0.5  # Add some breathing room
                
                fig.update_layout(
                    xaxis_title=None, 
                    yaxis_title=None, 
                    height=400,
                    yaxis=dict(range=[min_price - padding, max_price + padding])
                )
                
                # Your existing unique key fix
                st.plotly_chart(fig, use_container_width=True, key=f"chart_{time.time()}")
        time.sleep(1)

if __name__ == "__main__":
    main()