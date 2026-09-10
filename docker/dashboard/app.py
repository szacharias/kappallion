"""
app.py
Cold-Chain IoT Streaming Lakehouse Dashboard.
Declarative Streamlit entrypoint strictly decoupled into services and presentation views.
"""

import datetime
import os
import sys
import streamlit as st

# Add local app directory to sys.path for service and view imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.lakehouse_service import (
    fetch_gold_df,
    fetch_silver_df,
    fetch_stream_stats,
    init_spark_session,
    register_delta_views,
)
from views.fleet_view import render_fleet_manager
from views.kpi_view import render_fleet_kpis
from views.log_feed_view import render_ingestion_heartbeat, render_layer_logs_feed
from views.map_view import render_geospatial_fleet_map
from views.styles import inject_custom_styles

# 1. Page Configuration & Theme
st.set_page_config(
    page_title="Cold-Chain IoT Streaming Lakehouse Dashboard",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_custom_styles()

# 2. Spark Session & Environment
try:
    lakehouse = init_spark_session()
    spark = lakehouse.spark
    register_delta_views(spark, lakehouse)
    spark_connected = True
except Exception as e:
    st.error(f"Failed to connect to Spark: {e}")
    st.stop()

# 3. Sidebar Controls
st.sidebar.title("❄️ Cold-Chain Lakehouse")
st.sidebar.markdown("Queries **Gold** and **Silver** tables in the Delta Lake on MinIO.")

if "auto_refresh_enabled" not in st.session_state:
    st.session_state.auto_refresh_enabled = False

auto_refresh = st.sidebar.checkbox("Auto Refresh Data", value=st.session_state.auto_refresh_enabled, key="auto_refresh_enabled")
refresh_rate = st.sidebar.slider("Refresh Interval (seconds)", min_value=3, max_value=60, value=10, disabled=not auto_refresh)
st.sidebar.caption("💡 Page is static by default to conserve memory & CPU. Click Refresh to fetch latest records.")

if st.sidebar.button("🔄 Refresh Data Now", type="primary", use_container_width=True):
    st.cache_data.clear()
    register_delta_views(spark, lakehouse)
    st.rerun()

st.sidebar.caption(f"Last fetched: `{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")

# 4. Data Loading (Cached)
stats = fetch_stream_stats(spark, lakehouse.path_bronze, lakehouse.path_silver, lakehouse.path_gold)
silver_df = fetch_silver_df(spark, lakehouse.path_silver)
gold_df = fetch_gold_df(spark, lakehouse.path_gold)

# 5. Sidebar Views
render_ingestion_heartbeat(stats)
render_layer_logs_feed(spark, lakehouse, silver_df, gold_df)
render_fleet_manager()

# 6. Main Canvas Views
render_fleet_kpis(silver_df, gold_df)
render_geospatial_fleet_map(silver_df)
