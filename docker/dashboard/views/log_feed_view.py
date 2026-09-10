"""
log_feed_view.py
Presentation component rendering the ingestion heartbeat and layer logs feed.
"""

import pandas as pd
import streamlit as st
from services.lakehouse_service import fetch_bronze_df, fetch_delta_history


def render_ingestion_heartbeat(stats: dict):
    """Render real-time event counts and timestamps in the sidebar."""
    st.sidebar.markdown("---")
    st.sidebar.subheader("📡 Ingestion Heartbeat")

    st.sidebar.markdown(
        f"**🟢 Bronze (Kafka Raw)**\n\n"
        f"Events: **{stats['bronze_count']:,}** | Offset: `{stats['bronze_offset']}`\n\n"
        f"Latest: `{stats['bronze_latest']}`"
    )

    st.sidebar.markdown(
        f"**🔵 Silver (Cleaned IoT)**\n\n"
        f"Records: **{stats['silver_count']:,}**\n\n"
        f"Latest: `{stats['silver_latest']}`"
    )

    st.sidebar.markdown(
        f"**🟡 Gold (Aggregated Windows)**\n\n"
        f"Windows: **{stats['gold_count']:,}**\n\n"
        f"Latest: `{stats['gold_latest']}`"
    )


def render_layer_logs_feed(spark, lakehouse_env, silver_df: pd.DataFrame, gold_df: pd.DataFrame):
    """Render expandable raw Kafka, Silver cleaned, Gold aggregated, and commit history logs."""
    st.sidebar.markdown("---")
    st.sidebar.subheader("📋 Layer Logs Feed")

    with st.sidebar.expander("🟢 Bronze Kafka Logs", expanded=False):
        bronze_records = fetch_bronze_df(spark, lakehouse_env.path_bronze)
        if not bronze_records.empty:
            for _, b_row in bronze_records.head(5).iterrows():
                st.code(f"[{b_row['timestamp']}] Offset {b_row['offset']}\n{b_row['payload'][:110]}...", language="json")
        else:
            st.info("No Bronze logs available yet.")

    with st.sidebar.expander("🔵 Silver Cleaned Logs", expanded=False):
        if not silver_df.empty:
            for _, s_row in silver_df.head(5).iterrows():
                st.caption(f"**{s_row['Vehicle_ID']}** @ `{s_row['Timestamp']}`\n* Cargo: `{s_row['Cargo_Temp']}°C` | Door: `{s_row['Door_Status']}` | Δ: `{s_row['Temp_Delta']}°C`")
        else:
            st.info("No Silver logs available yet.")

    with st.sidebar.expander("🟡 Gold Window Logs", expanded=False):
        if not gold_df.empty:
            for _, g_row in gold_df.head(5).iterrows():
                st.caption(f"**{g_row['Vehicle_ID']}** [{str(g_row['Window_Start'])[-8:]} - {str(g_row['Window_End'])[-8:]}]\n* Avg Cargo: `{g_row['Avg_Cargo_Temp']}°C` | Anomaly: `{g_row['Anomaly_Flag']}`")
        else:
            st.info("No Gold logs available yet.")

    with st.sidebar.expander("📜 Delta Commit History", expanded=False):
        layer_sel = st.selectbox("Select Layer", ["Bronze", "Silver", "Gold"], key="hist_layer_sel")
        layer_path = lakehouse_env.path_bronze if layer_sel == "Bronze" else (lakehouse_env.path_silver if layer_sel == "Silver" else lakehouse_env.path_gold)
        hist_df = fetch_delta_history(spark, layer_path)
        if not hist_df.empty:
            st.dataframe(hist_df, hide_index=True, use_container_width=True)
        else:
            st.info("No commit history.")
