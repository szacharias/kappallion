"""
lakehouse_service.py
Service responsible for PySpark Delta Lake queries and caching.
"""

import logging
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)


@st.cache_resource
def init_spark_session():
    """Initialize SparkSession and Lakehouse environment context."""
    import sys
    sys.path.append("/app/apps/core")
    from lakehouse_spark_env import LakehouseSparkEnv
    return LakehouseSparkEnv("LakehouseDashboard", log_level="ERROR")


def register_delta_views(spark, lakehouse_env):
    """Register or replace temporary SQL views for Bronze, Silver, Gold Delta paths."""
    for name, path in [
        ("bronze", lakehouse_env.path_bronze),
        ("silver", lakehouse_env.path_silver),
        ("gold", lakehouse_env.path_gold),
    ]:
        try:
            spark.read.format("delta").load(path).createOrReplaceTempView(name)
        except Exception as ex:
            logger.debug("View '%s' skipped or not yet initialized: %s", name, ex)


@st.cache_data(ttl=60)
def fetch_stream_stats(_spark, path_bronze: str, path_silver: str, path_gold: str) -> dict:
    """Fetch total event counts and latest arrival timestamps across layers."""
    stats = {
        "bronze_count": 0, "bronze_latest": "N/A", "bronze_offset": 0,
        "silver_count": 0, "silver_latest": "N/A",
        "gold_count": 0, "gold_latest": "N/A",
    }
    try:
        b_df = _spark.sql(f"SELECT count(*) as total, max(timestamp) as latest_ts, max(offset) as latest_offset FROM delta.`{path_bronze}`").toPandas()
        if not b_df.empty and pd.notna(b_df["total"].iloc[0]):
            stats["bronze_count"] = int(b_df["total"].iloc[0])
            stats["bronze_latest"] = str(b_df["latest_ts"].iloc[0])
            stats["bronze_offset"] = int(b_df["latest_offset"].iloc[0])
    except Exception:
        pass

    try:
        s_df = _spark.sql(f"SELECT count(*) as total, max(Timestamp) as latest_ts FROM delta.`{path_silver}`").toPandas()
        if not s_df.empty and pd.notna(s_df["total"].iloc[0]):
            stats["silver_count"] = int(s_df["total"].iloc[0])
            stats["silver_latest"] = str(s_df["latest_ts"].iloc[0])
    except Exception:
        pass

    try:
        g_df = _spark.sql(f"SELECT count(*) as total, max(Window_End) as latest_ts FROM delta.`{path_gold}`").toPandas()
        if not g_df.empty and pd.notna(g_df["total"].iloc[0]):
            stats["gold_count"] = int(g_df["total"].iloc[0])
            stats["gold_latest"] = str(g_df["latest_ts"].iloc[0])
    except Exception:
        pass

    return stats


@st.cache_data(ttl=60)
def fetch_bronze_df(_spark, path: str, limit: int = 20) -> pd.DataFrame:
    """Query recent raw Kafka events from Bronze table."""
    try:
        return _spark.sql(f"SELECT timestamp, partition, offset, substring(CAST(value AS STRING), 1, 150) as payload FROM delta.`{path}` ORDER BY timestamp DESC LIMIT {limit}").toPandas()
    except Exception as ex:
        logger.debug("Bronze fetch error: %s", ex)
        return pd.DataFrame()


@st.cache_data(ttl=60)
def fetch_silver_df(_spark, path: str, limit: int = 500) -> pd.DataFrame:
    """Query recent cleansed IoT events from Silver table."""
    try:
        return _spark.sql(f"SELECT * FROM delta.`{path}` ORDER BY Timestamp DESC LIMIT {limit}").toPandas()
    except Exception as ex:
        logger.debug("Silver fetch error: %s", ex)
        return pd.DataFrame()


@st.cache_data(ttl=60)
def fetch_gold_df(_spark, path: str, limit: int = 100) -> pd.DataFrame:
    """Query aggregated window metrics from Gold table."""
    try:
        return _spark.sql(f"SELECT * FROM delta.`{path}` ORDER BY Window_End DESC LIMIT {limit}").toPandas()
    except Exception as ex:
        logger.debug("Gold fetch error: %s", ex)
        return pd.DataFrame()


@st.cache_data(ttl=60)
def fetch_delta_history(_spark, path: str, limit: int = 10) -> pd.DataFrame:
    """Retrieve ACID commit log history for a Delta table."""
    try:
        return _spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT {limit}").select("version", "timestamp", "operation").toPandas()
    except Exception:
        return pd.DataFrame()
