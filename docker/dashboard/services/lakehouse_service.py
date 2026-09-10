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
    """Initialize SparkSession, register Delta views, and Lakehouse environment context."""
    import sys
    sys.path.append("/app/apps/core")
    from lakehouse_spark_env import LakehouseSparkEnv
    env = LakehouseSparkEnv("LakehouseDashboard", log_level="ERROR")
    register_delta_views(env.spark, env)
    return env


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


def compute_stream_stats(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    gold_df: pd.DataFrame,
) -> dict:
    """Compute event counts and latest arrival timestamps in-memory from cached DataFrames."""
    stats = {
        "bronze_count": 0, "bronze_latest": "N/A", "bronze_offset": 0,
        "silver_count": 0, "silver_latest": "N/A",
        "gold_count": 0, "gold_latest": "N/A",
    }

    def _is_not_na(val):
        if hasattr(pd, "notna"):
            try:
                return bool(pd.notna(val))
            except Exception:
                pass
        return val is not None

    if not bronze_df.empty:
        if "offset" in bronze_df.columns and _is_not_na(bronze_df["offset"].iloc[0]):
            stats["bronze_offset"] = int(bronze_df["offset"].iloc[0])
            stats["bronze_count"] = stats["bronze_offset"] + 1
        if "timestamp" in bronze_df.columns and _is_not_na(bronze_df["timestamp"].iloc[0]):
            stats["bronze_latest"] = str(bronze_df["timestamp"].iloc[0])

    if not silver_df.empty:
        stats["silver_count"] = len(silver_df)
        if "Timestamp" in silver_df.columns and _is_not_na(silver_df["Timestamp"].iloc[0]):
            stats["silver_latest"] = str(silver_df["Timestamp"].iloc[0])

    if not gold_df.empty:
        stats["gold_count"] = len(gold_df)
        if "Window_End" in gold_df.columns and _is_not_na(gold_df["Window_End"].iloc[0]):
            stats["gold_latest"] = str(gold_df["Window_End"].iloc[0])

    return stats


@st.cache_data(ttl=60)
def fetch_stream_stats(_spark, path_bronze: str, path_silver: str, path_gold: str) -> dict:
    """Fetch event counts and latest timestamps using bounded queries and in-memory stats."""
    bronze_df = fetch_bronze_df(_spark, path_bronze, limit=1)
    silver_df = fetch_silver_df(_spark, path_silver, limit=500)
    gold_df = fetch_gold_df(_spark, path_gold, limit=100)
    return compute_stream_stats(bronze_df, silver_df, gold_df)


@st.cache_data(ttl=60)
def fetch_bronze_df(_spark, path: str, limit: int = 10) -> pd.DataFrame:
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
