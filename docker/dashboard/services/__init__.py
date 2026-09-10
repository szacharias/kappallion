"""Dashboard backend services."""
from .fleet_manager_srv import get_active_fleet_size, scale_active_fleet
from .lakehouse_service import (
    fetch_bronze_df,
    fetch_delta_history,
    fetch_gold_df,
    fetch_silver_df,
    fetch_stream_stats,
    init_spark_session,
)

__all__ = [
    "get_active_fleet_size",
    "scale_active_fleet",
    "init_spark_session",
    "fetch_stream_stats",
    "fetch_bronze_df",
    "fetch_silver_df",
    "fetch_gold_df",
    "fetch_delta_history",
]
