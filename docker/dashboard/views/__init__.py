"""Dashboard UI presentation views."""
from .fleet_view import render_fleet_manager
from .kpi_view import render_fleet_kpis
from .log_feed_view import render_ingestion_heartbeat, render_layer_logs_feed
from .map_view import render_geospatial_fleet_map
from .styles import inject_custom_styles

__all__ = [
    "inject_custom_styles",
    "render_fleet_kpis",
    "render_geospatial_fleet_map",
    "render_ingestion_heartbeat",
    "render_layer_logs_feed",
    "render_fleet_manager",
]
