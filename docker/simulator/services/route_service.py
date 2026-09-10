"""
route_service.py
Service responsible for discovering and loading real road network coordinates.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_ROUTE_SEARCH_PATHS: List[str] = [
    "/app/config/routes.json",
    "/app/routes.json",
    "config/routes.json",
    "docker/simulator/routes.json",
]


def load_hub_routes(
    custom_path: Optional[str] = None,
    search_defaults: bool = True,
) -> Dict[str, Any]:
    """
    Discover and load pre-computed OpenStreetMap road coordinates for fleet hubs.
    
    Args:
        custom_path: Optional explicit file path to routes JSON.
        search_defaults: Whether to search default paths if custom_path is not found.
        
    Returns:
        Dictionary mapping hub name to route coordinates metadata.
    """
    search_paths: List[str] = []
    if custom_path:
        search_paths.append(custom_path)

    if not custom_path or search_defaults:
        env_path = os.environ.get("ROUTES_FILE")
        if env_path:
            search_paths.append(env_path)
        search_paths.extend(DEFAULT_ROUTE_SEARCH_PATHS)

    for path in search_paths:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict) and data:
                        logger.info("Loaded real road routes from %s", path)
                        return data
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to parse routes from %s: %s", path, e)
                continue

    logger.warning("No valid routes file discovered. Returning empty routes dictionary.")
    return {}
