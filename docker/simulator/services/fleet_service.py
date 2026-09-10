"""
fleet_service.py
Service responsible for fleet configuration discovery and target size resolution.
"""

import configparser
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_SEARCH_PATHS = [
    "/app/config/pipeline.conf",
    "config/pipeline.conf",
]


def resolve_target_fleet_size(
    default_size: int = 5,
    config_path: Optional[str] = None,
    default_val: Optional[int] = None,
) -> int:
    """
    Resolve target fleet size from configuration files, environment variables, or default.

    Precedence:
    1. Explicit config_path or FLEET_CONFIG environment variable (if provided)
    2. Default config files (/app/config/pipeline.conf, config/pipeline.conf)
    3. NUM_TRUCKS environment variable
    4. default_val / default_size
    """
    actual_default = default_val if default_val is not None else default_size
    search_paths = []

    if config_path:
        search_paths.append(config_path)
    elif "FLEET_CONFIG" in os.environ:
        search_paths.append(os.environ["FLEET_CONFIG"])
    else:
        search_paths.extend(DEFAULT_CONFIG_SEARCH_PATHS)

    for path in search_paths:
        if os.path.exists(path):
            try:
                cp = configparser.ConfigParser()
                cp.read(path)
                if cp.has_section("fleet") and cp.has_option("fleet", "num_trucks"):
                    size = int(cp.get("fleet", "num_trucks"))
                    if size > 0:
                        return size
            except Exception as ex:
                logger.warning("Error reading fleet size from %s: %s", path, ex)
                continue

    env_val = os.environ.get("NUM_TRUCKS")
    if env_val:
        try:
            val = int(env_val)
            if val > 0:
                return val
        except ValueError:
            pass

    return actual_default
