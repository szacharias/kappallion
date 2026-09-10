"""
fleet_manager_srv.py
Backend service for querying and updating fleet size configurations.
"""

import configparser
import logging
import os

logger = logging.getLogger(__name__)
DEFAULT_CONFIG_PATH = "/app/config/pipeline.conf"


def get_active_fleet_size(config_path: str = DEFAULT_CONFIG_PATH) -> int:
    """Retrieve current configured target fleet size."""
    if os.path.exists(config_path):
        try:
            cp = configparser.ConfigParser()
            cp.read(config_path)
            if cp.has_section("fleet") and cp.has_option("fleet", "num_trucks"):
                return int(cp.get("fleet", "num_trucks"))
        except Exception as ex:
            logger.warning("Failed to read fleet config from %s: %s", config_path, ex)
    return 5


def scale_active_fleet(delta: int, config_path: str = DEFAULT_CONFIG_PATH) -> int:
    """Increment or scale the active target fleet size and persist to INI."""
    current = get_active_fleet_size(config_path)
    new_size = max(1, current + delta)

    cp = configparser.ConfigParser()
    if os.path.exists(config_path):
        try:
            cp.read(config_path)
        except Exception as ex:
            logger.warning("Failed reading %s prior to write: %s", config_path, ex)

    if not cp.has_section("fleet"):
        cp.add_section("fleet")

    cp.set("fleet", "num_trucks", str(new_size))
    with open(config_path, "w", encoding="utf-8") as f:
        cp.write(f)

    logger.info("Scaled fleet from %d to %d in %s", current, new_size, config_path)
    return new_size
