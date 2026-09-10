"""Unit tests for dashboard services and fleet scaling logic.

Target module: docker/dashboard/services/fleet_manager_srv.py
Adheres strictly to FIRST principles and AAA pattern.
"""

import importlib.util
import os
import sys
import pytest

_candidates = [
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "dashboard", "services", "fleet_manager_srv.py")),
    "/dashboard/services/fleet_manager_srv.py",
]
_target_file = next((p for p in _candidates if os.path.exists(p)), None)
if not _target_file:
    raise ImportError(f"Could not locate fleet_manager_srv.py in candidates: {_candidates}")

spec = importlib.util.spec_from_file_location("dashboard_fleet_srv", _target_file)
fleet_srv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fleet_srv)

get_active_fleet_size = fleet_srv.get_active_fleet_size
scale_active_fleet = fleet_srv.scale_active_fleet


@pytest.mark.unit
class TestDashboardFleetManagerService:
    """Unit test suite for dashboard fleet manager service."""

    def test_get_active_fleet_size_returns_ini_value(self, tmp_path):
        """get_active_fleet_size reads integer from pipeline.conf [fleet] section."""
        # Arrange
        conf_file = tmp_path / "pipeline.conf"
        conf_file.write_text("[fleet]\nnum_trucks = 7\n", encoding="utf-8")

        # Act
        size = get_active_fleet_size(config_path=str(conf_file))

        # Assert
        assert size == 7

    def test_get_active_fleet_size_falls_back_when_file_missing(self):
        """Missing config file falls back to default 5."""
        assert get_active_fleet_size(config_path="/missing/pipeline.conf") == 5

    def test_scale_active_fleet_increments_and_persists(self, tmp_path):
        """scale_active_fleet increments existing value and writes updated INI."""
        # Arrange
        conf_file = tmp_path / "pipeline.conf"
        conf_file.write_text("[fleet]\nnum_trucks = 5\n", encoding="utf-8")

        # Act: Scale by +3
        new_size = scale_active_fleet(delta=3, config_path=str(conf_file))
        read_back = get_active_fleet_size(config_path=str(conf_file))

        # Assert
        assert new_size == 8
        assert read_back == 8
