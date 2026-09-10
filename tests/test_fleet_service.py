"""Unit tests for fleet sizing configuration service.

Target module: docker/simulator/services/fleet_service.py
Adheres strictly to FIRST principles and AAA pattern.
"""

import os
import sys
import pytest

_SIM_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "simulator"))
if _SIM_DIR not in sys.path:
    sys.path.insert(0, _SIM_DIR)

from services.fleet_service import resolve_target_fleet_size


@pytest.mark.unit
class TestFleetService:
    """Unit test suite for fleet sizing service."""

    def test_resolve_from_valid_ini_file(self, tmp_path):
        """INI config section [fleet] num_trucks takes primary precedence."""
        # Arrange
        conf_file = tmp_path / "pipeline.conf"
        conf_file.write_text("[fleet]\nnum_trucks = 8\n", encoding="utf-8")

        # Act
        result = resolve_target_fleet_size(default_size=3, config_path=str(conf_file))

        # Assert
        assert result == 8

    def test_resolve_from_environment_variable(self, monkeypatch, tmp_path):
        """When config file missing, NUM_TRUCKS environment variable is used."""
        # Arrange
        monkeypatch.setenv("NUM_TRUCKS", "6")
        missing_path = str(tmp_path / "missing.conf")

        # Act
        result = resolve_target_fleet_size(default_size=3, config_path=missing_path)

        # Assert
        assert result == 6

    def test_resolve_fallback_to_default(self, monkeypatch, tmp_path):
        """When config and env var are absent, falls back to default_size."""
        # Arrange
        monkeypatch.delenv("NUM_TRUCKS", raising=False)
        missing_path = str(tmp_path / "missing.conf")

        # Act
        result = resolve_target_fleet_size(default_size=4, config_path=missing_path)

        # Assert
        assert result == 4
