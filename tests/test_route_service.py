"""Unit tests for route loading service.

Target module: docker/simulator/services/route_service.py
Adheres strictly to FIRST principles and AAA pattern.
"""

import json
import os
import sys
import pytest

_SIM_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "simulator"))
if _SIM_DIR not in sys.path:
    sys.path.insert(0, _SIM_DIR)

from services.route_service import load_hub_routes


@pytest.mark.unit
class TestRouteService:
    """Unit test suite for route loading service."""

    def test_load_routes_from_custom_path(self, tmp_path):
        """Custom path should take precedence and load valid JSON routes."""
        # Arrange
        route_file = tmp_path / "custom_routes.json"
        mock_data = {"Hub-Aurora": {"waypoints": [[41.88, -87.65], [41.76, -88.32]]}}
        route_file.write_text(json.dumps(mock_data), encoding="utf-8")

        # Act
        result = load_hub_routes(custom_path=str(route_file))

        # Assert
        assert "Hub-Aurora" in result
        assert len(result["Hub-Aurora"]["waypoints"]) == 2

    def test_load_routes_returns_empty_dict_when_all_missing(self, monkeypatch):
        """When no route files exist, returns empty dict gracefully."""
        # Arrange
        monkeypatch.setenv("ROUTES_FILE", "/nonexistent/path/routes.json")

        # Act
        result = load_hub_routes(custom_path="/another/missing/path.json", search_defaults=False)

        # Assert
        assert result == {}

    def test_load_routes_handles_corrupted_json_gracefully(self, tmp_path):
        """Corrupted JSON should be caught without raising an unhandled exception."""
        # Arrange
        corrupt_file = tmp_path / "corrupt.json"
        corrupt_file.write_text("{ incomplete json ...", encoding="utf-8")

        # Act
        result = load_hub_routes(custom_path=str(corrupt_file), search_defaults=False)

        # Assert: Gracefully handled, returns empty dict
        assert result == {}
