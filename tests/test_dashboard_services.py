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


# Safe loader for lakehouse_service with mock fallbacks
from unittest.mock import MagicMock

if "streamlit" not in sys.modules:
    sys.modules["streamlit"] = MagicMock()
if "pandas" not in sys.modules:
    sys.modules["pandas"] = MagicMock()

_lakehouse_candidates = [
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "dashboard", "services", "lakehouse_service.py")),
    "/dashboard/services/lakehouse_service.py",
]
_lh_target = next((p for p in _lakehouse_candidates if os.path.exists(p)), None)
if _lh_target:
    lh_spec = importlib.util.spec_from_file_location("lakehouse_service", _lh_target)
    lakehouse_srv = importlib.util.module_from_spec(lh_spec)
    lh_spec.loader.exec_module(lakehouse_srv)
    compute_stream_stats = lakehouse_srv.compute_stream_stats


class DummySeries:
    """Lightweight pure-python Series stub for isolated testing."""
    def __init__(self, values):
        self._values = values

    @property
    def iloc(self):
        return self._values


class DummyDF:
    """Lightweight pure-python DataFrame stub for isolated testing."""
    def __init__(self, data=None, length=0):
        self._data = data or {}
        self.columns = list(self._data.keys())
        self.empty = length == 0
        self._length = length

    def __getitem__(self, item):
        return DummySeries(self._data[item])

    def __len__(self):
        return self._length


@pytest.mark.unit
class TestLakehouseStreamStatsService:
    """Unit test suite for compute_stream_stats in lakehouse_service."""

    def test_compute_stream_stats_with_empty_dfs(self):
        """Empty DataFrames produce zero counts and 'N/A' timestamps without error."""
        # Arrange
        empty_bronze = DummyDF(length=0)
        empty_silver = DummyDF(length=0)
        empty_gold = DummyDF(length=0)

        # Act
        stats = compute_stream_stats(empty_bronze, empty_silver, empty_gold)

        # Assert
        assert stats["bronze_count"] == 0
        assert stats["bronze_offset"] == 0
        assert stats["bronze_latest"] == "N/A"
        assert stats["silver_count"] == 0
        assert stats["silver_latest"] == "N/A"
        assert stats["gold_count"] == 0
        assert stats["gold_latest"] == "N/A"

    def test_compute_stream_stats_with_populated_dfs(self):
        """Populated DataFrames accurately extract offset, counts, and timestamps."""
        # Arrange
        bronze_df = DummyDF(
            data={"offset": [42], "timestamp": ["2026-09-10 03:00:00"]},
            length=1,
        )
        silver_df = DummyDF(
            data={"Timestamp": ["2026-09-10 03:00:05"]},
            length=150,
        )
        gold_df = DummyDF(
            data={"Window_End": ["2026-09-10 03:01:00"]},
            length=35,
        )

        # Act
        stats = compute_stream_stats(bronze_df, silver_df, gold_df)

        # Assert
        assert stats["bronze_offset"] == 42
        assert stats["bronze_count"] == 43
        assert stats["bronze_latest"] == "2026-09-10 03:00:00"
        assert stats["silver_count"] == 150
        assert stats["silver_latest"] == "2026-09-10 03:00:05"
        assert stats["gold_count"] == 35
        assert stats["gold_latest"] == "2026-09-10 03:01:00"

    def test_compute_stream_stats_handles_missing_columns_gracefully(self):
        """DataFrames missing expected columns fall back cleanly without raising KeyError."""
        # Arrange
        malformed_bronze = DummyDF(data={"unexpected_col": [1]}, length=1)
        malformed_silver = DummyDF(data={"other_col": ["val"]}, length=10)
        malformed_gold = DummyDF(data={}, length=0)

        # Act
        stats = compute_stream_stats(malformed_bronze, malformed_silver, malformed_gold)

        # Assert
        assert stats["bronze_count"] == 0
        assert stats["bronze_offset"] == 0
        assert stats["bronze_latest"] == "N/A"
        assert stats["silver_count"] == 10
        assert stats["silver_latest"] == "N/A"
        assert stats["gold_count"] == 0
        assert stats["gold_latest"] == "N/A"

