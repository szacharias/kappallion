"""
test_anomaly_rules.py
Unit tests for cold-chain anomaly detection and threshold evaluation rules.
Target: docker/dashboard/domain/anomaly_rules.py
Adheres strictly to GEMINI.md Clean Code guidelines and FIRST principles.
"""

import importlib.util
import os
import pytest

_candidates = [
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "dashboard", "domain", "anomaly_rules.py")),
    "/dashboard/domain/anomaly_rules.py",
]
_target_file = next((p for p in _candidates if os.path.exists(p)), None)
if not _target_file:
    raise ImportError(f"Could not locate anomaly_rules.py in candidates: {_candidates}")

spec = importlib.util.spec_from_file_location("anomaly_rules", _target_file)
anomaly_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(anomaly_mod)

CRITICAL_CARGO_TEMP_C = anomaly_mod.CRITICAL_CARGO_TEMP_C
CRITICAL_VIBRATION_G = anomaly_mod.CRITICAL_VIBRATION_G
DOOR_OPEN_STATUS = anomaly_mod.DOOR_OPEN_STATUS
is_anomaly = anomaly_mod.is_anomaly
get_anomaly_reasons = anomaly_mod.get_anomaly_reasons
format_anomaly_description = anomaly_mod.format_anomaly_description


@pytest.mark.unit
class TestAnomalyRules:
    """Unit test suite for cold-chain anomaly rules."""

    def test_normal_telemetry_is_not_anomaly(self):
        """Standard operating parameters return False with no reasons."""
        # Arrange
        temp = 3.5
        door = "CLOSED"
        vib = 0.5
        condensation = False

        # Act
        anomaly = is_anomaly(temp, door, vib, condensation)
        desc = format_anomaly_description(temp, door, vib, condensation)

        # Assert
        assert anomaly is False
        assert desc == "Normal Cold-Chain Conditions"

    def test_temperature_breach_detected(self):
        """Cargo temperature exceeding 8.0C triggers anomaly."""
        # Arrange
        temp = 8.5
        door = "CLOSED"
        vib = 0.2

        # Act
        anomaly = is_anomaly(temp, door, vib)
        reasons = get_anomaly_reasons(temp, door, vib)

        # Assert
        assert anomaly is True
        assert any("High Temp" in r for r in reasons)
        assert any("8.5" in r for r in reasons)

    def test_door_open_breach_detected(self):
        """Door open in transit triggers immediate anomaly."""
        # Arrange
        temp = 3.0
        door = "OPEN"
        vib = 0.1

        # Act
        anomaly = is_anomaly(temp, door, vib)
        reasons = get_anomaly_reasons(temp, door, vib)

        # Assert
        assert anomaly is True
        assert "Door OPEN in Transit" in reasons

    def test_vibration_shock_breach_detected(self):
        """Vibration exceeding 3.0G triggers road shock anomaly."""
        # Arrange
        temp = 4.0
        door = "CLOSED"
        vib = 4.2

        # Act
        anomaly = is_anomaly(temp, door, vib)
        reasons = get_anomaly_reasons(temp, door, vib)

        # Assert
        assert anomaly is True
        assert any("Road Shock" in r for r in reasons)
        assert any("4.2" in r for r in reasons)

    def test_condensation_risk_detected(self):
        """Condensation risk flag triggers anomaly."""
        # Arrange
        temp = 2.0
        door = "CLOSED"
        vib = 0.3
        condensation = True

        # Act
        anomaly = is_anomaly(temp, door, vib, condensation)
        reasons = get_anomaly_reasons(temp, door, vib, condensation)

        # Assert
        assert anomaly is True
        assert "Condensation Risk" in reasons

    def test_multiple_simultaneous_breaches(self):
        """Compound breaches format all active violation descriptions."""
        # Arrange
        temp = 11.2
        door = "OPEN"
        vib = 3.8
        condensation = True

        # Act
        reasons = get_anomaly_reasons(temp, door, vib, condensation)
        desc = format_anomaly_description(temp, door, vib, condensation)

        # Assert
        assert len(reasons) == 4
        assert "High Temp" in desc
        assert "Door OPEN" in desc
        assert "Road Shock" in desc
        assert "Condensation Risk" in desc

    def test_boundary_conditions(self):
        """Values exactly at threshold are not breaches; values slightly above are."""
        # Exact threshold: 8.0C is not > 8.0
        assert is_anomaly(8.0, "CLOSED", 3.0, False) is False
        # Slightly above threshold
        assert is_anomaly(8.01, "CLOSED", 3.0, False) is True
        assert is_anomaly(8.0, "CLOSED", 3.01, False) is True
