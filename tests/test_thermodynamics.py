"""Unit tests for thermodynamics physics and state machine transitions.

Target module: docker/simulator/domain/thermodynamics.py
Adheres strictly to FIRST principles and AAA pattern.
"""

import os
import sys
from unittest.mock import patch
import pytest

_SIM_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "simulator"))
if _SIM_DIR not in sys.path:
    sys.path.insert(0, _SIM_DIR)

from domain.models import DoorStatus, VehicleState
from domain.thermodynamics import (
    calculate_cooling_decay,
    calculate_relative_humidity,
    sample_vibration,
    step_thermodynamic_state,
    update_ambient_temperature,
    update_normal_cargo_temperature,
)


@pytest.mark.unit
class TestThermodynamics:
    """Unit tests for thermodynamic functions and physics calculations."""

    def test_ambient_temperature_fluctuation(self):
        """Ambient temperature should fluctuate with mean zero."""
        # Arrange
        initial = 25.0

        # Act
        with patch("random.normalvariate", return_value=0.08):
            result = update_ambient_temperature(initial)

        # Assert
        assert result == pytest.approx(25.08, abs=0.001)

    def test_normal_cargo_temp_clamping(self):
        """Normal cargo temp must stay bounded within [1.5, 5.5]°C."""
        # Arrange & Act
        with patch("random.normalvariate", return_value=10.0):
            clamped_high = update_normal_cargo_temperature(5.0)
        with patch("random.normalvariate", return_value=-10.0):
            clamped_low = update_normal_cargo_temperature(2.0)

        # Assert
        assert clamped_high == 5.5
        assert clamped_low == 1.5

    def test_newton_cooling_decay(self):
        """Cooling decay should draw cargo temperature towards ambient."""
        # Arrange
        cargo = 4.0
        ambient = 30.0
        rate = 0.15

        # Act
        decayed = calculate_cooling_decay(cargo, ambient, rate)

        # Assert: dT = 0.15 * (30 - 4) = 3.9 -> 7.9
        assert decayed == pytest.approx(7.9, abs=0.01)

    def test_humidity_inverse_calculation_and_bounds(self):
        """Humidity inversely tracks cargo temperature and clamps between 10% and 95%."""
        # At 0C: 60 - 0 = 60%
        assert calculate_relative_humidity(0.0) == 60.0
        # At 10C: 60 - 15 = 45%
        assert calculate_relative_humidity(10.0) == 45.0
        # Very cold -> clamped at 95%
        assert calculate_relative_humidity(-30.0) == 95.0
        # Very hot -> clamped at 10%
        assert calculate_relative_humidity(40.0) == 10.0

    def test_door_open_anomaly_lifecycle(self):
        """Door open state must decay rapidly and auto-resolve after 10 ticks."""
        # Arrange
        state = VehicleState.DOOR_OPEN
        ambient = 28.0
        cargo = 4.0
        ticks = 10

        # Act: 11th tick should resolve back to NORMAL
        new_state, door, amb, new_cargo, hum, new_ticks = step_thermodynamic_state(
            state, ambient, cargo, ticks
        )

        # Assert
        assert new_state == VehicleState.NORMAL
        assert door == DoorStatus.CLOSED
        assert new_ticks == 0
        assert new_cargo > 4.0
