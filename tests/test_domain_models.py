"""Unit tests for domain models and data contracts.

Target module: docker/simulator/domain/models.py
Adheres strictly to FIRST principles and AAA pattern.
"""

import os
import sys
import pytest

_SIM_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "simulator"))
if _SIM_DIR not in sys.path:
    sys.path.insert(0, _SIM_DIR)

from domain.models import (
    Coordinate,
    DoorStatus,
    RouteContext,
    SensorTelemetry,
    TelemetryPayload,
    VehicleState,
)


@pytest.mark.unit
class TestDomainModels:
    """Unit test suite for domain models and contracts."""

    def test_door_status_enum_values(self):
        """DoorStatus Enum must define OPEN and CLOSED."""
        assert DoorStatus.OPEN.value == "OPEN"
        assert DoorStatus.CLOSED.value == "CLOSED"

    def test_vehicle_state_enum_values(self):
        """VehicleState Enum must define NORMAL, DOOR_OPEN, COMPRESSOR_FAILURE."""
        assert VehicleState.NORMAL.value == "NORMAL"
        assert VehicleState.DOOR_OPEN.value == "DOOR_OPEN"
        assert VehicleState.COMPRESSOR_FAILURE.value == "COMPRESSOR_FAILURE"

    def test_coordinate_immutability_and_conversion(self):
        """Coordinate dataclass must be frozen and convert cleanly to [lat, lon]."""
        # Arrange
        coord = Coordinate(latitude=41.887529, longitude=-87.651341)

        # Act & Assert
        assert coord.to_list() == [41.88753, -87.65134]
        with pytest.raises(AttributeError):
            coord.latitude = 42.0  # Must be immutable

    def test_telemetry_payload_to_dict_matches_kafka_schema(self):
        """TelemetryPayload.to_dict() must match the exact streaming JSON contract."""
        # Arrange
        route_ctx = RouteContext(
            origin="Base Warehouse",
            destination="Hub-Aurora",
            latitude=41.8856,
            longitude=-87.6510,
        )
        sensors = SensorTelemetry(
            ambientTempC=25.2,
            cargoContainerTempC=3.4,
            relativeHumidityPct=54.5,
            vibrationG=0.035,
            doorStatus=DoorStatus.CLOSED.value,
        )
        payload = TelemetryPayload(
            eventId="123e4567-e89b-12d3-a456-426614174000",
            vehicleId="TRK-CHI-0001",
            timestamp="2026-09-09 21:00:00",
            routeContext=route_ctx,
            telemetry=sensors,
        )

        # Act
        payload_dict = payload.to_dict()

        # Assert
        assert payload_dict["eventId"] == "123e4567-e89b-12d3-a456-426614174000"
        assert payload_dict["vehicleId"] == "TRK-CHI-0001"
        assert payload_dict["routeContext"]["origin"] == "Base Warehouse"
        assert payload_dict["routeContext"]["destination"] == "Hub-Aurora"
        assert payload_dict["routeContext"]["latitude"] == 41.8856
        assert payload_dict["telemetry"]["ambientTempC"] == 25.2
        assert payload_dict["telemetry"]["cargoContainerTempC"] == 3.4
        assert payload_dict["telemetry"]["doorStatus"] == "CLOSED"
