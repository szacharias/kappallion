"""Domain models and core business entities for vehicle simulation."""
from .models import Coordinate, DoorStatus, RouteContext, SensorTelemetry, TelemetryPayload, VehicleState

__all__ = [
    "Coordinate",
    "DoorStatus",
    "RouteContext",
    "SensorTelemetry",
    "TelemetryPayload",
    "VehicleState",
]
