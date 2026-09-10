"""
models.py
Domain models and typed data contracts for the cold-chain telemetry platform.
Strictly decoupled from frameworks, messaging queues, or storage engines.
"""

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict


class DoorStatus(str, Enum):
    """Physical door sensor states."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"


class VehicleState(str, Enum):
    """Internal operating state for anomaly modeling."""
    NORMAL = "NORMAL"
    DOOR_OPEN = "DOOR_OPEN"
    COMPRESSOR_FAILURE = "COMPRESSOR_FAILURE"


@dataclass(frozen=True)
class Coordinate:
    """Geographic coordinate on Earth (WGS84)."""
    latitude: float
    longitude: float

    def to_list(self) -> list[float]:
        """Return [lat, lon] tuple as a list."""
        return [round(self.latitude, 5), round(self.longitude, 5)]


@dataclass(frozen=True)
class RouteContext:
    """Contextual transit information of a moving vehicle."""
    origin: str
    destination: str
    latitude: float
    longitude: float


@dataclass(frozen=True)
class SensorTelemetry:
    """Physical sensor readings from cold-chain telemetry instrumentation."""
    ambientTempC: float
    cargoContainerTempC: float
    relativeHumidityPct: float
    vibrationG: float
    doorStatus: str


@dataclass(frozen=True)
class TelemetryPayload:
    """Complete message contract published to the streaming lakehouse."""
    eventId: str
    vehicleId: str
    timestamp: str
    routeContext: RouteContext
    telemetry: SensorTelemetry

    def to_dict(self) -> Dict[str, Any]:
        """Convert the typed payload to an exact JSON-compatible dictionary."""
        return {
            "eventId": self.eventId,
            "vehicleId": self.vehicleId,
            "timestamp": self.timestamp,
            "routeContext": asdict(self.routeContext),
            "telemetry": asdict(self.telemetry),
        }
