"""
anomaly_rules.py
Pure domain business rules for evaluating cold-chain anomalies and safety thresholds.
Adheres strictly to GEMINI.md Clean Code guidelines (pure Python, zero I/O).
"""

from typing import List

# Named domain constants eliminating magic numbers
CRITICAL_CARGO_TEMP_C: float = 8.0
CRITICAL_VIBRATION_G: float = 3.0
DOOR_OPEN_STATUS: str = "OPEN"


def is_anomaly(
    cargo_temp: float,
    door_status: str,
    vibration: float,
    condensation_risk: bool = False,
) -> bool:
    """Determine if telemetry readings violate cold-chain safety thresholds."""
    return (
        cargo_temp > CRITICAL_CARGO_TEMP_C
        or door_status == DOOR_OPEN_STATUS
        or vibration > CRITICAL_VIBRATION_G
        or bool(condensation_risk)
    )


def get_anomaly_reasons(
    cargo_temp: float,
    door_status: str,
    vibration: float,
    condensation_risk: bool = False,
) -> List[str]:
    """Return specific threshold violation descriptions."""
    reasons: List[str] = []
    if cargo_temp > CRITICAL_CARGO_TEMP_C:
        reasons.append(f"High Temp ({cargo_temp:.1f}°C > {CRITICAL_CARGO_TEMP_C:.1f}°C)")
    if door_status == DOOR_OPEN_STATUS:
        reasons.append("Door OPEN in Transit")
    if vibration > CRITICAL_VIBRATION_G:
        reasons.append(f"Road Shock ({vibration:.1f}G > {CRITICAL_VIBRATION_G:.1f}G)")
    if condensation_risk:
        reasons.append("Condensation Risk")
    return reasons


def format_anomaly_description(
    cargo_temp: float,
    door_status: str,
    vibration: float,
    condensation_risk: bool = False,
) -> str:
    """Format human-readable summary of active anomalies for tooltips and badges."""
    reasons = get_anomaly_reasons(cargo_temp, door_status, vibration, condensation_risk)
    return " | ".join(reasons) if reasons else "Normal Cold-Chain Conditions"
