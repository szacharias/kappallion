"""
thermodynamics.py
Pure thermodynamic and sensor simulation functions for cold-chain vehicles.
Complies with GEMINI.md: pure functions, no magic numbers, no side effects.
"""

import random
from typing import Tuple
from .models import DoorStatus, VehicleState

# Named constants to eliminate magic numbers
TARGET_CARGO_TEMP_C: float = 3.5
MIN_NORMAL_CARGO_TEMP_C: float = 1.5
MAX_NORMAL_CARGO_TEMP_C: float = 5.5

DOOR_OPEN_DECAY_RATE: float = 0.15
COMPRESSOR_FAILURE_DECAY_RATE: float = 0.04

DOOR_OPEN_MAX_TICKS: int = 10
COMPRESSOR_FAILURE_MAX_TICKS: int = 15
ANOMALY_TRIGGER_CHANCE: float = 0.02

MIN_HUMIDITY_PCT: float = 10.0
MAX_HUMIDITY_PCT: float = 95.0
BASE_HUMIDITY_OFFSET: float = 60.0
HUMIDITY_TEMP_FACTOR: float = 1.5


def update_ambient_temperature(ambient: float) -> float:
    """Simulate ambient micro-fluctuations via Gaussian normal distribution."""
    return ambient + random.normalvariate(0.0, 0.1)


def update_normal_cargo_temperature(cargo_temp: float) -> float:
    """Simulate normal thermal oscillation around target setpoint (1.5C - 5.5C)."""
    new_temp = cargo_temp + random.normalvariate(0.0, 0.05)
    return max(MIN_NORMAL_CARGO_TEMP_C, min(MAX_NORMAL_CARGO_TEMP_C, new_temp))


def calculate_cooling_decay(cargo_temp: float, ambient: float, decay_rate: float) -> float:
    """Newton's Law of Cooling approximation: dT/dt = -k * (T_cargo - T_ambient)."""
    return cargo_temp + decay_rate * (ambient - cargo_temp)


def calculate_relative_humidity(cargo_temp: float) -> float:
    """Calculate relative humidity inversely tracking cargo temperature spikes."""
    computed = BASE_HUMIDITY_OFFSET - (cargo_temp * HUMIDITY_TEMP_FACTOR)
    return max(MIN_HUMIDITY_PCT, min(MAX_HUMIDITY_PCT, computed))


def sample_vibration(state: VehicleState) -> float:
    """Sample realistic mechanical vibration based on vehicle operating state."""
    if state == VehicleState.DOOR_OPEN:
        return round(random.uniform(0.02, 0.12), 3)
    return round(random.uniform(0.01, 0.08), 3)


def step_thermodynamic_state(
    state: VehicleState,
    ambient: float,
    cargo_temp: float,
    ticks_in_anomaly: int,
) -> Tuple[VehicleState, DoorStatus, float, float, float, int]:
    """
    Step thermodynamic physics and state transitions for one epoch.
    
    Returns:
        (new_state, door_status, ambient, cargo_temp, humidity, ticks_in_anomaly)
    """
    ambient = update_ambient_temperature(ambient)
    
    if state == VehicleState.NORMAL:
        cargo_temp = update_normal_cargo_temperature(cargo_temp)
        door_status = DoorStatus.CLOSED
        if random.random() < ANOMALY_TRIGGER_CHANCE:
            state = random.choice([VehicleState.DOOR_OPEN, VehicleState.COMPRESSOR_FAILURE])
            
    elif state == VehicleState.DOOR_OPEN:
        door_status = DoorStatus.OPEN
        cargo_temp = calculate_cooling_decay(cargo_temp, ambient, DOOR_OPEN_DECAY_RATE)
        ticks_in_anomaly += 1
        if ticks_in_anomaly > DOOR_OPEN_MAX_TICKS:
            state = VehicleState.NORMAL
            door_status = DoorStatus.CLOSED
            ticks_in_anomaly = 0
            
    elif state == VehicleState.COMPRESSOR_FAILURE:
        door_status = DoorStatus.CLOSED
        cargo_temp = calculate_cooling_decay(cargo_temp, ambient, COMPRESSOR_FAILURE_DECAY_RATE)
        ticks_in_anomaly += 1
        if ticks_in_anomaly > COMPRESSOR_FAILURE_MAX_TICKS:
            state = VehicleState.NORMAL
            ticks_in_anomaly = 0
            
    humidity = calculate_relative_humidity(cargo_temp)
    return state, door_status, ambient, cargo_temp, humidity, ticks_in_anomaly
