"""
navigation.py
Pure navigation, waypoint traversal, and bearing trigonometry functions.
Adheres strictly to GEMINI.md: pure functions, low arity, no side effects.
"""

import math
import random
from typing import List, Tuple

LANE_CHOICE_JITTER_MAX: float = 0.00006  # ~5m lane placement on highways
DISTANCE_ARRIVAL_THRESHOLD: float = 0.008

MOMENTUM_WEIGHT: float = 0.65
STEERING_WEIGHT: float = 0.35


def compute_initial_heading(start_lat: float, start_lon: float, dest_lat: float, dest_lon: float) -> float:
    """Compute initial heading angle in radians from start to destination."""
    return math.atan2(dest_lat - start_lat, dest_lon - start_lon)


def step_road_waypoints(
    waypoints: List[List[float]],
    current_idx: int,
    direction: int,
    step_size: int,
) -> Tuple[int, int, float, float]:
    """
    Advance position along high-resolution road waypoints.
    Automatically turns around at route endpoints (hub or base).

    Returns:
        (new_idx, new_direction, latitude, longitude)
    """
    total_points = len(waypoints)
    new_idx = current_idx + (direction * step_size)

    if new_idx >= total_points - 1:
        new_idx = total_points - 1
        direction = -1  # Reached hub -> reverse to return to base
    elif new_idx <= 0:
        new_idx = 0
        direction = 1   # Reached base -> reverse to head out to hub

    base_pt = waypoints[new_idx]
    # Add realistic multi-lane micro-placement jitter
    lat = base_pt[0] + random.uniform(-LANE_CHOICE_JITTER_MAX, LANE_CHOICE_JITTER_MAX)
    lon = base_pt[1] + random.uniform(-LANE_CHOICE_JITTER_MAX, LANE_CHOICE_JITTER_MAX)
    return new_idx, direction, lat, lon


def step_vector_heading(
    lat: float,
    lon: float,
    dest_lat: float,
    dest_lon: float,
    heading: float,
    speed: float,
) -> Tuple[float, float, float, bool]:
    """
    Advance position using synthetic vector heading with +-45 deg organic turns.

    Returns:
        (new_lat, new_lon, new_heading, reached_destination)
    """
    d_lat = dest_lat - lat
    d_lon = dest_lon - lon
    dist = math.sqrt(d_lat**2 + d_lon**2)

    if dist > DISTANCE_ARRIVAL_THRESHOLD:
        target_heading = math.atan2(d_lat, d_lon)
        diff = (target_heading - heading + math.pi) % (2 * math.pi) - math.pi
        rand_turn = random.uniform(-math.pi / 4, math.pi / 4)
        new_heading = heading + (rand_turn * MOMENTUM_WEIGHT) + (diff * STEERING_WEIGHT)

        new_lat = lat + math.sin(new_heading) * speed
        new_lon = lon + math.cos(new_heading) * speed
        return new_lat, new_lon, new_heading, False
    else:
        # Reached destination boundary
        return lat, lon, heading, True
