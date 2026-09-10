"""Unit tests for navigation, road waypoint traversal, and bearing trigonometry.

Target module: docker/simulator/domain/navigation.py
Adheres strictly to FIRST principles and AAA pattern.
"""

import math
import os
import sys
import pytest

_SIM_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "simulator"))
if _SIM_DIR not in sys.path:
    sys.path.insert(0, _SIM_DIR)

from domain.navigation import (
    LANE_CHOICE_JITTER_MAX,
    compute_initial_heading,
    step_road_waypoints,
    step_vector_heading,
)


@pytest.mark.unit
class TestNavigation:
    """Unit test suite for navigation functions."""

    def test_initial_heading_calculation(self):
        """Initial heading should point directly toward destination."""
        # Moving due North (lat increases, lon constant) -> pi / 2
        heading_north = compute_initial_heading(41.0, -87.0, 42.0, -87.0)
        assert heading_north == pytest.approx(math.pi / 2, abs=0.001)

    def test_step_road_waypoints_advancement_and_reversal(self):
        """Waypoints advance by step_size and reverse at endpoints."""
        # Arrange
        waypoints = [
            [41.0, -87.0],
            [41.1, -87.1],
            [41.2, -87.2],
            [41.3, -87.3],
            [41.4, -87.4],
        ]
        curr_idx = 0
        direction = 1
        step_size = 2

        # Act: Step forward
        idx_1, dir_1, lat_1, lon_1 = step_road_waypoints(waypoints, curr_idx, direction, step_size)

        # Assert: advanced from 0 to 2
        assert idx_1 == 2
        assert dir_1 == 1
        assert lat_1 == pytest.approx(41.2, abs=LANE_CHOICE_JITTER_MAX * 1.5)

        # Act: Step to end -> should clamp to 4 and flip direction to -1
        idx_2, dir_2, lat_2, lon_2 = step_road_waypoints(waypoints, idx_1, dir_1, step_size=3)
        assert idx_2 == 4
        assert dir_2 == -1

    def test_step_vector_heading_destination_reached(self):
        """When within threshold distance, destination_reached flag must be True."""
        # Arrange: Points separated by only 0.002 (< 0.008 threshold)
        lat, lon = 41.880, -87.650
        dest_lat, dest_lon = 41.881, -87.651
        heading = 0.0
        speed = 0.005

        # Act
        new_lat, new_lon, new_heading, reached = step_vector_heading(
            lat, lon, dest_lat, dest_lon, heading, speed
        )

        # Assert
        assert reached is True
