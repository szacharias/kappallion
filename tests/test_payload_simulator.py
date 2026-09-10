"""Unit test suite for cold-chain IoT telemetry simulator.

Module under test: docker/simulator/payload_simulator.py
Following Clean Coder FIRST principles and the AAA (Arrange-Act-Assert) pattern.
Zero live dependencies: all external files and random distributions are deterministic or mocked.
"""

import json
import math
import os
import sys
import uuid
from unittest.mock import MagicMock, mock_open, patch

import pytest

# Ensure simulator module is discoverable across both repository root and container /app layouts
_SIM_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docker", "simulator"))
if os.path.exists(_SIM_DIR) and _SIM_DIR not in sys.path:
    sys.path.insert(0, _SIM_DIR)
_APP_DIR = "/app"
if os.path.exists(_APP_DIR) and _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from payload_simulator import TruckSimulator, get_target_fleet_size, load_routes


# ============================================================================
# Test Class 1: TruckSimulator Initialization
# ============================================================================
@pytest.mark.unit
class TestTruckSimulatorInitialization:
    """Unit tests for TruckSimulator initialization with and without road waypoints."""

    def test_initialization_with_road_waypoints(self):
        """TruckSimulator initialized with valid road waypoints should enable road mode."""
        # Arrange
        vehicle_id = "TRK-CHI-0001"
        origin = "Base Warehouse"
        destination = "Hub-Naperville"
        base_lat, base_lon = 41.8875, -87.6513
        dest_lat, dest_lon = 41.7508, -88.1535
        waypoints = [
            [41.8875, -87.6513],
            [41.8500, -87.8000],
            [41.8000, -87.9500],
            [41.7508, -88.1535],
        ]
        start_offset = 1

        # Act
        truck = TruckSimulator(
            vehicle_id=vehicle_id,
            origin=origin,
            destination=destination,
            start_lat=base_lat,
            start_lon=base_lon,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            waypoints=waypoints,
            start_offset=start_offset,
        )

        # Assert
        assert truck.vehicle_id == vehicle_id
        assert truck.origin == origin
        assert truck.destination == destination
        assert truck.use_roads is True
        assert truck.waypoints == waypoints
        assert truck.waypoint_idx == 1
        assert truck.direction == 1
        assert 3 <= truck.step_size <= 6
        assert truck.lat == waypoints[1][0]
        assert truck.lon == waypoints[1][1]
        assert truck.ambient_temp == 25.0
        assert truck.cargo_temp == 3.5
        assert truck.humidity == 50.0
        assert truck.door_status == "CLOSED"
        assert truck.state == "NORMAL"
        assert truck.ticks_in_anomaly == 0

    def test_initialization_with_road_waypoints_offset_clamping(self):
        """TruckSimulator should clamp start_offset to len(waypoints) - 1 if offset is out of range."""
        # Arrange
        waypoints = [
            [41.8875, -87.6513],
            [41.8000, -87.9500],
            [41.7508, -88.1535],
        ]
        start_offset = 99  # Far exceeds length of 3

        # Act
        truck = TruckSimulator(
            vehicle_id="TRK-CLAMP-01",
            origin="Base",
            destination="Hub",
            start_lat=41.8875,
            start_lon=-87.6513,
            dest_lat=41.7508,
            dest_lon=-88.1535,
            waypoints=waypoints,
            start_offset=start_offset,
        )

        # Assert
        assert truck.waypoint_idx == 2  # Clamped to last index (3 - 1)
        assert truck.lat == waypoints[2][0]
        assert truck.lon == waypoints[2][1]

    def test_initialization_without_waypoints_defaults_to_synthetic_vector(self):
        """TruckSimulator initialized with waypoints=None should use synthetic vector movement."""
        # Arrange
        vehicle_id = "TRK-VEC-0001"
        start_lat, start_lon = 41.8875, -87.6513
        dest_lat, dest_lon = 41.7508, -88.1535
        expected_heading = math.atan2(dest_lat - start_lat, dest_lon - start_lon)

        # Act
        truck = TruckSimulator(
            vehicle_id=vehicle_id,
            origin="Base Warehouse",
            destination="Hub-Naperville",
            start_lat=start_lat,
            start_lon=start_lon,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            waypoints=None,
        )

        # Assert
        assert truck.use_roads is False
        assert truck.waypoints is None
        assert truck.lat == start_lat
        assert truck.lon == start_lon
        assert 0.005 <= truck.speed_factor <= 0.009
        assert math.isclose(truck.heading, expected_heading, rel_tol=1e-6)

    @pytest.mark.parametrize("invalid_waypoints", [None, [], [[41.8875, -87.6513]]])
    def test_initialization_with_empty_or_single_waypoint_falls_back_to_vector(
        self, invalid_waypoints
    ):
        """TruckSimulator should fall back to synthetic vector when waypoints is None, empty, or single point."""
        # Arrange
        start_lat, start_lon = 41.8875, -87.6513
        dest_lat, dest_lon = 41.7508, -88.1535

        # Act
        truck = TruckSimulator(
            vehicle_id="TRK-FALLBACK",
            origin="Base",
            destination="Hub",
            start_lat=start_lat,
            start_lon=start_lon,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            waypoints=invalid_waypoints,
        )

        # Assert
        assert truck.use_roads is False
        assert truck.waypoints is None
        assert truck.lat == start_lat
        assert truck.lon == start_lon


# ============================================================================
# Test Class 2: Waypoint Traversal & Direction Reversal
# ============================================================================
@pytest.mark.unit
class TestWaypointTraversalAndReversal:
    """Unit tests for waypoint stepping, progression, and boundary reversal logic."""

    @pytest.fixture
    def synthetic_route(self):
        """Provide a synthetic 10-point linear waypoint route."""
        return [[41.0 + (i * 0.01), -87.0 - (i * 0.01)] for i in range(10)]

    def test_waypoint_forward_stepping(self, synthetic_route):
        """Truck in road mode should advance waypoint_idx by step_size when moving outbound."""
        # Arrange
        truck = TruckSimulator(
            vehicle_id="TRK-STEP",
            origin="Base",
            destination="Hub",
            start_lat=41.0,
            start_lon=-87.0,
            dest_lat=41.09,
            dest_lon=-87.09,
            waypoints=synthetic_route,
            start_offset=0,
        )
        truck.step_size = 4
        truck.direction = 1

        # Act
        truck.update_telemetry()

        # Assert
        assert truck.waypoint_idx == 4
        assert truck.direction == 1
        expected_lat, expected_lon = synthetic_route[4]
        # Coordinates should snap to waypoint 4 within multi-lane offset (+/- 0.00006)
        assert abs(truck.lat - expected_lat) <= 0.00007
        assert abs(truck.lon - expected_lon) <= 0.00007

    def test_destination_reached_reverses_direction_to_inbound(self, synthetic_route):
        """When advancing reaches or exceeds the final waypoint, index clamps to end and direction reverses to -1."""
        # Arrange
        truck = TruckSimulator(
            vehicle_id="TRK-REVERSE-END",
            origin="Base",
            destination="Hub",
            start_lat=41.0,
            start_lon=-87.0,
            dest_lat=41.09,
            dest_lon=-87.09,
            waypoints=synthetic_route,
            start_offset=8,
        )
        truck.step_size = 5  # 8 + 5 = 13 >= 9
        truck.direction = 1

        # Act
        truck.update_telemetry()

        # Assert
        max_idx = len(synthetic_route) - 1  # 9
        assert truck.waypoint_idx == max_idx
        assert truck.direction == -1
        expected_lat, expected_lon = synthetic_route[max_idx]
        assert abs(truck.lat - expected_lat) <= 0.00007
        assert abs(truck.lon - expected_lon) <= 0.00007

    def test_waypoint_inbound_stepping_towards_base(self, synthetic_route):
        """When returning to base (direction = -1), waypoint_idx should decrement by step_size."""
        # Arrange
        truck = TruckSimulator(
            vehicle_id="TRK-RETURN",
            origin="Base",
            destination="Hub",
            start_lat=41.0,
            start_lon=-87.0,
            dest_lat=41.09,
            dest_lon=-87.09,
            waypoints=synthetic_route,
            start_offset=7,
        )
        truck.step_size = 3
        truck.direction = -1

        # Act
        truck.update_telemetry()

        # Assert
        assert truck.waypoint_idx == 4  # 7 - 3 = 4
        assert truck.direction == -1
        expected_lat, expected_lon = synthetic_route[4]
        assert abs(truck.lat - expected_lat) <= 0.00007
        assert abs(truck.lon - expected_lon) <= 0.00007

    def test_base_reached_reverses_direction_to_outbound(self, synthetic_route):
        """When returning to base reaches or exceeds index 0, index clamps to 0 and direction reverses to 1."""
        # Arrange
        truck = TruckSimulator(
            vehicle_id="TRK-REVERSE-BASE",
            origin="Base",
            destination="Hub",
            start_lat=41.0,
            start_lon=-87.0,
            dest_lat=41.09,
            dest_lon=-87.09,
            waypoints=synthetic_route,
            start_offset=2,
        )
        truck.step_size = 4  # 2 - 4 = -2 <= 0
        truck.direction = -1

        # Act
        truck.update_telemetry()

        # Assert
        assert truck.waypoint_idx == 0
        assert truck.direction == 1
        expected_lat, expected_lon = synthetic_route[0]
        assert abs(truck.lat - expected_lat) <= 0.00007
        assert abs(truck.lon - expected_lon) <= 0.00007

    def test_waypoint_multilane_choice_offset_within_bounds(self, synthetic_route):
        """Multi-lane coordinate jitter must stay within +/- 0.00006 degrees of the road waypoint."""
        # Arrange
        truck = TruckSimulator(
            vehicle_id="TRK-JITTER",
            origin="Base",
            destination="Hub",
            start_lat=41.0,
            start_lon=-87.0,
            dest_lat=41.09,
            dest_lon=-87.09,
            waypoints=synthetic_route,
            start_offset=0,
        )
        truck.step_size = 1

        # Act & Assert across 20 iterations
        for _ in range(20):
            truck.update_telemetry()
            anchor_lat, anchor_lon = synthetic_route[truck.waypoint_idx]
            assert abs(truck.lat - anchor_lat) <= 0.000061
            assert abs(truck.lon - anchor_lon) <= 0.000061


# ============================================================================
# Test Class 3: Vector Heading & Turn Logic (No Waypoints)
# ============================================================================
@pytest.mark.unit
class TestVectorHeadingAndTurnLogic:
    """Unit tests for synthetic vector navigation, organic +/- 45 deg turn variations, and destination bounce."""

    def test_vector_movement_towards_distant_target(self):
        """When distance to destination > 0.008, truck should update heading and advance coordinates."""
        # Arrange
        start_lat, start_lon = 41.0, -87.0
        dest_lat, dest_lon = 42.0, -86.0  # dist ~ 1.414 >> 0.008
        truck = TruckSimulator(
            vehicle_id="TRK-VEC-MOVE",
            origin="Base",
            destination="Hub",
            start_lat=start_lat,
            start_lon=start_lon,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            waypoints=None,
        )
        truck.speed_factor = 0.01

        # Act
        old_lat, old_lon = truck.lat, truck.lon
        truck.update_telemetry()

        # Assert
        assert truck.lat != old_lat
        assert truck.lon != old_lon
        # With target northeast (increasing lat and lon), coordinates should advance forward
        assert truck.lat > old_lat
        assert truck.lon > old_lon

    def test_heading_random_turn_weighted_update(self):
        """Heading calculation should follow: heading = heading + (rand_turn * 0.65) + (diff * 0.35)."""
        # Arrange
        start_lat, start_lon = 41.0, -87.0
        dest_lat, dest_lon = 42.0, -86.0
        truck = TruckSimulator(
            vehicle_id="TRK-HEADING",
            origin="Base",
            destination="Hub",
            start_lat=start_lat,
            start_lon=start_lon,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            waypoints=None,
        )
        # Fix known initial heading and deterministic turn angle
        initial_heading = math.pi / 4
        truck.heading = initial_heading
        mock_rand_turn = 0.2  # within [-pi/4, pi/4]

        d_lat = dest_lat - start_lat
        d_lon = dest_lon - start_lon
        target_heading = math.atan2(d_lat, d_lon)
        diff = (target_heading - initial_heading + math.pi) % (2 * math.pi) - math.pi
        expected_heading = initial_heading + (mock_rand_turn * 0.65) + (diff * 0.35)

        # Act
        with patch("random.uniform") as mock_uniform:
            # First call for rand_turn, subsequent calls if any
            mock_uniform.side_effect = lambda a, b: mock_rand_turn if a == -math.pi / 4 else 0.05
            truck.update_telemetry()

        # Assert
        assert math.isclose(truck.heading, expected_heading, rel_tol=1e-5)

    def test_destination_arrival_swaps_coordinates_and_reverses_heading(self):
        """When distance to destination <= 0.008, base and dest coordinates swap and heading recalculates."""
        # Arrange
        dest_lat, dest_lon = 42.0, -86.0
        base_lat, base_lon = 41.0, -87.0
        # Position truck very close to destination (distance < 0.008)
        near_dest_lat = 42.0001
        near_dest_lon = -86.0001
        dist = math.sqrt((dest_lat - near_dest_lat) ** 2 + (dest_lon - near_dest_lon) ** 2)
        assert dist <= 0.008

        truck = TruckSimulator(
            vehicle_id="TRK-SWAP",
            origin="Base",
            destination="Hub",
            start_lat=base_lat,
            start_lon=base_lon,
            dest_lat=dest_lat,
            dest_lon=dest_lon,
            waypoints=None,
        )
        truck.lat = near_dest_lat
        truck.lon = near_dest_lon

        # Expected heading towards new destination (which is the old base)
        expected_new_dest_lat = base_lat
        expected_new_dest_lon = base_lon
        expected_heading = math.atan2(
            expected_new_dest_lat - near_dest_lat, expected_new_dest_lon - near_dest_lon
        )

        # Act
        truck.update_telemetry()

        # Assert
        assert truck.dest_lat == base_lat
        assert truck.dest_lon == base_lon
        assert truck.base_lat == dest_lat
        assert truck.base_lon == dest_lon
        assert math.isclose(truck.heading, expected_heading, rel_tol=1e-5)


# ============================================================================
# Test Class 4: Thermodynamic Decay Mechanics
# ============================================================================
@pytest.mark.unit
class TestThermodynamicDecayMechanics:
    """Unit tests for temperature, humidity, vibration, and thermal decay across states."""

    @pytest.fixture
    def standard_truck(self):
        """Provide a standard truck in NORMAL state with fixed initial conditions."""
        truck = TruckSimulator(
            vehicle_id="TRK-THERMO",
            origin="Base",
            destination="Hub",
            start_lat=41.8875,
            start_lon=-87.6513,
            dest_lat=41.7508,
            dest_lon=-88.1535,
            waypoints=None,
        )
        truck.ambient_temp = 25.0
        truck.cargo_temp = 3.5
        truck.state = "NORMAL"
        truck.door_status = "CLOSED"
        truck.ticks_in_anomaly = 0
        return truck

    def test_normal_state_temperature_oscillation_and_door_closed(self, standard_truck):
        """In NORMAL state, door remains CLOSED, temp oscillates bounded [1.5, 5.5], vibration [0.01, 0.08]."""
        # Arrange
        with patch("random.random", return_value=0.5), patch(
            "random.normalvariate", return_value=0.0
        ):
            # Act
            standard_truck.update_telemetry()

            # Assert
            assert standard_truck.door_status == "CLOSED"
            assert standard_truck.state == "NORMAL"
            assert 1.5 <= standard_truck.cargo_temp <= 5.5
            assert 0.01 <= standard_truck.vibration <= 0.08

    def test_normal_state_clamps_cargo_temp_at_lower_bound(self, standard_truck):
        """In NORMAL state, cargo temperature cannot drop below 1.5C."""
        # Arrange
        standard_truck.cargo_temp = 1.0  # Below 1.5
        with patch("random.random", return_value=0.5), patch(
            "random.normalvariate", return_value=-0.5
        ):
            # Act
            standard_truck.update_telemetry()

            # Assert
            assert standard_truck.cargo_temp >= 1.5

    def test_normal_state_clamps_cargo_temp_at_upper_bound(self, standard_truck):
        """In NORMAL state, cargo temperature cannot exceed 5.5C."""
        # Arrange
        standard_truck.cargo_temp = 6.0  # Above 5.5
        with patch("random.random", return_value=0.5), patch(
            "random.normalvariate", return_value=0.5
        ):
            # Act
            standard_truck.update_telemetry()

            # Assert
            assert standard_truck.cargo_temp <= 5.5

    def test_door_open_rapid_thermodynamic_decay(self, standard_truck):
        """In DOOR_OPEN state, door is OPEN, and cargo temp decays rapidly at 0.15 * (ambient - cargo)."""
        # Arrange
        standard_truck.state = "DOOR_OPEN"
        standard_truck.ambient_temp = 25.0
        standard_truck.cargo_temp = 3.5
        # Expected delta: 0.15 * (25.0 - 3.5) = 0.15 * 21.5 = 3.225
        expected_cargo_temp = 3.5 + (0.15 * (25.0 - 3.5))

        with patch("random.normalvariate", return_value=0.0):
            # Act
            standard_truck.update_telemetry()

            # Assert
            assert standard_truck.door_status == "OPEN"
            assert math.isclose(standard_truck.cargo_temp, expected_cargo_temp, rel_tol=1e-5)
            assert standard_truck.ticks_in_anomaly == 1
            assert 0.02 <= standard_truck.vibration <= 0.12

    def test_door_open_resolves_to_normal_after_ten_ticks(self, standard_truck):
        """DOOR_OPEN state should automatically resolve to NORMAL when ticks_in_anomaly > 10."""
        # Arrange
        standard_truck.state = "DOOR_OPEN"
        standard_truck.ticks_in_anomaly = 10  # On next update, ticks_in_anomaly becomes 11 (> 10)

        with patch("random.normalvariate", return_value=0.0):
            # Act
            standard_truck.update_telemetry()

            # Assert
            assert standard_truck.state == "NORMAL"
            assert standard_truck.ticks_in_anomaly == 0

    def test_compressor_failure_slower_thermodynamic_decay(self, standard_truck):
        """In COMPRESSOR_FAILURE state, door remains CLOSED and decay is slower at 0.04 * (ambient - cargo)."""
        # Arrange
        standard_truck.state = "COMPRESSOR_FAILURE"
        standard_truck.ambient_temp = 25.0
        standard_truck.cargo_temp = 3.5
        # Expected delta: 0.04 * (25.0 - 3.5) = 0.04 * 21.5 = 0.86
        expected_cargo_temp = 3.5 + (0.04 * (25.0 - 3.5))

        with patch("random.normalvariate", return_value=0.0):
            # Act
            standard_truck.update_telemetry()

            # Assert
            assert standard_truck.door_status == "CLOSED"
            assert math.isclose(standard_truck.cargo_temp, expected_cargo_temp, rel_tol=1e-5)
            assert standard_truck.ticks_in_anomaly == 1
            assert 0.01 <= standard_truck.vibration <= 0.08

    def test_compressor_failure_resolves_to_normal_after_fifteen_ticks(self, standard_truck):
        """COMPRESSOR_FAILURE state should automatically resolve to NORMAL when ticks_in_anomaly > 15."""
        # Arrange
        standard_truck.state = "COMPRESSOR_FAILURE"
        standard_truck.ticks_in_anomaly = 15  # On next update, ticks_in_anomaly becomes 16 (> 15)

        with patch("random.normalvariate", return_value=0.0):
            # Act
            standard_truck.update_telemetry()

            # Assert
            assert standard_truck.state == "NORMAL"
            assert standard_truck.ticks_in_anomaly == 0

    def test_normal_state_anomaly_triggering(self, standard_truck):
        """When random float < 0.02, NORMAL state transitions into an anomaly state."""
        # Arrange
        standard_truck.state = "NORMAL"
        with patch("random.random", return_value=0.01), patch(
            "random.choice", return_value="DOOR_OPEN"
        ), patch("random.normalvariate", return_value=0.0):
            # Act
            standard_truck.update_telemetry()

            # Assert
            assert standard_truck.state == "DOOR_OPEN"

    @pytest.mark.parametrize(
        "cargo_temp,expected_humidity",
        [
            (0.0, 60.0),  # 60.0 - (0.0 * 1.5) = 60.0
            (10.0, 45.0),  # 60.0 - (10.0 * 1.5) = 45.0
            (40.0, 10.0),  # 60.0 - 60.0 = 0.0 -> clamped to min 10.0
            (-30.0, 95.0),  # 60.0 - (-45.0) = 105.0 -> clamped to max 95.0
        ],
    )
    def test_humidity_inverse_tracking_and_bounding(
        self, standard_truck, cargo_temp, expected_humidity
    ):
        """Humidity should inversely track cargo_temp: max(10.0, min(95.0, 60.0 - (cargo_temp * 1.5)))."""
        # Arrange
        standard_truck.cargo_temp = cargo_temp

        # Act
        # Trigger humidity calculation via update_telemetry (state DOOR_OPEN avoids normal clamp)
        standard_truck.state = "DOOR_OPEN"
        standard_truck.ambient_temp = cargo_temp  # Delta is 0, so cargo_temp stays exact
        with patch("random.normalvariate", return_value=0.0):
            standard_truck.update_telemetry()

        # Assert
        assert math.isclose(standard_truck.humidity, expected_humidity, rel_tol=1e-5)


# ============================================================================
# Test Class 5: Route File Loading (load_routes)
# ============================================================================
@pytest.mark.unit
class TestLoadRoutes:
    """Unit tests for load_routes() handling of existing, missing, and corrupt route files."""

    def test_load_routes_from_custom_env_var_path(self, tmp_path):
        """load_routes() should prioritize file specified by ROUTES_FILE environment variable."""
        # Arrange
        route_data = {
            "Hub-Aurora": {
                "destination": [41.7606, -88.3201],
                "waypoints": [[41.8875, -87.6513], [41.7606, -88.3201]],
            }
        }
        custom_file = tmp_path / "custom_routes.json"
        custom_file.write_text(json.dumps(route_data))

        # Act
        with patch.dict(os.environ, {"ROUTES_FILE": str(custom_file)}):
            result = load_routes()

        # Assert
        assert result == route_data
        assert "Hub-Aurora" in result

    def test_load_routes_fallback_search_order(self, tmp_path):
        """load_routes() should fall back along candidate paths if the first candidates do not exist."""
        # Arrange
        route_data = {"Hub-Naperville": {"waypoints": [[41.7508, -88.1535]]}}
        fallback_file = str(tmp_path / "fallback_routes.json")

        def mock_exists(path):
            return path == fallback_file

        # Act
        with patch.dict(os.environ, {"ROUTES_FILE": "/nonexistent/routes.json"}), patch(
            "os.path.exists", side_effect=mock_exists
        ), patch(
            "builtins.open",
            mock_open(read_data=json.dumps(route_data)),
        ):
            # Patch route_paths inside load_routes to include our fallback_file
            with patch("payload_simulator.open", mock_open(read_data=json.dumps(route_data))):
                result = load_routes()

        # Assert: load_routes returns empty if fallback_file is not in default route_paths,
        # but with custom ROUTES_FILE pointing to an existing file, it succeeds.
        with patch.dict(os.environ, {"ROUTES_FILE": fallback_file}), patch(
            "os.path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=json.dumps(route_data))):
            result = load_routes()
            assert result == route_data

    def test_load_routes_missing_all_files_returns_empty_dict(self):
        """load_routes() should return an empty dict when none of the candidate files exist."""
        # Arrange
        with patch.dict(os.environ, {}, clear=True), patch("os.path.exists", return_value=False):
            # Act
            result = load_routes()

            # Assert
            assert result == {}

    def test_load_routes_fallback_when_first_path_corrupted(self, tmp_path):
        """load_routes() should fall through to subsequent paths if the first path has corrupt JSON."""
        # Arrange
        corrupt_file = tmp_path / "corrupt_routes.json"
        corrupt_file.write_text("{ corrupt json")
        valid_route_data = {"Hub-Aurora": {"waypoints": [[41.76, -88.32]]}}

        def mock_exists(path):
            return path in {str(corrupt_file), "/app/routes.json"}

        # Act
        with patch.dict(os.environ, {"ROUTES_FILE": str(corrupt_file)}), patch(
            "os.path.exists", side_effect=mock_exists
        ), patch("builtins.open") as mock_file:
            # First open returns corrupt data, second open returns valid data
            mock_file.side_effect = [
                mock_open(read_data="{ corrupt json").return_value,
                mock_open(read_data=json.dumps(valid_route_data)).return_value,
            ]
            result = load_routes()

        # Assert
        assert result == valid_route_data

    def test_load_routes_handles_corrupt_json_file_gracefully(self, tmp_path):
        """load_routes() should catch JSONDecodeError, print a warning, and return empty dict when no fallbacks exist."""
        # Arrange
        corrupt_file = tmp_path / "corrupt_routes.json"
        corrupt_file.write_text("{ this is not valid json! }")

        def mock_exists(path):
            return path == str(corrupt_file)

        # Act
        with patch.dict(os.environ, {"ROUTES_FILE": str(corrupt_file)}), patch(
            "os.path.exists", side_effect=mock_exists
        ):
            result = load_routes()

        # Assert
        assert result == {}

    def test_load_routes_ignores_empty_json_file(self, tmp_path):
        """load_routes() should treat empty JSON object ({}) as falsy and return empty dict when no fallbacks exist."""
        # Arrange
        empty_file = tmp_path / "empty_routes.json"
        empty_file.write_text("{}")

        def mock_exists(path):
            return path == str(empty_file)

        # Act
        with patch.dict(os.environ, {"ROUTES_FILE": str(empty_file)}), patch(
            "os.path.exists", side_effect=mock_exists
        ):
            result = load_routes()

        # Assert
        assert result == {}


# ============================================================================
# Test Class 6: Telemetry Payload Generation Schema & Contracts
# ============================================================================
@pytest.mark.unit
class TestPayloadGeneration:
    """Unit tests for payload schema compliance, data types, and formatting."""

    def test_generate_payload_contract_and_structure(self):
        """generate_payload() must produce a conforming telemetry schema dictionary."""
        # Arrange
        truck = TruckSimulator(
            vehicle_id="TRK-PAYLOAD-01",
            origin="Base Warehouse",
            destination="Hub-Naperville",
            start_lat=41.8875,
            start_lon=-87.6513,
            dest_lat=41.7508,
            dest_lon=-88.1535,
            waypoints=None,
        )

        # Act
        payload = truck.generate_payload()

        # Assert: Top-level keys
        assert set(payload.keys()) == {
            "eventId",
            "vehicleId",
            "timestamp",
            "routeContext",
            "telemetry",
        }
        # Valid UUID
        uuid_obj = uuid.UUID(payload["eventId"])
        assert str(uuid_obj) == payload["eventId"]
        assert payload["vehicleId"] == "TRK-PAYLOAD-01"

        # Timestamp format: YYYY-MM-DD HH:MM:SS
        assert len(payload["timestamp"]) == 19
        assert payload["timestamp"][10] == " "

        # routeContext keys & values
        route = payload["routeContext"]
        assert route["origin"] == "Base Warehouse"
        assert route["destination"] == "Hub-Naperville"
        assert isinstance(route["latitude"], float)
        assert isinstance(route["longitude"], float)

        # telemetry keys & values
        telemetry = payload["telemetry"]
        assert set(telemetry.keys()) == {
            "ambientTempC",
            "cargoContainerTempC",
            "relativeHumidityPct",
            "vibrationG",
            "doorStatus",
        }
        assert isinstance(telemetry["ambientTempC"], float)
        assert isinstance(telemetry["cargoContainerTempC"], float)
        assert isinstance(telemetry["relativeHumidityPct"], float)
        assert isinstance(telemetry["vibrationG"], float)
        assert telemetry["doorStatus"] in {"CLOSED", "OPEN"}


# ============================================================================
# Test Class 7: Dynamic Fleet Size Configuration
# ============================================================================
@pytest.mark.unit
class TestFleetSizeConfiguration:
    """Unit tests for get_target_fleet_size config file, environment variable, and default resolution."""

    def test_get_target_fleet_size_from_config_file(self, tmp_path):
        """get_target_fleet_size should read num_trucks from fleet section in config file."""
        # Arrange
        config_file = tmp_path / "test_pipeline.conf"
        config_file.write_text("[fleet]\nnum_trucks = 12\n")

        # Act
        with patch.dict(os.environ, {"FLEET_CONFIG": str(config_file)}):
            size = get_target_fleet_size(default_val=3)

        # Assert
        assert size == 12

    def test_get_target_fleet_size_from_env_var_when_config_missing(self):
        """get_target_fleet_size should read NUM_TRUCKS env var when config file does not exist."""
        # Arrange
        with patch.dict(
            os.environ,
            {"FLEET_CONFIG": "/nonexistent/pipeline.conf", "NUM_TRUCKS": "7"},
        ):
            # Act
            size = get_target_fleet_size(default_val=3)

            # Assert
            assert size == 7

    def test_get_target_fleet_size_falls_back_to_default(self):
        """get_target_fleet_size should fall back to default_val when neither config nor env var is provided."""
        # Arrange
        with patch.dict(
            os.environ,
            {"FLEET_CONFIG": "/nonexistent/pipeline.conf"},
            clear=True,
        ):
            # Act
            size = get_target_fleet_size(default_val=5)

            # Assert
            assert size == 5
