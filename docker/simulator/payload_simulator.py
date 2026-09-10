#!/usr/bin/env python3
"""
payload_simulator.py
Clean, modular cold-chain IoT telemetry simulator.
Orchestrates domain models and services adhering to Uncle Bob's Clean Code principles.
"""

import argparse
from datetime import datetime
import json
import logging
import os
import sys
import time
import uuid
from zoneinfo import ZoneInfo

# Add local directory to path for submodule resolution
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from domain.models import Coordinate, DoorStatus, RouteContext, SensorTelemetry, TelemetryPayload, VehicleState
from domain.navigation import compute_initial_heading, step_road_waypoints, step_vector_heading
from domain.thermodynamics import sample_vibration, step_thermodynamic_state
from services.fleet_service import resolve_target_fleet_size
from services.kafka_publisher import KafkaTelemetryPublisher
from services.route_service import load_hub_routes

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Simulator")

# Base coordinates of Chicago distribution headquarters
BASE_WAREHOUSE_LAT = 41.8875294
BASE_WAREHOUSE_LON = -87.6513405

CHICAGOLAND_HUBS = {
    "Hub-Aurora": (41.7606, -88.3201),
    "Hub-Naperville": (41.7508, -88.1535),
    "Hub-Joliet": (41.5250, -88.0817),
    "Hub-Elgin": (42.0354, -88.2826),
    "Hub-Waukegan": (42.3636, -87.8448),
    "Hub-Schaumburg": (42.0334, -88.0834),
    "Hub-Evanston": (42.0451, -87.6877),
    "Hub-Arlington Heights": (42.0884, -87.9806),
    "Hub-Bolingbrook": (41.6986, -88.0684),
}


class TruckSimulator:
    """Represents a physical refrigerated transport truck moving along regional routes."""

    def __init__(
        self,
        vehicle_id: str,
        origin: str,
        destination: str,
        start_lat: float,
        start_lon: float,
        dest_lat: float,
        dest_lon: float,
        waypoints: list = None,
        start_offset: int = 0,
    ):
        self.vehicle_id = vehicle_id
        self.origin = origin
        self.destination = destination
        self.dest_lat = dest_lat
        self.dest_lon = dest_lon
        self.base_lat = start_lat
        self.base_lon = start_lon

        if waypoints and len(waypoints) > 1:
            self.waypoints = waypoints
            self.use_roads = True
            self.waypoint_idx = min(start_offset, len(self.waypoints) - 1)
            self.direction = 1
            self.step_size = 4
            self.lat, self.lon = self.waypoints[self.waypoint_idx][0], self.waypoints[self.waypoint_idx][1]
        else:
            self.waypoints = None
            self.use_roads = False
            self.lat = start_lat
            self.lon = start_lon
            self.speed_factor = 0.007
            self.heading = compute_initial_heading(start_lat, start_lon, dest_lat, dest_lon)

        self.ambient_temp = 25.0
        self.cargo_temp = 3.5
        self.humidity = 50.0
        self.door_status = DoorStatus.CLOSED.value
        self.state = VehicleState.NORMAL
        self.ticks_in_anomaly = 0
        self.vibration = 0.03

    def update_telemetry(self):
        """Update physics, position, and sensor telemetry for one simulation tick."""
        # 1. Update thermodynamics and anomaly states
        (
            self.state,
            door_state,
            self.ambient_temp,
            self.cargo_temp,
            self.humidity,
            self.ticks_in_anomaly,
        ) = step_thermodynamic_state(
            self.state, self.ambient_temp, self.cargo_temp, self.ticks_in_anomaly
        )
        self.door_status = door_state.value
        self.vibration = sample_vibration(self.state)

        # 2. Advance location
        if self.use_roads:
            self.waypoint_idx, self.direction, self.lat, self.lon = step_road_waypoints(
                self.waypoints, self.waypoint_idx, self.direction, self.step_size
            )
        else:
            self.lat, self.lon, self.heading, reached = step_vector_heading(
                self.lat, self.lon, self.dest_lat, self.dest_lon, self.heading, self.speed_factor
            )
            if reached:
                self.dest_lat, self.base_lat = self.base_lat, self.dest_lat
                self.dest_lon, self.base_lon = self.base_lon, self.dest_lon
                self.heading = compute_initial_heading(self.lat, self.lon, self.dest_lat, self.dest_lon)

    def generate_payload(self) -> dict:
        """Advance simulation and return serialized IoT telemetry JSON."""
        self.update_telemetry()
        now_str = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d %H:%M:%S")
        route_ctx = RouteContext(
            origin=self.origin,
            destination=self.destination,
            latitude=round(self.lat, 4),
            longitude=round(self.lon, 4),
        )
        telemetry = SensorTelemetry(
            ambientTempC=round(self.ambient_temp, 2),
            cargoContainerTempC=round(self.cargo_temp, 2),
            relativeHumidityPct=round(self.humidity, 2),
            vibrationG=round(self.vibration, 3),
            doorStatus=self.door_status,
        )
        payload = TelemetryPayload(
            eventId=str(uuid.uuid4()),
            vehicleId=self.vehicle_id,
            timestamp=now_str,
            routeContext=route_ctx,
            telemetry=telemetry,
        )
        return payload.to_dict()


# Backward-compatibility aliases for existing imports and test suite
load_routes = load_hub_routes
get_target_fleet_size = resolve_target_fleet_size


def parse_arguments() -> argparse.Namespace:
    """Parse command line parameters."""
    parser = argparse.ArgumentParser(description="Cold-chain IoT Fleet Telemetry Simulator")
    parser.add_argument("--num-trucks", type=int, default=3, help="Default active fleet size")
    parser.add_argument("--total-time", type=int, default=99999, help="Total execution duration (seconds)")
    parser.add_argument("--time-per-epoch", type=float, default=2.0, help="Epoch duration (seconds)")
    parser.add_argument("--use-kafka", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--kafka-bootstrap-servers", type=str, default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--kafka-topic", type=str, default=os.environ.get("KAFKA_TOPIC", "cold-chain-telemetry"))
    return parser.parse_args()


def main():
    """Main execution loop for fleet simulation."""
    args = parse_arguments()
    publisher = KafkaTelemetryPublisher(args.kafka_bootstrap_servers, args.kafka_topic, enabled=args.use_kafka)
    publisher.connect()

    routes_data = load_hub_routes()
    hub_choices = list(CHICAGOLAND_HUBS.keys())

    fleet = []
    target_trucks = resolve_target_fleet_size(args.num_trucks)

    def spawn_truck(idx: int) -> TruckSimulator:
        v_id = f"TRK-CHI-{idx + 1:04d}"
        dest = hub_choices[idx % len(hub_choices)]
        dest_lat, dest_lon = CHICAGOLAND_HUBS[dest]
        pts = routes_data.get(dest, {}).get("waypoints", [])
        stagger = int((idx * 45) % max(1, len(pts) - 20)) if pts else 0
        return TruckSimulator(v_id, "Base Warehouse", dest, BASE_WAREHOUSE_LAT, BASE_WAREHOUSE_LON, dest_lat, dest_lon, pts, stagger)

    for i in range(target_trucks):
        fleet.append(spawn_truck(i))

    logger.info("Simulator running with %d trucks. Epoch: %.1fs", len(fleet), args.time_per_epoch)

    elapsed = 0.0
    epoch = 0
    try:
        while elapsed < args.total_time:
            current_target = resolve_target_fleet_size(args.num_trucks)
            while len(fleet) < current_target:
                new_truck = spawn_truck(len(fleet))
                fleet.append(new_truck)
                logger.info("🚨 Dynamically scaled fleet: added %s", new_truck.vehicle_id)

            for truck in fleet:
                payload = truck.generate_payload()
                print(json.dumps(payload, indent=2))
                print("-" * 40)
                publisher.publish(payload)

            epoch += 1
            time.sleep(args.time_per_epoch)
            elapsed += args.time_per_epoch
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user after %d epochs.", epoch)
    finally:
        publisher.close()


if __name__ == "__main__":
    main()