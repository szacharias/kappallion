import time
import json
import random
import uuid
import argparse
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from kafka import KafkaProducer
import os

def load_routes():
    route_paths = [
        os.environ.get("ROUTES_FILE", "/app/config/routes.json"),
        "/app/routes.json",
        "config/routes.json",
        "docker/simulator/routes.json"
    ]
    for rp in route_paths:
        if os.path.exists(rp):
            try:
                with open(rp, "r") as f:
                    data = json.load(f)
                    if data:
                        return data
            except Exception as e:
                print(f"Failed to read routes from {rp}: {e}", flush=True)
    return {}

class TruckSimulator:
    def __init__(self, vehicle_id, origin, destination, start_lat, start_lon, dest_lat, dest_lon, waypoints=None, start_offset=0):
        self.vehicle_id = vehicle_id
        self.origin = origin
        self.destination = destination
        self.dest_lat = dest_lat
        self.dest_lon = dest_lon
        self.base_lat = start_lat
        self.base_lon = start_lon
        
        # Real road waypoints if provided
        if waypoints and len(waypoints) > 1:
            self.waypoints = waypoints
            self.waypoint_idx = min(start_offset, len(waypoints) - 1)
            self.direction = 1  # 1 = outbound to hub, -1 = return to base
            self.step_size = random.randint(3, 6)  # Advance along road polyline
            pt = self.waypoints[self.waypoint_idx]
            self.lat = pt[0]
            self.lon = pt[1]
            self.use_roads = True
        else:
            self.waypoints = None
            self.use_roads = False
            self.lat = start_lat
            self.lon = start_lon
            self.speed_factor = random.uniform(0.005, 0.009)
            self.heading = math.atan2(self.dest_lat - self.lat, self.dest_lon - self.lon)
        
        # Physical baselines
        self.ambient_temp = 25.0  # Summer ambient temp C
        self.cargo_temp = 3.5     # Target cold chain temp C
        self.humidity = 50.0

        ## Refers to actual sensor values 
        self.door_status = "CLOSED"
        
        # Referrs to it's current state, created to simulate issue classification
        # State control: "NORMAL", "DOOR_OPEN", or "COMPRESSOR_FAILURE"
        self.state = "NORMAL"
        self.ticks_in_anomaly = 0

    def update_telemetry(self):
        # 1. Simulate micro-fluctuations in ambient temperature
        self.ambient_temp += random.normalvariate(0, 0.1)
        
        # 2. Progress coordinates along real roads or synthetic vector
        if self.use_roads and self.waypoints:
            self.waypoint_idx += self.direction * self.step_size
            if self.waypoint_idx >= len(self.waypoints) - 1:
                self.waypoint_idx = len(self.waypoints) - 1
                self.direction = -1  # Turn around at the hub, head back to base
            elif self.waypoint_idx <= 0:
                self.waypoint_idx = 0
                self.direction = 1   # Turn around at base, head back out
            
            # Snap to real road waypoint with +/- ~5m multi-lane choice offset
            base_pt = self.waypoints[self.waypoint_idx]
            self.lat = base_pt[0] + random.uniform(-0.00006, 0.00006)
            self.lon = base_pt[1] + random.uniform(-0.00006, 0.00006)
        else:
            # Fallback to heading / angle movement with +/- 45 deg organic random variation
            d_lat = self.dest_lat - self.lat
            d_lon = self.dest_lon - self.lon
            dist = math.sqrt(d_lat**2 + d_lon**2)
            if dist > 0.008:
                target_heading = math.atan2(d_lat, d_lon)
                diff = (target_heading - self.heading + math.pi) % (2 * math.pi) - math.pi
                rand_turn = random.uniform(-math.pi / 4, math.pi / 4)
                self.heading = self.heading + (rand_turn * 0.65) + (diff * 0.35)
                self.lat += math.sin(self.heading) * self.speed_factor
                self.lon += math.cos(self.heading) * self.speed_factor
            else:
                self.dest_lat, self.base_lat = self.base_lat, self.dest_lat
                self.dest_lon, self.base_lon = self.base_lon, self.dest_lon
                self.heading = math.atan2(self.dest_lat - self.lat, self.dest_lon - self.lon)
        
        # 3. Handle state mechanics and thermodynamic decay
        if self.state == "NORMAL":
            # Normal thermal oscillation around target 3.5C
            self.cargo_temp += random.normalvariate(0, 0.05)
            self.cargo_temp = max(1.5, min(5.5, self.cargo_temp)) # Keep bounded
            self.door_status = "CLOSED"
            self.vibration = random.uniform(0.01, 0.08)
            
            # 2% chance to trigger an anomaly state on any given tick
            if random.random() < 0.02:
                self.state = random.choice(["DOOR_OPEN", "COMPRESSOR_FAILURE"])
                
        elif self.state == "DOOR_OPEN":
            self.door_status = "OPEN"
            self.vibration = random.uniform(0.02, 0.12)
            # Newton's law of cooling approximation (rapid decay)
            self.cargo_temp += 0.15 * (self.ambient_temp - self.cargo_temp)
            self.ticks_in_anomaly += 1
            
            # Resolve anomaly after 10 ticks
            if self.ticks_in_anomaly > 10:
                self.state = "NORMAL"
                self.ticks_in_anomaly = 0
                
        elif self.state == "COMPRESSOR_FAILURE":
            self.door_status = "CLOSED"
            self.vibration = random.uniform(0.01, 0.08)
            # Slower thermal decay since door is shut but cooling is off
            self.cargo_temp += 0.04 * (self.ambient_temp - self.cargo_temp)
            self.ticks_in_anomaly += 1
            
            if self.ticks_in_anomaly > 15:
                self.state = "NORMAL"
                self.ticks_in_anomaly = 0

        # Relative humidity inversely tracks cargo temp spikes
        self.humidity = max(10.0, min(95.0, 60.0 - (self.cargo_temp * 1.5)))

    def generate_payload(self):
        self.update_telemetry()
        return {
            "eventId": str(uuid.uuid4()),
            "vehicleId": self.vehicle_id,
            "timestamp": datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d %H:%M:%S"),
            "routeContext": {
                "origin": self.origin,
                "destination": self.destination,
                "latitude": round(self.lat, 4),
                "longitude": round(self.lon, 4)
            },
            "telemetry": {
                "ambientTempC": round(self.ambient_temp, 2),
                "cargoContainerTempC": round(self.cargo_temp, 2),
                "relativeHumidityPct": round(self.humidity, 2),
                "vibrationG": round(self.vibration, 3),
                "doorStatus": self.door_status
            }
        }

def get_target_fleet_size(default_val):
    cfg_file = os.environ.get("FLEET_CONFIG", "/app/config/pipeline.conf")
    if os.path.exists(cfg_file):
        try:
            import configparser
            cp = configparser.ConfigParser()
            cp.read(cfg_file)
            if cp.has_section("fleet") and cp.has_option("fleet", "num_trucks"):
                return int(cp.get("fleet", "num_trucks"))
        except Exception:
            pass
    return int(os.environ.get("NUM_TRUCKS", default_val))

# Execution Block for local verification
if __name__ == "__main__":

    # Defaults to running for 99999 seconds which is 1.157396 days 
    parser = argparse.ArgumentParser(description="Run the cold chain truck simulator.")
    parser.add_argument(
        "--num-trucks", 
        type=int, 
        default=3, 
        help="Number of trucks to simulate in the fleet"
    )
    parser.add_argument(
        "--total-time",
        type=int, 
        default=99999,
        help="Total time to run the simulation"
    )
    parser.add_argument(
        "--time-per-epoch",
        type=float, 
        default=2.0,
        help="Every epoch time (seconds) that payload is generated"
    )
    parser.add_argument(
        "--use-kafka", 
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable or disable Kafka integration (use --no-use-kafka to disable)"
    )
    parser.add_argument(
        "--kafka-bootstrap-servers", 
        type=str, 
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap servers connection string"
    )
    parser.add_argument(
        "--kafka-topic", 
        type=str, 
        default=os.environ.get("KAFKA_TOPIC", "cold-chain-telemetry"),
        help="Kafka topic to send telemetry to"
    )
    args = parser.parse_args() 
    
    producer = None
    if args.use_kafka:
        # wait for kafka to be available 
        print(f"Waiting for Kafka at {args.kafka_bootstrap_servers}...", flush=True)
        for i in range(30):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=args.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                print("Connected to Kafka!", flush=True)
                break
            except Exception as e:
                print(f"Waiting for Kafka ({i+1}/30)... Error: {e}", flush=True)
                time.sleep(2)
    else:
        print("Not using Kafka, running simulator locally only")
        
    # Baseline coordinates of the region
    base_lat = 41.8875294
    base_lon = -87.6513405
    
    fleet = []

    chicagoland_hubs = {
        "Hub-Aurora": (41.7606, -88.3201),
        "Hub-Naperville": (41.7508, -88.1535),
        "Hub-Joliet": (41.5250, -88.0817),
        "Hub-Elgin": (42.0354, -88.2826),
        "Hub-Waukegan": (42.3636, -87.8448),
        "Hub-Schaumburg": (42.0334, -88.0834),
        "Hub-Evanston": (42.0451, -87.6877),
        "Hub-Arlington Heights": (42.0884, -87.9806),
        "Hub-Bolingbrook": (41.6986, -88.0684)
    }

    # Load real road network waypoints from OpenStreetMap
    routes_data = load_routes()
    if routes_data:
        print(f"🗺️ Successfully loaded real road routes for {len(routes_data)} hubs from OpenStreetMap OSRM!", flush=True)
    else:
        print("⚠️ No road routes found in config/routes.json, using vector trajectory fallback.", flush=True)

    # Predictable, consistent vehicle IDs and distinct destination routes
    hub_choices = list(chicagoland_hubs.keys())
    target_trucks = get_target_fleet_size(args.num_trucks)
    for i in range(target_trucks): 
        v_id = f"TRK-CHI-{i+1:04d}"
        origin = "Base Warehouse"
        dest = hub_choices[i % len(hub_choices)]
        dest_lat, dest_lon = chicagoland_hubs[dest]
        route_info = routes_data.get(dest, {})
        pts = route_info.get("waypoints", [])
        # Stagger initial positions along the road network so trucks are naturally distributed
        stagger = int((i * 45) % max(1, len(pts) - 20)) if pts else 0
        fleet.append(TruckSimulator(v_id, origin, dest, base_lat, base_lon, dest_lat, dest_lon, waypoints=pts, start_offset=stagger))
    
    print(f"Simulator Started simulating {len(fleet)} trucks ({', '.join([t.vehicle_id for t in fleet])}). Press Ctrl+C to stop.\n")
    print(f"total time scheduled {args.total_time}s, time per epoch {args.time_per_epoch}s, and total rounds is {args.total_time/args.time_per_epoch}")
    print("Simulation starting in 3 seconds...")
    for i in range(3, 0, -1):
        print(f"{i}...")
        time.sleep(1)
    epoch = 0 
    elapsed_time = 0.0
    try:
        while elapsed_time < args.total_time:
            # Check for dynamic fleet expansion from config or environment
            current_target = get_target_fleet_size(args.num_trucks)
            while len(fleet) < current_target:
                new_idx = len(fleet)
                new_vid = f"TRK-CHI-{new_idx+1:04d}"
                new_dest = hub_choices[new_idx % len(hub_choices)]
                n_dest_lat, n_dest_lon = chicagoland_hubs[new_dest]
                route_info = routes_data.get(new_dest, {})
                pts = route_info.get("waypoints", [])
                stagger = int((new_idx * 45) % max(1, len(pts) - 20)) if pts else 0
                fleet.append(TruckSimulator(new_vid, origin, new_dest, base_lat, base_lon, n_dest_lat, n_dest_lon, waypoints=pts, start_offset=stagger))
                print(f"🚨 Dynamically added truck to active fleet: {new_vid} (Route: {new_dest}, Road Waypoints: {len(pts)})", flush=True)

            for truck in fleet:
                payload = truck.generate_payload()
                print(json.dumps(payload, indent=2))
                print("-" * 40)
                if producer:
                    producer.send(args.kafka_topic, value=payload)
            
            print(f"{'='*40}\nepoch : {epoch}\n{'='*40}")
            epoch += 1 # number of rounds is the number of times events are generated
            time.sleep(args.time_per_epoch)  # Emit events every epoch time seconds
            elapsed_time += args.time_per_epoch
            
    except KeyboardInterrupt:
        print("\nSimulator stopped.")
        print(f"total rounds : {epoch}")

    print(f"total time simulated : {elapsed_time}")
    print(f"Total rounds : {epoch}") 
    print("Number of trucks: ", args.num_trucks)    