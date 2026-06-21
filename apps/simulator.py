import time
import json
import random
import uuid
import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

class TruckSimulator:
    def __init__(self, vehicle_id, origin, destination, start_lat, start_lon):
        self.vehicle_id = vehicle_id
        self.origin = origin
        self.destination = destination
        self.lat = start_lat
        self.lon = start_lon
        
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
        
        # 2. Progress coordinates slightly toward destination (simulating a route)
        self.lat += random.uniform(-0.001, 0.001)
        self.lon += random.uniform(-0.001, 0.001)
        
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

# Execution Block for local verification
if __name__ == "__main__":
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
    args = parser.parse_args() 
    
    # Baseline coordinates of the region
    base_lat = 41.8875294
    base_lon = -87.6513405
    
    fleet = []
    generated_ids = set()

    chicagoland_cities = [
        "Chicago",
        "Aurora",
        "Naperville",
        "Joliet",
        "Elgin",
        "Waukegan",
        "Schaumburg",
        "Evanston",
        "Arlington Heights",
        "Bolingbrook"
    ]

    # non duplicate ids for vehicles
    # method of deduplication could be refined, if fleet gets too large, this would definitely break or take too long to run. 
    for i in range(args.num_trucks): 
        while True:
            random_id = random.randint(1000, 9999)
            v_id = f"TRK-CHI-{random_id:04d}"
            if v_id not in generated_ids:
                generated_ids.add(v_id)
                break

        origin = "Base Warehouse"
        dest = f"Hub-{random.choice(chicagoland_cities)}"
        
        # Append the new simulator instance using the exact base coordinates
        fleet.append(TruckSimulator(v_id, origin, dest, base_lat, base_lon))
    
    print(f"Simulator Started simulating {len(fleet)} trucks. Press Ctrl+C to stop.\n")
    print(f"total time scheduled {args.total_time}s, time per epoch {args.time_per_epoch}s, and total rounds is {args.total_time/args.time_per_epoch}")
    print("Simulation starting in 3 seconds...")
    for i in range(3, 0, -1):
        print(f"{i}...")
        time.sleep(1)
    epoch = 0 
    elapsed_time = 0.0
    try:
        while elapsed_time < args.total_time:
            for truck in fleet:
                payload = truck.generate_payload()
                print(json.dumps(payload, indent=2))
                print("-" * 40)
            
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