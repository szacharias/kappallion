import time
import json
import random
import os
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cold-chain-telemetry")

print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)

# Wait for Kafka to become available
producer = None
for i in range(30):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connected to Kafka!", flush=True)
        break
    except Exception as e:
        print(f"Waiting for Kafka ({i+1}/30)... Error: {e}", flush=True)
        time.sleep(2)

if not producer:
    print("Could not connect to Kafka. Exiting.", flush=True)
    exit(1)

# Fleet definitions
vehicles = {
    "VEHICLE_001": {
        "base_ambient": 25.0,
        "base_cargo": 4.0,
        "base_humidity": 85.0,
        "base_vibration": 0.2,
        "cargo_temp": 4.0,
        "door_open": False,
        "cooling_failure": False,
    },
    "VEHICLE_002": {
        "base_ambient": 28.0,
        "base_cargo": 3.5,
        "base_humidity": 82.0,
        "base_vibration": 0.3,
        "cargo_temp": 3.5,
        "door_open": False,
        "cooling_failure": False,
    },
    "VEHICLE_003": {
        "base_ambient": 22.0,
        "base_cargo": 4.5,
        "base_humidity": 88.0,
        "base_vibration": 0.1,
        "cargo_temp": 4.5,
        "door_open": False,
        "cooling_failure": False,
    }
}

start_time = time.time()

print(f"Starting IoT Telemetry simulation. Topic: {KAFKA_TOPIC}", flush=True)

while True:
    elapsed = int(time.time() - start_time)
    
    # Anomaly scheduling logic (PoC cycles)
    # Vehicle 1: Cooling failure every 60 seconds, lasting 15 seconds
    v1_cycle = elapsed % 60
    if 40 <= v1_cycle < 55:
        vehicles["VEHICLE_001"]["cooling_failure"] = True
    else:
        vehicles["VEHICLE_001"]["cooling_failure"] = False

    # Vehicle 2: Door open anomaly every 80 seconds, lasting 12 seconds
    v2_cycle = elapsed % 80
    if 50 <= v2_cycle < 62:
        vehicles["VEHICLE_002"]["door_open"] = True
    else:
        vehicles["VEHICLE_002"]["door_open"] = False

    # Vehicle 3: Severe vibration spike every 45 seconds, lasting 5 seconds
    v3_cycle = elapsed % 45
    v3_spike = (35 <= v3_cycle < 40)

    for vehicle_id, state in vehicles.items():
        # Baseline noise
        ambient_noise = random.uniform(-0.5, 0.5)
        humidity_noise = random.uniform(-1.0, 1.0)
        vibration_noise = random.uniform(-0.05, 0.05)
        
        ambient = state["base_ambient"] + ambient_noise
        humidity = state["base_humidity"] + humidity_noise
        vibration = state["base_vibration"] + vibration_noise
        door_status = "CLOSED"

        # Apply anomaly effects
        if vehicle_id == "VEHICLE_001" and state["cooling_failure"]:
            # Rapid thermal decay: cargo temp rises rapidly
            state["cargo_temp"] += random.uniform(0.3, 0.6)
            vibration += random.uniform(0.1, 0.3)  # Compressor struggling
        elif vehicle_id == "VEHICLE_002" and state["door_open"]:
            # Door left open: cargo temp rises, ambient air leaks in, door is OPEN
            door_status = "OPEN"
            state["cargo_temp"] += random.uniform(0.2, 0.4)
            humidity -= random.uniform(1.0, 2.0)
        elif vehicle_id == "VEHICLE_003" and v3_spike:
            # Vibration anomaly
            vibration = random.uniform(4.0, 6.0)
            state["cargo_temp"] += random.uniform(-0.05, 0.05)
        else:
            # Gradual return to base cargo temperature when no anomaly is active
            diff = state["cargo_temp"] - state["base_cargo"]
            if abs(diff) > 0.05:
                state["cargo_temp"] -= (diff * 0.1) # Smooth decay back to baseline
            else:
                state["cargo_temp"] = state["base_cargo"] + random.uniform(-0.1, 0.1)

        # Enforce physical limits or noise
        cargo = round(state["cargo_temp"], 2)
        ambient = round(ambient, 2)
        humidity = max(0.0, min(100.0, round(humidity, 2)))
        vibration = max(0.0, round(vibration, 2))

        payload = {
            "Timestamp": datetime.utcnow().isoformat() + "Z",
            "Vehicle_ID": vehicle_id,
            "Ambient_Temp": ambient,
            "Cargo_Temp": cargo,
            "Humidity": humidity,
            "Vibration": vibration,
            "Door_Status": door_status
        }
        
        try:
            producer.send(KAFKA_TOPIC, payload)
        except Exception as e:
            print(f"Failed to send telemetry for {vehicle_id}: {e}", flush=True)

    producer.flush()
    time.sleep(1)
