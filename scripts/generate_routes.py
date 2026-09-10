#!/usr/bin/env python3
"""
generate_routes.py
Fetches high-resolution real road coordinates from OpenStreetMap OSRM API
for all Chicagoland distribution hub routes from the Base Warehouse.
Saves the routes into config/routes.json.
"""

import json
import os
import time
import urllib.request
import urllib.error

BASE_LAT = 41.8875294
BASE_LON = -87.6513405

HUBS = {
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

def fetch_road_route(start_lon, start_lat, end_lon, end_lat):
    url = (
        f"http://router.project-osrm.org/route/v1/driving/"
        f"{start_lon},{start_lat};{end_lon},{end_lat}"
        f"?overview=full&geometries=geojson"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "LakehouseColdChainDemo/1.0"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("code") == "Ok" and "routes" in data and len(data["routes"]) > 0:
                    raw_coords = data["routes"][0]["geometry"]["coordinates"]
                    # Convert GeoJSON [lon, lat] -> [lat, lon]
                    route_lat_lon = [[round(pt[1], 5), round(pt[0], 5)] for pt in raw_coords]
                    return route_lat_lon
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}. Retrying...", flush=True)
            time.sleep(2)
    return None

def main():
    print("🚗 Fetching real road routes from OpenStreetMap OSRM...", flush=True)
    all_routes = {}
    
    for hub_name, (dest_lat, dest_lon) in HUBS.items():
        print(f"Fetching route for {hub_name} (dest: {dest_lat}, {dest_lon})...", flush=True)
        waypoints = fetch_road_route(BASE_LON, BASE_LAT, dest_lon, dest_lat)
        if waypoints:
            print(f"  ✅ Retrieved {len(waypoints)} real road points for {hub_name}", flush=True)
            all_routes[hub_name] = {
                "origin": "Base Warehouse",
                "destination": hub_name,
                "origin_coords": [round(BASE_LAT, 5), round(BASE_LON, 5)],
                "dest_coords": [round(dest_lat, 5), round(dest_lon, 5)],
                "waypoints": waypoints
            }
        else:
            print(f"  ❌ Failed to retrieve route for {hub_name}. Generating interpolated fallback.", flush=True)
            steps = 200
            interp = []
            for i in range(steps + 1):
                f = i / steps
                lat = round(BASE_LAT + f * (dest_lat - BASE_LAT), 5)
                lon = round(BASE_LON + f * (dest_lon - BASE_LON), 5)
                interp.append([lat, lon])
            all_routes[hub_name] = {
                "origin": "Base Warehouse",
                "destination": hub_name,
                "origin_coords": [round(BASE_LAT, 5), round(BASE_LON, 5)],
                "dest_coords": [round(dest_lat, 5), round(dest_lon, 5)],
                "waypoints": interp
            }
        time.sleep(1)

    out_paths = [
        "config/routes.json",
        "docker/simulator/routes.json"
    ]
    for p in out_paths:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            json.dump(all_routes, f, indent=2)
        print(f"Saved road routes to {p}", flush=True)

if __name__ == "__main__":
    main()
