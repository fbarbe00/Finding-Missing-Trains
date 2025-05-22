import requests
import pandas as pd
from multiprocessing import Pool, cpu_count
from scipy.spatial import cKDTree
import csv
import random
import time
from tqdm import tqdm
import os
import argparse
from datetime import datetime

# --- Configuration ---
# Default maximum travel duration for spatial filtering (8 hours in seconds)
DEFAULT_MAX_DURATION_SECONDS = 8 * 3600
# Default approximate speed for spatial filtering (e.g., 90 km/h converted to degrees/sec for rough filtering)
# This is a rough guide for initial filtering, actual duration comes from OSRM.
DEFAULT_FILTERING_SPEED_KMH = 90

# OSRM backend servers for car routing
OSRM_CAR_BACKENDS = [
    # you can use your own OSRM server here
    {'url': 'http://router.project-osrm.org', 'requires_key': False},
    {'url': 'http://routing.openstreetmap.de/routed-car', 'requires_key': False},
    {'url': 'https://routing.geofabrik.de/c60a60513689427f829b69ebaf70e655', 'requires_key': False},
]
current_backend_index = 0

# Dictionary of major European capital stations (UIC codes)
CAPITAL_STATIONS_UIC = {
    "Brussels": "8814001",  # Bruxelles-Midi
    "Paris": "8727100",  # Paris Gare du Nord
    "Vienna": "8101003",  # Wien Hauptbahnhof
    "Zagreb": "7872480",  # Zagreb Glavni Kolodvor
    "Prague": "5457076",  # Praha hlavní nádraží
    "Copenhagen": "8600626",  # København H
    "Helsinki": "1000001",  # Helsinki Asema
    "Berlin": "8065969",  # Berlin Hauptbahnhof
    "Athens": "7300106",  # Athína
    "Budapest": "5510017",  # Budapest Keleti
    "Rome": "8308409",  # Roma Termini
    "Riga": "2509501",  # Rīga
    "Vilnius": "2412000",  # Vilnius
    "Luxembourg": "8291601",  # Luxembourg
    "Amsterdam": "8400058",  # Amsterdam Centraal
    "Warsaw": "5103865",  # Warszawa Wschodnia
    "Lisbon": "9430007",  # Lisboa Santa Apolónia
    "Bucharest": "5310017",  # București Nord
    "Bratislava": "5613206",  # Bratislava hlavná stanica
    "Ljubljana": "7942300",  # Ljubljana
    "Madrid": "7160000",  # Madrid Puerta de Atocha
    "Stockholm": "7403751",  # Stockholm Central
}

# --- Helper Functions ---

def load_stations(stations_csv_path):
    """Loads station data from a CSV file."""
    try:
        stations_df = (
            pd.read_csv(
                stations_csv_path,
                sep=";",
                usecols=["name", "latitude", "longitude", "uic"],
            )
            .reset_index()
            .rename(columns={"index": "id"})
        )
        stations_df = stations_df.dropna(subset=["latitude", "longitude", "uic", "name"])
        stations_df["uic"] = stations_df["uic"].astype(int)
        return stations_df
    except FileNotFoundError:
        tqdm.write(f"Error: Stations CSV file not found at {stations_csv_path}")
        exit(1)
    except Exception as e:
        tqdm.write(f"Error loading stations CSV: {e}")
        exit(1)

def get_osrm_route_duration(start_lon, start_lat, end_lon, end_lat):
    """
    Fetches route duration from an OSRM backend.
    Rotates through backends if one fails.
    """
    global current_backend_index
    
    # Small chance to proactively switch backend to distribute load
    if random.random() < 0.01: # 1% chance to switch proactively
        current_backend_index = (current_backend_index + 1) % len(OSRM_CAR_BACKENDS)

    for i in range(len(OSRM_CAR_BACKENDS)):
        backend_to_try = OSRM_CAR_BACKENDS[current_backend_index]
        osrm_url = backend_to_try['url']
        route_url = f"{osrm_url}/route/v1/driving/{start_lon},{start_lat};{end_lon},{end_lat}?overview=false"
        headers = backend_to_try.get('headers', {})
        
        try:
            response = requests.get(route_url, headers=headers, timeout=10) # 10-second timeout
            if response.status_code == 200:
                data = response.json()
                if data.get('routes') and len(data['routes']) > 0:
                    return data['routes'][0]['duration'] # Duration in seconds
                else:
                    # Valid response but no route found
                    tqdm.write(f"No route found between ({start_lon},{start_lat}) and ({end_lon},{end_lat}) using {osrm_url}.")
                    return None
            else:
                tqdm.write(f"OSRM API error from {osrm_url}: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Request failed for {osrm_url}: {e}")

        # If request failed or no route, try next backend
        current_backend_index = (current_backend_index + 1) % len(OSRM_CAR_BACKENDS)
        if i < len(OSRM_CAR_BACKENDS) - 1: # Avoid sleeping after the last attempt
            time.sleep(1) # Wait a bit before trying the next backend

    tqdm.write(f"Error: Failed to fetch route data from all OSRM backends for ({start_lon},{start_lat}) to ({end_lon},{end_lat}).")
    return None


def process_destination_station(args_tuple):
    """
    Helper function for multiprocessing. Gets duration to one destination.
    Returns (destination_station_id, duration_seconds, start_time_iso, transfers) or None.
    """
    start_coords_lon, start_coords_lat, dest_station_series, calculation_timestamp_iso = args_tuple
    dest_coords = (dest_station_series["longitude"], dest_station_series["latitude"])
    
    try:
        duration_seconds = get_osrm_route_duration(
            start_coords_lon, start_coords_lat,
            dest_coords[0], dest_coords[1]
        )
        if duration_seconds is not None:
            return (
                dest_station_series["id"], # Using internal DataFrame ID
                int(round(duration_seconds)),
                calculation_timestamp_iso,
                0  # Transfers for car travel is 0
            )
    except Exception as e:
        tqdm.write(f"Error processing destination station {dest_station_series['id']} ({dest_station_series['name']}): {e}")
    return None

def compute_travel_times_for_start_station(start_station_series, all_stations_df, output_dir, capitals_only, capital_station_ids):
    """
    Computes travel times from a single start station to either all other capital cities
    or all reachable stations.
    """
    start_station_id = start_station_series["id"]
    start_station_name = start_station_series["name"]
    start_coords = (start_station_series["longitude"], start_station_series["latitude"])
    
    output_csv_file = os.path.join(output_dir, f"car_from_{start_station_id}_{start_station_name.replace(' ', '_')}.csv")

    if os.path.exists(output_csv_file):
        tqdm.write(f"Output file {output_csv_file} already exists. Skipping.")
        return

    tqdm.write(f"Processing car travel times from: {start_station_name} (ID: {start_station_id})")
    
    destinations_to_process = []
    if capitals_only:
        # Filter for other capital stations
        for capital_id in capital_station_ids:
            if capital_id != start_station_id:
                dest_station = all_stations_df[all_stations_df['id'] == capital_id].iloc[0]
                destinations_to_process.append(dest_station)
        tqdm.write(f"Mode: Capitals only. Found {len(destinations_to_process)} destination capitals.")
    else:
        # Spatially filter all stations to find potentially reachable ones
        max_dist_km = (DEFAULT_MAX_DURATION_SECONDS / 3600) * DEFAULT_FILTERING_SPEED_KMH
        # Rough conversion: 1 degree latitude approx 111 km. Longitude varies.
        max_dist_deg = max_dist_km / 111.0  

        station_coords_array = all_stations_df[['longitude', 'latitude']].values
        try:
            tree = cKDTree(station_coords_array)
            # Query for stations within the bounding box / rough distance
            reachable_indices = tree.query_ball_point(start_coords, max_dist_deg)
            potential_destinations_df = all_stations_df.iloc[reachable_indices]
            # Exclude the start station itself from destinations
            destinations_to_process = [row for _, row in potential_destinations_df.iterrows() if row['id'] != start_station_id]
            tqdm.write(f"Mode: All reachable. Spatially filtered to {len(destinations_to_process)} potential destinations from {len(all_stations_df)}.")
        except Exception as e:
            tqdm.write(f"Error during spatial filtering: {e}. Proceeding without pre-filtering if list is short.")
            destinations_to_process = [row for _, row in all_stations_df.iterrows() if row['id'] != start_station_id]


    if not destinations_to_process:
        tqdm.write(f"No destination stations to process for {start_station_name}.")
        # Create empty CSV with headers
        with open(output_csv_file, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["destination_station_id", "duration_seconds", "start_time_isoformat", "transfers"])
        return

    # Prepare arguments for parallel processing
    calculation_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    tasks = [(start_coords[0], start_coords[1], dest_station, calculation_timestamp) for dest_station in destinations_to_process]

    results = []
    # Limit pool size to avoid overwhelming OSRM servers, even if cpu_count is high
    pool_size = min(cpu_count(), 4, len(tasks)) 
    if pool_size > 0:
        with Pool(pool_size) as pool:
            with tqdm(total=len(tasks), desc=f"Calculating routes from {start_station_name}", position=1, leave=False) as pbar:
                for result in pool.imap_unordered(process_destination_station, tasks):
                    if result:
                        results.append(result)
                    pbar.update(1)
    else: # Fallback for very few tasks
        for task in tasks:
            result = process_destination_station(task)
            if result:
                results.append(result)
                
    # Write results to CSV
    with open(output_csv_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["destination_station_id", "duration_seconds", "start_time_isoformat", "transfers"])
        if results:
            writer.writerows(results)
    tqdm.write(f"Finished processing for {start_station_name}. Results saved to {output_csv_file}")


parser = argparse.ArgumentParser(description="Compute car travel times between train stations using OSRM.")
parser.add_argument("--stations_csv", default="../../data/trainline/stations.csv",
                    help="Path to the stations CSV file (default: ../../data/trainline/stations.csv).")
parser.add_argument("--output_dir", default="travel_times_output/car",
                    help="Directory to save the output CSV files (default: travel_times_output/car).")
parser.add_argument("--capitals_only", action="store_true",
                    help="Only compute travel times between capital city stations defined in the script.")
parser.add_argument("--max_duration_seconds", type=int, default=DEFAULT_MAX_DURATION_SECONDS,
                    help=f"Maximum travel duration in seconds for filtering (default: {DEFAULT_MAX_DURATION_SECONDS}). Relevant if not capitals_only.")
parser.add_argument("--filtering_speed_kmh", type=float, default=DEFAULT_FILTERING_SPEED_KMH,
                    help=f"Assumed average speed in km/h for initial spatial filtering (default: {DEFAULT_FILTERING_SPEED_KMH}). Relevant if not capitals_only.")

args = parser.parse_args()

DEFAULT_MAX_DURATION_SECONDS = args.max_duration_seconds
DEFAULT_FILTERING_SPEED_KMH = args.filtering_speed_kmh

os.makedirs(args.output_dir, exist_ok=True)

all_stations_df = load_stations(args.stations_csv)
if all_stations_df.empty:
    print("No stations found in the CSV file. Exiting.")
    exit(1)

capital_station_ids = []
start_stations_list = []

for name, uic_code_str in CAPITAL_STATIONS_UIC.items():
    uic_code = int(uic_code_str)
    match = all_stations_df[all_stations_df["uic"] == uic_code]
    if not match.empty:
        station_series = match.iloc[0]
        capital_station_ids.append(station_series["id"])
        start_stations_list.append(station_series)
    else:
        tqdm.write(f"Warning: Capital station {name} (UIC: {uic_code}) not found in stations file.")

if not start_stations_list:
    tqdm.write("No capital stations found to process as start points. Exiting.")
    exit(1)

tqdm.write(f"Found {len(start_stations_list)} capital stations to use as starting points.")

for start_station_series in tqdm(start_stations_list, desc="Processing Start Capitals", position=0):
    compute_travel_times_for_start_station(
        start_station_series,
        all_stations_df,
        args.output_dir,
        args.capitals_only,
        capital_station_ids
    )

tqdm.write("All processing complete.")
