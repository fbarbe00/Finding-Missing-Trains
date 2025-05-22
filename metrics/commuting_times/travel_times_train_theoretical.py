import requests
import pandas as pd
from multiprocessing import Pool, cpu_count
from scipy.spatial import cKDTree
import csv
import random
from tqdm import tqdm
import os
import argparse

# --- Configuration ---
# Default maximum travel duration for spatial filtering (8 hours in seconds)
DEFAULT_MAX_DURATION_SECONDS = 8 * 3600
# Default approximate speed for spatial filtering (e.g., 150 km/h for trains)
DEFAULT_FILTERING_SPEED_KMH = 150  # Adjusted for train context

DEFAULT_OSRM_TRAIN = (
    "http://localhost:5000" 
)

DEFAULT_START_TIME_ISO = "2025-03-21T08:00:00.000Z"

CAPITAL_STATIONS_UIC_FOR_START = {
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
            .rename(columns={"index": "id"})  # Internal ID
        )
        stations_df = stations_df.dropna(
            subset=["latitude", "longitude", "uic", "name"]
        )
        stations_df["uic"] = stations_df["uic"].astype(int)
        return stations_df
    except FileNotFoundError:
        tqdm.write(f"Error: Stations CSV file not found at {stations_csv_path}")
        exit(1)
    except Exception as e:
        tqdm.write(f"Error loading stations CSV: {e}")
        exit(1)


def get_osrm_train_duration(
    start_lon, start_lat, end_lon, end_lat, osrm_backends, session
):
    """
    Fetches route duration from OSRM backends using a requests session.
    Randomly picks a backend.
    Note: Uses 'driving' profile as per original script. A dedicated 'train' profile on OSRM would be ideal.
    """
    if not osrm_backends:
        raise ValueError("No OSRM backends provided.")

    backend_url = random.choice(osrm_backends)

    url = f"{backend_url}/route/v1/driving/{start_lon},{start_lat};{end_lon},{end_lat}?overview=false"

    try:
        response = session.get(url, timeout=15)  # 15-second timeout
        response.raise_for_status()  # Will raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        if data.get("code") == "Ok" and data.get("routes"):
            return data["routes"][0]["duration"]  # Duration in seconds
        else:
            tqdm.write(
                f"OSRM error or no route from {backend_url}: {data.get('message', data.get('code', 'Unknown error'))}"
            )
            return None
    except requests.exceptions.RequestException as e:
        tqdm.write(f"Request failed for OSRM backend {backend_url}: {e}")
        return None
    except Exception as e:  # Catch other potential errors like JSON parsing
        tqdm.write(f"An unexpected error occurred with OSRM backend {backend_url}: {e}")
        return None


def process_destination_station_osrm_train(args_tuple):
    """
    Helper function for multiprocessing. Gets OSRM train duration to one destination.
    Returns (destination_station_id, duration_seconds, start_time_iso, transfers) or None.
    """
    (
        start_coords_lon,
        start_coords_lat,
        dest_station_series,
        calculation_time_iso,
        osrm_backends,
        session,
    ) = args_tuple
    dest_coords = (dest_station_series["longitude"], dest_station_series["latitude"])

    try:
        duration_seconds = get_osrm_train_duration(
            start_coords_lon,
            start_coords_lat,
            dest_coords[0],
            dest_coords[1],
            osrm_backends,
            session,
        )
        if duration_seconds is not None:
            return (
                dest_station_series["id"],  # Internal DataFrame ID
                int(round(duration_seconds)),
                calculation_time_iso,  # Using the fixed calculation time for consistency
                0,  # Transfers for OSRM direct route is 0
            )
    except Exception as e:
        tqdm.write(
            f"Error processing OSRM train destination {dest_station_series['id']} ({dest_station_series['name']}): {e}"
        )
    return None


def compute_travel_times_for_start_station_osrm_train(
    start_station_series,
    all_stations_df,
    output_dir,
    max_duration_filter_sec,
    filtering_speed_kmh,
    osrm_backends,
    start_time_iso,
):
    """
    Computes OSRM "train" travel times from a single start station to all reachable stations.
    """
    start_station_id = start_station_series["id"]
    start_station_name = start_station_series["name"]
    start_coords = (start_station_series["longitude"], start_station_series["latitude"])

    output_csv_file = os.path.join(
        output_dir,
        f"osrmtrain_from_{start_station_id}_{start_station_name.replace(' ', '_')}.csv",
    )

    if os.path.exists(output_csv_file):
        tqdm.write(f"Output file {output_csv_file} already exists. Skipping.")
        return

    tqdm.write(
        f"Processing OSRM 'train' travel times from: {start_station_name} (ID: {start_station_id})"
    )

    # Spatially filter all stations to find potentially reachable ones
    max_dist_km = (max_duration_filter_sec / 3600) * filtering_speed_kmh
    max_dist_deg = max_dist_km / 111.0  # Rough conversion

    station_coords_array = all_stations_df[["longitude", "latitude"]].values
    destinations_to_process = []
    try:
        tree = cKDTree(station_coords_array)
        reachable_indices = tree.query_ball_point(start_coords, max_dist_deg)
        potential_destinations_df = all_stations_df.iloc[reachable_indices]
        destinations_to_process = [
            row
            for _, row in potential_destinations_df.iterrows()
            if row["id"] != start_station_id
        ]
        tqdm.write(
            f"Spatially filtered to {len(destinations_to_process)} potential destinations from {len(all_stations_df)}."
        )
    except Exception as e:
        tqdm.write(
            f"Error during spatial filtering: {e}. Processing all other stations (this might be slow)."
        )
        destinations_to_process = [
            row
            for _, row in all_stations_df.iterrows()
            if row["id"] != start_station_id
        ]

    if not destinations_to_process:
        tqdm.write(f"No destination stations to process for {start_station_name}.")
        with open(output_csv_file, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "destination_station_id",
                    "duration_seconds",
                    "start_time_isoformat",
                    "transfers",
                ]
            )
        return

    session = requests.Session()
    tasks = [
        (
            start_coords[0],
            start_coords[1],
            dest_station,
            start_time_iso,
            osrm_backends,
            session,
        )
        for dest_station in destinations_to_process
    ]

    results = []
    pool_size = min(cpu_count(), 8, len(tasks))  # Limit pool size

    if pool_size > 0:
        with Pool(pool_size) as pool:
            with tqdm(
                total=len(tasks),
                desc=f"Calculating OSRM routes from {start_station_name}",
                position=1,
                leave=False,
            ) as pbar:
                for result in pool.imap_unordered(
                    process_destination_station_osrm_train, tasks
                ):
                    if result:
                        results.append(result)
                    pbar.update(1)
    else:
        for task in tasks:
            result = process_destination_station_osrm_train(task)
            if result:
                results.append(result)

    with open(output_csv_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "destination_station_id",
                "duration_seconds",
                "start_time_isoformat",
                "transfers",
            ]
        )
        if results:
            writer.writerows(results)
    tqdm.write(
        f"Finished OSRM 'train' processing for {start_station_name}. Results: {output_csv_file}"
    )


parser = argparse.ArgumentParser(
    description="Compute 'train' travel times (using OSRM driving profile) from selected stations."
)
parser.add_argument(
    "--stations_csv",
    default="../../data/trainline/stations.csv",
    help="Path to the stations CSV file.",
)
parser.add_argument(
    "--output_dir",
    default="travel_times_output/osrm_train",
    help="Directory to save the output CSV files.",
)
parser.add_argument(
    "--osrm_url",
    default=DEFAULT_OSRM_TRAIN,
    help="OSRM backend URL.",
)
parser.add_argument(
    "--start_time_iso",
    default=DEFAULT_START_TIME_ISO,
    help="Fixed ISO start time for record-keeping.",
)
parser.add_argument(
    "--max_duration_seconds",
    type=int,
    default=DEFAULT_MAX_DURATION_SECONDS,
    help=f"Max travel duration for filtering (default: {DEFAULT_MAX_DURATION_SECONDS}).",
)
parser.add_argument(
    "--filtering_speed_kmh",
    type=float,
    default=DEFAULT_FILTERING_SPEED_KMH,
    help=f"Assumed avg speed for spatial filtering (default: {DEFAULT_FILTERING_SPEED_KMH}).",
)

args = parser.parse_args()

os.makedirs(args.output_dir, exist_ok=True)
all_stations_df = load_stations(args.stations_csv)
if all_stations_df.empty:
    tqdm.write("Error: No stations found in the CSV file. Please check the file.")
    exit(1)

osrm_backends = list(
    set(filter(None, [args.osrm_url]))
)
if not osrm_backends:
    tqdm.write(
        "Error: No OSRM backend URLs provided. Please specify with --osrm_url."
    )
    exit(1)
tqdm.write(f"Using OSRM backends: {osrm_backends}")

start_stations_to_process = []
for name, uic_code_str in CAPITAL_STATIONS_UIC_FOR_START.items():
    uic_code = int(uic_code_str)
    match = all_stations_df[all_stations_df["uic"] == uic_code]
    if not match.empty:
        start_stations_to_process.append(match.iloc[0])
    else:
        tqdm.write(f"Warning: Start station {name} (UIC: {uic_code}) not found.")

if not start_stations_to_process:
    tqdm.write("No start stations found to process. Exiting.")
    exit(1)

tqdm.write(
    f"Found {len(start_stations_to_process)} stations to use as starting points."
)

for start_station_series in tqdm(
    start_stations_to_process, desc="Processing Start Stations", position=0
):
    compute_travel_times_for_start_station_osrm_train(
        start_station_series,
        all_stations_df,
        args.output_dir,
        args.max_duration_seconds,
        args.filtering_speed_kmh,
        osrm_backends,
        args.start_time_iso,
    )

tqdm.write("All OSRM 'train' processing complete.")
