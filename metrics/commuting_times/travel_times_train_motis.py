import requests
import pandas as pd
from multiprocessing import Pool, cpu_count
from scipy.spatial import cKDTree
import csv
import random
from tqdm import tqdm
import os
import argparse
from datetime import datetime as dt_datetime  # Alias to avoid conflict
import pytz
from timezonefinder import TimezoneFinder

# --- Configuration ---
# Default start time for MOTIS queries (local time at the origin station)
DEFAULT_LOCAL_START_TIME_STR = "2025-03-21T08:00:00"  # YYYY-MM-DDTHH:MM:SS
# Default maximum travel duration for spatial filtering (8 hours in seconds)
DEFAULT_MAX_TRAVEL_DURATION_SECONDS = 8 * 3600
# Default approximate speed for spatial filtering (e.g., 100 km/h for overall public transport)
DEFAULT_FILTERING_SPEED_KMH = 100
DEFAULT_MOTIS_URL = "http://localhost:8080"

# Dictionary of major European capital stations (UIC codes) - to select start stations
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


def local_to_utc_iso(lat, lon, local_dt_str):
    """Converts a local datetime string at a lat/lon to UTC ISO 8601 Zulu format."""
    tf = TimezoneFinder()
    tz_name = tf.timezone_at(lat=lat, lng=lon)
    if not tz_name:
        tqdm.write(
            f"Warning: Could not determine timezone for ({lat}, {lon}). Assuming UTC."
        )
        tz_name = "UTC"  # Fallback to UTC

    try:
        local_dt_obj = dt_datetime.strptime(local_dt_str, "%Y-%m-%dT%H:%M:%S")
        local_tz = pytz.timezone(tz_name)
        localized_dt = local_tz.localize(
            local_dt_obj, is_dst=None
        )  # is_dst=None handles ambiguous times
    except (pytz.exceptions.UnknownTimeZoneError, ValueError) as e:
        tqdm.write(
            f"Error with timezone ({tz_name}) or datetime parsing ({local_dt_str}): {e}. Assuming UTC."
        )
        local_dt_obj = dt_datetime.strptime(local_dt_str, "%Y-%m-%dT%H:%M:%S")
        localized_dt = pytz.utc.localize(local_dt_obj)

    utc_dt = localized_dt.astimezone(pytz.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def get_motis_itineraries(
    start_lon, start_lat, end_lon, end_lat, departure_time_utc_iso, motis_backends
):
    """
    Calls the MOTIS /intermodal endpoint (or /plan) to compute routes.
    Returns a list of unique itineraries (min_duration, min_transfers).
    Each itinerary: (duration_seconds, transfers, start_time_iso_api)
    """
    if not motis_backends:
        raise ValueError("No MOTIS backends provided.")

    motis_backend_url = random.choice(
        motis_backends
    )  # Simple random choice for load distribution
    # Using /plan endpoint as in the original script. /intermodal might also be an option.
    url = f"{motis_backend_url}/api/v1/plan"

    # MOTIS expects "latitude,longitude" for from/to Place.
    # The original used "latitude,longitude,level" (level 0)
    from_place = f"{start_lat},{start_lon}"
    to_place = f"{end_lat},{end_lon}"

    if from_place == to_place:  # Should be caught by filtering earlier but good check
        return []

    # Search window: how long after departure_time_utc_iso to search for connections.
    # 18 hours was in original, seems generous.
    search_window_seconds = 18 * 3600

    params = {
        "fromPlace": from_place,
        "toPlace": to_place,
        "time": departure_time_utc_iso,
        "searchForwards": True,  # True for departures, False for arrivals (arriveBy)
        "searchInterval": search_window_seconds,
        "numConnections": 5,  # Request a few connections to find good options
        # Parameters from original script (adjust if using /intermodal or need different features)
        # "timetableView": True, # This was in original, might be for specific views
        # "maxMatchingDistance": 150, # For matching to stations
        # The original had "numItineraries": 20 and "detailedTransfers": False
    }

    itineraries_found = []
    try:
        response = requests.get(url, params=params, timeout=20)  # 20-second timeout
        response.raise_for_status()
        data = response.json()

        # Original script checked "itineraries" and "direct" keys.
        # The /plan endpoint usually returns connections in a list under 'connections' key.
        # Let's adapt to typical MOTIS /plan response structure.
        raw_itineraries = data.get("connections", [])

        for itin in raw_itineraries:
            duration_seconds = itin.get("duration")  # Typically in seconds
            transfers = itin.get("transfers")
            # Get actual departure time of this specific itinerary from API
            # MOTIS times are usually full ISO strings with timezone.
            # We want the start time of the journey from the origin.
            api_start_time_unix = itin.get("departure", {}).get(
                "time"
            )  # Unix timestamp

            if (
                duration_seconds is not None
                and transfers is not None
                and api_start_time_unix is not None
            ):
                # Convert API start time (Unix epoch) to ISO format
                try:
                    api_start_dt_utc = dt_datetime.fromtimestamp(
                        api_start_time_unix, tz=pytz.utc
                    )
                    api_start_time_iso = api_start_dt_utc.strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    )
                except Exception:
                    api_start_time_iso = departure_time_utc_iso  # Fallback

                itineraries_found.append(
                    {
                        "duration": int(round(duration_seconds)),
                        "transfers": int(transfers),
                        "start_time": api_start_time_iso,
                    }
                )

        if not itineraries_found:
            return []

        # Filter to keep only the itinerary with the least transfers and the one with the least duration
        # (as per original script's logic)
        min_transfers_itin = min(itineraries_found, key=lambda x: x["transfers"])
        min_duration_itin = min(itineraries_found, key=lambda x: x["duration"])

        # Use a set of tuples to ensure uniqueness before returning
        unique_itineraries_set = set()
        if min_transfers_itin:
            unique_itineraries_set.add(
                (
                    min_transfers_itin["duration"],
                    min_transfers_itin["transfers"],
                    min_transfers_itin["start_time"],
                )
            )
        if min_duration_itin:
            unique_itineraries_set.add(
                (
                    min_duration_itin["duration"],
                    min_duration_itin["transfers"],
                    min_duration_itin["start_time"],
                )
            )
        return list(unique_itineraries_set)

    except requests.exceptions.RequestException as e:
        tqdm.write(f"MOTIS API request failed for {url} with params {params}: {e}")
    except Exception as e:  # Catch other errors like JSON parsing
        tqdm.write(f"Error processing MOTIS response from {url}: {e}")
    return []


def process_destination_station_motis(args_tuple):
    """
    Helper function for multiprocessing. Gets MOTIS itineraries to one destination.
    Returns a list of tuples: (destination_station_id, duration_sec, start_time_iso, transfers)
    """
    (
        start_coords_lon,
        start_coords_lat,
        dest_station_series,
        departure_time_utc_iso,
        motis_backends,
    ) = args_tuple
    dest_coords_lon, dest_coords_lat = (
        dest_station_series["longitude"],
        dest_station_series["latitude"],
    )

    results_for_dest = []
    try:
        itineraries = get_motis_itineraries(
            start_coords_lon,
            start_coords_lat,
            dest_coords_lon,
            dest_coords_lat,
            departure_time_utc_iso,
            motis_backends,
        )
        for duration, transfers, api_start_time in itineraries:
            results_for_dest.append(
                (
                    dest_station_series["id"],  # Internal DataFrame ID
                    duration,
                    api_start_time,
                    transfers,
                )
            )
    except Exception as e:
        tqdm.write(
            f"Error getting MOTIS itineraries for dest {dest_station_series['id']} ({dest_station_series['name']}): {e}"
        )
    return results_for_dest


def compute_itineraries_for_start_station_motis(
    start_station_series,
    all_stations_df,
    output_dir,
    local_start_time_str,
    max_travel_duration_sec,
    filtering_speed_kmh,
    motis_backends,
):
    """
    Computes MOTIS itineraries from a single start station to all reachable stations.
    """
    start_station_id = start_station_series["id"]
    start_station_name = start_station_series["name"]
    start_coords_lon, start_coords_lat = (
        start_station_series["longitude"],
        start_station_series["latitude"],
    )

    output_csv_file = os.path.join(
        output_dir,
        f"motis_from_{start_station_id}_{start_station_name.replace(' ', '_')}.csv",
    )

    if os.path.exists(output_csv_file):
        tqdm.write(f"Output file {output_csv_file} already exists. Skipping.")
        return

    tqdm.write(
        f"Processing MOTIS itineraries from: {start_station_name} (ID: {start_station_id})"
    )

    # Convert local start time at origin to UTC ISO for MOTIS query
    try:
        departure_time_utc_iso = local_to_utc_iso(
            start_coords_lat, start_coords_lon, local_start_time_str
        )
        tqdm.write(
            f"Effective UTC departure time for {start_station_name}: {departure_time_utc_iso}"
        )
    except Exception as e:
        tqdm.write(
            f"Failed to convert local start time to UTC for {start_station_name}: {e}. Skipping this station."
        )
        return

    # Spatially filter all stations
    max_dist_km = (max_travel_duration_sec / 3600) * filtering_speed_kmh
    max_dist_deg = max_dist_km / 111.0

    station_coords_array = all_stations_df[["longitude", "latitude"]].values
    destinations_to_process = []
    try:
        tree = cKDTree(station_coords_array)
        reachable_indices = tree.query_ball_point(
            (start_coords_lon, start_coords_lat), max_dist_deg
        )
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

    tasks = [
        (
            start_coords_lon,
            start_coords_lat,
            dest_station,
            departure_time_utc_iso,
            motis_backends,
        )
        for dest_station in destinations_to_process
    ]

    all_results_for_start_station = []
    pool_size = min(
        cpu_count(), 6, len(tasks)
    )  # Limit pool size for MOTIS (can be API intensive)

    if pool_size > 0:
        with Pool(pool_size) as pool:
            with tqdm(
                total=len(tasks),
                desc=f"Querying MOTIS from {start_station_name}",
                position=1,
                leave=False,
            ) as pbar:
                for list_of_itineraries in pool.imap_unordered(
                    process_destination_station_motis, tasks
                ):
                    if (
                        list_of_itineraries
                    ):  # Will be a list of itineraries for that destination
                        all_results_for_start_station.extend(list_of_itineraries)
                    pbar.update(1)
    else:  # Fallback
        for task in tasks:
            list_of_itineraries = process_destination_station_motis(task)
            if list_of_itineraries:
                all_results_for_start_station.extend(list_of_itineraries)

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
        if all_results_for_start_station:
            writer.writerows(all_results_for_start_station)
    tqdm.write(
        f"Finished MOTIS processing for {start_station_name}. Results saved to {output_csv_file}"
    )


parser = argparse.ArgumentParser(
    description="Compute train/public transport itineraries using MOTIS API."
)
parser.add_argument(
    "--stations_csv",
    default="../../data/trainline/stations.csv",
    help="Path to the stations CSV file.",
)
parser.add_argument(
    "--output_dir",
    default="travel_times_output/motis_train",
    help="Directory to save the output CSV files.",
)
parser.add_argument(
    "--motis_url",
    default=DEFAULT_MOTIS_URL,
    help="MOTIS API URL.",
)
parser.add_argument(
    "--departure_time_local",
    default=DEFAULT_LOCAL_START_TIME_STR,
    help=f"Local departure time at origin (YYYY-MM-DDTHH:MM:SS), default: {DEFAULT_LOCAL_START_TIME_STR}.",
)
parser.add_argument(
    "--max_travel_duration_sec",
    type=int,
    default=DEFAULT_MAX_TRAVEL_DURATION_SECONDS,
    help=f"Max travel duration for spatial filtering (default: {DEFAULT_MAX_TRAVEL_DURATION_SECONDS}).",
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
    tqdm.write("Error: No stations found in the CSV file.")
    exit(1)

motis_backends = list(
    set(filter(None, [args.motis_url]))
)
if not motis_backends:
    tqdm.write("Error: No MOTIS backend URLs provided.")
    exit(1)
tqdm.write(f"Using MOTIS backends: {motis_backends}")

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
    start_stations_to_process, desc="Processing Start Stations (MOTIS)", position=0
):
    compute_itineraries_for_start_station_motis(
        start_station_series,
        all_stations_df,
        args.output_dir,
        args.departure_time_local,
        args.max_travel_duration_sec,
        args.filtering_speed_kmh,
        motis_backends,
    )

tqdm.write("All MOTIS processing complete.")
