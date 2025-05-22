import requests
import json
from tqdm import tqdm
import csv
import io
import os
import time
from datetime import datetime, timezone
import argparse

# --- Configuration ---
DEFAULT_AIRPORTS_DAT_URL = (
    "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
)
DEFAULT_FLIGHTS_CACHE_FILE = "flightstats_cache.json"
DEFAULT_FS_APP_ID = None
DEFAULT_FS_APP_KEY = None

EUROPEAN_CAPITAL_AIRPORT_CODES = {
    "Vienna": ["VIE"],
    "Brussels": ["BRU", "CRL"],
    "Sofia": ["SOF"],
    "Nicosia": ["LCA", "PFO"],
    "Zagreb": ["ZAG"],
    "Prague": ["PRG"],
    "Copenhagen": ["CPH"],
    "Tallinn": ["TLL"],
    "Helsinki": ["HEL"],
    "Paris": ["CDG", "ORY"],
    "Berlin": ["TXL", "SXF"],
    "Athens": ["ATH"],
    "Budapest": ["BUD"],
    "Dublin": ["DUB"],
    "Rome": ["FCO", "CIA"],
    "Riga": ["RIX"],
    "Vilnius": ["VNO"],
    "Luxembourg": ["LUX"],
    "Valletta": ["MLA"],
    "Amsterdam": ["AMS"],
    "Warsaw": ["WAW", "WMI"],
    "Lisbon": ["LIS"],
    "Bucharest": ["OTP", "BBU"],
    "Bratislava": ["BTS"],
    "Ljubljana": ["LJU"],
    "Madrid": ["MAD"],
    "Stockholm": ["ARN", "BMA"],
    "London": ["LHR", "LGW", "STN", "LTN", "LCY", "SEN"],
}

# --- Helper Functions ---


def load_openflights_airports(airports_dat_url):
    """Loads airport data from OpenFlights airports.dat URL."""
    airports = {}
    try:
        tqdm.write(f"Loading airport data from {airports_dat_url}...")
        response = requests.get(airports_dat_url, timeout=15)
        response.raise_for_status()
        csvfile = io.StringIO(response.text)
        reader = csv.reader(csvfile)

        for row in reader:
            # Relevant columns: 0:ID, 1:Name, 2:City, 3:Country, 4:IATA, 5:ICAO, 6:Lat, 7:Lon
            if len(row) < 8 or not row[4] or row[4] == "\\N":  # Ensure IATA code exists
                continue
            try:
                latitude, longitude = float(row[6]), float(row[7])
                # Basic filter for Europe, can be adjusted
                if not (34 < latitude < 72 and -25 < longitude < 45):
                    continue

                airports[row[4]] = {  # Keyed by IATA
                    "id": row[0],
                    "name": row[1],
                    "city": row[2],
                    "country": row[3],
                    "iata": row[4],
                    "icao": row[5],
                    "latitude": latitude,
                    "longitude": longitude,
                }
            except ValueError:
                # tqdm.write(f"Skipping airport row due to parsing error: {row}")
                continue
        tqdm.write(f"Loaded {len(airports)} airports in the European region.")
        return airports
    except requests.exceptions.RequestException as e:
        tqdm.write(f"Error fetching airport data: {e}")
        return {}  # Return empty dict on error
    except Exception as e:
        tqdm.write(f"An unexpected error occurred while loading airports: {e}")
        return {}


def load_flight_cache(cache_filepath):
    """Loads flight duration cache from a JSON file."""
    if os.path.exists(cache_filepath):
        try:
            with open(cache_filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            tqdm.write(
                f"Warning: Cache file {cache_filepath} is corrupted. Starting with an empty cache."
            )
        except Exception as e:
            tqdm.write(
                f"Warning: Could not load cache file {cache_filepath}: {e}. Starting with an empty cache."
            )
    return {}


def save_flight_cache(cache_filepath, cache_data):
    """Saves flight duration cache to a JSON file."""
    try:
        with open(cache_filepath, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        tqdm.write(f"Error saving flight cache to {cache_filepath}: {e}")


def get_flight_details_from_flightstats(
    start_iata,
    end_iata,
    search_date_str,  # search_date_str: "YYYY/MM/DD"
    fs_app_id,
    fs_app_key,
    retries=2,
):
    """
    Fetches flight schedules from FlightStats API for a given date.
    Returns a list of flight details: (duration_seconds, departure_time_iso)
    Caches results.
    """
    if start_iata == end_iata:
        return []  # No travel needed

    year, month, day = search_date_str.split("/")

    # FlightStats API endpoint for schedules by route departing on a date
    url = (
        f"https://api.flightstats.com/flex/schedules/rest/v1/json/from/{start_iata}/to/{end_iata}/departing/"
        f"{year}/{month}/{day}?appId={fs_app_id}&appKey={fs_app_key}"
    )

    flight_options = []
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=20)  # 20-second timeout
            if response.status_code == 403:  # Forbidden, likely API key issue
                tqdm.write(
                    f"FlightStats API Forbidden (403): Check App ID/Key. URL: {url.replace(fs_app_key, '***KEY***')}"
                )
                return []  # Don't retry on auth failure
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

            data = response.json()
            scheduled_flights = data.get("scheduledFlights", [])

            for flight in scheduled_flights:
                dep_time_str = flight.get(
                    "departureTime"
                )  # e.g., "2024-09-01T06:00:00.000" (local to airport)
                arr_time_str = flight.get(
                    "arrivalTime"
                )  # e.g., "2024-09-01T08:00:00.000" (local to airport)

                if not dep_time_str or not arr_time_str:
                    continue

                try:
                    dep_dt_local = datetime.fromisoformat(dep_time_str)
                    arr_dt_local = datetime.fromisoformat(arr_time_str)
                    duration = arr_dt_local - dep_dt_local
                    duration_seconds = int(duration.total_seconds())

                    if duration_seconds > 0:
                        try:
                            dep_time_iso_for_output = dep_dt_local.isoformat() + "Z"
                        except Exception:
                            dep_time_iso_for_output = dep_time_str

                        flight_options.append(
                            (duration_seconds, dep_time_iso_for_output)
                        )

                except ValueError as ve:
                    tqdm.write(
                        f"Could not parse flight times for {start_iata}->{end_iata}: {dep_time_str}, {arr_time_str}. Error: {ve}"
                    )
                except Exception as e_parse:
                    tqdm.write(f"Unexpected error parsing flight times: {e_parse}")

            return flight_options  # Return all valid options found

        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 404:  # Not Found
                tqdm.write(
                    f"No flights found (404) for {start_iata}->{end_iata} on {search_date_str} via FlightStats."
                )
                return []  # No flights, not an error to retry usually
            tqdm.write(
                f"FlightStats API HTTP error: {http_err} for {start_iata}->{end_iata} on attempt {attempt + 1}. URL: {url.replace(fs_app_key, '***KEY***')}"
            )
        except requests.exceptions.RequestException as req_err:
            tqdm.write(
                f"FlightStats API request error: {req_err} for {start_iata}->{end_iata} on attempt {attempt + 1}."
            )

        if attempt < retries - 1:
            time.sleep(5 * (attempt + 1))  # Exponential backoff

    tqdm.write(
        f"Failed to get flight details for {start_iata}->{end_iata} after {retries} attempts."
    )
    return []


def process_flights_from_start_airport(
    start_airport_iata,
    all_airports_data,
    search_date_str,
    output_dir,
    fs_app_id,
    fs_app_key,
    flight_cache,
):
    """
    Processes all potential destination airports from a single start airport.
    Writes results to a CSV file.
    """
    start_airport_info = all_airports_data.get(start_airport_iata)
    if not start_airport_info:
        tqdm.write(
            f"Start airport {start_airport_iata} not found in loaded airport data. Skipping."
        )
        return

    start_airport_name = (
        start_airport_info.get("name", start_airport_iata)
        .replace(" ", "_")
        .replace("/", "_")
    )
    date_for_filename = search_date_str.replace("/", "")
    output_csv_file = os.path.join(
        output_dir,
        f"flight_from_{start_airport_iata}_{start_airport_name}_on_{date_for_filename}.csv",
    )

    if os.path.exists(output_csv_file):
        tqdm.write(f"Output file {output_csv_file} already exists. Skipping.")
        return

    tqdm.write(
        f"Processing flights from: {start_airport_iata} ({start_airport_info.get('name', '')}) on {search_date_str}"
    )

    # Initialize cache for this specific date and start_iata if not present
    flight_cache.setdefault(search_date_str, {}).setdefault(start_iata, {})

    all_flight_results_for_start_airport = []

    # Iterate through all known airports as potential destinations
    destination_iatas = [
        iata for iata in all_airports_data.keys() if iata != start_iata
    ]

    for end_iata in tqdm(
        destination_iatas,
        desc=f"Destinations from {start_iata}",
        position=1,
        leave=False,
    ):
        # Check cache first
        if end_iata in flight_cache[search_date_str][start_iata]:
            cached_options = flight_cache[search_date_str][start_iata][end_iata]
            if cached_options is None:  # Explicitly cached as no flights
                # tqdm.write(f"Using cached: No flights for {start_iata}->{end_iata} on {search_date_str}")
                pass
            else:
                # tqdm.write(f"Using cached: {len(cached_options)} flight options for {start_iata}->{end_iata} on {search_date_str}")
                for duration, dep_time_iso in cached_options:
                    all_flight_results_for_start_airport.append(
                        (end_iata, duration, dep_time_iso, 0)
                    )  # 0 transfers
        else:
            # If not in cache, query API
            flight_options_api = get_flight_details_from_flightstats(
                start_iata, end_iata, search_date_str, fs_app_id, fs_app_key
            )
            if not flight_options_api:  # No flights found by API
                flight_cache[search_date_str][start_iata][
                    end_iata
                ] = None  # Cache that no flights were found
            else:
                flight_cache[search_date_str][start_iata][
                    end_iata
                ] = flight_options_api  # Cache the found options
                for duration, dep_time_iso in flight_options_api:
                    all_flight_results_for_start_airport.append(
                        (end_iata, duration, dep_time_iso, 0)
                    )  # 0 transfers
            # Small delay to respect API rate limits if any (FlightStats can be sensitive)
            time.sleep(0.2)

    # Write all collected flight data for this start_airport to its CSV
    with open(output_csv_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "destination_airport_iata",
                "duration_seconds",
                "start_time_isoformat",
                "transfers",
            ]
        )
        if all_flight_results_for_start_airport:
            # Sort by destination IATA, then by duration for consistent output
            all_flight_results_for_start_airport.sort(key=lambda x: (x[0], x[1]))
            writer.writerows(all_flight_results_for_start_airport)

    tqdm.write(
        f"Finished processing flights for {start_iata}. Results: {output_csv_file}"
    )


parser = argparse.ArgumentParser(
    description="Compute flight travel times using FlightStats API."
)
parser.add_argument(
    "--airports_dat_url",
    default=DEFAULT_AIRPORTS_DAT_URL,
    help="URL to airports.dat file from OpenFlights.",
)
parser.add_argument(
    "--output_dir",
    default="travel_times_output/flight",
    help="Directory to save the output CSV files.",
)
parser.add_argument(
    "--fs_app_id",
    required=not DEFAULT_FS_APP_ID,
    default=DEFAULT_FS_APP_ID,
    help="FlightStats Application ID.",
)
parser.add_argument(
    "--fs_app_key",
    required=not DEFAULT_FS_APP_KEY,
    default=DEFAULT_FS_APP_KEY,
    help="FlightStats Application Key.",
)
parser.add_argument(
    "--cache_file",
    default=DEFAULT_FLIGHTS_CACHE_FILE,
    help="Path to the JSON file for caching flight data.",
)
parser.add_argument(
    "--date",
    default=datetime.now(timezone.utc).strftime("%Y/%m/%d"),
    help="Date for flight search (YYYY/MM/DD), default: today UTC.",
)

args = parser.parse_args()

if not args.fs_app_id or not args.fs_app_key:
    tqdm.write(
        "Error: FlightStats App ID and Key are required. Provide them via --fs_app_id and --fs_app_key."
    )
    exit(1)

os.makedirs(args.output_dir, exist_ok=True)

all_airports_data = load_openflights_airports(args.airports_dat_url)
if not all_airports_data:
    tqdm.write("No airport data loaded. Exiting.")
    exit(1)

flight_cache = load_flight_cache(args.cache_file)

# Get unique IATA codes for start airports from the capitals dictionary
start_airport_iatas_to_process = set()
for city, iata_list in EUROPEAN_CAPITAL_AIRPORT_CODES.items():
    for iata_code in iata_list:
        if iata_code in all_airports_data:  # Ensure the airport is in our loaded list
            start_airport_iatas_to_process.add(iata_code)
        else:
            tqdm.write(
                f"Warning: Capital airport {iata_code} for {city} not found in loaded airport data."
            )

if not start_airport_iatas_to_process:
    tqdm.write("No valid start capital airports to process. Exiting.")
    exit(1)

tqdm.write(
    f"Processing flights from {len(start_airport_iatas_to_process)} unique capital city airport IATAs."
)

try:
    for start_iata in tqdm(
        sorted(list(start_airport_iatas_to_process)),
        desc="Processing Start Airports (Flights)",
        position=0,
    ):
        process_flights_from_start_airport(
            start_iata,
            all_airports_data,
            args.date,
            args.output_dir,
            args.fs_app_id,
            args.fs_app_key,
            flight_cache,
        )
finally:
    # Save the updated cache regardless of how the loop finishes (e.g., Ctrl+C)
    tqdm.write("\nSaving flight cache...")
    save_flight_cache(args.cache_file, flight_cache)
    tqdm.write("Flight cache saved.")

tqdm.write("All flight processing complete.")
