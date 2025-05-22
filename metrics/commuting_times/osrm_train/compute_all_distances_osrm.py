import csv
import itertools
import requests
import os
from tqdm import tqdm
import argparse

parser = argparse.ArgumentParser(description="Compute all pairwise distances between a set of coordinates using the OSRM API.")
parser.add_argument("input_file", help="Path to the file containing the coordinates.")
parser.add_argument("output_file", help="Path to the output CSV file.")
parser.add_argument("--overwrite", action="store_true", help="Overwrite the output file if it already exists.")
parser.add_argument("--osrm-server", default="http://localhost:5000/route/v1/driving", help="URL of the OSRM server.")
args = parser.parse_args()

# Define OSRM server URL
OSRM_SERVER_URL = args.osrm_server

# Input and output file paths
INPUT_COORDS_FILE = args.input_file
OUTPUT_FILE = args.output_file
if os.path.exists(OUTPUT_FILE) and not args.overwrite:
    raise FileExistsError(f"Output file '{OUTPUT_FILE}' already exists. Use --overwrite to overwrite it.")

def read_coordinates(file_path):
    """Read coordinates from a file."""
    coordinates = []
    with open(file_path, "r") as file:
        for line in file:
            lat, lon = map(float, line.strip().split(","))
            coordinates.append((lon, lat))
    return coordinates

def query_osrm(server_url, coord1, coord2):
    """Query the OSRM server for the route between two coordinates."""
    coords = f"{coord1[0]},{coord1[1]};{coord2[0]},{coord2[1]}"
    response = requests.get(f"{server_url}/{coords}", params={"overview": "false"})
    
    if response.status_code == 200:
        data = response.json()
        if data["code"] == "Ok":
            route = data["routes"][0]
            return route["distance"], route["duration"]
    
    # Return None if the request fails or no valid route is found
    return None, None

# Read coordinates from the file
coordinates = read_coordinates(INPUT_COORDS_FILE)

def estimate_file_size(csvfile, current_idx, total_rows):
    """Estimate the file size based on the current progress."""
    current_pos = csvfile.tell()
    estimated_total_size = (current_pos / (current_idx + 1)) * total_rows
    return estimated_total_size

# Open the output CSV file
with open(OUTPUT_FILE, "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["From Index", "To Index", "Distance (meters)", "Duration (seconds)"])

    total = len(coordinates) * (len(coordinates) - 1) // 2

    # Iterate over all possible pairs (both directions)
    for idx, (i, j) in enumerate(tqdm(itertools.combinations(range(len(coordinates)), 2), total=total)):
        distance, duration = query_osrm(OSRM_SERVER_URL, coordinates[i], coordinates[j])
        if distance is not None and duration is not None:
            writer.writerow([i, j, distance, duration])
            # writer.writerow([j, i, distance, duration])
        
        # Estimate file size every 100 iterations
        # if (idx + 1) % 100 == 0:
        #     estimated_size = estimate_file_size(csvfile, idx, total)
        #     tqdm.write(f"Estimated file size after {idx + 1} rows: {estimated_size / (1024 * 1024 * 1024):.2f} GB")
