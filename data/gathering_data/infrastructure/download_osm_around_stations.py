import os
import csv
import zipfile
import logging
import argparse
import requests
import subprocess
import numpy as np
from tqdm import tqdm
from io import TextIOWrapper
from scipy.spatial import cKDTree
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import concurrent.futures
from tqdm.contrib.logging import logging_redirect_tqdm

# ----------------------------- #
#         Overpass Query        #
# ----------------------------- #
def overpass_query(lat, lon, distance=1000):
    """
    Create Overpass QL query to extract highways and platforms within a distance.
    """
    return f"""
    [out:xml][timeout:600];
    (
        way["highway"](around:{distance},{lat},{lon});
        way["public_transport"="platform"](around:{distance},{lat},{lon});
        way["railway"="platform"](around:{distance},{lat},{lon});
    );
    (._;>;);
    out meta;
    """

# ----------------------------- #
#     Fetch and Convert OSM    #
# ----------------------------- #
def fetch_osm_data(station_id, lat, lon, distance=1000, skip_existing=False):
    """
    Downloads OSM XML data from Overpass API for a given location.
    """
    try:
        osm_file = os.path.join(OUTPUT_FOLDER, f"station_{station_id}.osm")
        if skip_existing and (os.path.exists(osm_file) or os.path.exists(osm_file.replace(".osm", ".osm.pbf"))):
            logging.info(f"Skipping station {station_id}; file exists.")
            return osm_file

        query = overpass_query(lat, lon, distance)
        response = requests.post(OVERPASS_URL, data=query, timeout=60)
        response.raise_for_status()

        with open(osm_file, "wb") as f:
            f.write(response.content)
        logging.info(f"Fetched OSM for station {station_id}")
        return osm_file

    except requests.RequestException as e:
        logging.error(f"Failed to fetch station {station_id} ({lat}, {lon}): {e}")
        return None

def convert_osm_to_pbf(osm_file, overwrite=True, skip_existing=False):
    """
    Converts .osm XML file to .osm.pbf format using Osmium tool.
    """
    try:
        pbf_file = osm_file.replace(".osm", ".osm.pbf")
        if skip_existing and os.path.exists(pbf_file):
            logging.info(f"PBF already exists, skipping: {pbf_file}")
            return pbf_file

        command = ["osmium", "cat", osm_file, "-o", pbf_file]
        if overwrite:
            command.append("--overwrite")

        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info(f"Converted to PBF: {pbf_file}")
        os.remove(osm_file)
        return pbf_file

    except subprocess.CalledProcessError as e:
        logging.error(f"Conversion failed for {osm_file}: {e}")
        return None

def process_station(station_id, lat, lon, distance=1000, skip_existing=False):
    """
    Fetch and convert OSM data for a single station.
    """
    osm_file = fetch_osm_data(station_id, lat, lon, distance, skip_existing)
    if osm_file:
        return convert_osm_to_pbf(osm_file, skip_existing=skip_existing)
    return None

# ----------------------------- #
#         GTFS Parsing         #
# ----------------------------- #
def get_locations(zip_file_path):
    """
    Extract (lon, lat) stop coordinates from GTFS stops.txt in a ZIP archive.
    """
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        with zip_ref.open('stops.txt') as file:
            reader = csv.DictReader(TextIOWrapper(file))
            return {(float(row['stop_lon']), float(row['stop_lat'])) for row in reader}

# ----------------------------- #
#      Spatial De-duplication  #
# ----------------------------- #
def filter_points_within_radius(points, radius=1000):
    """
    Filters out points that are within `radius` meters of another.
    Keeps only the first in each neighborhood.
    """
    if len(points) <= 1:
        return points

    def to_cartesian(lon, lat):
        lat, lon = np.radians(lat), np.radians(lon)
        x = np.cos(lat) * np.cos(lon)
        y = np.cos(lat) * np.sin(lon)
        z = np.sin(lat)
        return x, y, z

    cartesian_points = np.array([to_cartesian(lon, lat) for lon, lat in points])
    tree = cKDTree(cartesian_points)
    angular_radius = radius / 6371000  # Earth radius in meters
    neighbors = tree.query_ball_tree(tree, angular_radius)
    
    return neighbors

# ----------------------------- #
#     Merge All PBF Outputs    #
# ----------------------------- #
def merge_pbf_files(output_folder, final_output, overwrite=True):
    """
    Merge all .osm.pbf files into a single combined output.
    """
    try:
        pbf_files = [os.path.join(output_folder, f) for f in os.listdir(output_folder) if f.endswith(".pbf")]
        if not pbf_files:
            logging.warning("No PBF files to merge.")
            return
        command = ["osmium", "merge", *pbf_files, "-o", final_output]
        if overwrite:
            command.append("--overwrite")

        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logging.info(f"Created combined file: {final_output}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Merging failed: {e}")

# ----------------------------- #
#            Main              #
# ----------------------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and convert OSM data around GTFS stops.")
    parser.add_argument("data_dir", help="Directory containing GTFS ZIP files")
    parser.add_argument("--output", default="combined.osm.pbf", help="Name of final merged output file")
    parser.add_argument("--output_folder", default="osm_data", help="Folder to store intermediate OSM/PBF files")
    parser.add_argument("--logging", default="INFO", help="Logging level (DEBUG, INFO, WARNING, etc.)")
    parser.add_argument("--parallel", type=int, default=10, help="Number of parallel threads")
    parser.add_argument("--distance", type=int, default=1000, help="Radius from station in meters")
    parser.add_argument("--skip-existing", action="store_true", help="Skip already processed files")
    args = parser.parse_args()

    # Configuration
    DATA_DIR = args.data_dir
    OUTPUT_FOLDER = args.output_folder
    FINAL_OUTPUT = args.output
    OVERPASS_URL = "http://overpass-api.de/api/interpreter"
    MAX_WORKERS = args.parallel
    DISTANCE = args.distance

    # Setup
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    logging.basicConfig(level=args.logging.upper(), format="%(asctime)s - %(levelname)s - %(message)s")

    # Extract GTFS stop coordinates
    gtfs_files = os.listdir(DATA_DIR)
    all_locations = set()

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(get_locations, os.path.join(DATA_DIR, zip_file)) for zip_file in gtfs_files]
        for future in tqdm(futures, desc='Extracting GTFS stop locations'):
            all_locations.update(future.result())

    # Remove close-by duplicates
    points = list(all_locations)
    neighbors = filter_points_within_radius(points, radius=(DISTANCE / 2))
    visited = set()

    for neighbors_list in tqdm(neighbors, desc=f'Filtering duplicates <{DISTANCE/2}m'):
        if neighbors_list[0] in visited:
            continue
        for idx in neighbors_list[1:]:
            lon, lat = points[idx]
            all_locations.discard((lon, lat))
            visited.add(idx)

    # Fetch and convert OSM data
    with logging_redirect_tqdm():
        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(process_station, i + 1, lat, lon, DISTANCE, args.skip_existing): (i + 1, lat, lon)
                    for i, (lon, lat) in enumerate(all_locations)
                }

                for future in tqdm(concurrent.futures.as_completed(futures), total=len(all_locations), desc="Processing stations"):
                    station_id, lat, lon = futures[future]
                    try:
                        result = future.result()
                        if result:
                            logging.info(f"Station {station_id} processed.")
                        else:
                            logging.warning(f"Station {station_id} failed.")
                    except Exception as e:
                        logging.error(f"Error for station {station_id} ({lat}, {lon}): {e}")

            merge_pbf_files(OUTPUT_FOLDER, FINAL_OUTPUT)

        except Exception as e:
            logging.critical(f"Fatal error: {e}")
