import networkx as nx
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import zipfile
import os
import glob
import shutil
import argparse
import csv

rail_services = [
    2,    # Rail (intercity or long-distance)
    100,  # Railway Service
    101,  # High Speed Rail Service
    102,  # Long Distance Trains
    103,  # Inter Regional Rail Service
    105,  # Sleeper Rail Service
    106,  # Regional Rail Service
    107,  # Tourist Railway Service
    109,  # Suburban Railway
    116,  # Rack and Pinion Railway
    117   # Additional Rail Service
]

urban_railway_services = [
    0,    # Tram, Streetcar, Light rail
    1,    # Subway, Metro
    300,
    400,  # Urban Railway Service
    401,  # Metro Service
    402,  # Underground Service
    405   # Monorail
]

tram_services = [
    0,    # Tram, Streetcar, Light rail
    5,    # Cable tram
    900,  # Tram Service
    901,  # City Tram Service
    902   # Local Tram Service
]

parser = argparse.ArgumentParser(description='Process GTFS files')
parser.add_argument('--data_dir', type=str, help='Directory containing GTFS files')
parser.add_argument('--ignore_gtfs_zips', type=str, help='File containing list of GTFS files to ignore', default='ignore_gtfs_zips.txt')
parser.add_argument('--logging_level', type=str, help='Logging level', default='INFO')
parser.add_argument('--route_types', nargs='+', type=int, help='Route types to include', default=rail_services)
parser.add_argument('--save_graph_location', type=str, help='Location to save the graph', default='graph.gpickle')
parser.add_argument('--remove_incorrect_nodes', action='store_true', help='Remove nodes with incorrect coordinates')
parser.add_argument('--include_subfolders', action='store_true', help='Include subfolders in data directory')
parser.add_argument('--visualise', action='store_true', help='Visualise the graph')
args = parser.parse_args()

DATA_DIR = args.data_dir or max(glob.iglob('../data/raw/gtfs_*'), key=os.path.getctime)
FILES_TO_EXTRACT = ['routes.txt', 'trips.txt', 'stop_times.txt', 'stops.txt']


tqdm.write(f"Data directory: {DATA_DIR}")
tqdm.write(f"Route types: {args.route_types}")

if os.path.exists(args.ignore_gtfs_zips):
    with open(args.ignore_gtfs_zips, 'r') as f:
        ignore_gtfs_zips = set(f.read().splitlines())
else:
    ignore_gtfs_zips = set()

G = nx.DiGraph()

def interleave_round_robin(files):
    sorted_files = sorted(files, key=lambda x: os.stat(os.path.join(DATA_DIR, x)).st_size)
    n = len(sorted_files)

    # Split into quartiles
    very_large = sorted_files[3 * n // 4:]
    large = sorted_files[n // 2:3 * n // 4]
    medium = sorted_files[n // 4:n // 2]
    small = sorted_files[:n // 4]

    interleaved = []
    while any([very_large, large, medium, small]):
        if very_large:
            interleaved.append(very_large.pop())
        if small:
            interleaved.append(small.pop())
        if large:
            interleaved.append(large.pop())
        if medium:
            interleaved.append(medium.pop())

    return interleaved

if args.include_subfolders:
    gtfs_files = []
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith('.zip') and os.path.relpath(os.path.join(root, file), DATA_DIR) not in ignore_gtfs_zips:
                gtfs_files.append(os.path.relpath(os.path.join(root, file), DATA_DIR))
else:
    gtfs_files = [file for file in os.listdir(DATA_DIR) if file not in ignore_gtfs_zips and file.endswith('.zip')]
gtfs_files = interleave_round_robin(gtfs_files)

def process_zip_file(zip_file, include_edges=True):
    if not zip_file.endswith('.zip') or zip_file in ignore_gtfs_zips:
        return
    extract_dir = os.path.join(DATA_DIR, zip_file.split('.')[0])
    try:
        files_to_unzip = []
        with zipfile.ZipFile(os.path.join(DATA_DIR, zip_file), 'r') as zip_ref:
            for file in FILES_TO_EXTRACT:
                corresponding_files = [f for f in zip_ref.namelist() if f == file or f.endswith('/' + file)]
                if not corresponding_files:
                    raise Exception(f"Missing file: {file}")
                elif len(corresponding_files) > 1:
                    raise Exception(f"Multiple files found: {corresponding_files}")
                files_to_unzip.append(corresponding_files[0])

            zip_ref.extractall(extract_dir, files_to_unzip)
        contents = os.listdir(extract_dir)
        if len(contents) == 1 and os.path.isdir(os.path.join(extract_dir, contents[0])):
            inner_dir = os.path.join(extract_dir, contents[0])
            for item in os.listdir(inner_dir):
                shutil.move(os.path.join(inner_dir, item), os.path.join(extract_dir, item))
            os.rmdir(inner_dir)
    except Exception as e:
        if isinstance(e, KeyboardInterrupt):
            raise e
        tqdm.write(f"Error extracting {zip_file}: {e}")
        ignore_gtfs_zips.add(zip_file)
        shutil.rmtree(extract_dir, ignore_errors=True)
        return
    try:
        with open(os.path.join(extract_dir, 'routes.txt'), 'r', encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)
            # route_id_idx = header.index('route_id')
            # route_type_idx = header.index('route_type')
            for i, h in enumerate(header):
                if h.strip() == 'route_type':
                    route_type_idx = i
                elif h.strip() == 'route_id':
                    route_id_idx = i

            # routes = {row[route_id_idx] for row in reader if row and int(row[route_type_idx]) in args.route_types}
            routes = set()
            for row in reader:
                stripped_route_id = row[route_id_idx].strip()
                if row and int(row[route_type_idx]) in args.route_types and stripped_route_id:
                    routes.add(stripped_route_id)

        if not routes:
            raise Exception("No routes found")

        with open(os.path.join(extract_dir, 'trips.txt'), 'r', encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)
            # route_id_idx = header.index('route_id')
            # trip_id_idx = header.index('trip_id')
            for i, h in enumerate(header):
                if h.strip() == 'route_id':
                    route_id_idx = i
                elif h.strip() == 'trip_id':
                    trip_id_idx = i
            # trips = {row[trip_id_idx] for row in reader if row and row[route_id_idx] in routes}
            trips = set()
            for row in reader:
                stripped_trip_id = row[trip_id_idx].strip()
                if row and row[route_id_idx] in routes and stripped_trip_id:
                    trips.add(stripped_trip_id)

        del routes

        if not trips:
            raise Exception("No trips found")

        with open(os.path.join(extract_dir, 'stop_times.txt'), 'r', encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)
            # trip_id_idx = header.index('trip_id')
            # stop_id_idx = header.index('stop_id')
            # stop_sequence_idx = header.index('stop_sequence')
            for i, h in enumerate(header):
                if h.strip() == 'trip_id':
                    trip_id_idx = i
                elif h.strip() == 'stop_id':
                    stop_id_idx = i
                elif h.strip() == 'stop_sequence':
                    stop_sequence_idx = i

            stop_times = {}
            stops = set()

            for row in reader:
                if row:
                    trip_id = row[trip_id_idx].strip()
                    if trip_id in trips:
                        stop_id = row[stop_id_idx].strip()
                        stops.add(stop_id)
                        stop_sequence = int(row[stop_sequence_idx])
                        if trip_id not in stop_times:
                            stop_times[trip_id] = []
                        stop_times[trip_id].append((stop_id, stop_sequence))

        del trips

        for trip_id in stop_times:
            stop_times[trip_id].sort(key=lambda x: x[1])

        if not stop_times:
            raise Exception("No stop_times found")

        with open(os.path.join(extract_dir, 'stops.txt'), 'r', encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)
            # stop_id_idx = header.index('stop_id')
            # stop_name_idx = header.index('stop_name')
            # stop_lat_idx = header.index('stop_lat')
            # stop_lon_idx = header.index('stop_lon')
            for i, h in enumerate(header):
                if h.strip() == 'stop_id':
                    stop_id_idx = i
                elif h.strip() == 'stop_name':
                    stop_name_idx = i
                elif h.strip() == 'stop_lat':
                    stop_lat_idx = i
                elif h.strip() == 'stop_lon':
                    stop_lon_idx = i

            for row in reader:
                if row:
                    stop_id = row[stop_id_idx].strip()
                    if stop_id in stops:
                        G.add_node(stop_id, name=row[stop_name_idx], lat=float(row[stop_lat_idx]), lon=float(row[stop_lon_idx]))

        if include_edges:
            for trip_id, stop_time in stop_times.items():
                for (stop1, _), (stop2, _) in zip(stop_time, stop_time[1:]):
                    if stop1 in G and stop2 in G:
                        G.add_edge(stop1, stop2, trip_id=trip_id)
                    else:
                        tqdm.write(f"Missing nodes in file {zip_file}: {stop1}, {stop2}")
        
        del stop_times, stops
        
    except Exception as e:
        if isinstance(e, KeyboardInterrupt):
            raise e
        tqdm.write(f"Error processing {zip_file}: {e}")
        ignore_gtfs_zips.add(zip_file)
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)

max_workers = os.cpu_count()
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    list(tqdm(executor.map(lambda zip_file: process_zip_file(zip_file, include_edges=True), gtfs_files), desc='Processing GTFS files', position=0, leave=True, total=len(gtfs_files)))

with open(args.ignore_gtfs_zips, 'w') as f:
    f.write('\n'.join(ignore_gtfs_zips))

if args.remove_incorrect_nodes:
    nodes_to_remove = [node for node in G.nodes if 'lat' not in G.nodes[node] or 'lon' not in G.nodes[node] or G.nodes[node]['lat'] == 0 or G.nodes[node]['lon'] == 0]
    tqdm.write(f"Removing {len(nodes_to_remove)} nodes with incorrect coordinates")
    G.remove_nodes_from(nodes_to_remove)
import pickle
with open(args.save_graph_location, 'wb') as f:
    pickle.dump(G, f)
tqdm.write(f"Graph saved to {args.save_graph_location}")

if args.visualise:
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    fig = plt.figure(figsize=(10, 10))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())

    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.BORDERS, linestyle=':')

    for node in tqdm(G.nodes, desc='Plotting nodes', position=0, leave=True, total=len(G.nodes)):
        ax.plot(G.nodes[node]['lon'], G.nodes[node]['lat'], 'ro', markersize=1)

    plt.tight_layout()
    plt.savefig('out.png')
    plt.show()