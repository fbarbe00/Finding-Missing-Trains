import os
import pickle
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm
import numpy as np
import zipfile
import csv
from datetime import datetime, timedelta
import collections
from io import TextIOWrapper
from concurrent.futures import ProcessPoolExecutor
from sklearn.neighbors import BallTree
from scipy.spatial import cKDTree
from multiprocessing import Manager
import argparse


def parse_date(date_str):
    """Parses a date string into a date object."""
    return datetime.strptime(date_str, "%Y%m%d").date()

def count_service_days(service_start, service_end, weekdays, query_start, query_end):
    """
    Counts service days within a query period.

    Args:
        service_start (date): Start date of the service.
        service_end (date): End date of the service.
        weekdays (dict): Dictionary indicating active weekdays (0=Monday, 6=Sunday).
        query_start (date): Start date of the query period.
        query_end (date): End date of the query period.

    Returns:
        int: Number of service days.
    """
    start = max(service_start, query_start)
    end = min(service_end, query_end)
    if start > end:
        return 0
    total = 0
    for wd, active in weekdays.items():
        if not active:
            continue
        days_ahead = (wd - start.weekday() + 7) % 7
        first_occurrence = start + timedelta(days=days_ahead)
        if first_occurrence > end:
            continue
        count = ((end - first_occurrence).days // 7) + 1
        total += count
    return total

def process_calendar(zf, query_start, query_end):
    """Processes calendar.txt to compute frequency for each service_id."""
    service_freq = {}
    if "calendar.txt" not in zf.namelist():
        return service_freq
    with zf.open("calendar.txt") as f:
        reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            service_id = row["service_id"]
            try:
                service_start = parse_date(row["start_date"])
                service_end = parse_date(row["end_date"])
            except Exception:
                continue
            weekdays = {
                i: (row.get(day, "0") == "1")
                for i, day in enumerate(["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"])
            }
            service_freq[service_id] = count_service_days(service_start, service_end, weekdays, query_start, query_end)
    return service_freq

def process_calendar_dates(zf, query_start, query_end, service_freq):
    """Adjusts service frequencies using calendar_dates.txt."""
    if "calendar_dates.txt" not in zf.namelist():
        return
    with zf.open("calendar_dates.txt") as f:
        reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            service_id = row["service_id"]
            try:
                date_val = parse_date(row["date"])
            except Exception:
                continue
            if query_start <= date_val <= query_end:
                if row.get("exception_type") == "1":  # Service added
                    service_freq[service_id] = service_freq.get(service_id, 0) + 1
                elif row.get("exception_type") == "2":  # Service removed
                    service_freq[service_id] = service_freq.get(service_id, 0) - 1

def process_trips(zf, service_freq):
    """Assigns frequency to trips from trips.txt."""
    trips_freq = {}
    if "trips.txt" not in zf.namelist():
        return trips_freq
    with zf.open("trips.txt") as f:
        reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            service_id = row.get("service_id")
            trip_id = row.get("trip_id")
            freq = service_freq.get(service_id, 0)
            if freq > 0 and trip_id:
                trips_freq[trip_id] = freq
    return trips_freq

def process_stop_times(zf, trips_freq, feed_prefix, global_seen):
    """
    Processes stop_times.txt to group rows by trip, remove duplicate trips,
    and accumulate segment frequencies.
    """
    temp_segment_freq = collections.defaultdict(collections.Counter)
    trip_stop_times = collections.defaultdict(list)
    stops_ids = set()

    if "stop_times.txt" not in zf.namelist():
        return temp_segment_freq, stops_ids

    with zf.open("stop_times.txt") as f:
        reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            trip_id = row.get("trip_id")
            if trip_id not in trips_freq:
                continue
            try:
                seq = int(row.get("stop_sequence", "0"))
            except ValueError:
                continue
            trip_stop_times[trip_id].append((
                seq,
                row.get("stop_id"),
                row.get("departure_time", ""),
                row.get("arrival_time", "")
            ))

    for trip_id, stops in trip_stop_times.items():
        if not stops:
            continue
        stops.sort(key=lambda x: x[0])
        first = stops[0]
        last = stops[-1]
        trip_key = (first[1], first[2], last[1], last[3])

        freq = trips_freq[trip_id]
        for i in range(len(stops) - 1):
            from_stop = f"{feed_prefix}_{stops[i][1]}"
            to_stop = f"{feed_prefix}_{stops[i+1][1]}"
            temp_segment_freq[trip_key][(from_stop, to_stop)] += freq
            stops_ids.add(from_stop)
            stops_ids.add(to_stop)
    return temp_segment_freq, stops_ids

def process_stops(zf, stops_ids, feed_prefix):
    """Reads stops.txt and gets coordinates for relevant stops."""
    stops_info = {}
    if "stops.txt" not in zf.namelist():
        return stops_info
    with zf.open("stops.txt") as f:
        reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8-sig"))
        for row in reader:
            stop_id = f"{feed_prefix}_{row.get('stop_id')}"
            if stop_id not in stops_ids:
                continue
            try:
                lat = float(row.get("stop_lat", "0"))
                lon = float(row.get("stop_lon", "0"))
            except ValueError:
                lat, lon = 0.0, 0.0
            stops_info[stop_id] = {"lat": lat, "lon": lon}
    return stops_info

def process_gtfs_file(zip_file_path, query_start, query_end, global_seen):
    """
    Processes a single GTFS zip file.

    Args:
        zip_file_path (str): Path to the GTFS zip file.
        query_start (date): Start date for the query.
        query_end (date): End date for the query.
        global_seen (multiprocessing.Manager.dict): Shared dictionary to track seen trip keys.

    Returns:
        tuple: (segment_freq: Counter, stops_info: dict).
    """
    segment_freq = collections.Counter()
    stops_info = {}
    try:
        feed_prefix = os.path.splitext(os.path.basename(zip_file_path))[0]
        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            service_freq = process_calendar(zf, query_start, query_end)
            process_calendar_dates(zf, query_start, query_end, service_freq)
            trips_freq = process_trips(zf, service_freq)
            temp_seg_freq, stops_ids = process_stop_times(zf, trips_freq, feed_prefix, global_seen)
            stops_info = process_stops(zf, stops_ids, feed_prefix)
            for (start_id, start_time, end_id, end_time), trip_seg_freq in temp_seg_freq.items():
                start_coords = stops_info.get(f"{feed_prefix}_{start_id}", {})
                end_coords = stops_info.get(f"{feed_prefix}_{end_id}", {})
                if not start_coords or not end_coords:
                    continue
                # Use a combined key for duplicate trip detection
                trip_key = (
                    start_coords.get("lat"), start_coords.get("lon"), start_time,
                    end_coords.get("lat"), end_coords.get("lon"), end_time
                )
                if trip_key in global_seen:
                    continue
                global_seen[trip_key] = True
                segment_freq.update(trip_seg_freq)
    except Exception as e:
        print(f"Error processing {zip_file_path}: {e}")
    return segment_freq, stops_info

def merge_results(results):
    """Merges segment frequency counters and stops dictionaries."""
    total_segments = collections.Counter()
    total_stops = {}
    for seg_counter, stops in results:
        total_segments.update(seg_counter)
        total_stops.update(stops)
    return total_segments, total_stops

def merge_stops_with_balltree(total_stops, distance_threshold):
    """
    Merges stops within a given distance threshold using a BallTree.

    Args:
        total_stops (dict): Dictionary of stop_id to {'lat', 'lon'}.
        distance_threshold (int): Distance in meters to consider stops as duplicates.

    Returns:
        tuple: (merged_stops: dict, stop_id_to_merged_id: dict).
    """
    stop_ids = list(total_stops.keys())
    # Filter out stops without coordinates
    valid_stop_ids = [s for s in stop_ids if 'lat' in total_stops[s] and 'lon' in total_stops[s] and total_stops[s]['lat'] is not None and total_stops[s]['lon'] is not None]
    
    if not valid_stop_ids:
        return {}, {} # Return empty if no valid stops

    coords = np.array([[total_stops[s]['lat'], total_stops[s]['lon']] for s in valid_stop_ids])
    coords_rad = np.radians(coords)
    tree = BallTree(coords_rad, metric='haversine')
    radius = distance_threshold / 6371000.0  # Convert to radians

    parent = list(range(len(valid_stop_ids)))
    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    def union(i, j):
        pi, pj = find(i), find(j)
        if pi != pj:
            parent[pj] = pi

    for i, coord in enumerate(coords_rad):
        ind = tree.query_radius([coord], r=radius)[0]
        for j in ind:
            if i != j:
                union(i, j)

    clusters = collections.defaultdict(list)
    for i in range(len(valid_stop_ids)):
        root = find(i)
        clusters[root].append(i)

    merged_stops = {}
    stop_id_to_merged_id = {}
    for indices in clusters.values():
        cluster_stop_ids = [valid_stop_ids[i] for i in indices]
        cluster_coords = coords[indices]
        center = cluster_coords.mean(axis=0)
        # Use the first stop ID in the cluster as the merged ID
        merged_id = cluster_stop_ids[0]
        merged_stops[merged_id] = {'lat': center[0], 'lon': center[1], 'stop_ids': cluster_stop_ids}
        for s in cluster_stop_ids:
            stop_id_to_merged_id[s] = merged_id
    return merged_stops, stop_id_to_merged_id

def plot_graph(G, title, plot_nodes=True, save_path=None):
    """
    Plots a NetworkX graph with geographical coordinates.

    Args:
        G (nx.Graph): The graph to plot.
        title (str): Title of the plot.
        plot_nodes (bool): Whether to plot nodes or just edges.
        save_path (str, optional): Path to save the plot. Defaults to None.
    """
    plt.figure(figsize=(10, 8))
    ax = plt.gca()
    pos = {n: (G.nodes[n]['lon'], G.nodes[n]['lat']) for n in G.nodes if 'lat' in G.nodes[n] and 'lon' in G.nodes[n]}

    edge_weights = [G[u][v].get('weight', 0) for u, v in G.edges]
    if sum(edge_weights) == 0:
        edge_colors = 'lightgrey'
        max_log_weight = 1
    else:
        log_weights = [np.log(w + 1) for w in edge_weights]
        max_log_weight = max(log_weights) if log_weights else 1
        normalized_weights = [w / max_log_weight for w in log_weights]
        cmap = plt.cm.viridis
        edge_colors = [cmap(w) for w in normalized_weights]

    if plot_nodes:
        nx.draw(
            G, pos, node_size=0.5, edge_color=edge_colors, node_color='darkblue', with_labels=False, ax=ax
        )
    else:
        nx.draw_networkx_edges(
            G, pos, edge_color=edge_colors, width=1, ax=ax
        )
    plt.title(title)
    plt.axis('off')

    if not plot_nodes and max_log_weight > 0:
        sm = plt.cm.ScalarMappable(cmap=plt.cm.viridis, norm=plt.Normalize(vmin=0, vmax=max_log_weight))
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax, label="Track Count")
        num_ticks = min(10, len(edge_weights)) if edge_weights else 0
        if num_ticks > 0:
            tick_values = np.linspace(0, max_log_weight, num_ticks)
            cbar.set_ticks(tick_values)
            cbar.set_ticklabels([int(np.exp(t) - 1) for t in tick_values])

    if save_path:
        plt.savefig(save_path, format=save_path.split('.')[-1])
    plt.show()


parser = argparse.ArgumentParser(description="Analyze GTFS and OSM railway track data.")
parser.add_argument("--graph_folder", type=str, default="graphs",
                    help="Folder containing OSM graph files.")
parser.add_argument("--gtfs_input_dir", type=str, default="../../data/gtfs/downloaded_feeds_filtered",
                    help="Directory containing GTFS zip files.")
parser.add_argument("--query_start_date", type=str, default="20250324",
                    help="Start date for GTFS data query (YYYYMMDD).")
parser.add_argument("--query_end_date", type=str, default="20250330",
                    help="End date for GTFS data query (YYYYMMDD).")
parser.add_argument("--distance_threshold", type=int, default=200,
                    help="Distance threshold in meters for merging GTFS stops.")
parser.add_argument("--output_osm_plot", type=str, default="track_count_osm.pdf",
                    help="Output path for the merged OSM graph plot (PDF).")
parser.add_argument("--output_gtfs_plot", type=str, default="track_count_gtfs.pdf",
                    help="Output path for the GTFS graph plot (PDF).")
args = parser.parse_args()
# --- OSM Graph Processing ---
graph_files = [f for f in tqdm(os.listdir(args.graph_folder)) if f.endswith(".gpickle")]

summary = []
all_OSM_G = nx.Graph()

for fname in tqdm(graph_files, desc="Loading OSM graphs"):
    path = os.path.join(args.graph_folder, fname)
    with open(path, "rb") as f:
        G = pickle.load(f)

    G.remove_edges_from(list(nx.selfloop_edges(G)))
    G.remove_nodes_from(list(nx.isolates(G)))

    num_nodes = G.number_of_nodes()
    num_edges = G.number_of_edges()
    country = fname.split(".")[0]

    summary.append({
        "Country": country,
        "Nodes": num_nodes,
        "Edges": num_edges,
        "Connected Components": nx.number_connected_components(G) if num_nodes > 0 else 0,
        "Average Degree": sum(dict(G.degree()).values()) / num_nodes if num_nodes > 0 else 0
    })

    # Add country name as a node attribute
    for n in G.nodes:
        G.nodes[n]['country'] = country
    all_OSM_G = nx.compose(all_OSM_G, G)

summary_df = pd.DataFrame(summary).sort_values("Nodes", ascending=False)
print("--- OSM Graph Summary ---")
print(summary_df.to_string(index=False))

# Merge nodes in all_OSM_G that have exact same lat/lon
nodes_to_merge = collections.defaultdict(list)
for n in all_OSM_G.nodes:
    if 'lat' in all_OSM_G.nodes[n] and 'lon' in all_OSM_G.nodes[n]:
        coord = (all_OSM_G.nodes[n]['lon'], all_OSM_G.nodes[n]['lat'])
        nodes_to_merge[coord].append(n)

for coord, nodes in nodes_to_merge.items():
    if len(nodes) > 1:
        main_node = nodes[0]
        for i in range(1, len(nodes)):
            all_OSM_G = nx.contracted_nodes(all_OSM_G, main_node, nodes[i], self_loops=False)

all_OSM_G.remove_edges_from(list(nx.selfloop_edges(all_OSM_G)))
all_OSM_G.remove_nodes_from(list(nx.isolates(all_OSM_G)))

print(f"\nTotal nodes in merged OSM graph: {all_OSM_G.number_of_nodes()}, edges: {all_OSM_G.number_of_edges()}")

plot_graph(all_OSM_G, title="Merged OSM Railway Track Graph", save_path=args.output_osm_plot)

# --- GTFS Graph Processing ---
query_start = parse_date(args.query_start_date)
query_end = parse_date(args.query_end_date)
if query_start > query_end:
    raise ValueError("Start date must be before end date")

gtfs_files = [os.path.join(args.gtfs_input_dir, f) for f in os.listdir(args.gtfs_input_dir) if f.endswith('.zip')]

with Manager() as manager:
    global_seen = manager.dict()  # Shared dictionary for duplicate trip detection
    all_results = []
    max_workers = os.cpu_count() or 1
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_gtfs_file, file_path, query_start, query_end, global_seen)
            for file_path in gtfs_files
        ]
        for future in tqdm(futures, desc="Processing GTFS files"):
            all_results.append(future.result())

total_segments, total_stops = merge_results(all_results)

# Propagate missing coordinates (repeat several times to allow for transitive filling)
SOUTH_MOST_EUROPEAN_LAT = 35.0  # approximate latitude to identify missing/invalid coords
for _ in range(5): # Repeat 5 times
    for stop_id, coords in total_stops.items():
        if coords["lat"] < SOUTH_MOST_EUROPEAN_LAT: # If lat is likely invalid
            lat_sum = 0.0
            lon_sum = 0.0
            count = 0
            for (from_stop, to_stop), freq in total_segments.items():
                # Check if the current stop is part of this segment
                if from_stop == stop_id and total_stops.get(to_stop, {}).get("lat", 0) >= SOUTH_MOST_EUROPEAN_LAT:
                    lat_sum += total_stops[to_stop]["lat"]
                    lon_sum += total_stops[to_stop]["lon"]
                    count += 1
                elif to_stop == stop_id and total_stops.get(from_stop, {}).get("lat", 0) >= SOUTH_MOST_EUROPEAN_LAT:
                    lat_sum += total_stops[from_stop]["lat"]
                    lon_sum += total_stops[from_stop]["lon"]
                    count += 1
            if count > 0:
                total_stops[stop_id]["lat"] = lat_sum / count
                total_stops[stop_id]["lon"] = lon_sum / count

merged_stops, stop_id_to_merged_id = merge_stops_with_balltree(total_stops, args.distance_threshold)

# Remap segments to merged stops, avoiding self-loops.
merged_segments = collections.Counter()
for (from_stop, to_stop), freq in total_segments.items():
    merged_from = stop_id_to_merged_id.get(from_stop, from_stop)
    merged_to = stop_id_to_merged_id.get(to_stop, to_stop)
    if merged_from != merged_to:
        merged_segments[(merged_from, merged_to)] += freq

print(f"\nNumber of merged GTFS stops: {len(merged_stops)}")
print(f"Number of merged GTFS segments: {len(merged_segments)}")

# Convert merged GTFS data to a graph
gtfs_graph = nx.Graph()
for stop_id, coords in merged_stops.items():
    gtfs_graph.add_node(stop_id, lat=coords["lat"], lon=coords["lon"])
for (from_stop, to_stop), freq in merged_segments.items():
    gtfs_graph.add_edge(from_stop, to_stop, weight=freq)
    gtfs_graph.add_edge(to_stop, from_stop, weight=freq) # Add reverse edge for undirected graph

gtfs_graph.remove_edges_from(list(nx.selfloop_edges(gtfs_graph)))
gtfs_graph.remove_nodes_from(list(nx.isolates(gtfs_graph)))

plot_graph(gtfs_graph, title="GTFS Track Count", plot_nodes=False, save_path=args.output_gtfs_plot)

# --- Graph Comparison ---
# Match every station in gtfs_graph to the nearest station in all_OSM_G
gtfs_nodes_with_coords = {n: data for n, data in gtfs_graph.nodes(data=True) if 'lat' in data and 'lon' in data}
osm_nodes_with_coords = {n: data for n, data in all_OSM_G.nodes(data=True) if 'lat' in data and 'lon' in data}

if not gtfs_nodes_with_coords or not osm_nodes_with_coords:
    print("\nNot enough nodes with coordinates in either GTFS or OSM graph for comparison.")
    exit(0)

gtfs_node_ids = list(gtfs_nodes_with_coords.keys())
gtfs_coords = np.array([[gtfs_nodes_with_coords[n]['lon'], gtfs_nodes_with_coords[n]['lat']] for n in gtfs_node_ids])

osm_node_ids = list(osm_nodes_with_coords.keys())
osm_coords = np.array([[osm_nodes_with_coords[n]['lon'], osm_nodes_with_coords[n]['lat']] for n in osm_node_ids])

tree = cKDTree(osm_coords)
distances, indices = tree.query(gtfs_coords, k=1)

gtfs_to_all_OSM_G = {}
for i, gtfs_node_id in enumerate(gtfs_node_ids):
    matched_osm_id = osm_node_ids[indices[i]]
    gtfs_graph.nodes[gtfs_node_id]['matched_osm_id'] = matched_osm_id
    gtfs_to_all_OSM_G[gtfs_node_id] = matched_osm_id

# Store comparison results
comparison_results = []

# Iterate through each country for detailed comparison
for country in summary_df["Country"]:
    # Filter OSM graph for the current country
    country_osm_nodes = [n for n, data in all_OSM_G.nodes(data=True) if data.get("country") == country]
    country_osm_subgraph = all_OSM_G.subgraph(country_osm_nodes)

    # Filter GTFS graph for the current country based on matched OSM nodes
    country_gtfs_nodes = [n for n in gtfs_graph.nodes if gtfs_graph.nodes[n].get("matched_osm_id") in country_osm_nodes]
    country_gtfs_subgraph = gtfs_graph.subgraph(country_gtfs_nodes)

    # Map GTFS edges to OSM space
    country_gtfs_mapped_edges = {
        tuple(sorted((gtfs_to_all_OSM_G[u], gtfs_to_all_OSM_G[v])))
        for u, v in country_gtfs_subgraph.edges()
        if gtfs_to_all_OSM_G[u] != gtfs_to_all_OSM_G[v]
    }

    # OSM edges for the country
    country_osm_edges = {tuple(sorted(edge)) for edge in country_osm_subgraph.edges()}

    # Compare edges
    missing_in_gtfs = country_osm_edges - country_gtfs_mapped_edges
    missing_in_osm = country_gtfs_mapped_edges - country_osm_edges

    comparison_results.append({
        "Country": country.upper(),
        "Edges in OSM but Missing in GTFS": len(missing_in_gtfs),
        "Edges in GTFS but Missing in OSM": len(missing_in_osm)
    })

print("\n--- Edge Comparison (OSM vs. GTFS) ---")
comparison_df = pd.DataFrame(comparison_results)
print(comparison_df.to_string(index=False))
