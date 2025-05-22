import osmium
import networkx as nx
import numpy as np
from tqdm import tqdm
from scipy.spatial import cKDTree
import concurrent.futures
import pandas as pd
import os
import argparse
from collections import Counter
from itertools import combinations
import pickle

def load_station_data(station_file):
    stations_df = pd.read_csv(
        station_file,
        sep=";",
        usecols=["name", "latitude", "longitude", "uic", "country"]
    ).dropna().reset_index().rename(columns={"index": "id"})
    return stations_df


def build_kdtree(stations_df):
    coords = list(zip(stations_df["latitude"], stations_df["longitude"]))
    return cKDTree(coords)


def find_nearest_stations(batch, tree, stations_df, threshold=100):
    obj_ids, latitudes, longitudes = zip(*batch)
    coords = np.column_stack((latitudes, longitudes))
    distances, indices = tree.query(coords, k=1)
    earth_radius = 6371000
    distances_m = distances * (np.pi / 180) * earth_radius
    valid_mask = distances_m <= threshold

    results = []
    for obj_id, index, lat, lon in zip(
        np.array(obj_ids)[valid_mask],
        np.array(indices)[valid_mask],
        np.array(latitudes)[valid_mask],
        np.array(longitudes)[valid_mask]
    ):
        name, uic = stations_df.iloc[index][["name", "uic"]]
        results.append((obj_id, lat, lon, name, uic))

    return results


def process_osm_file(osm_file, stations_df, tree, output_path, station_threshold):
    output_file = os.path.join(output_path, f'{os.path.basename(osm_file).split("-filtered")[0].split(".")[0]}.gpickle')

    if os.path.exists(output_file):
        tqdm.write(f"Graph already exists for {osm_file}, skipping.")
        return

    if not os.path.exists(osm_file):
        tqdm.write(f"WARNING: OSM file {osm_file} does not exist, skipping.")
        return

    fp = osmium.FileProcessor(osm_file)
    G = nx.Graph()
    identical_stations_map = {}
    node_count = Counter()
    last_appearance = {}
    important_nodes = set()
    node_batch = []
    batch_size = 1000
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count())
    futures = []
    uic_to_node = {}

    # === First Pass: Station nodes and important junctions ===
    total_nodes = 18977013
    for i, obj in tqdm(enumerate(fp), total=total_nodes, unit="objects", miniters=10000, desc="First pass", position=1, leave=False):
        if isinstance(obj, osmium.osm.Node) and obj.tags.get('railway') in ['station', 'halt', 'stop']:
            if not any(x in obj.tags for x in ['abandoned', 'disused']) and obj.tags.get('subway') != 'yes' and obj.tags.get('tram') != 'yes':
                lon, lat = obj.location.lon, obj.location.lat
                node_id = int(obj.id)
                node_batch.append((node_id, lat, lon))

                if len(node_batch) >= batch_size:
                    futures.append(executor.submit(find_nearest_stations, node_batch, tree, stations_df, station_threshold))
                    node_batch = []

        if isinstance(obj, osmium.osm.Way) and obj.tags.get('railway') in ['rail', 'narrow_gauge']:
            if 'abandoned' in obj.tags or 'disused' in obj.tags:
                continue
            if node_batch:
                futures.append(executor.submit(find_nearest_stations, node_batch, tree, stations_df, station_threshold))
                node_batch = []

            for nd in obj.nodes:
                node_ref = int(nd.ref)
                node_count[node_ref] += 1
                if node_count[node_ref] > 1:
                    last_appearance[node_ref] = i
                    important_nodes.add(node_ref)
    total_nodes = i + 1
    executor.shutdown(wait=True)

    for future in tqdm(futures, desc="Matching stations", position=1):
        for obj_id, lat, lon, name, uic in future.result():
            important_nodes.add(obj_id)
            if uic in uic_to_node:
                identical_stations_map[obj_id] = uic_to_node[uic]
            else:
                G.add_node(obj_id, lat=lat, lon=lon, name=name, uic=uic)
                uic_to_node[uic] = obj_id

    station_nodes = set(G.nodes())

    # === Second Pass: Create edges from ways ===
    fp = osmium.FileProcessor(osm_file)
    for index, obj in tqdm(enumerate(fp), total=total_nodes, unit="objects", miniters=10000, desc="Processing ways", position=1):
        if not isinstance(obj, osmium.osm.Way):
            continue
        if obj.tags.get('railway') not in ['rail', 'narrow_gauge'] or 'abandoned' in obj.tags or 'disused' in obj.tags:
            continue

        way_nodes = [int(nd.ref) for nd in obj.nodes if int(nd.ref) in important_nodes]
        if not way_nodes:
            continue

        removed_nodes = set()
        for i in range(len(way_nodes) - 1):
            n1 = identical_stations_map.get(way_nodes[i], way_nodes[i])
            n2 = identical_stations_map.get(way_nodes[i + 1], way_nodes[i + 1])
            G.add_edge(n1, n2)

            if n1 not in station_nodes and last_appearance.get(way_nodes[i], 0) <= index:
                neighbors = list(G.neighbors(n1))
                G.add_edges_from(combinations(neighbors, 2))
                G.remove_node(n1)
                removed_nodes.add(n1)

        # Handle last node
        n2 = identical_stations_map.get(way_nodes[-1], way_nodes[-1])
        if n2 not in station_nodes and last_appearance.get(way_nodes[-1], 0) <= index and n2 in G.nodes:
            neighbors = list(G.neighbors(n2))
            G.add_edges_from(combinations(neighbors, 2))
            G.remove_node(n2)
            removed_nodes.add(n2)

        for node in removed_nodes:
            last_appearance.pop(node, None)

    tqdm.write(f"Number of nodes in the graph: {len(G.nodes())}")
    tqdm.write(f"Number of edges in the graph: {len(G.edges())}")
    tqdm.write("All nodes have lat attribute: " + ("✅" if all("lat" in G.nodes[n] for n in G.nodes()) else "❌"))

    # Save graph
    os.makedirs(output_path, exist_ok=True)
    with open(output_file, "wb") as f:
        pickle.dump(G, f)
    tqdm.write(f"Graph saved to {output_file}")


parser = argparse.ArgumentParser(description="Build railway graph from OSM data.")
parser.add_argument("--stations", type=str, required=True, help="Path to stations CSV file")
parser.add_argument("--osm_dir", type=str, required=True, help="Directory containing OSM .pbf files")
parser.add_argument("--output_dir", type=str, default="graphs", help="Output directory for graph files")
parser.add_argument("--threshold", type=float, default=500, help="Max distance (in meters) for matching station nodes")
args = parser.parse_args()

# Load stations and KDTree
stations_df = load_station_data(args.stations)
tree = build_kdtree(stations_df)

# Collect all filtered railway files
osm_files = [os.path.join(args.osm_dir, fname) for fname in os.listdir(args.osm_dir) if fname.endswith(".osm.pbf")]

for osm_file in tqdm(osm_files, position=0, desc="Processing OSM files"):
    process_osm_file(osm_file, stations_df, tree, args.output_dir, args.threshold)
