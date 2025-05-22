import argparse
import pandas as pd
import matplotlib.pyplot as plt
from geopy.distance import geodesic
from tqdm import tqdm
import sqlite3
import osmium

def load_rinf_data(csv_path):
    # Load RINF data and rename columns for easier access
    df = pd.read_csv(csv_path)
    df.columns = [
        "line_url", "mgr", "line_id", "start_name", "start_url", "start_lat", "start_lng",
        "end_name", "end_url", "end_lat", "end_lng", "valid_from", "valid_to", "length_km"
    ]
    for col in ["start_lat", "start_lng", "end_lat", "end_lng"]:
        df[col] = df[col].astype(float)
    return df

def compute_geodesic_lengths(df):
    # Compute straight-line (geodesic) distances between start and end points
    def _geo_len(row):
        return geodesic((row["start_lat"], row["start_lng"]), (row["end_lat"], row["end_lng"])).kilometers
    df["length_calculated_km"] = df.apply(_geo_len, axis=1)
    df["length_diff_km"] = df["length_calculated_km"] - df["length_km"]
    return df

def plot_length_differences(df, log=True):
    # Plot histogram of differences between reported and calculated segment lengths
    plt.figure(figsize=(12, 6))
    df["length_diff_km"].hist(bins=100)
    if log:
        plt.yscale('log')
    plt.xlabel("Length Difference (km)")
    plt.ylabel("Frequency")
    plt.title("RINF: Calculated vs Reported Length Differences")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def extract_rail_nodes_and_ways(osm_path, total_objs):
    # First pass over OSM data to collect railways and determine which node IDs are needed
    needed_nodes = set()
    rail_ways = []
    fp = osmium.FileProcessor(osm_path)
    for obj in tqdm(fp, total=total_objs, desc="gathering rail ways"):
        if isinstance(obj, osmium.osm.Way):
            tags = {k.lower(): v.lower() for k, v in obj.tags}
            if tags.get("railway") in ("rail", "light_rail") and not any("abandoned" in v for v in tags.values()):
                nodes = [n.ref for n in obj.nodes]
                rail_ways.append(nodes)
                needed_nodes.update(nodes)
    return rail_ways, needed_nodes

def store_node_coords(osm_path, needed_nodes, db_path, total_objs):
    # Second pass: store only relevant node coordinates in a SQLite database
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS nodes(id INTEGER PRIMARY KEY, lat REAL, lon REAL)")
    c.execute("DELETE FROM nodes")
    fp = osmium.FileProcessor(osm_path)
    for obj in tqdm(fp, total=total_objs, desc="writing node coords"):
        if isinstance(obj, osmium.osm.Node) and obj.id in needed_nodes:
            c.execute("INSERT INTO nodes(id, lat, lon) VALUES (?, ?, ?)",
                      (obj.id, obj.location.lat, obj.location.lon))
    conn.commit()
    conn.close()

def compute_osm_length(db_path, rail_ways):
    # Final pass: calculate total length of OSM railways using stored node coordinates
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    total_km = 0.0
    for nodes in tqdm(rail_ways, desc="computing distances"):
        for a, b in zip(nodes, nodes[1:]):
            c.execute("SELECT lat, lon FROM nodes WHERE id IN (?, ?)", (a, b))
            rows = c.fetchall()
            if len(rows) == 2:
                total_km += geodesic(rows[0], rows[1]).kilometers
    conn.close()
    return total_km

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process RINF and OSM railway data.")
    parser.add_argument("rinf_csv", help="Path to RINF CSV file")
    parser.add_argument("osm_pbf", help="Path to OSM PBF file")
    parser.add_argument("--sqlite_db", default="rail_nodes.sqlite", help="Path to SQLite database for node coords")
    parser.add_argument("--total_objects", type=int, default=11525946, help="Total number of OSM objects for progress bars")
    args = parser.parse_args()

    # Load and clean RINF data
    df = load_rinf_data(args.rinf_csv)
    df = compute_geodesic_lengths(df)

    # Print basic statistics
    distance = 1
    percentage_diff = (df["length_diff_km"].abs() > distance).mean() * 100
    avg_diff = df["length_diff_km"].abs().mean()
    print(f"Percentage of lines with > {distance} km difference: {percentage_diff:.2f}%")
    print(f"Average length difference: {avg_diff:.2f} km")

    # Show distribution of length discrepancies
    plot_length_differences(df)

    # OSM processing: extract rail segments and store relevant node coords
    rail_ways, needed_nodes = extract_rail_nodes_and_ways(args.osm_pbf, args.total_objects)
    store_node_coords(args.osm_pbf, needed_nodes, args.sqlite_db, args.total_objects)

    # Compute total railway length from OSM data
    total_osm_km = compute_osm_length(args.sqlite_db, rail_ways)

    print(f"Total OSM railway length: {total_osm_km:.2f} km")
    print(f"Total RINF length: {df['length_km'].sum():.2f} km")
