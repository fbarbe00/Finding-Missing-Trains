import argparse
import glob
import json
import os
import pandas as pd
from scipy.spatial import cKDTree
import geopandas as gpd
import matplotlib.pyplot as plt
from tqdm import tqdm


def load_trainline(path):
    df = pd.read_csv(
        path,
        sep=";",
        usecols=["name", "latitude", "longitude", "uic", "db_id"],
        dtype={"name": str, "latitude": float, "longitude": float, "uic": str, "db_id": str}
    ).dropna(subset=["latitude", "longitude"])

    df['uic'] = df['uic'].fillna('').astype(str)
    df['db_id'] = df['db_id'].fillna('').astype(str)
    return df


def load_osm_latest(osm_dir):
    osm_file = sorted(glob.glob(os.path.join(osm_dir, "overpass_results_*.json")))[-1]
    with open(osm_file, "r", encoding="utf-8") as f:
        osm_data = json.load(f)

    osm_rows, ignored_count = [], 0
    for cc, data in osm_data.items():
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            if any("abandoned" in str(k).lower() or "disused" in str(k).lower() or
                   "abandoned" in str(v).lower() or "disused" in str(v).lower()
                   for k, v in tags.items()):
                ignored_count += 1
                continue

            lat, lon = (el["lat"], el["lon"]) if el["type"] == "node" else el.get("center", {}).get("lat"), el.get("center", {}).get("lon")
            if lat is None or lon is None:
                continue

            osm_rows.append({
                "source": "OSM",
                "name": tags.get("name", ""),
                "latitude": lat,
                "longitude": lon,
                "uic": tags.get("uic_ref", "")
            })

    print(f"Ignored {ignored_count} items with 'abandoned' or 'disused' tags or values.")
    return pd.DataFrame(osm_rows)


def load_wikidata(wd_dir):
    frames = []
    for fn in os.listdir(wd_dir):
        if fn.endswith(".csv"):
            df = pd.read_csv(os.path.join(wd_dir, fn))
            frames.append(df)

    df = pd.concat(frames, ignore_index=True)
    df["lon"] = df["Coordinates"].str.extract(r"Point\(([^ ]+) ")[0].astype(float)
    df["lat"] = df["Coordinates"].str.extract(r" ([^ ]+)\)")[0].astype(float)

    df = df.rename(columns={"Station": "name", "UIC Code": "uic", "IBNR ID": "db_id"})
    df = df.dropna(subset=["lat", "lon"]).rename(columns={"lat": "latitude", "lon": "longitude"})
    df["source"] = "Wikidata"

    df["uic"] = df["uic"].fillna(0).astype(int).astype(str).replace("0", "")
    df["db_id"] = df["db_id"].fillna(0).astype(int).astype(str).replace("0", "")

    df = df[["source", "name", "latitude", "longitude", "uic", "db_id"]]
    df = df.drop_duplicates(subset=["name", "latitude", "longitude", "uic"])

    return df


def classify_matches(df, train_coords, train_uic, train_db_id, df_train):
    tree = cKDTree(train_coords)
    coords = df[["latitude", "longitude"]].to_numpy()
    distance_upper_bound = 500 / 111_319.9  # Convert 500 meters to degrees

    dists, idxs = tree.query(coords, k=7, distance_upper_bound=distance_upper_bound)
    results = []

    for i, (dist_list, idx_list) in tqdm(enumerate(zip(dists, idxs)), total=len(dists)):
        src_uic = str(df.iloc[i]["uic"])
        matched = None
        category = None

        for dist, idx in zip(dist_list, idx_list):
            if idx == len(train_uic):
                continue
            if src_uic and src_uic == train_uic[idx]:
                matched, category = idx, "same_uic"
                break

        if matched is None:
            for dist, idx in zip(dist_list, idx_list):
                if idx == len(train_uic):
                    continue
                if src_uic and train_db_id[idx] and src_uic == train_db_id[idx]:
                    matched, category = idx, "uic_equals_db_id"
                    break

        if matched is None and idx_list[0] != len(train_uic):
            matched = idx_list[0]
            category = "no_uic" if not src_uic else "non_matching_uic"

        if matched is None:
            category = "unmatched_no_uic" if not src_uic else (
                "unmatched_uic_exists_elsewhere" if src_uic in train_uic else "unmatched_uic_not_exists"
            )

        results.append({
            "matched_idx": matched,
            "category": category,
            "dist_m": dist_list[0],
            "name_diff": (
                df.iloc[i]["name"],
                df_train.loc[matched, "name"] if matched is not None and matched in df_train.index else ""
            )
        })

    out = pd.DataFrame(results, index=df.index)
    return pd.concat([df, out], axis=1)


def plot_uic_presence(df, title, filename, europe, xlim, ylim):
    colors = {"no_uic": "#a6cee3", "uic_exists": "#1f78b4"}
    df = df.assign(uic_exists=df['uic'].astype(bool))

    fig, ax = plt.subplots(figsize=(7, 7))
    europe.plot(ax=ax, color="whitesmoke", edgecolor="gray", linewidth=0.3)
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.axis("off")
    ax.set_title(title, fontsize=14)

    ax.scatter(df.loc[~df.uic_exists, "longitude"], df.loc[~df.uic_exists, "latitude"],
               color=colors["no_uic"], label="No UIC", s=5, alpha=0.6)
    ax.scatter(df.loc[df.uic_exists, "longitude"], df.loc[df.uic_exists, "latitude"],
               color=colors["uic_exists"], label="UIC Exists", s=5, alpha=0.6)

    ax.legend(loc="lower left", fontsize=9, markerscale=2)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close(fig)


def plot_match_categories(df, title, filename, europe):
    match_colors = {
        "same_uic": "#a1d99b",
        "no_uic": "#d9d9d9",
        "uic_equals_db_id": "#fcae91",
        "non_matching_uic": "#fb6a4a"
    }

    fig, ax = plt.subplots(figsize=(10, 7), subplot_kw={'aspect': 'equal'})
    europe.plot(ax=ax, color="whitesmoke", edgecolor="gray", linewidth=0.3)
    ax.set_xlim(-10, 40)
    ax.set_ylim(35, 70)
    ax.axis("off")
    ax.set_title(title, fontsize=14)

    for cat, color in match_colors.items():
        sub = df[df.category == cat]
        ax.scatter(sub.longitude, sub.latitude, color=color, label=cat.replace('_', ' ').title(), s=6, alpha=0.7)

    ax.legend(loc='lower left', fontsize=9, markerscale=2)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close(fig)


parser = argparse.ArgumentParser(description="Compare train station datasets.")
parser.add_argument("--train_csv", required=True, help="Path to trainline stations CSV file")
parser.add_argument("--osm_dir", required=True, help="Directory containing Overpass JSON files")
parser.add_argument("--wikidata_dir", required=True, help="Directory containing Wikidata CSVs")
parser.add_argument("--outdir", required=True, help="Output directory for plots")
args = parser.parse_args()

os.makedirs(args.outdir, exist_ok=True)
# Load data
df_train = load_trainline(args.train_csv)
df_osm = load_osm_latest(args.osm_dir)
df_wikidata = load_wikidata(args.wikidata_dir)

train_uic = df_train['uic'].to_numpy()
train_db_id = df_train['db_id'].to_numpy()
train_coords = df_train[['latitude', 'longitude']].to_numpy()

# Classify matches
df_osm = classify_matches(df_osm, train_coords, train_uic, train_db_id, df_train)
df_wikidata = classify_matches(df_wikidata, train_coords, train_uic, train_db_id, df_train)

# Load Europe basemap
world = gpd.read_file("https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip")
europe = world[(world['CONTINENT'] == 'Europe') & ~world['NAME'].isin(['Russia', 'Greenland'])]

# Plot UIC presence
xlim, ylim = (-10, 40), (35, 70)
plot_uic_presence(df_train, "Trainline", os.path.join(args.outdir, "trainline_uic_presence.pdf"), europe, xlim, ylim)
plot_uic_presence(df_osm, "OSM", os.path.join(args.outdir, "osm_uic_presence.pdf"), europe, xlim, ylim)
plot_uic_presence(df_wikidata, "Wikidata", os.path.join(args.outdir, "wikidata_uic_presence.pdf"), europe, xlim, ylim)

# Plot match categories
plot_match_categories(df_osm, "Matched OSM stations", os.path.join(args.outdir, "osm_trainline_match_categories.pdf"), europe)
plot_match_categories(df_wikidata, "Matched Wikidata stations", os.path.join(args.outdir, "wikidata_trainline_match_categories.pdf"), europe)

# Print stats
for name, df in [("OSM", df_osm), ("Wikidata", df_wikidata)]:
    total = len(df)
    matched = df["matched_idx"].notnull().sum()
    print(f"{name}: total stations = {total}, matched = {matched} ({matched/total:.1%})")
    print(df["category"].value_counts(), "\n")