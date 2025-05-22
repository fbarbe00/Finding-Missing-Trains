import os
import argparse
import json
import requests
import pandas as pd
from scipy.spatial import cKDTree
from rapidfuzz import fuzz
import xml.etree.ElementTree as ET
from tqdm import tqdm

COUNTRIES = [
    "FR", "CH", "DE", "BE", "ES", "IT", "AD", "GB", "NL", "AT", "LU", "PT", "PL", "RU", "BY",
    "HU", "CZ", "SK", "HR", "DK", "SE", "SI", "MA", "IE", "BG", "GR", "LT", "LV", "MK", "NO",
    "RO", "UA", "TR", "RS", "ME", "BA", "FI", "LI", "AL", "MT", "MD", "EE", "CY"
]

def get_overpass_query(cc: str) -> str:
    return f"""
    [out:json];
    area["ISO3166-1"="{cc}"];
    node(area)["railway"~"station|halt"];
    out body;
    """

def fetch_osm_data(countries, cache_file=None):
    rows = []
    if cache_file and os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            overpass_results = json.load(f)
    else:
        overpass_results = {}
        for cc in tqdm(countries, desc="Fetching OSM data"):
            query = get_overpass_query(cc)
            res = requests.post("http://overpass-api.de/api/interpreter", data={"data": query})
            data = res.json()
            overpass_results[cc] = data
        if cache_file:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(overpass_results, f, indent=2)

    for cc_data in overpass_results.values():
        for el in cc_data.get("elements", []):
            if el.get("type") != "node":
                continue
            rows.append({
                "id": el["id"],
                "latitude": el.get("lat"),
                "longitude": el.get("lon"),
                "uic": el.get("tags", {}).get("uic_ref", ""),
                "name": el.get("tags", {}).get("name", ""),
                "railway": el.get("tags", {}).get("railway", "station")
            })

    return pd.DataFrame(rows), overpass_results


def assign_nuts_regions(df, nuts_file_path):
    if not os.path.exists(nuts_file_path):
        raise FileNotFoundError(f"NUTS file not found: {nuts_file_path}")
    df_nuts = pd.read_csv(nuts_file_path)
    tree = cKDTree(df_nuts[["latitude", "longitude"]].to_numpy())
    coords = df[["latitude", "longitude"]].to_numpy()
    _, idxs = tree.query(coords, k=1)
    df["NUTS_ID"] = df_nuts["NUTS_ID"].iloc[idxs].values
    return df


def match_stations(df_osm, df_train):
    coords_osm = df_osm[["latitude", "longitude"]].to_numpy()
    coords_train = df_train[["latitude", "longitude"]].to_numpy()
    tree = cKDTree(coords_train)
    distances, indices = tree.query(coords_osm)

    name_similarities = [
        fuzz.ratio(osm_name, df_train.iloc[i]["name"]) / 100
        for osm_name, i in zip(df_osm["name"], indices)
    ]
    df_matches = df_osm.copy()
    df_matches["nearest_station_uic"] = df_train["uic"].iloc[indices].values
    df_matches["nearest_station_distance"] = distances
    df_matches["nearest_station_name_similarity"] = name_similarities
    df_matches["mismatched_db_id_uic"] = df_matches["uic"] != df_matches["nearest_station_uic"]
    df_matches["matched_uic_exists"] = df_matches["uic"].isin(df_train["uic"])
    return df_matches


def filter_mismatches(df_matches, nuts_file=None):
    mismatches = df_matches[
        (~df_matches["matched_uic_exists"]) &
        (df_matches["nearest_station_distance"] < 100) &
        (df_matches["nearest_station_name_similarity"] < 0.4)
    ]
    if nuts_file:
        mismatches = assign_nuts_regions(mismatches, nuts_file)
        mismatches["area_id"] = mismatches["NUTS_ID"].fillna("unknown")
        mismatches["area_id"] = mismatches["area_id"].apply(
            lambda x: x[:2] if mismatches["area_id"].value_counts().get(x, 0) < 20 else x
        )
    return mismatches


def find_element_by_id(el_id, overpass_data):
    for country_data in overpass_data.values():
        for el in country_data.get("elements", []):
            if el.get("id") == el_id:
                return el
    return None


def generate_osm_xml(mismatches, overpass_data, output_file):
    root = ET.Element("osm", version="0.6", generator="uic-mismatch-fixer")
    for _, row in mismatches.iterrows():
        el = find_element_by_id(row["id"], overpass_data)
        if not el:
            continue
        node = ET.SubElement(root, "node", id=str(int(row["id"])), lat=str(row["latitude"]), lon=str(row["longitude"]), version="1", action="modify")
        tags = el.get("tags", {})
        for k, v in tags.items():
            if k != "uic_ref":
                ET.SubElement(node, "tag", k=k, v=v)
        ET.SubElement(node, "tag", k="uic_ref", v=row["nearest_station_uic"])
        source = tags.get("source", "")
        source += ";https://github.com/trainline-eu/stations" if source else "https://github.com/trainline-eu/stations"
        ET.SubElement(node, "tag", k="source", v=source)
    ET.ElementTree(root).write(output_file, encoding="UTF-8", xml_declaration=True)
    print(f"✔ XML written to {output_file} with {len(mismatches)} mismatches.")


parser = argparse.ArgumentParser(description="Fix mislabelled UIC codes in OSM railway stations.")
parser.add_argument("--train-csv", required=True, help="Reference Trainline CSV")
parser.add_argument("--osm-json", help="Optional Overpass cached JSON file")
parser.add_argument("--nuts-csv", help="Optional NUTS region CSV")
parser.add_argument("--output-xml", required=True, help="Output OSM XML file")
args = parser.parse_args()

df_train = pd.read_csv(args.train_csv, sep=";", dtype={"uic": str})
df_train.dropna(subset=["latitude", "longitude", "uic"], inplace=True)

df_osm, overpass_data = fetch_osm_data(COUNTRIES, args.osm_json)
df_osm.dropna(subset=["latitude", "longitude"], inplace=True)

df_matches = match_stations(df_osm, df_train)
mismatches = filter_mismatches(df_matches, args.nuts_csv)

if mismatches.empty:
    print("No mismatches found.")
else:
    generate_osm_xml(mismatches, overpass_data, args.output_xml)

