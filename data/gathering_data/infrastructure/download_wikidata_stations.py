import os
import argparse
import pandas as pd
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON

# EU countries
DEFAULT_EU_COUNTRIES = [
    "Q40", "Q37", "Q31", "Q219", "Q224", "Q213", "Q35", "Q191", "Q33", "Q142",
    "Q183", "Q41", "Q28", "Q27", "Q38", "Q55", "Q211", "Q32", "Q36", "Q45",
    "Q218", "Q214", "Q215", "Q29", "Q34", "Q145", "Q39", "Q225", "Q403", "Q236", "Q20"
]

DEFAULT_EXCLUDED = {"Q233", "Q229"}  # Malta and Cyprus

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

def query_stations(country_id):
    query = f"""
    SELECT ?station ?stationLabel ?uic ?coord ?osmNode ?trainline ?osmRelation ?ibnr WHERE {{
      ?station wdt:P31/wdt:P279* wd:Q55488.
      ?station wdt:P17 wd:{country_id}.
      FILTER NOT EXISTS {{ ?station wdt:P31 wd:Q106772341. }}
      FILTER NOT EXISTS {{ ?station wdt:P576 ?dissolutionDate. }}
      FILTER NOT EXISTS {{ ?station wdt:P5817 wd:Q11639308. }}
      FILTER NOT EXISTS {{ ?station wdt:P31/wdt:P279* wd:Q928830. }}
      OPTIONAL {{ ?station wdt:P722 ?uic. }}
      OPTIONAL {{ ?station wdt:P625 ?coord. }}
      OPTIONAL {{ ?station wdt:P11693 ?osmNode. }}
      OPTIONAL {{ ?station wdt:P6724 ?trainline. }}
      OPTIONAL {{ ?station wdt:P402 ?osmRelation. }}
      OPTIONAL {{ ?station wdt:P954 ?ibnr. }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    ORDER BY ?stationLabel
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    data = []
    for result in results["results"]["bindings"]:
        data.append({
            "Station": result.get("stationLabel", {}).get("value", ""),
            "Wikidata ID": result.get("station", {}).get("value", "").split("/")[-1],
            "UIC Code": result.get("uic", {}).get("value", ""),
            "Coordinates": result.get("coord", {}).get("value", ""),
            "OSM Node ID": result.get("osmNode", {}).get("value", ""),
            "Trainline ID": result.get("trainline", {}).get("value", ""),
            "OSM Relation ID": result.get("osmRelation", {}).get("value", ""),
            "IBNR ID": result.get("ibnr", {}).get("value", ""),
            "Country": country_id,
        })

    return data

parser = argparse.ArgumentParser(description="Download EU railway station data from Wikidata.")
parser.add_argument("--output-dir", type=str, default="downloaded_stations", help="Directory to store CSV files")
parser.add_argument("--include", nargs="+", help="Additional country Wikidata IDs to include")
parser.add_argument("--exclude", nargs="+", help="Country Wikidata IDs to exclude")
parser.add_argument("--overwrite", action="store_true", help="Overwrite existing CSV files")

args = parser.parse_args()
# Handle countries
countries = set(DEFAULT_EU_COUNTRIES)
if args.include:
    countries.update(args.include)
if args.exclude:
    countries.difference_update(args.exclude)
else:
    countries.difference_update(DEFAULT_EXCLUDED)

countries = sorted(countries)
print(f"Processing {len(countries)} countries...")

os.makedirs(args.output_dir, exist_ok=True)

for country in tqdm(countries, desc="Processing EU countries"):
    filename = os.path.join(args.output_dir, f"eu_railway_stations_{country}.csv")
    
    if os.path.exists(filename) and not args.overwrite:
        tqdm.write(f"Data for {country} already exists, skipping...")
        continue

    try:
        stations = query_stations(country)
    except Exception as e:
        tqdm.write(f"Failed to query stations for {country}: {str(e)}")
        continue

    df = pd.DataFrame(stations)
    df.to_csv(filename, index=False, encoding="utf-8")

    if not df.empty:
        tqdm.write(f"Data for {country} saved to {filename}")
    else:
        tqdm.write(f"No data found for {country}, saved empty file.")
