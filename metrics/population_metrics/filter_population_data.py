import argparse
import pandas as pd
import requests
import geopandas as gpd
from tqdm import tqdm
import os

def get_wikidata_details_batch(q_ids):
    """
    Fetches latitude, longitude, and official languages for a batch of Wikidata QIDs.

    Args:
        q_ids (list): A list of Wikidata QIDs (e.g., ['Q1017', 'Q213']).

    Returns:
        dict: A dictionary where keys are QIDs and values are dictionaries
              containing 'latitude', 'longitude', and 'official_languages' if found.
    """
    url = ("https://www.wikidata.org/w/api.php?action=wbgetentities"
           f"&ids={'|'.join(q_ids)}&format=json&props=claims")
    data = requests.get(url).json()
    details_dict = {}
    for q_id in q_ids:
        entity = data.get("entities", {}).get(q_id, {})
        claims = entity.get("claims", {})
        details = {}
        if "P625" in claims:  # P625 is the property for geographic coordinates
            coords = claims["P625"][0]["mainsnak"]["datavalue"]["value"]
            details["latitude"] = coords["latitude"]
            details["longitude"] = coords["longitude"]
        if "P37" in claims:  # P37 is the property for official language
            details["official_languages"] = [
                lang["mainsnak"]["datavalue"]["value"]["id"] for lang in claims["P37"]
            ]
        if details:
            details_dict[q_id] = details
    return details_dict

def fetch_details_from_wikidata(row, details_dict):
    """
    Applies Wikidata details to a DataFrame row if coordinates or languages are missing.

    Args:
        row (pd.Series): A row from the DataFrame.
        details_dict (dict): The dictionary of Wikidata details fetched in batch.

    Returns:
        pd.Series: The updated row.
    """
    q_id = row['DCID'].split('/')[-1]
    # Only try to fetch if latitude or longitude or official_languages are missing
    if pd.notnull(row['latitude']) and pd.notnull(row['longitude']) and isinstance(row['official_languages'], list):
        return row
    details = details_dict.get(q_id, {})
    row['latitude'] = details.get('latitude', row['latitude'])
    row['longitude'] = details.get('longitude', row['longitude'])
    row['official_languages'] = details.get('official_languages', row['official_languages'])
    return row

def find_city_wikidata(city_name, country_name=None, country_code=None):
    """
    Searches Wikidata for a city and returns its QID, coordinates, and official languages.

    Args:
        city_name (str): The name of the city to search for.
        country_name (str, optional): The name of the country to refine the search.
        country_code (str, optional): The Wikidata QID for the country to refine the search.

    Returns:
        dict: A dictionary containing 'QID', 'latitude', 'longitude', and 'official_languages'
              if found, otherwise None.
    """
    city_name = city_name.replace("'", "\\'")  # Escape single quotes for SPARQL query

    query = f"""
    SELECT ?city ?cityLabel ?coord ?languageLabel WHERE {{
      ?city wdt:P31/wdt:P279* wd:Q515. # Instance of a city
      ?city rdfs:label "{city_name}"@en.
      OPTIONAL {{ ?city wdt:P625 ?coord. }} # Coordinates
      OPTIONAL {{ ?city wdt:P17 ?country. ?country wdt:P37 ?language. }} # Official language of the country
      {'?city wdt:P17 ?country. ?country rdfs:label "' + country_name + '"@en.' if country_name else ''}
      {'?city wdt:P17 wd:' + country_code + '.' if country_code else ''}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    """

    url = "https://query.wikidata.org/sparql"
    headers = {"Accept": "application/json"}
    response = requests.get(url, params={"query": query, "format": "json"}, headers=headers)

    try:
        data = response.json()
        if "results" in data and data["results"]["bindings"]:
            result = data["results"]["bindings"][0]
            qid = result["city"]["value"].split("/")[-1]
            coord = result.get("coord", {}).get("value", None)
            language = result.get("languageLabel", {}).get("value", None)
            
            lat, lon = None, None
            if coord:
                lon, lat = coord.replace("Point(", "").replace(")", "").split()
            
            return {
            "QID": qid,
            "latitude": float(lat) if lat else None,
            "longitude": float(lon) if lon else None,
            "official_languages": [language] if language else []
            }
    except Exception as e:
        print(f"Error finding city '{city_name}': {e}\nResponse: {response.text}")
    return None

parser = argparse.ArgumentParser(
    description="Process population data, enrich with Wikidata information, and save."
)
parser.add_argument(
    "--input_file",
    type=str,
    default="../../data/population/Population in Cities of Europe (1831 to 2030).csv",
    help="Path to the input population CSV file. Default: ../../data/population/Population in Cities of Europe (1831 to 2030).csv"
)
parser.add_argument(
    "--output_file",
    type=str,
    default="population_cities_europe_latest_coordinates.csv",
    help="Path to save the output CSV file. Default: population_cities_europe_latest_coordinates.csv"
)
args = parser.parse_args()

# --- Load Population Data ---
if not os.path.exists(args.input_file):
    print(f"Warning: Input file not found at '{args.input_file}'.")
    print("Please download the data from https://data-explorer.oecd.org/vis?fs[0]=Topic%2C0%7CRegional%252C%20rural%20and%20urban%20development%23GEO%23&pg=40&fc=Topic&bp=true&snb=117&df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_REG_DEMO%40DF_POP_5Y&df[ag]=OECD.CFE.EDS&df[vs]=2.0&dq=A.......&to[TIME_PERIOD]=false&vw=ov&pd=%2C&ly[cl]=TIME_PERIOD&ly[rs]=COMBINED_MEASURE%2CCOMBINED_UNIT_MEASURE%2CSEX&ly[rw]=COMBINED_REF_AREA and save it as 'Population in Cities of Europe (1831 to 2030).csv' in the '../../data/population/' directory.")
    return

population_df = pd.read_csv(args.input_file)
population_df = population_df.rename(columns={
    "Entity DCID": "DCID",
    "Entity properties isoCode": "isoCode",
    "Entity properties name": "city",
    "Variable observation date": "date",
    "Variable observation metadata importName": "importName",
    "Variable observation metadata provenanceUrl": "provenanceUrl",
    "Variable observation metadata scalingFactor": "scalingFactor",
    "Variable observation metadata unit": "unit",
    "Variable observation metadata unitDisplayName": "unitDisplayName",
    "Variable observation value": "population",
    "Variable properties name": "variable",
})
population_df["population"] = population_df["population"].astype(float)
population_df["date"] = population_df["date"].str.extract(r"^(\d{4})").astype(int)
population_df = population_df[population_df["date"] >= 2000]

# Keep only the latest population for each city
latest_population_df = population_df.loc[population_df.groupby("city")["date"].idxmax()]

# Initialize new columns if they don't exist
for col in ['latitude', 'longitude', 'official_languages']:
    if col not in latest_population_df.columns:
        latest_population_df[col] = None

# --- Fetch Wikidata Details for Existing Wikidata IDs ---
# Process rows that already have a Wikidata ID
wikidata_rows = latest_population_df[latest_population_df['DCID'].str.startswith('wikidataId/')]
q_ids = wikidata_rows['DCID'].apply(lambda x: x.split('/')[-1]).tolist()
batch_size = 50
print("Fetching Wikidata details for existing Wikidata IDs...")
for i in tqdm(range(0, len(q_ids), batch_size)):
    batch_q_ids = q_ids[i:i + batch_size]
    details_dict = get_wikidata_details_batch(batch_q_ids)
    # Apply details to the original DataFrame using update
    # Create a temporary DataFrame for updating, as apply on a slice might not update the original df directly
    temp_df = wikidata_rows[wikidata_rows['DCID'].isin([f'wikidataId/{qid}' for qid in batch_q_ids])].copy()
    temp_df = temp_df.apply(fetch_details_from_wikidata, axis=1, args=(details_dict,))
    latest_population_df.update(temp_df)

# --- Convert NUTS IDs to Wikidata IDs and Fetch Details ---
# Drop columns not needed for the final output
latest_population_df = latest_population_df.drop(columns=[
    "isoCode", "Variable DCID", "importName", "provenanceUrl",
    "scalingFactor", "unit", "unitDisplayName", "variable"
], errors='ignore')

nuts_ids_to_process = latest_population_df[latest_population_df['DCID'].str.startswith('nuts/')]['DCID'].unique()
nuts_ids_to_process = [x.split('/')[-1] for x in nuts_ids_to_process]

# Mapping NUTS country codes to full country names for better Wikidata search
country_names = {
    'AT': 'Austria', 'CZ': 'Czech Republic', 'DE': 'Germany', 'ES': 'Spain',
    'FR': 'France', 'HU': 'Hungary', 'LV': 'Latvia', 'NL': 'Netherlands',
    'NO': 'Norway', 'PL': 'Poland', 'SO': 'Slovakia', 'UK': 'United Kingdom'
}

print("\nConverting NUTS IDs to Wikidata IDs and fetching details...")
for n_id in tqdm(nuts_ids_to_process):
    # Select the row(s) corresponding to the current NUTS ID
    # Use .copy() to avoid SettingWithCopyWarning if you modify it directly afterwards
    rows_to_update = latest_population_df[latest_population_df['DCID'] == f'nuts/{n_id}'].copy()

    if rows_to_update.empty:
        continue
    
    city_name = rows_to_update.iloc[0]['city']
    country_name = country_names.get(n_id[:2], None)
    
    result = find_city_wikidata(city_name, country_name=country_name)
    if result:
        q_id = result['QID']
        lat = result['latitude']
        lon = result['longitude']
        official_languages = result.get('official_languages', [])

        # Update the original DataFrame using .loc for safe assignment
        latest_population_df.loc[latest_population_df['DCID'] == f'nuts/{n_id}', 'DCID'] = f'wikidataId/{q_id}'
        latest_population_df.loc[latest_population_df['DCID'] == f'nuts/{n_id}', 'latitude'] = lat
        latest_population_df.loc[latest_population_df['DCID'] == f'nuts/{n_id}', 'longitude'] = lon
        latest_population_df.loc[latest_population_df['DCID'] == f'nuts/{n_id}', 'official_languages'] = [official_languages] # Ensure list format
    else:
        tqdm.write(f"Could not find '{city_name}' ({country_name}) in Wikidata.")

# --- Final pass to fetch remaining missing coordinates for Wikidata IDs ---
print("\nPerforming final pass to fetch any remaining missing coordinates...")
wikidata_rows_missing = latest_population_df[
    latest_population_df['DCID'].str.startswith('wikidataId/') &
    (latest_population_df['latitude'].isnull() | latest_population_df['longitude'].isnull())
]
q_ids_missing = wikidata_rows_missing['DCID'].apply(lambda x: x.split('/')[-1]).tolist()

for i in tqdm(range(0, len(q_ids_missing), batch_size)):
    batch_q_ids = q_ids_missing[i:i + batch_size]
    details_dict = get_wikidata_details_batch(batch_q_ids)
    temp_df_missing = wikidata_rows_missing[
        wikidata_rows_missing['DCID'].isin([f'wikidataId/{qid}' for qid in batch_q_ids])
    ].copy()
    temp_df_missing = temp_df_missing.apply(fetch_details_from_wikidata, axis=1, args=(details_dict,))
    latest_population_df.update(temp_df_missing)

# --- Save Results ---
latest_population_df.to_csv(args.output_file, index=False)
print(f"\nProcessed data saved to {args.output_file}")
print(f"Cities with missing coordinates: {latest_population_df['latitude'].isnull().sum()}")
