import overpy
import csv
import argparse
from tqdm import tqdm
import concurrent.futures

# List of countries (ISO Alpha-2 codes)
COUNTRIES = [
    'FR', 'CH', 'DE', 'BE', 'ES', 'IT', 'AD', 'GB', 'NL', 'AT', 'LU',
    'PT', 'PL', 'RU', 'BY', 'HU', 'CZ', 'SK', 'HR', 'DK', 'SE', 'SI',
    'MA', 'IE', 'BG', 'GR', 'LT', 'LV', 'MK', 'NO', 'RO', 'UA', 'TR',
    'RS', 'ME', 'BA', 'FI', 'LI', 'AL', 'MT', 'MD', 'EE', 'CY'
]

# Define the Overpass query for filtered train stations
def get_train_stations_query(country_code):
    return f"""
    [out:json];
    area["ISO3166-1"="{country_code}"]->.searchArea;
    node(area.searchArea)
      ["railway"~"station|halt|stop"]
      [!"abandoned"]
      [!"abandoned:railway"]
      [!"disused"]
      [subway!="yes"]
      [tram!="yes"];
    out body;
    """

# Initialize Overpy API
api = overpy.Overpass()

# Function to download train stations for a specific country
def download_train_stations_for_country(country):
    query = get_train_stations_query(country)
    try:
        result = api.query(query)
        stations_data = []

        for station in result.nodes:
            stations_data.append({
                'id': station.id,
                'uic': station.tags.get('uic_ref', ''),
                'latitude': station.lat,
                'longitude': station.lon,
                'country': country,
                'name': station.tags.get('name', '') or station.tags.get('uic_name', ''),
                'wikidata': station.tags.get('wikidata', '')
            })

        return stations_data

    except Exception as e:
        print(f"Failed to download train stations for {country}: {e}")
        return []

# Save station data to CSV
def save_to_csv(all_stations_data, output_file):
    with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'uic', 'latitude', 'longitude', 'country', 'name', 'wikidata']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for station_data in all_stations_data:
            writer.writerow(station_data)

parser = argparse.ArgumentParser(description='Download OSM train stations for multiple countries.')
parser.add_argument('--output', type=str, default='osm_train_stations.csv',
                    help='Path to output CSV file (default: osm_train_stations.csv)')
args = parser.parse_args()

all_stations_data = []

# Use ThreadPoolExecutor with one worker to avoid Overpass API rate limits
with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    results = executor.map(download_train_stations_for_country, COUNTRIES)

    for result in tqdm(results, total=len(COUNTRIES)):
        all_stations_data.extend(result)

# Save to CSV
save_to_csv(all_stations_data, args.output)
print(f"Finished saving {len(all_stations_data)} train stations to '{args.output}'.")
