import os
import argparse
import csv
import json
import time
from tqdm import tqdm

# -------------------------------
# Country codes to filter by
# -------------------------------
COUNTRIES = ['CH', 'FR', 'DE', 'BE', 'ES', 'IT', 'AD', 'GB', 'NL', 'AT', 'LU',
    'PT', 'PL', 'RU', 'BY', 'HU', 'CZ', 'SK', 'HR', 'DK', 'SE', 'SI',
    'MA', 'IE', 'BG', 'GR', 'LT', 'LV', 'MK', 'NO', 'RO', 'UA', 'TR',
    'RS', 'ME', 'BA', 'FI', 'LI', 'AL', 'MT', 'MD', 'EE', 'CY']

# -------------------------------
# Argument parsing
# -------------------------------
parser = argparse.ArgumentParser(description='Scrape GTFS feed URLs from Transitous repo')
parser.add_argument('--logging_file', type=str, default='feed_urls.csv', help='CSV file to log feed URLs')
parser.add_argument('--countries', nargs='+', help='Countries to scrape (country codes)', default=COUNTRIES)
parser.add_argument('--transitous_path', type=str, required=True, help='Path to the Transitous git repository')
args = parser.parse_args()


feeds_path = os.path.join(args.transitous_path, 'feeds')
json_files = [f for f in os.listdir(feeds_path) if f.endswith('.json')]

file_exists = os.path.isfile(args.logging_file)
transitland_ids = set()

with open(args.logging_file, 'a', newline='') as csvfile:
    fieldnames = ['url', 'run_id', 'time_added', 'source', 'id', 'country', 'license', 'known_status']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    if not file_exists:
        writer.writeheader()

    for file in tqdm(json_files, desc='Files', position=0):
        country_code = file.split('.')[0].upper()
        if country_code in args.countries:
            filepath = os.path.join(feeds_path, file)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                print(f"Error loading JSON from {file}: {e}")
                continue

            for s in data.get('sources', []):
                if s.get('type') == 'http' and s.get('spec') != 'gtfs-rt':
                    writer.writerow({
                        'url': s['url'],
                        'run_id': f'transitous_{time.strftime("%Y%m%d")}',
                        'time_added': int(time.time()),
                        'source': 'transitous',
                        'id': s.get('name', ''),
                        'country': country_code,
                        'license': None,
                        'known_status': None,
                    })
                elif s.get('type') == 'transitland-atlas':
                    transitland_ids.add(s.get('transitland-atlas-id'))

# -------------------------------
# Write Transitland IDs to file
# -------------------------------
with open('transitland_ids.txt', 'w') as f:
    for tid in sorted(transitland_ids):
        f.write(f"{tid}\n")

print(f"Scraped Transitous feeds. Logged URLs to: {args.logging_file}")
print(f"Saved {len(transitland_ids)} Transitland IDs to transitland_ids.txt")
