import requests
from tqdm import tqdm
import argparse
import time
import json
import dotenv
import os
import csv

# Load environment variables from a .env file
dotenv.load_dotenv()

REFRESH_TOKEN = os.environ['MD_REFRESH_TOKEN']

# Default list of countries to scrape (ISO country codes)
DEFAULT_COUNTRIES = [
    'FR', 'CH', 'DE', 'BE', 'ES', 'IT', 'AD', 'GB', 'NL', 'AT', 'LU',
    'PT', 'PL', 'RU', 'BY', 'HU', 'CZ', 'SK', 'HR', 'DK', 'SE', 'SI',
    'MA', 'IE', 'BG', 'GR', 'LT', 'LV', 'MK', 'NO', 'RO', 'UA', 'TR',
    'RS', 'ME', 'BA', 'FI', 'LI', 'AL', 'MT', 'MD', 'EE', 'CY'
]

VERBOSE = True  # Controls tqdm and debug output

# ----------------------
# CLI Argument Parser
# ----------------------
parser = argparse.ArgumentParser(
    description='Scrape GTFS feeds from the MobilityDatabase API and log metadata.'
)
parser.add_argument(
    '--logging_file', type=str, default='feed_urls.csv',
    help='CSV file where feed URLs and metadata will be stored'
)
parser.add_argument(
    '--countries', nargs='+', default=DEFAULT_COUNTRIES,
    help='List of country codes to scrape (default: European countries)'
)
parser.add_argument(
    '--limit', type=int, default=100,
    help='Number of feeds to request per API call (default: 100)'
)
args = parser.parse_args()

# ----------------------
# Authenticate to MobilityDatabase API
# ----------------------
auth_payload = {'refresh_token': REFRESH_TOKEN}
auth_response = requests.post(
    'https://api.mobilitydatabase.org/v1/tokens',
    headers={'Content-Type': 'application/json'},
    json=auth_payload
)

access_token = auth_response.json()['access_token']
mobility_headers = {
    'accept': 'application/json',
    'Authorization': f'Bearer {access_token}',
}

# ----------------------
# Setup logging CSV
# ----------------------
file_exists = os.path.isfile(args.logging_file)
fieldnames = [
    'url', 'run_id', 'time_added', 'source',
    'id', 'country', 'license', 'known_status'
]
all_feeds = []

with open(args.logging_file, 'a', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    if not file_exists:
        writer.writeheader()

    for country in tqdm(args.countries, desc='Countries', position=0, disable=not VERBOSE):
        offset = 0
        keep_fetching = True

        while keep_fetching:
            params = {
                'limit': args.limit,
                'offset': offset,
                'country_code': country,
                'status': 'active',
            }

            feeds_response = requests.get(
                'https://api.mobilitydatabase.org/v1/gtfs_feeds',
                headers=mobility_headers,
                params=params
            )

            try:
                feeds = feeds_response.json()
                assert feeds_response.status_code == 200
            except Exception as e:
                if VERBOSE:
                    tqdm.write(f"Error retrieving feeds: {e}")
                    tqdm.write(f"Response: {feeds_response.text}")
                break  # Skip to the next country

            for feed in tqdm(feeds, desc=f'Feeds {country} ({offset})', position=1, leave=False):
                if not any(existing['id'] == feed['id'] for existing in all_feeds):
                    all_feeds.append(feed)
                    url = feed['source_info'].get('producer_url', '').strip()
                    if url:
                        writer.writerow({
                            'url': url,
                            'run_id': f'mobilitydata_{time.strftime("%Y%m%d")}',
                            'time_added': int(time.time()),
                            'source': 'mobilitydatabase',
                            'id': feed['id'],
                            'country': country,
                            'license': feed['source_info'].get('license_url', ''),
                            'known_status': feed.get('status', '')
                        })
                elif VERBOSE:
                    tqdm.write(f"Feed {feed['id']} already processed.")

            # Stop fetching if fewer feeds than the limit were returned
            if len(feeds) < args.limit:
                keep_fetching = False

            offset += args.limit

# ----------------------
# Final summary
# ----------------------
if VERBOSE:
    print(f"Finished scraping feeds. Total feeds collected: {len(all_feeds)}")

print(args.logging_file)
