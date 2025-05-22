import requests
import argparse
from tqdm import tqdm
from urllib.parse import urljoin
import time
import dotenv
import os
import csv

# Load API key from .env file
dotenv.load_dotenv()
API_KEY = os.environ['TRANSITLAND_API_KEY']

# Base URL for the Transitland API
BASE_URL = 'https://api.transit.land/api/v2/rest/'

# Default set of countries to scrape (ISO country codes)
DEFAULT_COUNTRIES = [
    'CH', 'FR', 'DE', 'BE', 'ES', 'IT', 'AD', 'GB', 'NL', 'AT', 'LU',
    'PT', 'PL', 'RU', 'BY', 'HU', 'CZ', 'SK', 'HR', 'DK', 'SE', 'SI',
    'MA', 'IE', 'BG', 'GR', 'LT', 'LV', 'MK', 'NO', 'RO', 'UA', 'TR',
    'RS', 'ME', 'BA', 'FI', 'LI', 'AL', 'MT', 'MD', 'EE', 'CY'
]

VERBOSE = True  # Set to False to disable tqdm and debug output

# ----------------------
# Argument Parser Setup
# ----------------------
parser = argparse.ArgumentParser(
    description='Scrape GTFS feed metadata from Transitland API by country'
)
parser.add_argument(
    '--logging_file', type=str, default='feed_urls.csv',
    help='CSV file to store feed URLs and metadata'
)
parser.add_argument(
    '--countries', nargs='+', default=DEFAULT_COUNTRIES,
    help='List of country codes to scrape (default: broad European region)'
)
args = parser.parse_args()

requests_number = 0  # Track number of API requests made


# ----------------------
# Get onestop_id list per country
# ----------------------
def get_onestop_ids(country, limit=100):
    global requests_number
    onestop_ids = set()
    params = {
        'adm0_iso': country,
        'apikey': API_KEY,
        'limit': limit
    }

    keep_going = True
    while keep_going:
        response = requests.get(urljoin(BASE_URL, 'agencies'), params=params)
        requests_number += 1

        if not response.ok:
            tqdm.write(f"Failed to fetch agencies for {country} (status: {response.status_code})")
            break

        data = response.json()
        agencies = data.get('agencies', [])

        for agency in agencies:
            feed = agency.get('feed_version', {}).get('feed', {})
            if feed.get('onestop_id'):
                onestop_ids.add(feed['onestop_id'])

        # Handle pagination
        if 'meta' in data and 'after' in data['meta']:
            params['after'] = data['meta']['after']
        else:
            keep_going = False

    return list(onestop_ids)


# ----------------------
# Prepare CSV logging
# ----------------------
file_exists = os.path.isfile(args.logging_file)
seen_onestop_ids = set()

with open(args.logging_file, 'a', newline='') as csvfile:
    fieldnames = [
        'url', 'run_id', 'time_added', 'source',
        'id', 'country', 'license', 'known_status'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    if not file_exists:
        writer.writeheader()

    # ----------------------
    # Iterate by country
    # ----------------------
    for country in tqdm(args.countries, desc='Countries', disable=not VERBOSE, position=0):
        onestop_ids = get_onestop_ids(country)

        # ----------------------
        # Iterate over feeds (onestop_ids)
        # ----------------------
        for onestop_id in tqdm(onestop_ids, desc=f'Feeds for {country}', disable=not VERBOSE, position=1, leave=False):
            if onestop_id in seen_onestop_ids:
                continue
            seen_onestop_ids.add(onestop_id)

            try:
                params = {
                    'onestop_id': onestop_id,
                    'apikey': API_KEY
                }

                # Request feed metadata
                response = requests.get(urljoin(BASE_URL, 'feeds'), params=params)

                if response.status_code == 429:  # Rate limit
                    if VERBOSE:
                        tqdm.write("Rate limit hit. Waiting 60 seconds...")
                    time.sleep(60)
                    response = requests.get(urljoin(BASE_URL, 'feeds'), params=params)

                response.raise_for_status()
                requests_number += 1

                feeds = response.json().get('feeds', [])
                if not feeds:
                    if VERBOSE:
                        tqdm.write(f"No feeds found for {onestop_id}")
                    continue

                # Log first feed (even if multiple versions exist)
                feed = feeds[0]
                url = feed.get('urls', {}).get('static_current', '')
                license_info = feed.get('license', '')
                feed_state = feed.get('feed_state', {})
                status = feed_state.get('feed_version', {}).get('feed_version_gtfs_import', {}).get('success', '')
                known_status = 'active' if status else 'inactive'

                writer.writerow({
                    'url': url,
                    'run_id': f'transitland_{time.strftime("%Y%m%d")}',
                    'time_added': int(time.time()),
                    'source': 'transitland',
                    'id': onestop_id,
                    'country': country,
                    'license': license_info,
                    'known_status': known_status
                })

            except Exception as e:
                if VERBOSE:
                    tqdm.write(f"Error processing {onestop_id}: {e}")
                continue

# ----------------------
# Summary
# ----------------------
if VERBOSE:
    print(f"\nFinished scraping feeds with {requests_number} API requests.")
print(args.logging_file)
