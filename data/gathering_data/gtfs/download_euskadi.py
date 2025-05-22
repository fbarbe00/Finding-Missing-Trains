import ftplib
import argparse
import time
import os
import csv

VERBOSE = True

parser = argparse.ArgumentParser(description='Download GTFS feeds from Euskadi FTP server')
parser.add_argument(
    '--ignore_keywords',
    nargs='+',
    help='Folder or path keywords to ignore (case-insensitive)',
    default=['bus']
)
parser.add_argument(
    '--logging_file',
    type=str,
    default='feed_urls.csv',
    help='CSV file to log feed URLs and metadata'
)
args = parser.parse_args()

ftp = ftplib.FTP("ftp.geo.euskadi.net")
ftp.login()  # Anonymous login

all_feeds = []

file_exists = os.path.isfile(args.logging_file)
with open(args.logging_file, 'a', newline='') as csvfile:
    fieldnames = [
        'url', 'run_id', 'time_added', 'source',
        'id', 'country', 'license', 'known_status'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    if not file_exists:
        writer.writeheader()

    def collect_all_feed_links(ftp, path):
        """Recursively search the FTP server for .zip GTFS feeds."""
        # Check ignore conditions
        if any(keyword.lower() in path.lower() for keyword in args.ignore_keywords):
            if VERBOSE:
                print(f"Ignoring path (matched keyword): {path}")
            return

        try:
            original_path = ftp.pwd()
            ftp.cwd(path)
        except ftplib.error_perm as e:
            if VERBOSE:
                print(f"Permission error accessing {path}: {e}")
            return

        try:
            files = ftp.nlst()
        except ftplib.error_perm as e:
            if VERBOSE:
                print(f"Error listing directory {path}: {e}")
            ftp.cwd(original_path)
            return

        for file in files:
            # Recursively dive into subfolders (skip files)
            if '.' not in file:
                collect_all_feed_links(ftp, file)
                continue

            # If it’s a .zip file, treat it as a GTFS feed
            if file.lower().endswith(".zip"):
                current_path = ftp.pwd()
                feed_link = f"ftp://ftp.geo.euskadi.net{current_path}/{file}"
                all_feeds.append(feed_link)
                if VERBOSE:
                    print(f"Found feed link: {feed_link}")

                writer.writerow({
                    'url': feed_link,
                    'run_id': f'euskadi_{time.strftime("%Y%m%d")}',
                    'time_added': int(time.time()),
                    'source': 'euskadi',
                    'id': '',
                    'country': 'ES',
                    'license': '',
                    'known_status': 'active'
                })

        # Go back up one directory level
        ftp.cwd(original_path)

    collect_all_feed_links(ftp, "/cartografia/Transporte/Moveuskadi/")

ftp.quit()

if VERBOSE:
    print(f"\nFinished. Total feeds found: {len(all_feeds)}")
    print(f"Logged to: {args.logging_file}")
