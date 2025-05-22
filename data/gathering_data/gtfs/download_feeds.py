import argparse
import requests
import time
import random
from tqdm import tqdm
import ftplib
from urllib.parse import urlparse
import os
import threading
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from http.client import IncompleteRead
import csv
import hashlib
import urllib3

# ----------------------
# Argument parser setup
# ----------------------
parser = argparse.ArgumentParser(
    description="Download all feeds from a CSV file with optional retry, threading, SSL verification, and logging."
)
parser.add_argument("file", type=str, help="CSV file containing URLs under a 'url' column")
parser.add_argument("output", type=str, help="Folder to save downloaded .zip files")
parser.add_argument("--timeout", type=int, default=30, help="Timeout in seconds for HTTP requests (default: 30)")
parser.add_argument("--no-threads", action="store_true", help="Disable multi-threaded downloads")
parser.add_argument("--no-verify", action="store_true", help="Disable SSL certificate verification for HTTPS")
parser.add_argument("--logging_file", type=str, default="url_download.csv", help="CSV file to log download status")
parser.add_argument("--retry_failed", action="store_true", help="Retry failed or missing downloads")
args = parser.parse_args()


# ----------------------
# Helper Functions
# ----------------------

def calculate_checksum(file_path):
    """Return SHA256 checksum of the file, or None if file doesn't exist."""
    if not os.path.exists(file_path):
        return None
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def generate_short_hash(url):
    """Generate a short 8-char hash from the URL."""
    return hashlib.md5(url.encode()).hexdigest()[:8]


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(IncompleteRead),
)
def download_file_with_resume(url, download_location, headers=None, timeout=30, verify=True):
    """Download with resume support and retry on partial downloads."""
    headers = headers or {}
    temp_location = f"{download_location}.part"
    start_time = time.time()

    try:
        # Resume support
        downloaded_bytes = 0
        if os.path.exists(temp_location):
            downloaded_bytes = os.path.getsize(temp_location)
            headers.update({"Range": f"bytes={downloaded_bytes}-"})

        # Stream the download
        with requests.get(url, headers=headers, timeout=timeout, stream=True, verify=verify) as response:
            if response.status_code in [200, 206]:  # 206: Partial Content
                mode = "ab" if downloaded_bytes > 0 else "wb"
                with open(temp_location, mode) as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            try:
                                f.write(chunk)
                            except IncompleteRead as ir:
                                tqdm.write(f"IncompleteRead encountered: {ir}")
                                raise
                os.rename(temp_location, download_location)
                return time.time() - start_time, response.status_code, calculate_checksum(download_location)
            else:
                return time.time() - start_time, f"error: {response.status_code}", None

    except Exception as e:
        tqdm.write(f"Error fetching URL {url}: {e}")
        return time.time() - start_time, f"error: {e}", None


def fetch_url_content(url, download_location, log_writer):
    """Download content from HTTP/HTTPS or FTP and log the result."""
    if not url:
        return
    start_time = time.time()
    try:
        if url.startswith("ftp://"):
            ftp_url = urlparse(url)
            ftp = ftplib.FTP(ftp_url.netloc)
            ftp.login()
            ftp.cwd(os.path.dirname(ftp_url.path))
            filename = ftp_url.path.split("/")[-1]
            with open(download_location, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
            ftp.quit()
            status = "ftp"
            checksum = calculate_checksum(download_location)
        else:
            duration, status, checksum = download_file_with_resume(
                url, download_location, timeout=args.timeout, verify=verify
            )

        end_time = time.time()
        log_writer.writerow({
            "url": url,
            "start_time": int(start_time),
            "end_time": int(end_time),
            "status": status,
            "parameters": f"timeout={args.timeout}, verify={verify}, no_threads={args.no_threads}",
            "file_path": download_location,
            "file_checksum": checksum,
        })

    except Exception as e:
        end_time = time.time()
        tqdm.write(f"Error fetching URL {url}: {e}")
        log_writer.writerow({
            "url": url,
            "start_time": start_time,
            "end_time": end_time,
            "status": f"error: {e}",
            "parameters": f"timeout={args.timeout}, verify={verify}, no_threads={args.no_threads}",
            "file_path": download_location,
            "file_checksum": None,
        })


# ----------------------
# Main script logic
# ----------------------

# Load all URLs from input CSV
urls = set()
with open(args.file, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        urls.add(row["url"].strip())

# Retry mode: skip already downloaded files
if args.retry_failed:
    print("Retrying only failed or missing downloads...")
    initial_count = len(urls)
    for url in list(urls):
        short_hash = generate_short_hash(url)
        filename = os.path.join(args.output, f"{short_hash}.zip")
        if os.path.exists(filename):
            urls.discard(url)
    print(f"Skipped {initial_count - len(urls)} URLs already downloaded.")

# Setup
verify = not args.no_verify
if not verify:
    urllib3.disable_warnings()

os.makedirs(args.output, exist_ok=True)
threads = []
file_exists = os.path.isfile(args.logging_file)

print(f"Starting downloads: timeout={args.timeout}, verify={verify}, threading={not args.no_threads}")

# Open logging file
with open(args.logging_file, "a", newline="") as log_file:
    log_writer = csv.DictWriter(log_file, fieldnames=[
        "url", "start_time", "end_time", "status",
        "parameters", "file_path", "file_checksum"
    ])
    if not file_exists:
        log_writer.writeheader()

    if args.no_threads:
        # Sequential downloading
        for url in tqdm(urls, desc="Downloading"):
            short_hash = generate_short_hash(url)
            filename = os.path.join(args.output, f"{short_hash}.zip")
            if os.path.exists(filename):
                continue
            fetch_url_content(url, filename, log_writer)
    else:
        # Multi-threaded downloading
        for url in tqdm(urls, desc="Queueing"):
            short_hash = generate_short_hash(url)
            filename = os.path.join(args.output, f"{short_hash}.zip")
            if os.path.exists(filename):
                continue
            thread = threading.Thread(target=fetch_url_content, args=(url, filename, log_writer))
            threads.append(thread)
            thread.start()

        for thread in tqdm(threads, desc="Waiting for threads"):
            thread.join()

print(f"Finished processing {len(urls)} URLs.")
