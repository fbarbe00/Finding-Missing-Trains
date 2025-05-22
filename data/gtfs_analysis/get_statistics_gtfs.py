import argparse
from io import TextIOWrapper
import os
import time
import zipfile
import csv
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import subprocess

all_stations = set()
def quick_check(file_path):
    """Perform a quick comparison using size and CRC32."""
    crc32s = []
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            for file_info in sorted(zip_file.infolist(), key=lambda x: x.filename):
                crc32s.append((file_info.filename, file_info.CRC))
                with zip_file.open(file_info.filename) as _:
                    pass
        return tuple(crc32s)  # Immutable tuple as hash key
    except Exception:
        return None

def interleave_round_robin(files):
    sorted_files = sorted(files, key=lambda x: os.stat(x).st_size)
    n = len(sorted_files)
    very_large, large = sorted_files[3 * n // 4:], sorted_files[n // 2:3 * n // 4]
    medium, small = sorted_files[n // 4:n // 2], sorted_files[:n // 4]

    interleaved = []
    while any([very_large, large, medium, small]):
        if very_large: interleaved.append(very_large.pop())
        if small: interleaved.append(small.pop())
        if large: interleaved.append(large.pop())
        if medium: interleaved.append(medium.pop())

    return interleaved

def attempt_fix_zip(file_path, tmp_dir="tmp"):
    """Attempt to fix a corrupted zip file using the zip -FF command and remove problematic files."""
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        tmp_file = os.path.join(tmp_dir, os.path.basename(file_path).split('.')[0] + '_tmp.zip')
        fixed_file_path = os.path.join(tmp_dir, os.path.basename(file_path))
        result = subprocess.run(['zip', '-FF', file_path, '--out', tmp_file], input='y\n', capture_output=True, text=True)
        if result.returncode == 0:
            with zipfile.ZipFile(tmp_file, 'r') as in_zip:
                with zipfile.ZipFile(fixed_file_path, 'w') as out_zip:
                    for file_info in in_zip.infolist():
                        try:
                            with in_zip.open(file_info.filename) as infile:
                                out_zip.writestr(file_info, infile.read())
                        except Exception as e:
                            tqdm.write(f"Failed to copy file: {file_info.filename}: {e}")
                            pass
            os.remove(tmp_file)
            return fixed_file_path
        else:
            return None
    except Exception as e:
        tqdm.write(f"Failed to fix zip file: {file_path}: {e}")
        return None

def analyse_zip_file(zip_file_path):
    """Analyse GTFS files inside a ZIP archive."""
    route_types, start_date, end_date = set(), None, None
    bbox = [float('inf'), float('inf'), float('-inf'), float('-inf')]
    gtfs_files = []
    gtfs_files_sizes = {}

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.is_dir():
                continue
            gtfs_files.append(file_info.filename)
            gtfs_files_sizes[file_info.filename] = file_info.file_size
            with zip_ref.open(file_info.filename) as infile:
                reader = csv.DictReader(TextIOWrapper(infile, encoding='utf-8-sig', errors='replace'))
                if file_info.filename == 'routes.txt':
                    for row in reader:
                        route_types.add(row.get('route_type'))
                elif file_info.filename == 'calendar_dates.txt':
                    for row in reader:
                        date_str = row.get('date')
                        if date_str:
                            date = datetime.strptime(date_str, "%Y%m%d").date()
                            if start_date is None or date < start_date:
                                start_date = date
                            if end_date is None or date > end_date:
                                if date < datetime(2050, 1, 1).date():
                                    end_date = date
                elif file_info.filename == 'calendar.txt':
                    for row in reader:
                        start_date_str, end_date_str = row.get('start_date'), row.get('end_date')
                        if start_date_str and end_date_str:
                            start_date_date = datetime.strptime(start_date_str.strip(), "%Y%m%d").date()
                            end_date_date = datetime.strptime(end_date_str.strip(), "%Y%m%d").date()
                            if start_date is None or start_date_date < start_date:
                                start_date = start_date_date
                            if end_date is None or end_date_date > end_date:
                                if end_date_date < datetime(2050, 1, 1).date():
                                    end_date = end_date_date
                elif file_info.filename == 'stops.txt':
                    for row in reader:
                        stop_lat, stop_lon = row.get('stop_lat'), row.get('stop_lon')
                        if stop_lat and stop_lon:
                            lat, lon = float(stop_lat), float(stop_lon)
                            all_stations.add((lat, lon))
                            bbox[0], bbox[1] = min(bbox[0], lat), min(bbox[1], lon)
                            bbox[2], bbox[3] = max(bbox[2], lat), max(bbox[3], lon)

    return {
        'route_types': list(route_types),
        'start_date': start_date,
        'end_date': end_date,
        'bbox': bbox,
        'gtfs_files': gtfs_files,
        'gtfs_files_sizes': gtfs_files_sizes
    }

def log_statistics(log_file, stats):
    """Log the statistics to the CSV file."""
    file_exists = os.path.isfile(log_file)
    with open(log_file, 'a', newline='') as csvfile:
        fieldnames = ['filename', 'duplicate_file', 'corrupted_file', 'date', 'route_types', 'start_date', 'end_date', 'bbox', 'gtfs_files', 'gtfs_files_sizes']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        stats['filename'] = os.path.basename(stats['filename'])
        if stats['duplicate_file']:
            stats['duplicate_file'] = os.path.basename(stats['duplicate_file'])
        stats['route_types'] = ','.join(map(str, stats['route_types']))
        if stats['bbox'] == [float('inf'), float('inf'), float('-inf'), float('-inf')]:
            stats['bbox'] = ''
        else:
            stats['bbox'] = ','.join(map(str, stats['bbox']))
        stats['gtfs_files'] = ','.join(map(str, stats['gtfs_files']))
        stats['gtfs_files_sizes'] = str(stats['gtfs_files_sizes'])
        writer.writerow(stats)

parser = argparse.ArgumentParser(description='Clean up the zip files by removing unwanted route types')
parser.add_argument('input', type=str, help='Path to folder or file containing the zip files')
parser.add_argument('--logging_file', type=str, default='gtfs_file_info.csv', help='Path to the logging CSV file')

args = parser.parse_args()

gtfs_files = [os.path.join(args.input, f) for f in os.listdir(args.input) if f.endswith('.zip')]
gtfs_files = interleave_round_robin(gtfs_files)
max_workers = os.cpu_count()
unique_files, valid_files, fixed_files = {}, [], []

tmp_dir = "tmp"
with ProcessPoolExecutor(max_workers=max_workers) as executor:
    futures = []
    for file_path in tqdm(gtfs_files, desc='Checking GTFS files', position=0, leave=True):
        result = quick_check(file_path)
        if not result:
            tqdm.write(f"Invalid zip file: {file_path}")
            fixed_file_path = attempt_fix_zip(file_path, tmp_dir)
            if fixed_file_path:
                tqdm.write(f"Fixed zip file: {file_path}")
                result = quick_check(fixed_file_path)
            if not result:
                tqdm.write(f"Failed to fix zip file: {file_path}")
                log_statistics(args.logging_file, {
                    'filename': file_path,
                    'duplicate_file': None,
                    'corrupted_file': True,
                    'date': int(time.time()),
                    'route_types': [],
                    'start_date': None,
                    'end_date': None,
                    'bbox': [],
                    'gtfs_files': [],
                    'gtfs_files_sizes': {}
                })
                continue
            if fixed_file_path:
                file_path = fixed_file_path
                fixed_files.append(file_path)
        if result not in unique_files:
            futures.append(executor.submit(analyse_zip_file, file_path))
            unique_files[result] = file_path
            valid_files.append(file_path)
        else:
            tqdm.write(f"Duplicate zip file: {file_path} and {unique_files[result]}")
            log_statistics(args.logging_file, {
                'filename': os.path.basename(file_path),
                'duplicate_file': os.path.basename(unique_files[result]),
                'corrupted_file': 'fixed' if file_path in fixed_files else False,
                'date': int(time.time()),
                'route_types': [],
                'start_date': None,
                'end_date': None,
                'bbox': [],
                'gtfs_files': [],
                'gtfs_files_sizes': {}
            })
    for future, file_path in tqdm(zip(futures, valid_files), desc='Processing GTFS files', position=0, leave=True, total=len(futures)):
        stats = future.result()
        stats.update({
            'filename': os.path.basename(file_path),
            'duplicate_file': None,
            'corrupted_file': 'fixed' if file_path in fixed_files else False,
            'date': int(time.time())
        })
        log_statistics(args.logging_file, stats)

with open('all_stations.txt', 'w') as f:
    for lat, lon in sorted(all_stations):
        f.write(f"{lat},{lon}\n")