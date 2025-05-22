import argparse
import os
import zipfile
import csv
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from tempfile import NamedTemporaryFile
from io import TextIOWrapper
import shutil
from datetime import datetime
import subprocess

FILES_TO_INCLUDE = [
    "agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt",
    "calendar.txt", "calendar_dates.txt", "fare_attributes.txt", "fare_rules.txt",
    "timeframes.txt", "fare_media.txt", "fare_products.txt", "fare_leg_rules.txt",
    "fare_transfer_rules.txt", "areas.txt", "stop_areas.txt", "networks.txt",
    "route_networks.txt", "shapes.txt", "frequencies.txt", "transfers.txt",
    "pathways.txt", "levels.txt", "location_groups.txt", "location_group_stops.txt",
    "locations.geojson", "booking_rules.txt", "translations.txt", "feed_info.txt",
    "attributions.txt"
]
rail_services = [
    2,    # Rail (intercity or long-distance)
    100,  # Railway Service
    101,  # High Speed Rail Service
    102,  # Long Distance Trains
    103,  # Inter Regional Rail Service
    105,  # Sleeper Rail Service
    106,  # Regional Rail Service
    107,  # Tourist Railway Service
    109,  # Suburban Railway
    116,  # Rack and Pinion Railway
    117   # Additional Rail Service
]
rail_services = [2, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117]

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
    except Exception as e:
        return None

def interleave_round_robin(files):
    sorted_files = sorted(files, key=lambda x: os.stat(x).st_size)
    n = len(sorted_files)

    # Split into quartiles
    very_large = sorted_files[3 * n // 4:]
    large = sorted_files[n // 2:3 * n // 4]
    medium = sorted_files[n // 4:n // 2]
    small = sorted_files[:n // 4]

    interleaved = []
    while any([very_large, large, medium, small]):
        if very_large:
            interleaved.append(very_large.pop())
        if small:
            interleaved.append(small.pop())
        if large:
            interleaved.append(large.pop())
        if medium:
            interleaved.append(medium.pop())

    return interleaved

def filter_file_from_zip(zip_ref, input_file, output_folder, check_column_name, check_column_values, columns_to_keep=[], filter_function=None):
    """
    Filters a file inside the ZIP and stores the filtered content in output_file.
    It also returns the unique values of the columns_to_keep.
    If the output_file already exists, it reads and writes to the same file safely.
    """
    return_column_values = [set() for _ in columns_to_keep]
    corresponding_files = [f for f in zip_ref.namelist() if f == input_file or f.endswith('/' + input_file)]
    if not corresponding_files:
        return return_column_values

    output_file = os.path.join(output_folder, input_file)

    # Determine whether to read from an existing output file
    output_file_exists = os.path.exists(output_file)

    with (open(output_file, 'r', encoding='utf-8') if output_file_exists else zip_ref.open(corresponding_files[0], 'r')) as infile, \
        NamedTemporaryFile('w', delete=False, newline='\n', encoding='utf-8', dir=output_folder) as temp_outfile:

        with TextIOWrapper(infile, encoding='utf-8-sig') if not output_file_exists else infile as infile_wrapper:
            reader = csv.reader(infile_wrapper)
            writer = csv.writer(temp_outfile)

            try:
                header = [col.strip() for col in next(reader)]
            except UnicodeDecodeError:
                tqdm.write(f"Error reading file {input_file} ({zip_ref.filename}) - trying latin1 encoding")
                infile.seek(0)
                reader = csv.reader(TextIOWrapper(infile, encoding='latin1'))
                header = [col.strip() for col in next(reader)]
            writer.writerow(header)

            return_column_indices = [header.index(col) if col in header else None for col in columns_to_keep]

            try:
                column_idx = header.index(check_column_name)
            except ValueError:
                column_idx = None
                if not columns_to_keep:
                    writer.writerows(reader)
                    temp_outfile.close()
                    os.replace(temp_outfile.name, output_file)
                    return return_column_values

            if filter_function is None:
                filter_function = lambda value: check_column_values==True or value in check_column_values

            for row in reader:
                row = [value.strip() for value in row]
                if not row:
                    continue

                if column_idx is None or filter_function(row[column_idx]):
                    for i, idx in enumerate(return_column_indices):
                        if idx is not None and row[idx]:
                            return_column_values[i].add(row[idx])

                    writer.writerow(row)

    # Replace the original file with the temporary file
    os.replace(temp_outfile.name, output_file)

    return return_column_values


def filter_gtfs_by_route_type(zip_file_path, output_zip_file, route_types_to_keep, files_to_include, compresslevel=6, start_date=None, end_date=None):
    """
    Filters GTFS files inside a ZIP archive based on route_type.
    """
    tmp_folder = output_zip_file.split('.zip')[0]
    os.makedirs(tmp_folder, exist_ok=True)
    variables_to_keep = {}
    def get_variables_to_keep(key):
        if key.startswith('from_'):
            return variables_to_keep[key[5:]]
        if key.startswith('to_'):
            return variables_to_keep[key[3:]]
        if key == 'route_type':
            return [str(rt) for rt in route_types_to_keep]
        return variables_to_keep.get(key, [])
    
    file_dependencies = [
        ("routes.txt", "route_type", ["route_id", "agency_id"]),
        ("agency.txt", "agency_id"),
        ('trips.txt', 'route_id', ['trip_id', 'service_id']),
        ("stop_times.txt", "trip_id", ["stop_id"]),
        ("stops.txt", "stop_id"),
        ("calendar.txt", "service_id"),
        ("calendar_dates.txt", "service_id"),
        ("transfers.txt", "from_stop_id"),
        ("fare_rules.txt", "route_id"),
        ("timeframes.txt", "service_id"),
        ("route_networks.txt", "route_id"),
        ("frequencies.txt", "trip_id"),
        ("pathways.txt", "from_stop_id")
    ]
    if start_date and end_date:
        date_file_dependencies = [
            ("calendar_dates.txt", "date", ["service_id"], lambda value: start_date <= datetime.strptime(value, "%Y%m%d").date() <= end_date),
            ("calendar.txt", "start_date", ["service_id"], lambda value: datetime.strptime(value, "%Y%m%d").date() <= end_date, True),
            ("calendar.txt", "end_date", ["service_id"], lambda value: datetime.strptime(value, "%Y%m%d").date() >= start_date, True),
            # TODO: this doesn't fully filter, as it keeps rows that end a lot after the end date or start a lot before the start date
            # but it does remove the rows that are completely outside the range
            ("trips.txt", "service_id", ["service_id"]),
            ("trips.txt", "route_id", ["trip_id", "route_id"]),
            ("routes.txt", "route_id")
        ]
        trips_index = [i for i, file_data in enumerate(file_dependencies) if file_data[0] == 'trips.txt'][0]
        file_dependencies = file_dependencies[:trips_index] + date_file_dependencies + file_dependencies[trips_index+1:]
    
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        for file_data in file_dependencies:
            file = file_data[0]
            column = file_data[1]
            columns_to_keep = []
            filter_function = None
            add = False
            if len(file_data)>2:
                columns_to_keep = file_data[2]
            if len(file_data)>3:
                filter_function = file_data[3]
            if len(file_data)>4:
                add = file_data[4]
            if add:
                existing_values = variables_to_keep.get(columns_to_keep[0], [])
                new_values = filter_file_from_zip(zip_ref, file, tmp_folder, column, get_variables_to_keep(column), columns_to_keep, filter_function)
                for i, column in enumerate(columns_to_keep):
                    variables_to_keep[column] = set(existing_values).union(new_values[i])
            else:
                variables_to_keep.update(zip(columns_to_keep, filter_file_from_zip(zip_ref, file, tmp_folder, column, get_variables_to_keep(column), columns_to_keep, filter_function)))

            if 'route_id' in columns_to_keep and not variables_to_keep['route_id']:
                shutil.rmtree(tmp_folder)
                return False
            if 'agency_id' in columns_to_keep and not variables_to_keep['agency_id']:
                tqdm.write(f"Warn: No agency_id found in {zip_file_path}, keeping all agencies")
                variables_to_keep['agency_id'] = True

    # zip all files in tmp_folder (excluding the folder itself)
    with zipfile.ZipFile(output_zip_file, 'w') as zip_ref:
        for root, _, files in os.walk(tmp_folder):
            for file in files:
                if file in files_to_include:
                    zip_ref.write(os.path.join(root, file), file, compress_type=zipfile.ZIP_DEFLATED, compresslevel=compresslevel)
    shutil.rmtree(tmp_folder)
    return True

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

parser = argparse.ArgumentParser(description='Filter GTFS ZIP files by route type and date, remove duplicates, and fixing corrupted files.')
parser.add_argument('input', type=str, help='Folder or file path with GTFS ZIPs')
parser.add_argument('--output_folder', type=str, help='Destination folder for output ZIPs')
parser.add_argument('--route_types', type=int, nargs='+', default=rail_services, help='Route types to retain')
parser.add_argument('--files', type=str, nargs='+', default=FILES_TO_INCLUDE, help='Files to include in the output ZIP')
parser.add_argument('--compresslevel', type=int, default=9, choices=range(1, 10), help='ZIP compression level')
parser.add_argument('--startdate', type=str, help='Start date (YYYYMMDD)')
parser.add_argument('--enddate', type=str, help='End date (YYYYMMDD)')
args = parser.parse_args()

if os.path.isdir(args.input):
    if not args.output_folder:
        OUTPUT_DIR = args.input
    else:
        OUTPUT_DIR = args.output_folder
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    gtfs_files = [os.path.join(args.input, f) for f in os.listdir(args.input) if f.endswith('.zip')]
    gtfs_files = interleave_round_robin(gtfs_files)
else:
    gtfs_files = [args.input]
    OUTPUT_DIR = args.output_folder if args.output_folder else os.path.dirname(args.input)

start_date = datetime.strptime(args.startdate, "%Y%m%d").date() if args.startdate else None
end_date = datetime.strptime(args.enddate, "%Y%m%d").date() if args.enddate else None
# check if they are either both None or both not None
if (start_date is None) != (end_date is None):
    raise ValueError("Both start date and end date must be provided")
if start_date and end_date:
    if start_date > end_date:
        raise ValueError("Start date must be before end date")
    print(f"Filtering GTFS files from {start_date} to {end_date}")

print(f'Filtering the following route types: {args.route_types}')

max_workers = os.cpu_count()
unique_files = {}
valid_files = []
tmp_dir = "tmp"
with ProcessPoolExecutor(max_workers=max_workers) as executor:
    futures = []
    for file_path in tqdm(gtfs_files, desc='Checking GTFS files', position=0, leave=True):
        result = quick_check(file_path)
        if not result:
            tqdm.write(f"Invalid zip file: {file_path}")
            new_file_path = attempt_fix_zip(file_path, tmp_dir)
            if new_file_path:
                result = quick_check(new_file_path)
            if not result:
                tqdm.write(f"Failed to fix zip file: {file_path}")
                if OUTPUT_DIR == args.input:
                    os.remove(file_path)
                continue
            else:
                tqdm.write(f"Fixed zip file: {file_path}")
                file_path = new_file_path
        if result not in unique_files:
            futures.append(executor.submit(filter_gtfs_by_route_type, file_path, os.path.join(OUTPUT_DIR, os.path.basename(file_path)), args.route_types, args.files, args.compresslevel, start_date, end_date))
            unique_files[result] = file_path
            valid_files.append(file_path)
        else:
            tqdm.write(f"Duplicate zip file: {file_path} and {unique_files[result]}")
            if OUTPUT_DIR == args.input:
                os.remove(file_path)
    for future, file_path in tqdm(zip(futures, valid_files), desc='Processing GTFS files', position=0, leave=True, total=len(futures)):
        result = future.result()  # Wait for each future to complete
        if not result and OUTPUT_DIR == args.input:
            os.remove(os.path.join(OUTPUT_DIR, os.path.basename(file_path)))

shutil.rmtree(tmp_dir)