import os
import subprocess
import math
import argparse
import osmium
from scipy.spatial import cKDTree
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_osm_object_estimate(file_path):
    cmd = ["osmium", "fileinfo", "--no-progress", "-e", file_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        for line in result.stdout.splitlines():
            if line.strip().startswith("Nodes:"):
                return int(line.split(":")[1].strip().replace(',', ''))
        return 0
    except (subprocess.CalledProcessError, Exception) as e:
        logger.error(f"Error estimating object count: {e}")
        return 0

def perform_initial_tag_filter(input_file, output_file, tags_to_filter):
    logger.info(f"Applying initial tag filter on {input_file}")
    cmd = ["osmium", "tags-filter", input_file] + tags_to_filter + ["-o", output_file]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Tag filtering failed: {e.stderr.decode()}")
        return False

def split_osm_file(osm_file_path, bounding_box, num_parts, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    min_lon, min_lat, max_lon, max_lat = bounding_box
    grid_dimension = int(math.sqrt(num_parts))
    if grid_dimension * grid_dimension != num_parts:
        logger.warning(f"Adjusting num_parts to {grid_dimension*grid_dimension} for perfect square grid.")
        num_parts = grid_dimension * grid_dimension

    lon_step = (max_lon - min_lon) / grid_dimension
    lat_step = (max_lat - min_lat) / grid_dimension

    split_files = []
    for row in range(grid_dimension):
        for col in range(grid_dimension):
            part_idx = row * grid_dimension + col + 1
            part_min_lon = min_lon + col * lon_step
            part_max_lon = part_min_lon + lon_step
            part_min_lat = min_lat + row * lat_step
            part_max_lat = part_min_lat + lat_step

            extract_file_path = os.path.join(output_dir, f"part_{part_idx:04d}.osm.pbf")
            bbox_str = f"{part_min_lon},{part_min_lat},{part_max_lon},{part_max_lat}"
            cmd = ["osmium", "extract", "--bbox", bbox_str, "-o", extract_file_path, osm_file_path]
            logger.info(f"Extracting part {part_idx}/{num_parts}: {bbox_str}")
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            split_files.append(extract_file_path)
    return split_files

def filter_osm_part_by_locations(input_part_file, output_filtered_file,
                                  all_locations_array, radius_km,
                                  estimated_objects_in_part, query_ball_workers,
                                  part_bounding_box):
    min_lon, min_lat, max_lon, max_lat = part_bounding_box
    radius_deg_approx = radius_km / 111.32
    buffered_min_lon, buffered_min_lat = min_lon - radius_deg_approx, min_lat - radius_deg_approx
    buffered_max_lon, buffered_max_lat = max_lon + radius_deg_approx, max_lat + radius_deg_approx

    relevant_locations_array = np.array([
        loc for loc in all_locations_array
        if buffered_min_lat <= loc[0] <= buffered_max_lat and
           buffered_min_lon <= loc[1] <= buffered_max_lon
    ])

    if relevant_locations_array.size == 0:
        logger.info(f"No relevant locations for {os.path.basename(input_part_file)}. Creating empty file.")
        osmium.SimpleWriter(output_filtered_file).close()
        return output_filtered_file

    kdtree = cKDTree(relevant_locations_array)
    writer = osmium.BackReferenceWriter(output_filtered_file, ref_src=input_part_file, overwrite=True)
    
    with writer:
        for obj in tqdm(osmium.FileProcessor(input_part_file), total=estimated_objects_in_part,
                        unit="objects", miniters=5000, desc=f"Filtering {os.path.basename(input_part_file)}"):
            if isinstance(obj, osmium.osm.Node):
                if kdtree.query_ball_point((obj.location.lat, obj.location.lon), r=radius_km / 111.32,
                                             workers=query_ball_workers, return_length=True) > 0:
                    writer.add(obj)
    logger.info(f"Completed filtering {os.path.basename(input_part_file)}.")
    return output_filtered_file

def merge_osm_files(osm_file_paths, merged_output_file_path):
    if not osm_file_paths:
        logger.warning("No files to merge. Creating an empty output file.")
        osmium.SimpleWriter(merged_output_file_path).close()
        return merged_output_file_path

    logger.info(f"Merging {len(osm_file_paths)} files into: {merged_output_file_path}")
    cmd = ["osmium", "merge", "-o", merged_output_file_path] + osm_file_paths
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logger.info(f"Merged output saved to: {merged_output_file_path}")
    return merged_output_file_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process OSM data: optional tag filter, split, location-based filter, merge.")
    parser.add_argument("osm_input_file", help="Path to input .osm.pbf file")
    parser.add_argument("merged_output_file", help="Path to final merged output .osm.pbf file")
    parser.add_argument("--num-parts", type=int, default=4, help="Number of parts to split (perfect square)")
    parser.add_argument("--working-dir", default="osm_working_dir", help="Base directory for intermediate files")
    parser.add_argument("--initial-filter-tags", nargs='+', help="Osmium tags-filter expressions (e.g., 'w/highway')")
    parser.add_argument("--locations-file", type=str, required=True, help="Path to file with locations (lat,lon per line)")
    parser.add_argument("--radius", type=float, default=3, help="Radius in kilometers for filtering")
    parser.add_argument("--bounding-box", "-b", type=float, nargs=4, help="Overall bounding box (min_lon,min_lat,max_lon,max_lat)")
    parser.add_argument("--filter-workers", type=int, default=1, help="Parallel processes for filtering parts")
    parser.add_argument("--query-ball-workers", type=int, default=-1, help="Workers for KDTree queries (-1 for all cores)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files/directories")
    args = parser.parse_args()

    if os.path.exists(args.working_dir) and args.force: shutil.rmtree(args.working_dir)
    os.makedirs(args.working_dir, exist_ok=True)
    split_output_dir = os.path.join(args.working_dir, "split_parts")
    filtered_output_dir = os.path.join(args.working_dir, "filtered_parts")
    os.makedirs(split_output_dir, exist_ok=True)
    os.makedirs(filtered_output_dir, exist_ok=True)

    if os.path.exists(args.merged_output_file):
        if args.force: os.remove(args.merged_output_file)
        else:
            logger.error(f"Output file {args.merged_output_file} exists. Use --force.")
            exit(1)

    try:
        with open(args.locations_file, 'r') as f:
            locations_array = np.array([tuple(map(float, line.strip().split(","))) for line in f if line.strip()])
        if locations_array.size == 0:
            logger.error("Locations file is empty.")
            exit(1)
        logger.info(f"Loaded {len(locations_array)} locations.")
    except Exception as e:
        logger.error(f"Error reading locations file: {e}")
        exit(1)

    overall_bounding_box = args.bounding_box
    if not overall_bounding_box:
        min_lat, min_lon = np.min(locations_array, axis=0)
        max_lat, max_lon = np.max(locations_array, axis=0)
        radius_deg_buffer = args.radius / 111.32
        overall_bounding_box = (min_lon - radius_deg_buffer, min_lat - radius_deg_buffer,
                                max_lon + radius_deg_buffer, max_lat + radius_deg_buffer)
        logger.info(f"Derived overall bounding box: {overall_bounding_box}")
    else:
        logger.info(f"Using provided bounding box: {overall_bounding_box}")

    current_osm_input = args.osm_input_file
    if args.initial_filter_tags:
        pre_filtered_osm_file = os.path.join(args.working_dir, "initial_filtered.osm.pbf")
        if not perform_initial_tag_filter(args.osm_input_file, pre_filtered_osm_file, args.initial_filter_tags):
            exit(1)
        current_osm_input = pre_filtered_osm_file

    logger.info("Splitting input file...")
    split_files = split_osm_file(current_osm_input, tuple(overall_bounding_box), args.num_parts, split_output_dir)

    estimated_total_objects = get_osm_object_estimate(current_osm_input)
    per_file_estimate = max(1, estimated_total_objects // len(split_files)) if estimated_total_objects else 0

    logger.info(f"Filtering split files with {args.filter_workers} processes...")
    filter_tasks = []
    with ProcessPoolExecutor(max_workers=args.filter_workers) as executor:
        for idx, split_file_path in enumerate(split_files):
            filtered_file_path = os.path.join(filtered_output_dir, f"filtered_{os.path.basename(split_file_path)}")
            
            grid_dimension = int(math.sqrt(args.num_parts))
            row, col = idx // grid_dimension, idx % grid_dimension
            min_lon, min_lat, max_lon, max_lat = overall_bounding_box
            lon_step, lat_step = (max_lon - min_lon) / grid_dimension, (max_lat - min_lat) / grid_dimension
            current_part_bbox = (min_lon + col * lon_step, min_lat + row * lat_step,
                                 min_lon + (col + 1) * lon_step, min_lat + (row + 1) * lat_step)

            future = executor.submit(filter_osm_part_by_locations, split_file_path, filtered_file_path,
                                    locations_array, args.radius, per_file_estimate,
                                    args.query_ball_workers, current_part_bbox)
            filter_tasks.append(future)

        final_filtered_parts = []
        for future in as_completed(filter_tasks):
            try: final_filtered_parts.append(future.result())
            except Exception as e:
                logger.error(f"Error during filtering: {e}")
                raise

    logger.info("Merging filtered files...")
    merge_osm_files(final_filtered_parts, args.merged_output_file)
    logger.info("Processing complete!")