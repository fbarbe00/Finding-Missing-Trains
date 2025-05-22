import time
from tqdm import tqdm


def run_r5(osm_path, gtfs_files, point_a, point_b, date):
    import geopandas as gpd
    from shapely.geometry import Point
    result = {}

    tqdm.write("📦 Importing r5py...")
    start_import = time.time()
    import r5py
    result["import_time"] = time.time() - start_import

    # Step 1: Load transport network
    tqdm.write("🗺️ Building transport network...")
    start_load = time.time()
    network = r5py.TransportNetwork(osm_path, gtfs_files)
    result["network_load_time"] = time.time() - start_load

    # Step 2: Create origin and destination
    tqdm.write("📍 Creating origin/destination points")
    origins = gpd.GeoDataFrame({
        "id": [0],
        "geometry": [Point(point_a[1], point_a[0])]
    }, crs="EPSG:4326")

    destinations = gpd.GeoDataFrame({
        "id": [1],
        "geometry": [Point(point_b[1], point_b[0])]
    }, crs="EPSG:4326")

    # Step 3: Compute route
    tqdm.write("🚦 Computing route with DetailedItineraries...")
    total_query_time = 0
    total_runs = 50
    for _ in range(total_runs):
        start_query = time.time()
        itineraries = r5py.DetailedItineraries(
            network,
            origins=origins,
            destinations=destinations,
            snap_to_network=True,
            departure=date,
        )
        total_query_time += time.time() - start_query
    result["query_time"] = total_query_time / total_runs
    # result["itineraries"] = itineraries.to_dict()

    return result
