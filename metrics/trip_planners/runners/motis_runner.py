import subprocess
import time
import requests
from tqdm import tqdm


def start_motis_server():
    return subprocess.Popen(["./motis", "server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def wait_for_motis():
    # Wait for server to be available
    import socket
    for _ in range(60):
        try:
            s = socket.create_connection(("localhost", 8080), timeout=2)
            s.close()
            return True
        except Exception:
            time.sleep(1)
    raise RuntimeError("MOTIS server did not start in time")


def stop_process(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def run_motis(osm_path, gtfs_files, point_a, point_b, date):
    result = {}

    # Step 1: Update config
    gtfs_str = " ".join(gtfs_files)
    tqdm.write(f"⚙️ Updating config with OSM + GTFS")
    subprocess.run(["./motis", "config", osm_path, *gtfs_files], check=True)

    # Step 2: Import data
    tqdm.write(f"⬇️ Importing data...")
    start_import = time.time()
    subprocess.run(["./motis", "import"], check=True)
    result["import_time"] = time.time() - start_import

    # Step 3: Start server
    tqdm.write(f"🚀 Starting MOTIS server...")
    server_proc = start_motis_server()
    try:
        wait_for_motis()
        tqdm.write("✅ MOTIS server is running")

        # Step 4: Query route
        from_lat, from_lon = point_a
        to_lat, to_lon = point_b
        query_time = date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        query = {
            "fromPlace": f"{from_lat},{from_lon},0",
            "toPlace": f"{to_lat},{to_lon},0",
            "time": query_time
        }

        total_query_time = 0
        total_runs = 50
        for _ in range(total_runs):
            start_query = time.time()
            response = requests.get("http://localhost:8080/api/v1/plan", params=query)
            total_query_time += time.time() - start_query

            if response.status_code != 200:
                raise Exception(f"MOTIS query failed: {response.status_code}, {response.text}")

        result["query_time"] = total_query_time / total_runs

        result["response"] = response.json()
    except Exception as e:
        tqdm.write(f"Error: {e}")
    finally:
        stop_process(server_proc)
        tqdm.write("🛑 MOTIS server stopped")

    return result
