import subprocess
import time
import requests
import os
import shutil
from pathlib import Path
from tqdm import tqdm

def start_otp(folder_path, filename="otp-shaded-2.7.0.jar"):
    """Starts the OTP server in a subprocess."""
    return subprocess.Popen(
        ["java", "-Xmx4G", "-jar", filename, "--build", "--serve", str(folder_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )


def wait_for_otp(proc, timeout=5*60*60):
    """Waits for OTP to emit the ready message, or errors out if the process dies or times out."""
    start = time.time()
    while True:
        if proc.poll() is not None:
            raise RuntimeError("OTP process terminated unexpectedly during startup.")

        line = proc.stdout.readline()
        if line == "":
            # Avoid tight spin if no output yet
            time.sleep(0.5)
            continue

        tqdm.write(f"[OTP] {line.strip()}")

        if "Grizzly server running." in line:
            return True

        if time.time() - start > timeout:
            raise TimeoutError("Timed out waiting for OTP to start.")


def stop_process(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def run_otp(osm_path, gtfs_files, point_a, point_b, date):
    result = {}

    # Prepare temporary folder
    tmp_dir = Path("./otp_tmp")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)

    # Copy data
    tqdm.write(f"📁 Copying data to temp folder")
    shutil.copy(osm_path, tmp_dir / Path(osm_path).name)
    for gtfs in gtfs_files:
        if os.path.isdir(gtfs):
            for file in os.listdir(gtfs):
                if file.endswith(".zip"):
                    shutil.copy(os.path.join(gtfs, file), tmp_dir)
        else:
            shutil.copy(gtfs, tmp_dir)

    # Step 1: Start OTP
    tqdm.write("🚀 Starting OTP server...")
    start_time = time.time()
    proc = start_otp(tmp_dir)
    try:
        wait_for_otp(proc)
        result["startup_time"] = time.time() - start_time

        # Step 2: Send query
        from_lat, from_lon = point_a
        to_lat, to_lon = point_b

        payload = {
            "query": "query trip($from: Location!, $to: Location!) {\n  trip(from: $from, to: $to) {\n    tripPatterns {\n      duration\n    }\n  }\n}",
            "variables": {
                "from": {"coordinates": {"latitude": from_lat, "longitude": from_lon}},
                "to": {"coordinates": {"latitude": to_lat, "longitude": to_lon}},
                "dateTime": date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            },
            "operationName": "trip"
        }

        headers = {
            "content-type": "application/json"
        }

        tqdm.write("🛰️ Querying OTP route API 5 times...")
        query_times = []
        total_runs = 50

        for i in range(total_runs):
            tqdm.write(f"🔄 Query attempt {i + 1}...")
            start_query = time.time()
            resp = requests.post("http://localhost:8080/otp/transmodel/v3", headers=headers, json=payload)
            query_time = time.time() - start_query
            query_times.append(query_time)

            if resp.status_code != 200:
                raise Exception(f"OTP query failed on attempt {i + 1}: {resp.status_code}, {resp.text}")

        result["query_time"] = sum(query_times) / len(query_times)
        result["response"] = resp.json()

    finally:
        stop_process(proc)
        tqdm.write("🛑 OTP server stopped")
        shutil.rmtree(tmp_dir)

    return result
