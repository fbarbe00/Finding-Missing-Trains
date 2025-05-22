import os
import time
import json
import traceback
from pathlib import Path
from tqdm import tqdm
import argparse
from datetime import datetime

# Import runners
from runners.motis_runner import run_motis
from runners.r5_runner import run_r5
from runners.otp_runner import run_otp


def get_default_datasets(base_path: Path):
    """Define default datasets with paths and metadata."""
    return {
        "helsinki": {
            "osm": base_path / "osm/raw/kantakaupunki.osm.pbf",
            "gtfs": base_path / "gtfs/sample/helsinki_gtfs.zip",
            "point_a": (60.1695, 24.9354),
            "point_b": (60.1733, 24.9402),
            "time": datetime(2022, 2, 22, 8, 30),
        },
        "berlin": {
            "osm": base_path / "osm/filtered/berlin-filtered.osm.pbf",
            "gtfs": base_path / "gtfs/sample/vbb-gtfs.zip",
            "point_a": (52.5200, 13.4050),
            "point_b": (52.5309, 13.3847),
            "time": datetime(2025, 4, 7, 8, 0),
        },
        "aachen": {
            "osm": base_path / "osm/filtered/aachen-filtered.osm.pbf",
            "gtfs": base_path / "gtfs/sample/AVV_GTFS_Masten_mit_SPNV.zip",
            "point_a": (50.7766, 6.0834),
            "point_b": (50.7590, 6.1045),
            "time": datetime(2025, 4, 4, 8, 0),
        },
        "belgium": {
            "osm": base_path / "osm/filtered/belgium-filtered.osm.pbf",
            "gtfs": base_path / "gtfs/sample/belgium/",
            "point_a": (50.62455, 5.566698),
            "point_b": (50.85966, 4.36085),
            "time": datetime(2025, 3, 21, 8, 0),
        },
        "europe": {
            "osm": base_path / "osm/filtered/europe_filtered_locations.osm.pbf",
            "gtfs": base_path / "gtfs/downloaded_feeds_filtered/",
            "point_a": (48.8566, 2.3522),
            "point_b": (52.5200, 13.4050),
            "time": datetime(2025, 3, 20, 8, 0),
        },
    }


def run_safe(label, func, *args, **kwargs):
    """Wrapper to catch errors and measure execution time."""
    try:
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        return {"status": "success", "duration": end - start, "result": result}
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "trace": traceback.format_exc()
        }


def load_checkpoint(checkpoint_file: Path):
    """Load checkpoint if exists."""
    if checkpoint_file.exists():
        return json.loads(checkpoint_file.read_text())
    return {}


def save_checkpoint(state, checkpoint_file: Path):
    """Save current experiment checkpoint."""
    checkpoint_file.write_text(json.dumps(state, indent=2))


def run_tool(tool, info):
    """Dispatch function to the appropriate routing tool runner."""
    # Normalize GTFS input to list of zip files
    if isinstance(info["gtfs"], (str, Path)):
        if os.path.isdir(info["gtfs"]):
            info["gtfs"] = [
                str(os.path.join(info["gtfs"], file))
                for file in os.listdir(info["gtfs"])
                if file.endswith(".zip")
            ]
        else:
            info["gtfs"] = [str(info["gtfs"])]

    if tool == "motis":
        return run_motis(info["osm"], info["gtfs"], info["point_a"], info["point_b"], info.get("time"))
    elif tool == "r5py":
        return run_r5(info["osm"], info["gtfs"], info["point_a"], info["point_b"], info.get("time"))
    elif tool == "otp":
        return run_otp(info["osm"], info["gtfs"], info["point_a"], info["point_b"], info.get("time"))
    else:
        raise ValueError(f"Unknown tool: {tool}")


def run_experiments(dataset_base_path: Path, results_dir: Path):
    """Main loop to run experiments on datasets with available tools."""
    datasets = get_default_datasets(dataset_base_path)
    os.makedirs(results_dir, exist_ok=True)
    checkpoint_file = results_dir / "checkpoints.json"
    checkpoints = load_checkpoint(checkpoint_file)

    dataset_bar = tqdm(datasets.items(), desc="Datasets")

    for dataset_name, info in dataset_bar:
        dataset_bar.set_description(f"Dataset: {dataset_name}")
        checkpoints.setdefault(dataset_name, {})

        for tool in ["motis", "r5py", "otp"]:
            if checkpoints[dataset_name].get(tool) == "done":
                tqdm.write(f"✅ {tool} already done for {dataset_name}")
                continue

            tqdm.write(f"▶ Running {tool} on {dataset_name}")

            result = run_safe(f"{tool}-{dataset_name}", run_tool, tool, info)
            log_file = results_dir / f"{tool}_{dataset_name}.json"
            log_file.write_text(json.dumps(result, indent=2))

            if result["status"] == "success":
                checkpoints[dataset_name][tool] = "done"
                save_checkpoint(checkpoints, checkpoint_file)
            else:
                tqdm.write(f"❌ Failed {tool} on {dataset_name}: {result['error']}")




parser = argparse.ArgumentParser(description="Run routing experiments using MOTIS, R5, and OTP.")
parser.add_argument(
    "--data-root", type=Path, default=Path("../../../data"),
    help="Base path to data directory containing osm/ and gtfs/ subfolders"
)
parser.add_argument(
    "--results-dir", type=Path, default=Path("./experiment_logs"),
    help="Directory to store result logs and checkpoints"
)
args = parser.parse_args()
run_experiments(args.data_root, args.results_dir)
