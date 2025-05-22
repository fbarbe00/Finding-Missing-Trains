import requests
import time
import random
from tqdm import tqdm

NUM_RUNS = 1000
# osrm_train_backend = "https://signal.eu.org/osm/"
osrm_train_backend = "http://localhost:5000"
session = requests.Session()

start_time = time.time()
succesful_runs = 0

for i in tqdm(range(NUM_RUNS), desc="OSRM Speed Test", unit="run"):
    start_lon, start_lat = random.uniform(-10, 10), random.uniform(45, 60)
    end_lon, end_lat = random.uniform(-10, 10), random.uniform(45, 60)
    url = f"{osrm_train_backend}/route/v1/driving/{start_lon},{start_lat};{end_lon},{end_lat}?overview=false"
    response = session.get(url)
    data = response.json()
    if data["code"] != "Ok":
        tqdm.write(f"OSRM error: {data.get('message', data['code'])}")
    else:
        succesful_runs += 1

total_time = time.time() - start_time
avg_time = total_time / NUM_RUNS
print(f"{osrm_train_backend} - Average time: {avg_time:.2f}s, Successful runs: {succesful_runs}/{NUM_RUNS}")
# print query/s
print(f"Query/s: {NUM_RUNS / total_time:.2f}")