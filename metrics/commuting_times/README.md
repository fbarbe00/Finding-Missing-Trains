# Computing Commuting times
This folder contains scripts for computing commuting times for rail, car, and plane. In particular, it uses [MOTIS](https://github.com/motis-project/motis/) for computing the commute time between two stations given GTFS files, [OSRM](https://github.com/Project-OSRM/osrm-backend) for the commuting time by car and theoretical train time, and the [FlightStats API](https://developer.flightstats.com) for flight data.

The theoretical train time is calculated by looking at the rail tracks and attributes such as speed, figuring out how far a train could go in a limited amount of time if it could use the whole infrastructure. It uses a custom lua profile for using rail tracks instead of roads in OSRM. This custom lua profile was kindly provided by Wassil Janssen.

## OSRM
Information on how to run OSRM can be found [here](https://project-osrm.org/docs/v5.24.0/api/#).

For the custom rail planner, the OSM map first needs to be filtered (with a tool like `osmium`) to only keep `w/railway=rail`.
Running it with docker can be done with:
```bash
docker run -t -v "$(pwd)/data/osm/:/data" -v "$(pwd)/scripts/profiles/:/profiles" osrm/osrm-backend osrm-extract -p /profiles/$2.lua /data/$3/osrm.osm.pbf -t $1
docker run -t -v "$(pwd)/data/osm/:/data" osrm/osrm-backend osrm-contract /data/$1/osrm.osrm
docker run -t -p 5000:5000 -v "$(pwd)/data/osm/:/data" osrm/osrm-backend osrm-routed /data/$1/osrm.osrm
```
making sure to change the paths with the location of your data. The necessary files can be found in the `osrm_train` folder. The [OSRM frontend](https://github.com/fossgis-routing-server/osrm-frontend) (a project to which I also contributed) can be used to query the router.

Alternative, https://signal.eu.org/ can also be used as an alternative backend.

The script `osrm_speed.py` measures the speed of different OSRM servers, and distance can be computed using ``compute_all_distances_osrm.py`.

## Computing travel times
Each script outputs a csv for each start station, with columns `destination_station_id, duration_seconds, start_time_isoformat, transfers`.
The `travel_times_flight.py` requires a FlightStats API key, which can be requested [here](https://developer.flightstats.com/getting-started/).