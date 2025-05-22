# Trip planners

`compare_trip_planners.py` runs experiments comparing [MOTIS](https://github.com/motis-project/motis/), [OpenTripPlanner](https://github.com/opentripplanner/OpenTripPlanner) and [R5py](https://github.com/r5py/r5py).

The MOTIS binary can be downloaded from the official repository, or using the `update_motis.sh` script. The OpenTripPlanner jar file can be downloaded [from the release page](https://github.com/opentripplanner/OpenTripPlanner/releases), and R5py using `pip`.

The datasets used are the following:
- `helsinki`: Taken from [r5py's sample datasets](https://r5py.readthedocs.io/stable/user-guide/installation/installation.html#sample-data-sets)
- `berlin`: Using instructions from [OpenTripPlanner's quick start](https://docs.opentripplanner.org/en/latest/Container-Image/#quick-start)
- `aachen`: From [MOTIS's quick start](https://github.com/motis-project/motis/?tab=readme-ov-file#quick-start)
- `belgium`: data from Geofabrik, filtered file of the SNCB
- `europe`: data from Geofabrik, filtered around stations location, using all collected GTFS files