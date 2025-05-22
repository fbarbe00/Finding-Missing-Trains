# Infrastructure data analysis
This folder focuses on analysing station and track data from OSM, Wikidata, Trainline and RINF.

- `rinf_osm_differences.py`: matches rinf track data with OSM track data, and focuses on inconsistencies with RINF data
- `compare_uic_stations.py`: matches stations data from OSM, Wikidata and Trainline, compares the UIC attribute between the different datasets and with the Deutsche Bahn internal ID, and plots the results.
- `osm_track_inconsistencies.py`: looks for neighbouring tracks in osm data that have conflicting attributes, such as opposite track direction
- `contribute_osm_incorrect_uic.py`: finds incorrectly labelled UIC codes in OSM stations with high name similarity, and generates a file to contribute back to OSM.
- `build_track_graph.py`: builds a network where nodes are stops and edges are direct connections between two stops for multiple OSM files.
- `track_frequency_osm_gtfs.py`: builds a network where nodes are stops and edges are direct connections between two stops for GTFS data, and compares it with the OSM graph.