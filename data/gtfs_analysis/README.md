# GTFS files analysis
This directory focuses on processing and cleaning GTFS files, and extracting relevant metrics indicating their completeness, such as comparison with stations from other sources like OSM or Trainline.

`clean_gtfs_files.py` provides an optimised way to clean up a list of GTFS files, by identifying duplicates, filtering by route types and date, and removing unnecessary files. It uses threading, and is faster than other tested gtfs cleaning tools. Files are sorted intelligently using a Round Robin based on their size.

`get_statistics_gtfs.py` uses the same logic as `clean_gtfs_files.py`, but instead of outputting new zip files it collects statistics on the duplicate and other information contained in the GTFS files.

`GTFS_EDA.ipynb` is a Jupyter Notebook exploring the collected GTFS data, looking at time ranges, comparing different sources, and incorrect locations.

`gtfs_to_graph.ipynb` creates a networkx graph connecting stops based on the timetable and saves it to a file.