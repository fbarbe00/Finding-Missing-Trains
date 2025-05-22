# Gathering data
This directory provides different scripts to gather data from different data sources.

## GTFS feeds
The `gtfs` folder contains a collection of scripts for downloading GTFS data from different sources. Some require authentication tokens, which need to be put in a .env file in the same directory. The list of URLs for GTFS files can be downloaded from 3 sources: Mobility Database, Transit Land and Transitous. Additionally, an example of how a custom downloader can be made is shown with a script downloading data from a FTP server in Spain (`euskadi`).

The data can then be downloaded with `download_feeds.py`, which takes in a list of URLs and downloads them in parallel, using threads, and handling multiple types of errors. Files are then saved with a hash of the feed URL.

- `mobilitydata`: downloads the feed URLs from [Mobility Database](https://mobilitydatabase.org/). [Requires a MD_REFRESH_TOKEN](https://mobilitydata.github.io/mobility-feed-api/SwaggerUI/index.html).
- `transitland`: downloads the feed URLs from [Transit Land](https://www.transit.land/). [Requires a TRANSITLAND_API_KEY](https://www.transit.land/documentation#signing-up-for-an-api-key).
- `transitous`: extracts the feed URLs from [Transitous](https://github.com/public-transport/transitous). The repository needs to first be cloned to extract the data.

## Infrastructure data
Infrastructure data, specifically data for stations and tracks, can be taken from 4 sources: the [Trainline stations dataset](https://github.com/trainline-eu/stations/), [OpenStreetMap](https://www.openstreetmap.org/) (OSM), [Wikidata](https://www.wikidata.org/), and [RINF](https://data-interop.era.europa.eu/).

- Trainline data can be taken from their GitHub repository. It is important that all subsequent experiments are run on the same version of the trainline database, as the index in the row is used to identify a station, which can change with new versions.
- `download_wikidata_stations.py`: downloads the relevant information for train stations from Wikidata, attempting to filter historical and disused stations.
- `download_stations_osm.py`: downloads active train stations from OSM using Overpass (more info below)
- `download_osm_around_stations.py`: downloads road data around GTFS stations using Overpass.
- `filter_osm_around_stations.py`: an alternative (faster) approach that filters an existing OSM PBF file around GTFS stations.
- `download_rinf.py`: downloads RINF data given a query (`rinf_tracks_query.sparql` is provided for tracks)

### OSM data formats
OSM data can be downloaded either from Overpass (for small queries) or [GeoFabrik](https://download.geofabrik.de/europe.html) (entire regions). Files from GeoFabrik are downloaded in PBF format, which drastically reduces the size.

To process large PBF files, the [Osmium tool](https://osmcode.org/osmium-tool/) was used. For only keeping roads and transport data:
```bash
osmium tags-filter europe-latest.osm.pbf \\
w/highway w/public_transport=platform w/railway=platform w/park_ride r/type=restriction \\
-o europe-filtered.osm.pbf
```

The [PyOsmium](https://osmcode.org/pyosmium/) python package was also used, although this is much slower than running `osmium` directly.

