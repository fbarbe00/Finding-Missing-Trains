# Finding the missing trains: analysis of the European Railway Passenger Network using open data

This repository is the result of several months of work on my Master's thesis in Data Science for Decision Making at Maastricht University, and contributes to the assessment for my final grade.

These tools are designed to gather data about the railway passenger network in Europe, such as track data from OpenStreetMap (OSM) and GTFS (timetable) files, visualise the quality of the data and compare it across sources, and provide metrics and visualisations.
They are particularly useful to anyone starting doing research with transport data, as multiple tools have been developed to gather and clean GTFS files efficiently, and different alternatives have been explored. It also explores issues in different sources, such as incorrect UIC codes in OSM data.

All code can run on consumer hardware, with at least 6GB of RAM. 

Feel free to reach out regarding questions on the code. For more informations about the results of my thesis, you can read [my blog](https://fabiobarbero.eu/tags/on-trains/).

## Directory structure
The directories in this repository follow the layout of my Master Thesis. Each directory contains its own README file, explaining further how to get it running and limitations.

No data is provided in this repository, as it is subject to different licenses, and would not be up-to-date. However, because all the data is public and scripts to gather and process the data are provided, all experiments can easily be replicated.

- `data`: 
    - `gathering_data`: scripts to download data from different sources (OSM, Wikidata, GTFS, ...)
    - `infrastructure_analysis`: scripts to analyse infrastructure data on stations and tracks.
    - `gtfs_analysis`: scripts to analyse the GTFS files
- `metrics`:
    - `trip_planners`: comparison of different trip planners
    - `commuting_time`: computing commuting times for different modes (train, car, airplane)
    - `visualisations`: isochrone plots and distorted maps
    - `population_metrics`: computing connectivity metrics from commuting times, considering the population of each geographical area.

## Other comments
Because this code is used as an assesment for my grade and is directly linked to my master thesis work, I do not accept Pull Requests. However, I welcome anyone to take my code and start another repository. The code is licensed under a MIT license.

Some code containted in this repository has been generated using LLM assistance, such as Github Copilot. However, I declare that I have only used it for minor code speed-ups, have always reviewed the results of the output, and have manually typed most of the code. Therefore, I consider the code to be mine. An automatic python formatter was used on all python files to ensure PEP-8 readability.