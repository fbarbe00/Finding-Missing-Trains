# Population metrics

This script cleans up population data taken from [OECD](https://data-explorer.oecd.org/vis?fs[0]=Topic%2C0%7CRegional%252C%20rural%20and%20urban%20development%23GEO%23&pg=40&fc=Topic&bp=true&snb=117&df[ds]=dsDisseminateFinalDMZ&df[id]=DSD_REG_DEMO%40DF_POP_5Y&df[ag]=OECD.CFE.EDS&df[vs]=2.0&dq=A.......&to[TIME_PERIOD]=false&vw=ov&pd=%2C&ly[cl]=TIME_PERIOD&ly[rs]=COMBINED_MEASURE%2CCOMBINED_UNIT_MEASURE%2CSEX&ly[rw]=COMBINED_REF_AREA), and provides metrics for each station previously computed (see `commuting_times` for scripts to compute the data).

`filter_population_data.py` filters the downloaded file from OECD, gives it better columns names, and queries wikidata to retrieve the coordinates of the cities and other attributes (which are not included in the dataset).

The `population_metrics_stations.ipynb` then provides a notebook to compute different scores based on the commuting times and the population data.