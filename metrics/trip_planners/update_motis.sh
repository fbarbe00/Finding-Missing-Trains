#!/bin/bash

TARGET="linux-amd64"
# Check for the --all flag
if [[ "$1" == "--all" ]]; then
    rm -r data config.yml
fi

# Remove other files regardless of the flag
rm -r motis tiles-profiles ui motis-${TARGET}.tar.bz2

wget https://github.com/motis-project/motis/releases/latest/download/motis-${TARGET}.tar.bz2
tar xf motis-${TARGET}.tar.bz2
rm motis-${TARGET}.tar.bz2

