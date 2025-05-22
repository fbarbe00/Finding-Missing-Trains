import xml.etree.ElementTree as ET
import folium
import argparse
from collections import defaultdict

ATTRIBUTES_TO_CHECK = [
    'electrified',
    'frequency',
    'gauge',
    'highspeed',
    'maxspeed',
    'railway',
    'railway:bidirectional',
    'railway:kvb',
    'railway:preferred_direction',
]

def parse_osm(osm_file):
    """Parses the OSM XML file and extracts nodes and railway tracks."""
    print("Parsing OSM file...")
    nodes = {}
    tracks = {}

    context = ET.iterparse(osm_file, events=("start", "end"))
    context = iter(context)
    event, root = next(context)

    for event, elem in context:
        if event == "end" and elem.tag == "node":
            node_id = elem.attrib['id']
            try:
                lat = float(elem.attrib['lat'])
                lon = float(elem.attrib['lon'])
                nodes[node_id] = (lat, lon)
            except ValueError:
                pass
            elem.clear()

        elif event == "end" and elem.tag == "way":
            tags = {tag.attrib['k']: tag.attrib['v'] for tag in elem.findall('tag') if tag.attrib['k'] in ATTRIBUTES_TO_CHECK}
            if tags.get('railway') == 'rail':
                track_id = elem.attrib['id']
                node_refs = [nd.attrib['ref'] for nd in elem.findall('nd')]
                if len(node_refs) >= 2:
                    tracks[track_id] = {
                        'nodes': node_refs,
                        'tags': tags,
                        'inconsistencies': []
                    }
            elem.clear()
        root.clear()
    return nodes, tracks

def build_endpoint_index(tracks):
    """Creates a mapping from node ID to list of track IDs connected at that node."""
    node_to_tracks = defaultdict(list)
    for track_id, track in tracks.items():
        endpoints = [track['nodes'][0], track['nodes'][-1]]
        for node in endpoints:
            node_to_tracks[node].append(track_id)
    return node_to_tracks

def check_inconsistencies(nodes, tracks, node_to_tracks):
    """Compares track attributes and directional consistency between connected tracks."""
    for track_id, track in tracks.items():
        track_tags = track['tags']
        endpoints = [(track['nodes'][0], 'start'), (track['nodes'][-1], 'end')]

        for node, position in endpoints:
            connected_tracks = node_to_tracks.get(node, [])
            for other_id in connected_tracks:
                if other_id == track_id:
                    continue
                other_track = tracks[other_id]
                other_tags = other_track['tags']

                # Compare shared attributes
                for attr in ATTRIBUTES_TO_CHECK:
                    if attr in track_tags and attr in other_tags and track_tags[attr] != other_tags[attr]:
                        msg = (f"Attribute '{attr}' mismatch at node {node} with track {other_id} "
                               f"(this: {track_tags[attr]}, other: {other_tags[attr]})")
                        if msg not in track['inconsistencies']:
                            track['inconsistencies'].append(msg)

                # Directionality check
                if 'railway:preferred_direction' in track_tags:
                    this_dir = track_tags['railway:preferred_direction'].lower()
                    other_dir = other_tags.get('railway:preferred_direction', '').lower()
                    if this_dir != other_dir and other_dir:
                        msg = (f"Direction mismatch at node {node} with track {other_id} "
                               f"(this: {this_dir}, other: {other_dir})")
                        if msg not in track['inconsistencies']:
                            track['inconsistencies'].append(msg)
    return tracks

def create_map(nodes, tracks, output_file):
    """Creates and saves a folium map visualizing tracks and inconsistencies."""
    all_coords = list(nodes.values())
    if not all_coords:
        print("No nodes found. Cannot create map.")
        return

    avg_lat = sum(lat for lat, lon in all_coords) / len(all_coords)
    avg_lon = sum(lon for lat, lon in all_coords) / len(all_coords)

    m = folium.Map(location=(avg_lat, avg_lon), zoom_start=6)

    for track_id, track in tracks.items():
        coords = [nodes[node_id] for node_id in track['nodes'] if node_id in nodes]
        if len(coords) < 2:
            continue

        if track['inconsistencies']:
            color = 'red'
            tooltip = f"Issues in track {track_id}: " + " | ".join(track['inconsistencies'])
        else:
            color = 'green'
            tooltip = f"Track {track_id}: no issues"

        folium.PolyLine(
            locations=coords,
            color=color,
            weight=4,
            tooltip=tooltip
        ).add_to(m)

    m.save(output_file)
    print(f"Map saved to {output_file}")
    return m

def print_inconsistencies(tracks):
    """Prints a summary of found inconsistencies."""
    print("\n=== Inconsistencies Found ===")
    for track_id, track in tracks.items():
        if track['inconsistencies']:
            print(f"Track {track_id}:")
            for issue in track['inconsistencies']:
                print(f"  - {issue}")

parser = argparse.ArgumentParser(description="Check OSM rail track inconsistencies.")
parser.add_argument("-i", "--input", required=True, help="Input OSM XML file")
parser.add_argument("-o", "--output", default="rail_inconsistencies_map.html", help="Output HTML map file")
args = parser.parse_args()

print(f"Processing OSM file: {args.input}")
nodes, tracks = parse_osm(args.input)
print(f"Parsed {len(nodes)} nodes and {len(tracks)} tracks.")

node_to_tracks = build_endpoint_index(tracks)
tracks = check_inconsistencies(nodes, tracks, node_to_tracks)
print_inconsistencies(tracks)
create_map(nodes, tracks, args.output)
