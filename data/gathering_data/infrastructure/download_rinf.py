import argparse
import requests

def download_sparql_results(query_file: str, output_file: str, endpoint_url: str = "https://data-interop.era.europa.eu/api/sparql"):
    with open(query_file, "r", encoding="utf-8") as f:
        query = f.read()

    params = {"query": query, "format": "csv"}
    headers = {"Accept": "text/csv"}

    response = requests.get(endpoint_url, params=params, headers=headers, stream=True)
    response.raise_for_status()

    with open(output_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    print(f"Saved to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Output CSV file path")
    parser.add_argument("--query", default="rinf_tracks_query.sparql", help="Path to SPARQL query file (default: rinf_tracks_query.sparql)")
    args = parser.parse_args()

    download_sparql_results(args.query, args.out)
