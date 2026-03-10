#!/usr/bin/python3

import csv
import requests
import time
import argparse

API_BASE = "https://api.discogs.com"


def get_marketplace_value(release_id, token):
    url = f"{API_BASE}/marketplace/stats/{release_id}"
    headers = {
        "User-Agent": "DiscogsCollectionValueScript/0.1",
        "Authorization": f"Discogs token={token}",
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return ""
        data = r.json()
        if "lowest_price" in data and data["lowest_price"]:
            return data["lowest_price"].get("value", "")
    except Exception:
        pass
    return ""


def main():
    parser = argparse.ArgumentParser(
        description="Add Discogs marketplace value column to collection CSV"
    )
    parser.add_argument("input_csv")
    parser.add_argument("output_csv")
    parser.add_argument("--token", required=True)
    args = parser.parse_args()
    with open(args.input_csv, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
    fieldnames = reader.fieldnames + ["value"]
    for i, row in enumerate(rows):
        release_id = row.get("release_id", "").strip()
        if release_id:
            value = get_marketplace_value(release_id, args.token)
        else:
            value = ""
        row["value"] = value
        print(f"{i+1}/{len(rows)} release_id={release_id} value={value}")
        # Discogs rate limit safety
        time.sleep(1)
    with open(args.output_csv, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
