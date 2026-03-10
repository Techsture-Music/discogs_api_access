#!/usr/bin/python3

import argparse
import csv
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests


API_BASE = "https://api.discogs.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a Discogs seller's inventory with community ratings to CSV."
    )
    parser.add_argument(
        "seller_username",
        help="Discogs username of the seller whose inventory you want to export",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="discogs_inventory_export.csv",
        help="Output CSV filename (default: discogs_inventory_export.csv)",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("DISCOGS_TOKEN"),
        help="Discogs user token. Defaults to DISCOGS_TOKEN environment variable.",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Items per page to request from Discogs (default: 100)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds to sleep between API requests (default: 0.5)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--first-page-only",
        action="store_true",
        help="Only fetch the first page of seller inventory (useful for testing)",
    )
    return parser.parse_args()


def build_session(token: Optional[str]) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "MichaelDiscogsInventoryExporter/1.1",
            "Accept": "application/json",
        }
    )
    if token:
        session.headers["Authorization"] = f"Discogs token={token}"
    return session


def request_json(
    session: requests.Session,
    url: str,
    timeout: int,
    sleep_seconds: float,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    response = session.get(url, params=params, timeout=timeout)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        wait_time = float(retry_after) if retry_after else max(5.0, sleep_seconds * 4)
        print(
            f"Rate limited by Discogs. Sleeping for {wait_time:.1f} seconds...",
            file=sys.stderr,
        )
        time.sleep(wait_time)
        response = session.get(url, params=params, timeout=timeout)

    response.raise_for_status()
    time.sleep(sleep_seconds)
    return response.json()


def safe_get(data: Dict[str, Any], path: List[str], default: Any = "") -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def get_inventory(
    session: requests.Session,
    seller_username: str,
    timeout: int,
    sleep_seconds: float,
    per_page: int,
    first_page_only: bool,
) -> List[Dict[str, Any]]:
    all_listings: List[Dict[str, Any]] = []
    page = 1

    while True:
        url = f"{API_BASE}/users/{seller_username}/inventory"
        params = {
            "page": page,
            "per_page": per_page,
        }

        payload = request_json(
            session=session,
            url=url,
            timeout=timeout,
            sleep_seconds=sleep_seconds,
            params=params,
        )

        listings = payload.get("listings", [])
        pagination = payload.get("pagination", {})
        pages = pagination.get("pages", 1)

        all_listings.extend(listings)

        print(
            f"Fetched inventory page {page}/{pages} "
            f"({len(listings)} listings, {len(all_listings)} total)...",
            file=sys.stderr,
        )

        if first_page_only:
            print("Stopping after first page because --first-page-only was set.", file=sys.stderr)
            break

        if page >= pages:
            break

        page += 1

    return all_listings


def get_release_rating(
    session: requests.Session,
    release_id: int,
    timeout: int,
    sleep_seconds: float,
    cache: Dict[int, Tuple[str, str]],
) -> Tuple[str, str]:
    if release_id in cache:
        return cache[release_id]

    url = f"{API_BASE}/releases/{release_id}/rating"

    try:
        payload = request_json(
            session=session,
            url=url,
            timeout=timeout,
            sleep_seconds=sleep_seconds,
        )

        rating = payload.get("rating", {})
        average = rating.get("average", "")
        count = rating.get("count", "")
        result = (str(average), str(count))

    except requests.HTTPError as exc:
        print(
            f"Warning: could not fetch rating for release {release_id}: {exc}",
            file=sys.stderr,
        )
        result = ("", "")
    except requests.RequestException as exc:
        print(
            f"Warning: network error fetching rating for release {release_id}: {exc}",
            file=sys.stderr,
        )
        result = ("", "")

    cache[release_id] = result
    return result


def listing_to_row(
    listing: Dict[str, Any],
    rating_average: str,
    rating_count: str,
) -> Dict[str, Any]:
    release = listing.get("release", {})
    seller = listing.get("seller", {})
    price = listing.get("price", {})

    formats = release.get("format")
    if isinstance(formats, list):
        format_value = " | ".join(str(item) for item in formats)
    else:
        format_value = formats if formats is not None else ""

    artists = release.get("artist")
    if isinstance(artists, list):
        artist_value = " | ".join(str(item) for item in artists)
    else:
        artist_value = artists if artists is not None else ""

    labels = release.get("label")
    if isinstance(labels, list):
        label_value = " | ".join(str(item) for item in labels)
    else:
        label_value = labels if labels is not None else ""

    return {
        "listing_id": listing.get("id", ""),
        "seller_username": seller.get("username", ""),
        "release_id": release.get("id", ""),
        "artist": artist_value,
        "title": release.get("description", ""),
        "label": label_value,
        "format": format_value,
        "catalog_number": release.get("catalog_number", ""),
        "year": release.get("year", ""),
        "media_condition": listing.get("condition", ""),
        "sleeve_condition": listing.get("sleeve_condition", ""),
        "comments": listing.get("comments", ""),
        "price": price.get("value", ""),
        "currency": price.get("currency", ""),
        "status": listing.get("status", ""),
        "ships_from": listing.get("ships_from", ""),
        "location": listing.get("location", ""),
        "uri": listing.get("uri", ""),
        "community_rating_average": rating_average,
        "community_rating_count": rating_count,
    }


def write_csv(rows: List[Dict[str, Any]], output_file: str) -> None:
    fieldnames = [
        "listing_id",
        "seller_username",
        "release_id",
        "artist",
        "title",
        "label",
        "format",
        "catalog_number",
        "year",
        "media_condition",
        "sleeve_condition",
        "comments",
        "price",
        "currency",
        "status",
        "ships_from",
        "location",
        "uri",
        "community_rating_average",
        "community_rating_count",
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()

    if not args.token:
        print(
            "Error: no Discogs token provided.\n"
            "Pass --token YOUR_TOKEN or set DISCOGS_TOKEN in your environment.",
            file=sys.stderr,
        )
        sys.exit(1)

    session = build_session(args.token)

    try:
        listings = get_inventory(
            session=session,
            seller_username=args.seller_username,
            timeout=args.timeout,
            sleep_seconds=args.sleep,
            per_page=args.per_page,
            first_page_only=args.first_page_only,
        )
    except requests.HTTPError as exc:
        print(f"Error fetching inventory: {exc}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as exc:
        print(f"Network error fetching inventory: {exc}", file=sys.stderr)
        sys.exit(1)

    rating_cache: Dict[int, Tuple[str, str]] = {}
    rows: List[Dict[str, Any]] = []

    for idx, listing in enumerate(listings, start=1):
        release_id = safe_get(listing, ["release", "id"], default=None)

        if isinstance(release_id, int):
            rating_average, rating_count = get_release_rating(
                session=session,
                release_id=release_id,
                timeout=args.timeout,
                sleep_seconds=args.sleep,
                cache=rating_cache,
            )
        else:
            rating_average, rating_count = "", ""

        row = listing_to_row(
            listing=listing,
            rating_average=rating_average,
            rating_count=rating_count,
        )
        rows.append(row)

        if idx % 25 == 0 or idx == len(listings):
            print(
                f"Processed {idx}/{len(listings)} listings...",
                file=sys.stderr,
            )

    write_csv(rows, args.output)
    print(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()