import requests
import pandas as pd
import json
import time
import random
import logging
import argparse
import sys

# Configure logging with timestamp
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

BASE_URL = (
    "https://geoportal.esdm.go.id/monaresia/sharing/servers/"
    "48852a855c014a63acfd78bf06c2689a/rest/services/Pusat/WIUP_Publish/MapServer/0/query"
)

# Default query envelope & params
DEFAULT_PARAMS = {
    "f": "json",
    "returnGeometry": "true",
    "spatialRel": "esriSpatialRelIntersects",
    "geometry": json.dumps(
        {
            "xmin": 9904297.370044464,
            "ymin": -2433296.715522152,
            "xmax": 16224722.364887437,
            "ymax": 2302130.0607998283,
            "spatialReference": {"wkid": 102100},
        }
    ),
    "geometryType": "esriGeometryEnvelope",
    "inSR": "102100",
    "outFields": "*",
    "orderByFields": "objectid ASC",
    "outSR": "102100",
    "resultRecordCount": 90,
}

tasks = {
    "nickel": "LOWER(komoditas) LIKE '%nikel%'",
    "gold": "LOWER(komoditas) LIKE '%emas%'",
    "coal": "LOWER(komoditas) LIKE '%batubara%'",
    "all": None,
}


def construct_url_and_params(extra_filters: dict):
    params = DEFAULT_PARAMS.copy()
    params.update(extra_filters)
    return BASE_URL, params


def fetch_page(url: str, params: dict, max_retries: int = 5) -> dict:
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(
                f"Requesting offset={params.get('resultOffset')} (Attempt {attempt}/{max_retries})"
            )
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            # early check for broken base URL response
            if not isinstance(data, dict) or "features" not in data:
                raise ValueError(
                    "Unexpected response structure, possible broken URL or service down."
                )
            return data
        except (requests.RequestException, json.JSONDecodeError, ValueError) as e:
            logging.warning(f"Failed on attempt {attempt}: {e}")
            if attempt < max_retries:
                sleep_time = random.uniform(1, 3)
                logging.info(f"Sleeping {sleep_time:.2f}s before retry")
                time.sleep(sleep_time)
            else:
                logging.error(
                    f"Max retries reached for offset {params.get('resultOffset')}. Skipping further requests."
                )
                return {}


def scrape(where_clause: str = None) -> pd.DataFrame:
    frames = []
    offset = 0

    while True:
        extra = {"resultOffset": offset}
        if where_clause:
            extra["where"] = where_clause

        url, params = construct_url_and_params(extra)
        page = fetch_page(url, params)

        if not page or "features" not in page or not page["features"]:
            logging.info(f"No more data at offset={offset}. Ending scrape.")
            break

        records = []
        for feat in page["features"]:
            attr = feat.get("attributes", {})
            geom = feat.get("geometry", {}).get("rings")
            attr["geometry"] = geom
            records.append(attr)

        logging.info(f"Fetched {len(records)} records for offset={offset}")
        frames.append(pd.DataFrame(records))
        offset += DEFAULT_PARAMS["resultRecordCount"]

    result_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    logging.info(f"Total records scraped: {len(result_df)}")
    return result_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape ESDM minerba data for a specific commodity or all."
    )
    parser.add_argument(
        "commodity", choices=tasks.keys(), help="Which dataset to scrape: %(choices)s"
    )
    args = parser.parse_args()

    selected = args.commodity
    where_clause = tasks[selected]

    logging.info(f"Starting scrape for {selected}")

    try:
        df = scrape(where_clause=where_clause)
        if df.empty or len(df) < 7000:
            logging.warning(
                f"Scraped data is insufficient (row count = {len(df)}); existing CSV will not be overwritten."
            )
        else:
            filename = f"esdm_minerba_{selected}.csv"
            df.to_csv(filename, index=True)
            logging.info(f"Saved {len(df)} rows to {filename}")
    except Exception as e:
        logging.error(
            f"Scrape failed due to an unexpected error: {e}. Existing CSV remains unchanged."
        )
        sys.exit(1)
