import requests
import pandas as pd
import json
import time
import random
import logging

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


def construct_url_and_params(extra_filters: dict):
    """
    Combine DEFAULT_PARAMS with any extra_filters provided (e.g., where, resultOffset).
    """
    params = DEFAULT_PARAMS.copy()
    params.update(extra_filters)
    return BASE_URL, params


def fetch_page(url: str, params: dict, max_retries: int = 5) -> dict:
    """
    Request a single page of data, retrying up to max_retries on failure.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Requesting offset={params.get('resultOffset')} (Attempt {attempt}/{max_retries})"
            )
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()
            # Debug head of response
            if isinstance(data, dict) and "features" in data:
                logging.debug(
                    f"Head features: {json.dumps(data['features'][:2], indent=2)}"
                )
            return data

        except (requests.RequestException, json.JSONDecodeError) as e:
            logging.warning(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Failed on attempt {attempt}: {e}"
            )
            if attempt < max_retries:
                sleep_time = random.uniform(1, 3)
                logging.info(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Sleeping {sleep_time:.2f}s before retry"
                )
                time.sleep(sleep_time)
            else:
                logging.error(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Max retries reached for offset {params.get('resultOffset')}"
                )
                return {}
        finally:
            pass


def scrape(where_clause: str = None) -> pd.DataFrame:
    """
    Continues fetching pages of 90 records until an empty page is returned.
    Optionally filters by a SQL WHERE clause (e.g., komoditas).
    """
    frames = []
    offset = 0

    while True:
        extra = {"resultOffset": offset}
        if where_clause:
            extra["where"] = where_clause

        url, params = construct_url_and_params(extra)
        page = fetch_page(url, params)

        if not page or "features" not in page or not page["features"]:
            logging.info(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] No more data at offset={offset}. Ending scrape."
            )
            break

        records = []
        for feat in page["features"]:
            attr = feat.get("attributes", {})
            geom = feat.get("geometry", {}).get("rings")
            attr["geometry"] = geom
            records.append(attr)

        logging.info(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Fetched {len(records)} records for offset={offset}"
        )
        frames.append(pd.DataFrame(records))

        offset += DEFAULT_PARAMS["resultRecordCount"]

    result_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    logging.info(
        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Total records scraped: {len(result_df)}"
    )
    return result_df


if __name__ == "__main__":
    # Define filters and output filenames here
    tasks = [
        # {"name": "nickel", "where": "LOWER(komoditas) LIKE '%nikel%'"},
        # {"name": "gold", "where": "LOWER(komoditas) LIKE '%emas%'"},
        # {"name": "coal", "where": "LOWER(komoditas) LIKE '%batubara%'"},
        {"name": "all", "where": None},
    ]

    for task in tasks:
        logging.info(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting scrape for {task['name']}"
        )
        df = scrape(where_clause=task["where"])
        filename = f"scrapper/esdm_minerba_{task['name']}.csv"
        df.to_csv(filename, index=False)
        logging.info(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Saved {len(df)} rows to {filename}"
        )
