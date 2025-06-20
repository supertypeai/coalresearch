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

COMMODITY_MAP = {
    "TIMAH": "Tin",
    "BATUBARA": "Coal",
    "NICKEL": "Nickel",
    "BAUKSIT": "Bauxite",
    "EMAS": "Gold",
    "TEMBAGA": "Copper",
    "BATU GAMPING UNTUK INDUSTRI": "Limestone",
    "KERIKIL BERPASIR ALAMI (SIRTU)": "Sand",
    "ANDESIT": "Andesite",
    "ASPAL": "Asphalt",
    "HEMATITE": "Hematite",
    "ZEOLIT": "Zeolite",
    "MARMER": "Marble",
    "MANGAN": "Manganese",
    "BESI": "Iron",
    "KAOLIN": "Kaolin",
    "TANAH LIAT": "Clay",
    "LATERIT BESI": "Iron Laterite",
    "PASIR KUARSA": "Quartz",
    "BATU GAMPING": "Limestone",
    "GRANIT": "Granite",
    "BIJIH BESI": "Iron Ore",
    "CLAY": "Clay",
    "BIJIH NIKEL": "Nickel Ore",
    "BIJIH TIMAH": "Tin Ore",
    "BARIT": "Barite",
    "TIMAH PUTIH": "Tin",
    "PASIR TIMAH": "Tin",
    "ZIRKON": "Zircon",
    "BATUAN ASPAL": "Asphalt",
    "KROMIT": "Chromite",
    "DOLOMIT": "Dolomite",
    "BATU KAPUR": "Limestone",
    "GALENA": "Galena",
    "SIRTU": "Sirtu",
    "BATUPASIR": "Sandstone",
    "PASIR BESI": "Iron",
    "TIMBAL": "Lead",
    "BATUGAMPING": "Limestone",
    "PASIR URUG": "Sand",
    "BATUAN (TRASS)": "Trass",
    "TIMAH HITAM": "Lead",
    "RIJANG": "Chert",
    "BATU GUNUNG QUARRY BESAR": "Mountain Stone",
    "BATU ANDESIT": "Andesite Stone",
    "BATU GAMPING UNTUK SEMEN": "Limestone",
    "PASIR LAUT": "Sand",
    "GAMPING": "Limestone",
    "BATUAN": "Rock",
    "PASIR, BATU, KERIKIL": "Sand, Stone, Gravel",
    "PASIR": "Sand",
    "TANAH URUG": "Filling Soil",
    "LATERIT": "Laterite",
    "BATU BESI": "Iron Stone",
    "TANAH MERAH (LATERIT)": "Laterite",
    "ANTIMON": "Antimony",
    "ANTIMONI": "Antimony",
    "BATU GUNUNG": "Mountain Stone",
    "BASALT": "Basalt",
    "FELDSPAR": "Feldspar",
    "TRAS": "Trass",
    "BATU KAPUR/ GAMPING": "Limestone",
    "PASIR PASANG": "Sand",
    "PASIR DARAT": "Sand",
    "KERIKIL SUNGAI": "River Gravel",
    "BENTONIT": "Bentonite",
    "TRASS": "Trass",
    "BATU KAPUR UNTUK SEMEN": "Limestone",
    "BATU KALI": "Stone",
    "BATU GAMPING (BATUAN)": "Limestone",
    "PERIDOTIT": "Peridotite",
    "PASIR BATU": "Stone",
    "BALL CLAY": "Clay",
    "BATU LEMPUNG": "Clay",
    "BATUGAMPING UNTUK SEMEN": "Limestone",
    "BIJIH EMAS": "Gold Ore",
    "BATU LEMPUNG (TANAH LIAT)": "Clay",
    "PASIR BANGUNAN": "Sand",
    "SLATE": "Slate",
    "KALSIT": "Calcite",
    "DIORIT": "Diorite",
    "GABRO": "Gabbro",
    "FOSFAT": "Phosphate",
    "MOLIBDENUM": "Molybdenum",
    "PIROFILIT": "Pyrophyllite",
    "PASIR DAN BATU (SIRTU)": "Sand, Stone",
    "BATU KUARSA": "Quartz",
    "GRANODIORIT": "Granodiorite",
    "QUARRY BESAR": "Large Quarry",
    "OBSIDIAN": "Obsidian",
    "GAMPING UNTUK SEMEN": "Limestone",
    "SENG, TIMAH HITAM": "Zinc, Lead",
    "BATU GARNET": "Garnet",
    "GRAFIT": "Graphite",
    "KUARSIT": "Quartzite",
    "MINERAL BUKAN LOGAM": "Non-Metallic Mineral",
    "BATU GUNUNG KUARI BESAR": "Mountain Stone",
    "PERLIT": "Perlite",
    "MANGAAN": "Manganese",
    "NIKEL": "Nickel",
}


def construct_url_and_params(extra_filters: dict):
    params = DEFAULT_PARAMS.copy()
    params.update(extra_filters)
    return BASE_URL, params


def fetch_page(url: str, params: dict, max_retries: int = 10) -> dict:
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


def cleanse_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the dataframe by:
    - Trimming whitespace in all string columns
    - Removing rows with null, unreadable, or placeholder (‘-’) values in any column, except 'generasi' and 'kode_wil'
    - Removing embedded newlines in string fields
    - Standardizing 'komoditas' to mapped categories
    - Dropping rows where 'tgl_berlaku' equals 'tgl_akhir'
    """
    exemptions = {"generasi", "kode_wil"}
    # Normalize strings and remove newlines
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].str.replace(r"[\r\n]+", " ", regex=True)
    # Filter invalid rows
    valid = pd.Series(True, index=df.index)
    for col in df.columns:
        if col in exemptions:
            continue
        if df[col].dtype == object:
            invalid = df[col].isin(["", "-", None, "nan", "None"])
            valid &= ~invalid
        else:
            valid &= df[col].notnull()
    df = df[valid]
    # Drop rows with identical dates
    if "tgl_berlaku" in df.columns and "tgl_akhir" in df.columns:
        df = df[df["tgl_berlaku"] != df["tgl_akhir"]]
    # Map commodities
    if "komoditas" in df.columns:
        cleaned = df["komoditas"].str.upper().str.replace(r"\s+DMP$", "", regex=True)
        df["komoditas_mapped"] = cleaned.map(COMMODITY_MAP).fillna(cleaned.str.title())
    return df


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
        df = cleanse_df(df)
        if df.empty or len(df) < 3500:
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
