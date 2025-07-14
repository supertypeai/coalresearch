import requests
import pandas as pd
import json
import time
import random
import logging
import argparse
import sys
import re

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
    "KERIKIL BERPASIR ALAMI (SIRTU)": "Sand, Stone, Gravel",
    "ANDESIT": "Non-Metallic Mineral",
    "ASPAL": "Non-Metallic Mineral",
    "HEMATITE": "Non-Metallic Mineral",
    "ZEOLIT": "Non-Metallic Mineral",
    "MARMER": "Non-Metallic Mineral",
    "MANGAN": "Non-Metallic Mineral",
    "BESI": "Iron",
    "KAOLIN": "Non-Metallic Mineral",
    "TANAH LIAT": "Clay",
    "LATERIT BESI": "Iron",
    "PASIR KUARSA": "Non-Metallic Mineral",
    "BATU GAMPING": "Limestone",
    "GRANIT": "Granite",
    "BIJIH BESI": "Iron",
    "CLAY": "Clay",
    "BIJIH NIKEL": "Nickel",
    "BIJIH TIMAH": "Tin",
    "BARIT": "Non-Metallic Mineral",
    "TIMAH PUTIH": "Tin",
    "PASIR TIMAH": "Tin",
    "ZIRKON": "Non-Metallic Mineral",
    "BATUAN ASPAL": "Non-Metallic Mineral",
    "KROMIT": "Non-Metallic Mineral",
    "DOLOMIT": "Limestone",
    "BATU KAPUR": "Limestone",
    "GALENA": "Non-Metallic Mineral",
    "SIRTU": "Sand, Stone, Gravel",
    "BATUPASIR": "Sand, Stone, Gravel",
    "PASIR BESI": "Iron",
    "TIMBAL": "Non-Metallic Mineral",
    "BATUGAMPING": "Limestone",
    "PASIR URUG": "Sand",
    "BATUAN (TRASS)": "Non-Metallic Mineral",
    "TIMAH HITAM": "Non-Metallic Mineral",
    "RIJANG": "Non-Metallic Mineral",
    "BATU GUNUNG QUARRY BESAR": "Sand, Stone, Gravel",
    "BATU ANDESIT": "Sand, Stone, Gravel",
    "BATU GAMPING UNTUK SEMEN": "Limestone",
    "PASIR LAUT": "Sand, Stone, Gravel",
    "GAMPING": "Limestone",
    "BATUAN": "Sand, Stone, Gravel",
    "PASIR, BATU, KERIKIL": "Sand, Stone, Gravel",
    "PASIR": "Sand, Stone, Gravel",
    "TANAH URUG": "Non-Metallic Mineral",
    "LATERIT": "Non-Metallic Mineral",
    "BATU BESI": "Iron",
    "TANAH MERAH (LATERIT)": "Non-Metallic Mineral",
    "ANTIMON": "Non-Metallic Mineral",
    "ANTIMONI": "Non-Metallic Mineral",
    "BATU GUNUNG": "Sand, Stone, Gravel",
    "BASALT": "Sand, Stone, Gravel",
    "FELDSPAR": "Non-Metallic Mineral",
    "TRAS": "Non-Metallic Mineral",
    "BATU KAPUR/ GAMPING": "Limestone",
    "PASIR PASANG": "Sand, Stone, Gravel",
    "PASIR DARAT": "Sand, Stone, Gravel",
    "KERIKIL SUNGAI": "Sand, Stone, Gravel",
    "BENTONIT": "Non-Metallic Mineral",
    "TRASS": "Non-Metallic Mineral",
    "BATU KAPUR UNTUK SEMEN": "Limestone",
    "BATU KALI": "Sand, Stone, Gravel",
    "BATU GAMPING (BATUAN)": "Limestone",
    "PERIDOTIT": "Non-Metallic Mineral",
    "PASIR BATU": "Sand, Stone, Gravel",
    "BALL CLAY": "Clay",
    "BATU LEMPUNG": "Clay",
    "BATUGAMPING UNTUK SEMEN": "Limestone",
    "BIJIH EMAS": "Gold",
    "BATU LEMPUNG (TANAH LIAT)": "Clay",
    "PASIR BANGUNAN": "Sand, Stone, Gravel",
    "SLATE": "Non-Metallic Mineral",
    "KALSIT": "Non-Metallic Mineral",
    "DIORIT": "Sand, Stone, Gravel",
    "GABRO": "Sand, Stone, Gravel",
    "FOSFAT": "Non-Metallic Mineral",
    "MOLIBDENUM": "Non-Metallic Mineral",
    "PIROFILIT": "Non-Metallic Mineral",
    "PASIR DAN BATU (SIRTU)": "Sand, Stone, Gravel",
    "BATU KUARSA": "Non-Metallic Mineral",
    "GRANODIORIT": "Sand, Stone, Gravel",
    "QUARRY BESAR": "Sand, Stone, Gravel",
    "OBSIDIAN": "Non-Metallic Mineral",
    "GAMPING UNTUK SEMEN": "Limestone",
    "SENG, TIMAH HITAM": "Non-Metallic Mineral",
    "BATU GARNET": "Non-Metallic Mineral",
    "GRAFIT": "Non-Metallic Mineral",
    "KUARSIT": "Non-Metallic Mineral",
    "MINERAL BUKAN LOGAM": "Non-Metallic Mineral",
    "BATU GUNUNG KUARI BESAR": "Sand, Stone, Gravel",
    "PERLIT": "Non-Metallic Mineral",
    "MANGAAN": "Non-Metallic Mineral",
    "NIKEL": "Nickel",
    "Bauxite": "Bauxite", 
    "BATU BARA": "Coal"
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
    - Standardizing 'komoditas' to mapped categories (fillna with 'Others')
    - Dropping rows where 'tgl_berlaku' equals 'tgl_akhir'
    - Normalizing administrative names and locations
    """
    exemptions = {"generasi", "kode_wil", "cnc", "lokasi"}

    # Internal helper: normalize province or city names
    def normalize_admin(name: str) -> str:
        if pd.isna(name):
            return ""
        parts = [part.strip() for part in str(name).split(",") if part.strip()]
        cleaned = []
        for part in parts:
            s = part
            # expand abbreviations
            exp = {
                r"\bkab\.?\b": "kabupaten",
                r"\bprov\.?\b": "provinsi",
                r"\bkota\b": "kota",
            }
            for pat, sub in exp.items():
                s = re.sub(pat, sub, s, flags=re.IGNORECASE)
            # remove dots & collapse spaces
            s = s.replace(".", "")
            s = re.sub(r"\s{2,}", " ", s).strip()

            # title-case words (except 'dan')
            def _tc(w):
                return w.lower() if w.lower() == "dan" else w.capitalize()

            s = " ".join(_tc(w) for w in s.split())
            cleaned.append(s)
        return ", ".join(cleaned)

    # Internal helper: normalize free-form location strings
    def normalize_location(row) -> str:
        raw = str(row.get("lokasi", "")).strip()
        raw = re.sub(r"^[\.\s]+", "", raw)
        # DIGIT-ONLY → fallback to kab/prov
        if raw.isdigit():
            return (
                f"{row.get('nama_kab', '').title()}, {row.get('nama_prov', '').title()}"
            )
        # URLs → fallback
        if re.search(r"https?://|goo\.gl/", raw, flags=re.IGNORECASE):
            return f"{row.get('nama_kab', '')}, {row.get('nama_prov', '')}"
        loc = raw
        loc = re.sub(r"\bdesa/kelurahan\b", "Desa/Kelurahan", loc, flags=re.IGNORECASE)
        # Expand Ds or Ds.
        loc = re.sub(r"\bds\.?\b", "desa ", loc, flags=re.IGNORECASE)
        # Ensure Jl. and No.
        loc = re.sub(r"\bJl\.?\b", "Jl.", loc)
        loc = re.sub(r"\bNo\.?\b", "No.", loc)
        # Uppercase RT/RW
        loc = re.sub(r"\bRt\b", "RT", loc, flags=re.IGNORECASE)
        loc = re.sub(r"\bRw\b", "RW", loc, flags=re.IGNORECASE)
        # Other expansions
        expansions = {
            r"\bkec\.?\s*": "kecamatan ",
            r"\bkab\.?\s*": "kabupaten ",
            r"\bprov\.?\s*": "provinsi ",
            r"\bkel\.?\s*": "kelurahan ",
            r"\bdesa/kel\.?\s*": "desa/kelurahan ",
        }
        for pat, sub in expansions.items():
            loc = re.sub(pat, sub, loc, flags=re.IGNORECASE)
        # Fix fused words
        loc = re.sub(
            r"(?i)(kecamatan|kabupaten|provinsi|kelurahan)([A-Za-z])",
            lambda m: m.group(1) + " " + m.group(2),
            loc,
        )
        # Normalize spacing
        loc = re.sub(r"\s{2,}", " ", loc)
        loc = re.sub(r"\s*,\s*", ", ", loc).strip(" ,")

        def tc(w):
            return w.lower() if w.lower() == "dan" else w.capitalize()

        parts = [p.strip() for p in loc.split(",") if p.strip()]
        cleaned = [" ".join(tc(w) for w in part.split()) for part in parts]
        return ", ".join(cleaned)

    # 1) Trim and remove newlines in string fields
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].str.replace(r"[\r\n]+", " ", regex=True)
    # 2) Filter invalid rows
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
    # 3) Drop identical dates
    if "tgl_berlaku" in df.columns and "tgl_akhir" in df.columns:
        df = df[df["tgl_berlaku"] != df["tgl_akhir"]]
    # 4) Map commodities with fallback 'Others'
    if "komoditas" in df.columns:
        cleaned = df["komoditas"].str.upper().str.replace(r"\s+DMP$", "", regex=True)
        df["komoditas_mapped"] = cleaned.map(COMMODITY_MAP).fillna("Others")
    # 5) Normalize administrative names if present
    if "nama_prov" in df.columns:
        df["provinsi_norm"] = df["nama_prov"].apply(normalize_admin)
    if "nama_kab" in df.columns:
        df["kabupaten_norm"] = df["nama_kab"].apply(normalize_admin)
    if "kegiatan" in df.columns:
        df["kegiatan_norm"] = df["kegiatan"].apply(normalize_admin)
    # 6) Normalize free-form locations
    if "lokasi" in df.columns:
        df["lokasi_norm"] = df.apply(normalize_location, axis=1)
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
