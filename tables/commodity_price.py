import os
import requests
import pandas as pd
import sqlite3
import re
import json
from io import StringIO
from datetime import date
from dotenv import load_dotenv

# ─── CONFIGURATION ──────────────────────────────────────────────────────────────
DB_PATH = "db.sqlite"
load_dotenv()

# Minerba (ESDM) Configuration
HOME_URL = "https://www.minerba.esdm.go.id/harga_acuan"

# LBMA Configuration
LBMA_URLS = {
    "Emas": "https://prices.lbma.org.uk/json/gold_am.json",
    "Perak": "https://prices.lbma.org.uk/json/silver.json",
}

# ─── DYNAMIC DATE RANGE (FOR MINERBA) ───────────────────────────────────────────
today = date.today()
start = date(today.year - 15, today.month, 1)
if today.month == 12:
    next_month, next_year = 1, today.year + 1
else:
    next_month, next_year = today.month + 1, today.year
end = date(next_year, next_month, 1)

START_RANGE = f"{start.month:02d}/{start.year}"
END_RANGE = f"{end.month:02d}/{end.year}"


# ─── SHARED HELPERS ─────────────────────────────────────────────────────────────
MONTH_MAP = {
    "Januari": 1,
    "Februari": 2,
    "Maret": 3,
    "April": 4,
    "Mei": 5,
    "Juni": 6,
    "Juli": 7,
    "Agustus": 8,
    "September": 9,
    "Oktober": 10,
    "November": 11,
    "Desember": 12,
}
HEADER_RE = re.compile(
    r"^(?P<month>\w+)\s+(?P<year>\d{4})(?:\s*\(Periode\s+(?P<period>Pertama|Kedua)\))?$"
)

# Consolidated commodity name map
COMMODITY_NAME_MAP = {
    "Batubara": "Coal",
    "Batubara (hba 1)": "Coal (HBA 1)",
    "Batubara (hba 2)": "Coal (HBA 2)",
    "Batubara (hba 3)": "Coal (HBA 3)",
    "Nikel": "Nickel",
    "Kobalt": "Cobalt",
    "Timbal": "Lead",
    "Seng": "Zinc",
    "Aluminium": "Aluminum",
    "Tembaga": "Copper",
    "Emas sebagai mineral ikutan": "Gold as a co-product",
    "Perak sebagai mineral ikutan": "Silver as a co-product",
    "Mangan": "Manganese",
    "Bijih Besi Laterit/Hematit/Magnetit": "Iron",
    "Bijih Krom": "Chromium",
    "Konsentrat Titanium": "Titanium",
    "Emas": "Gold",
    "Perak": "Silver",
}


def parse_header_to_date(header: str) -> date:
    """Parses Minerba's table header into a date object."""
    m = HEADER_RE.match(header)
    if not m:
        raise ValueError(f"Unexpected header format: {header!r}")
    mon = MONTH_MAP[m.group("month")]
    yr = int(m.group("year"))
    pd_ = m.group("period")
    day = 1 if (pd_ is None or pd_ == "Pertama") else 15
    return date(yr, mon, day)


# ─── DATABASE FUNCTIONS ─────────────────────────────────────────────────────────


def init_db(path):
    """
    Initializes the SQLite database.
    Creates commodity_price table.
    """
    conn = sqlite3.connect(path)
    c = conn.cursor()

    # Create V2 table
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS commodity_price (
            name  TEXT    NOT NULL,
            date  TEXT    NOT NULL,
            price TEXT,
            PRIMARY KEY (name, date)
        );
        """
    )
    conn.commit()
    return conn

def get_db_conn(path):
    return sqlite3.connect(path)


# ─── MINERBA (ESDM) SCRAPING FUNCTIONS ──────────────────────────────────────────


def get_csrf_token(session, url):
    """Fetches the CSRF token from the website."""
    resp = session.get(url)
    resp.raise_for_status()
    token = session.cookies.get("csrf_cookie_name")
    if not token:
        raise RuntimeError("CSRF cookie not found!")
    return token


def fetch_minerba_html(session, url, csrf, start, end):
    """Fetches the pricing data table HTML from the Minerba website."""
    data = {"csrf_test_name": csrf, "bulan_awal": start, "bulan_akhir": end}
    resp = session.post(
        url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Referer": url},
    )
    resp.raise_for_status()
    return resp.text


def parse_minerba_table(html):
    """Parses the HTML table into a pandas DataFrame."""
    df = pd.read_html(StringIO(html))[0]
    df.replace("-", pd.NA, inplace=True)
    return df


def upsert_minerba_data(conn, df):
    """Upserts Minerba commodity data into the local database (commodity_price)."""
    c = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        full = row["Komoditas"]
        m = re.match(r"^(.*?)\s*\(([^)]+)\)$", full)
        if not m:
            continue
        name, unit = m.groups()
        english_name = COMMODITY_NAME_MAP.get(name.strip(), name.strip())

        # Collect rows to insert
        rows_to_insert = []
        for hdr in df.columns[1:]:
            val = row[hdr]
            if pd.isna(val):
                continue
            dt = parse_header_to_date(str(hdr))
            # Insert each price point as a row
            rows_to_insert.append((english_name, dt.isoformat(), str(val)))

        if not rows_to_insert:
            continue

        c.executemany(
            """
            INSERT INTO commodity_price (name, date, price)
            VALUES (?, ?, ?)
            ON CONFLICT(name, date) DO UPDATE
              SET price = excluded.price
            """,
            rows_to_insert,
        )
        count += len(rows_to_insert)

    conn.commit()
    print(f"Upserted {count} rows of Minerba data into commodity_price.")


# ─── LBMA SCRAPING FUNCTIONS ────────────────────────────────────────────────────


def fetch_lbma_price_data(url: str) -> pd.DataFrame:
    """Fetches daily price JSON from LBMA and returns a DataFrame."""
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    df = pd.json_normalize(data)
    df["high"] = df["v"].apply(lambda x: x[0])
    df.drop(columns=["v", "is_cms_locked"], inplace=True)
    df["date"] = pd.to_datetime(df["d"], format="%Y-%m-%d")
    df.drop(columns=["d"], inplace=True)
    df = df[df["high"] > 0]

    return df


def compute_lbma_monthly_avg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the Average (Mean) daily high for each month
    and sets the date to the 1st of the month to match Minerba format.
    """
    # 1. Convert the exact date to the 'First of the Month'
    #    e.g., 2024-01-23 becomes 2024-01-01
    df["month_start"] = df["date"].values.astype("datetime64[M]")

    # 2. Group by this new monthly date and calculate the Mean (Average)
    #    (If you strictly want the 'Max' price, change .mean() to .max())
    monthly = df.groupby("month_start")["high"].mean().reset_index()

    # 3. Rename columns for consistency
    monthly.rename(
        columns={"month_start": "date", "high": "monthly_price"}, inplace=True
    )

    return monthly


def upsert_lbma_data(conn, data: dict):
    cur = conn.cursor()
    total_rows = 0
    for name, df in data.items():
        rows_to_insert = []
        for _, row in df.iterrows():
            # Date is already the 1st of the month from the compute function
            date_str = row["date"].strftime("%Y-%m-%d")

            # Rounding to 2 decimal places looks cleaner for averages
            price_str = "{:.2f}".format(row["monthly_price"])

            rows_to_insert.append((name, date_str, price_str))

        if not rows_to_insert:
            continue

        cur.executemany(
            """
            INSERT INTO commodity_price (name, date, price)
            VALUES (?, ?, ?)
            ON CONFLICT(name, date) DO UPDATE SET price=excluded.price
            """,
            rows_to_insert,
        )
        total_rows += len(rows_to_insert)

    conn.commit()
    print(f"Upserted {total_rows} rows of LBMA data into commodity_price.")


# ─── MAIN EXECUTION ─────────────────────────────────────────────────────────────


def run_minerba_scraper(conn):
    """Main function to scrape and store Minerba data."""
    print("--- Starting Minerba Scraper ---")
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})

    try:
        csrf = get_csrf_token(sess, HOME_URL)
        html = fetch_minerba_html(sess, HOME_URL, csrf, START_RANGE, END_RANGE)
        df = parse_minerba_table(html)
        print(f"Fetched Minerba data: {df.shape[0]} rows")
        upsert_minerba_data(conn, df)
    except requests.RequestException as e:
        print(f"Error during Minerba scraping: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during Minerba processing: {e}")
    print("--- Minerba Scraper Finished ---")


def run_lbma_scraper(conn):
    """Main function to scrape and store LBMA data."""
    print("\n--- Starting LBMA Scraper ---")
    all_data = {}
    for name, url in LBMA_URLS.items():
        english_name = COMMODITY_NAME_MAP.get(name, name)
        try:
            print(f"Fetching LBMA data for {name}...")
            df = fetch_lbma_price_data(url)

            # --- CHANGED HERE: Use the new Average/First-of-Month logic ---
            monthly_data = compute_lbma_monthly_avg(df)

            all_data[english_name] = monthly_data
            print(f"Successfully processed monthly data for {name}.")
        except requests.RequestException as e:
            print(f"Error fetching LBMA data for {name}: {e}")
        except Exception as e:
            print(
                f"An unexpected error occurred during LBMA processing for {name}: {e}"
            )

    if all_data:
        upsert_lbma_data(conn, all_data)
    print("--- LBMA Scraper Finished ---")


def sync_commodity_price():
    db_conn = get_db_conn(DB_PATH)

    run_minerba_scraper(db_conn)
    run_lbma_scraper(db_conn)

    db_conn.close()

    print(f"\nProcess complete. Local database updated: {DB_PATH}")

if __name__ == "__main__":
    sync_commodity_price()