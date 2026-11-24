import os
import requests
import pandas as pd
import sqlite3
import re
import json
from io import BytesIO, StringIO
from datetime import date
from dotenv import load_dotenv

# ─── CONFIGURATION ──────────────────────────────────────────────────────────────
DB_PATH = "db.sqlite"
load_dotenv()

# Minerba (ESDM) Configuration
HOME_URL = "https://www.minerba.esdm.go.id/harga_acuan"
# Title of the page the whole Minerba site serves while it is down.
MINERBA_MAINTENANCE_MARKER = "Sistem Sedang Dalam Perbaikan"

# Seconds to wait on any request, so a hung server can't stall the CI job.
REQUEST_TIMEOUT = 60

# World Bank Configuration (gold and silver)
# LBMA's price JSON has refused automated requests since October 2026; LBMA
# data now needs a paid IBA licence. Gold and silver come instead from the
# World Bank's monthly "Pink Sheet", whose download URL changes with every
# release, so it is looked up on this page.
WORLD_BANK_PAGE_URL = "https://www.worldbank.org/en/research/commodity-markets"
WORLD_BANK_FILE_RE = re.compile(r"https?://[^\"'\s]*CMO-Historical-Data-Monthly\.xlsx")
# Pink Sheet column -> stored commodity name.
WORLD_BANK_SERIES = {"Gold": "Gold", "Silver": "Silver"}
# Both series are quoted in USD per troy ounce; the scrape fails if that changes.
WORLD_BANK_UNIT_LABEL = "($/troy oz)"
WORLD_BANK_CURRENCY, WORLD_BANK_PRICE_UNIT = "USD", "troy_oz"
WORLD_BANK_SOURCE = "World Bank"

# Minerba label units ("USD/ton", "USD/dmt", ...) mapped to stored codes.
# Matching is case- and space-insensitive. A unit not listed here makes the
# scrape skip that commodity and fail, so add it here once it is checked.
CURRENCY_MAP = {"usd": "USD"}
PRICE_UNIT_MAP = {
    "ton": "t",
    "tonne": "t",
    "mt": "t",
    "dmt": "dmt",
    "wmt": "wmt",
    "dmtu": "dmtu",
    "oz": "troy_oz",
    "troyoz": "troy_oz",
    "ozt": "troy_oz",
}
# Placeholder unit for rows whose real unit was never scraped. Minerba quotes
# these ores per dry metric tonne unit (iron 1.50 USD/dmtu ≈ 93 USD/t at 62% Fe);
# everything else falls back to tonnes.
FALLBACK_PRICE_UNITS = {
    "Iron": "dmtu",
    "Manganese": "dmtu",
    "Chromium": "dmtu",
    "Titanium": "dmtu",
}
FALLBACK_PRICE_UNIT = "t"

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
    "Aluminium": "Aluminium",  # same spelling as company and mining_site data
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
            name              TEXT NOT NULL,
            price_usd_per_ton REAL NOT NULL,
            date              DATE NOT NULL,
            price             REAL,
            currency          TEXT,
            price_unit        TEXT,
            source            TEXT,
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
    resp = session.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    if MINERBA_MAINTENANCE_MARKER in resp.text:
        raise RuntimeError(
            f"{url} is showing its maintenance page "
            f"({MINERBA_MAINTENANCE_MARKER!r}); try again once the site is back."
        )
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
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.text


def parse_minerba_table(html):
    """Parses the HTML table into a pandas DataFrame."""
    df = pd.read_html(StringIO(html))[0]
    df.replace("-", pd.NA, inplace=True)
    return df


def parse_minerba_unit(raw: str):
    """Maps a Minerba label unit such as "USD/ton" to (currency, price_unit).
    Returns None for a unit we don't know, so it is never stored unchecked."""
    norm = raw.lower().replace(" ", "").replace("us$", "usd")
    if "/" not in norm:
        return None
    cur, per = norm.split("/", 1)
    currency = CURRENCY_MAP.get(cur)
    price_unit = PRICE_UNIT_MAP.get(per)
    if not currency or not price_unit:
        return None
    return currency, price_unit


def upsert_prices(conn, rows):
    """rows: (name, date, price, currency, price_unit, source).
    Writes the price to both `price` and the deprecated `price_usd_per_ton`."""
    conn.executemany(
        """
        INSERT INTO commodity_price
            (name, date, price_usd_per_ton, price, currency, price_unit, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name, date) DO UPDATE
          SET price_usd_per_ton = excluded.price_usd_per_ton,
              price = excluded.price,
              currency = excluded.currency,
              price_unit = excluded.price_unit,
              source = excluded.source
        """,
        [(n, d, p, p, c, u, s) for n, d, p, c, u, s in rows],
    )


def upsert_minerba_data(conn, df):
    """Upserts Minerba commodity data into the local database (commodity_price).
    Returns the labels whose unit wasn't recognised (those rows are skipped)."""
    count = 0
    unknown = []
    for _, row in df.iterrows():
        full = row["Komoditas"]
        m = re.match(r"^(.*?)\s*\(([^)]+)\)$", full)
        if not m:
            continue
        name, raw_unit = m.groups()
        english_name = COMMODITY_NAME_MAP.get(name.strip(), name.strip())
        parsed = parse_minerba_unit(raw_unit)
        if parsed is None:
            unknown.append(full)
            continue
        currency, price_unit = parsed

        # Collect rows to insert
        rows_to_insert = []
        for hdr in df.columns[1:]:
            val = row[hdr]
            if pd.isna(val):
                continue
            dt = parse_header_to_date(str(hdr))
            # Insert each price point as a row
            rows_to_insert.append(
                (english_name, dt.isoformat(), float(val), currency, price_unit, "Minerba")
            )

        if not rows_to_insert:
            continue

        upsert_prices(conn, rows_to_insert)
        count += len(rows_to_insert)

    conn.commit()
    print(f"Upserted {count} rows of Minerba data into commodity_price.")
    if unknown:
        print(f"[!] Skipped labels with an unknown unit: {unknown}")
    return unknown


# ─── WORLD BANK (GOLD, SILVER) FUNCTIONS ────────────────────────────────────────


def find_world_bank_file_url() -> str:
    """Finds the current monthly Pink Sheet .xlsx link on the World Bank page."""
    resp = requests.get(WORLD_BANK_PAGE_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    m = WORLD_BANK_FILE_RE.search(resp.text)
    if not m:
        raise RuntimeError(f"No monthly Pink Sheet link found on {WORLD_BANK_PAGE_URL}")
    return m.group(0)


def parse_world_bank_monthly(content: bytes) -> pd.DataFrame:
    """Reads the Pink Sheet's "Monthly Prices" sheet into (name, date, price)
    rows for WORLD_BANK_SERIES. Each price is that month's average, dated the
    1st of the month like the Minerba rows. Months are labelled "2026M10"."""
    raw = pd.read_excel(BytesIO(content), sheet_name="Monthly Prices", header=None)
    cells = raw.astype(str).apply(lambda col: col.str.strip())

    header = next(
        (i for i in range(min(10, len(cells))) if (cells.iloc[i] == "Gold").any()),
        None,
    )
    if header is None:
        raise RuntimeError("Pink Sheet layout changed: no 'Gold' column header")

    months = cells[0].str.extract(r"^(\d{4})M(\d{2})$")
    is_month = months[0].notna()
    dates = pd.to_datetime(months[0][is_month] + "-" + months[1][is_month] + "-01")

    frames = []
    for col, name in WORLD_BANK_SERIES.items():
        matches = cells.columns[cells.iloc[header] == col]
        if len(matches) != 1:
            raise RuntimeError(f"Pink Sheet has {len(matches)} '{col}' columns, expected 1")
        j = matches[0]
        unit = cells.iat[header + 1, j]
        if unit != WORLD_BANK_UNIT_LABEL:
            raise RuntimeError(
                f"Pink Sheet quotes {col} in {unit!r}, expected {WORLD_BANK_UNIT_LABEL!r}"
            )
        prices = pd.to_numeric(raw.loc[is_month, j], errors="coerce")
        frames.append(pd.DataFrame({"name": name, "date": dates, "price": prices}))

    return pd.concat(frames, ignore_index=True).dropna(subset=["price"])


def upsert_world_bank_data(conn, df: pd.DataFrame):
    rows = [
        (
            name,
            dt.strftime("%Y-%m-%d"),
            round(float(price), 2),
            WORLD_BANK_CURRENCY,
            WORLD_BANK_PRICE_UNIT,
            WORLD_BANK_SOURCE,
        )
        for name, dt, price in zip(df["name"], df["date"], df["price"])
    ]
    upsert_prices(conn, rows)
    conn.commit()
    print(f"Upserted {len(rows)} rows of World Bank data into commodity_price.")


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
        if upsert_minerba_data(conn, df):
            return False  # some labels had an unknown unit; see above
    except requests.RequestException as e:
        print(f"Error during Minerba scraping: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during Minerba processing: {e}")
        return False
    print("--- Minerba Scraper Finished ---")
    return True


def run_world_bank_scraper(conn):
    """Main function to scrape and store World Bank gold and silver prices."""
    print("\n--- Starting World Bank Scraper (Gold, Silver) ---")
    try:
        url = find_world_bank_file_url()
        print(f"Fetching {url}")
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        df = parse_world_bank_monthly(resp.content)
        print(f"Fetched World Bank data: {len(df)} rows, up to {df['date'].max():%Y-%m}")
        upsert_world_bank_data(conn, df)
    except requests.RequestException as e:
        print(f"Error fetching World Bank data: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during World Bank processing: {e}")
        return False
    print("--- World Bank Scraper Finished ---")
    return True


def backfill_missing_units(conn):
    """Rows older than the scrape window are never re-scraped. Give them the
    currency and unit of the same commodity's newest row, since each series
    is quoted in one unit."""
    newest = """
        SELECT p.{col} FROM commodity_price p
        WHERE p.name = commodity_price.name AND p.price_unit IS NOT NULL
        ORDER BY p.date DESC LIMIT 1
    """
    cur = conn.execute(
        f"""
        UPDATE commodity_price
        SET price_unit = ({newest.format(col="price_unit")}),
            currency   = ({newest.format(col="currency")})
        WHERE price_unit IS NULL
          AND EXISTS (
            SELECT 1 FROM commodity_price p
            WHERE p.name = commodity_price.name AND p.price_unit IS NOT NULL
          )
        """
    )
    conn.execute(
        "UPDATE commodity_price SET price = price_usd_per_ton WHERE price IS NULL"
    )
    print(f"Backfilled price_unit on {cur.rowcount} rows.")

    # Placeholder for series whose real unit was never scraped (e.g. Minerba
    # unreachable). The next successful scrape overwrites it with the real unit.
    missing = conn.execute(
        "SELECT DISTINCT name FROM commodity_price WHERE price_unit IS NULL"
    ).fetchall()
    for (name,) in missing:
        unit = FALLBACK_PRICE_UNITS.get(name, FALLBACK_PRICE_UNIT)
        conn.execute(
            "UPDATE commodity_price SET price_unit = ?, currency = COALESCE(currency, 'USD') "
            "WHERE price_unit IS NULL AND name = ?",
            (unit, name),
        )
        print(f"[!] No scraped unit for {name}; set placeholder price_unit '{unit}'.")
    conn.commit()


def sync_commodity_price():
    db_conn = get_db_conn(DB_PATH)

    minerba_ok = run_minerba_scraper(db_conn)
    world_bank_ok = run_world_bank_scraper(db_conn)
    backfill_missing_units(db_conn)

    db_conn.close()

    print(f"\nProcess complete. Local database updated: {DB_PATH}")

    # Fail the job instead of silently keeping stale prices.
    if not (minerba_ok and world_bank_ok):
        raise RuntimeError("Commodity price scrape failed; see errors above.")

if __name__ == "__main__":
    sync_commodity_price()