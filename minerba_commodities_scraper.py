import requests
import pandas as pd
import sqlite3
import re
from io import StringIO
from datetime import date

# ─── DYNAMIC RANGE ──────────────────────────────────────────────────────────────
today = date.today()
# Start = same month, 10 years ago
start = date(today.year - 15, today.month, 1)
# End = first day of next month
if today.month == 12:
    next_month = 1
    next_year = today.year + 1
else:
    next_month = today.month + 1
    next_year = today.year
end = date(next_year, next_month, 1)

START_RANGE = f"{start.month:02d}/{start.year}"  # e.g. "05/2015"
END_RANGE = f"{end.month:02d}/{end.year}"  # e.g. "06/2025"

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
HOME_URL = "https://www.minerba.esdm.go.id/harga_acuan"
DB_PATH = "coal-db.sqlite"


# ─── HELPERS ────────────────────────────────────────────────────────────────────
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


def parse_header_to_date(header: str) -> date:
    m = HEADER_RE.match(header)
    if not m:
        raise ValueError(f"Unexpected header format: {header!r}")
    mon = MONTH_MAP[m.group("month")]
    yr = int(m.group("year"))
    period = m.group("period")
    day = 1 if (period is None or period == "Pertama") else 15
    return date(yr, mon, day)


# ─── STEP 1: SCRAPE ────────────────────────────────────────────────────────────
def get_csrf_token(session: requests.Session, url: str) -> str:
    resp = session.get(url)
    resp.raise_for_status()
    token = session.cookies.get("csrf_cookie_name")
    if not token:
        raise RuntimeError("CSRF cookie not found!")
    return token


def fetch_pricing_html(
    session: requests.Session, url: str, csrf_token: str, start: str, end: str
) -> str:
    data = {
        "csrf_test_name": csrf_token,
        "bulan_awal": start,
        "bulan_akhir": end,
    }
    resp = session.post(
        url,
        data=data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": url,
        },
    )
    resp.raise_for_status()
    return resp.text


def parse_table(html: str) -> pd.DataFrame:
    # wrap in StringIO to avoid the FutureWarning
    df = pd.read_html(StringIO(html))[0]
    # fix the '-' strings→NaN so float(val) won't blow up
    df.replace("-", pd.NA, inplace=True)
    return df


# ─── STEP 2: DATABASE SETUP ───────────────────────────────────────────────────
def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS commodities (
            commodity_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT    NOT NULL UNIQUE,
            unit           TEXT    NOT NULL
        );
    """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS commodity_prices (
            price_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_id   INTEGER NOT NULL
                             REFERENCES commodities(commodity_id),
            price_date     DATE    NOT NULL,
            price_value    REAL    NOT NULL,
            info           TEXT,
            UNIQUE(commodity_id, price_date)
        );
    """
    )
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prices_commodity_date
            ON commodity_prices (commodity_id, price_date);
    """
    )
    conn.commit()
    return conn


# ─── STEP 3: UPSERT DATA ──────────────────────────────────────────────────────
def upsert_commodities(conn: sqlite3.Connection, df: pd.DataFrame):
    c = conn.cursor()
    for full in df["Komoditas"]:
        match = re.match(r"^(.*?)\s*\(([^)]+)\)$", full)
        if not match:
            continue
        name, unit = match.groups()
        c.execute(
            """
            INSERT OR IGNORE INTO commodities (name, unit)
             VALUES (?, ?)
        """,
            (name.strip(), unit.strip()),
        )
    conn.commit()


def upsert_prices(conn: sqlite3.Connection, df: pd.DataFrame):
    c = conn.cursor()
    c.execute("SELECT commodity_id, name FROM commodities")
    id_map = {row[1]: row[0] for row in c.fetchall()}

    for _, row in df.iterrows():
        full = row["Komoditas"]
        name = re.match(r"^(.*?)\s*\(", full).group(1).strip()
        commodity_id = id_map.get(name)
        if commodity_id is None:
            continue

        for hdr in df.columns[1:]:
            val = row[hdr]
            if pd.isna(val):
                continue
            dt = parse_header_to_date(hdr)
            c.execute(
                """
                INSERT INTO commodity_prices
                  (commodity_id, price_date, price_value, info)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(commodity_id, price_date) DO UPDATE
                  SET price_value = excluded.price_value
            """,
                (commodity_id, dt.isoformat(), float(val), None),
            )
    conn.commit()


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    csrf = get_csrf_token(sess, HOME_URL)

    html = fetch_pricing_html(sess, HOME_URL, csrf, START_RANGE, END_RANGE)
    df = parse_table(html)
    print("Parsed table shape:", df.shape)
    print(df.head())  # ← actually call head()

    conn = init_db(DB_PATH)
    upsert_commodities(conn, df)
    upsert_prices(conn, df)
    conn.close()
    print("Data successfully saved to", DB_PATH)


if __name__ == "__main__":
    main()
