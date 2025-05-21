import os
import requests
import pandas as pd
import sqlite3
import re
import json
from io import StringIO
from datetime import date
from dotenv import load_dotenv

# ─── DYNAMIC RANGE ──────────────────────────────────────────────────────────────
today = date.today()
start = date(today.year - 15, today.month, 1)
if today.month == 12:
    next_month, next_year = 1, today.year + 1
else:
    next_month, next_year = today.month + 1, today.year
end = date(next_year, next_month, 1)

START_RANGE = f"{start.month:02d}/{start.year}"
END_RANGE = f"{end.month:02d}/{end.year}"

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
HOME_URL = "https://www.minerba.esdm.go.id/harga_acuan"
DB_PATH = "coal-db.sqlite"
load_dotenv()

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
    pd_ = m.group("period")
    day = 1 if (pd_ is None or pd_ == "Pertama") else 15
    return date(yr, mon, day)


# ─── SCRAPING ──────────────────────────────────────────────────────────────────


def get_csrf_token(session, url):
    resp = session.get(url)
    resp.raise_for_status()
    token = session.cookies.get("csrf_cookie_name")
    if not token:
        raise RuntimeError("CSRF cookie not found!")
    return token


def fetch_pricing_html(session, url, csrf, start, end):
    data = {"csrf_test_name": csrf, "bulan_awal": start, "bulan_akhir": end}
    resp = session.post(
        url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Referer": url},
    )
    resp.raise_for_status()
    return resp.text


def parse_table(html):
    df = pd.read_html(StringIO(html))[0]
    df.replace("-", pd.NA, inplace=True)
    return df


# ─── LOCAL DB SETUP ─────────────────────────────────────────────────────────────


def init_db(path):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS commodity (
            commodity_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT    NOT NULL UNIQUE,
            name_english   TEXT,
            unit           TEXT,
            price          TEXT
        );
        """
    )
    conn.commit()
    return conn


def upsert_local(conn, df):
    c = conn.cursor()
    for _, row in df.iterrows():
        full = row["Komoditas"]
        m = re.match(r"^(.*?)\s*\(([^)]+)\)$", full)
        if not m:
            continue
        name, unit = m.groups()
        # Build price list as JSON
        price_entries = []
        for hdr in df.columns[1:]:
            val = row[hdr]
            if pd.isna(val):
                continue
            dt = parse_header_to_date(str(hdr))
            price_entries.append({dt.isoformat(): str(val)})
        price_json = json.dumps(price_entries, ensure_ascii=False)

        # Upsert into commodity table
        c.execute(
            """
            INSERT INTO commodity (name, name_english, unit, price)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE
              SET unit = excluded.unit,
                  price = excluded.price
            """,
            (name.strip(), None, unit.strip(), price_json),
        )
    conn.commit()


# ─── MAIN ───────────────────────────────────────────────────────────────────


def main():
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0"})
    csrf = get_csrf_token(sess, HOME_URL)

    html = fetch_pricing_html(sess, HOME_URL, csrf, START_RANGE, END_RANGE)
    df = parse_table(html)

    print("Fetched:", df.shape)
    print(df.head())

    conn = init_db(DB_PATH)
    upsert_local(conn, df)
    conn.close()
    print(f"Local DB updated: {DB_PATH}")


if __name__ == "__main__":
    main()
