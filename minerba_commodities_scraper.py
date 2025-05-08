import os
import requests
import pandas as pd
import sqlite3
import re
from io import StringIO
from datetime import date
from libsql_client import create_client_sync
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
    CREATE TABLE IF NOT EXISTS commodities (
      commodity_id INTEGER PRIMARY KEY AUTOINCREMENT,
      name         TEXT NOT NULL UNIQUE,
      unit         TEXT NOT NULL
    );"""
    )
    c.execute(
        """
    CREATE TABLE IF NOT EXISTS commodity_prices (
      price_id     INTEGER PRIMARY KEY AUTOINCREMENT,
      commodity_id INTEGER NOT NULL REFERENCES commodities(commodity_id),
      price_date   DATE    NOT NULL,
      price_value  REAL    NOT NULL,
      info         TEXT,
      UNIQUE(commodity_id, price_date)
    );"""
    )
    c.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_prices_commodity_date
      ON commodity_prices (commodity_id, price_date);
    """
    )
    conn.commit()
    return conn


def upsert_local(conn, df):
    c = conn.cursor()
    # commodities
    for full in df["Komoditas"]:
        m = re.match(r"^(.*?)\s*\(([^)]+)\)$", full)
        if not m:
            continue
        name, unit = m.groups()
        c.execute(
            "INSERT OR IGNORE INTO commodities(name,unit) VALUES(?,?)",
            (name.strip(), unit.strip()),
        )
    conn.commit()

    # prices
    c.execute("SELECT commodity_id,name FROM commodities")
    id_map = {name: cid for cid, name in c.fetchall()}
    for _, row in df.iterrows():
        name = re.match(r"^(.*?)\s*\(", row["Komoditas"]).group(1).strip()
        cid = id_map.get(name)
        if not cid:
            continue
        for hdr in df.columns[1:]:
            val = row[hdr]
            if pd.isna(val):
                continue
            dt = parse_header_to_date(hdr)
            c.execute(
                """
            INSERT INTO commodity_prices(commodity_id,price_date,price_value,info)
            VALUES(?,?,?,?)
            ON CONFLICT(commodity_id,price_date) DO UPDATE
              SET price_value=excluded.price_value
            """,
                (cid, dt.isoformat(), float(val), None),
            )
    conn.commit()


def sync_two_tables_to_turso(sqlite_path: str):
    """
    Bulk-sync 'commodities' and 'commodity_prices' from local SQLite to Turso using HTTP client.

    Parameters:
      sqlite_path: Local SQLite DB file path
    """
    print("Starting sync_two_tables_to_turso function...")

    # Retrieve Turso credentials
    turso_url = os.getenv("TURSO_DATABASE_URL") or ""
    auth_token = os.getenv("TURSO_AUTH_TOKEN")

    # Normalize URL
    if turso_url.startswith("wss://"):
        db_url = "https://" + turso_url[len("wss://") :]
    elif turso_url.startswith("libsql://"):
        db_url = "https://" + turso_url[len("libsql://") :]
    else:
        db_url = turso_url

    print(f"TURSO_DATABASE_URL: {db_url}")
    print(f"TURSO_AUTH_TOKEN present: {auth_token is not None}")

    if not db_url or not auth_token:
        print("Skipping Turso sync: missing Turso credentials.")
        return

    client = None
    conn = None
    try:
        print(f"Debug: Using HTTP client against {db_url}")
        client = create_client_sync(url=db_url, auth_token=auth_token)
        print("Turso client created successfully.")

        # Ensure schema
        schema_stmts = [
            "CREATE TABLE IF NOT EXISTS commodities (commodity_id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, unit TEXT NOT NULL);",
            "CREATE TABLE IF NOT EXISTS commodity_prices (price_id INTEGER PRIMARY KEY AUTOINCREMENT, commodity_id INTEGER NOT NULL REFERENCES commodities(commodity_id), price_date DATE NOT NULL, price_value REAL NOT NULL, info TEXT, UNIQUE(commodity_id, price_date));",
        ]
        for stmt in schema_stmts:
            print(f"Debug: DDL: {stmt}")
            client.execute(stmt)
            print(f"Schema statement executed successfully: {stmt[:50]}...")

        # Read local data
        print(f"Connecting to local SQLite database at {sqlite_path}")
        conn = sqlite3.connect(sqlite_path)
        cur = conn.cursor()

        print("Fetching commodities from SQLite...")
        local_coms = cur.execute(
            "SELECT commodity_id, name, unit FROM commodities"
        ).fetchall()
        print(f"Found {len(local_coms)} commodities.")

        print("Fetching commodity prices from SQLite...")
        local_prices = cur.execute(
            "SELECT commodity_id, price_date, price_value, info FROM commodity_prices"
        ).fetchall()
        print(f"Found {len(local_prices)} commodity prices.")

        # Clear remote
        print("Clearing remote tables on Turso...")
        client.execute("DELETE FROM commodity_prices;")
        client.execute("DELETE FROM commodities;")
        print("Remote tables cleared.")

        # Insert commodities
        print("Inserting commodities into Turso...")
        for _, name, unit in local_coms:
            client.execute(
                "INSERT OR IGNORE INTO commodities(name,unit) VALUES(?,?);",
                (name, unit),
            )
        print("Finished inserting commodities.")

        # Fetch remote IDs
        print("Fetching remote commodity IDs...")
        res = client.execute("SELECT commodity_id, name FROM commodities;")
        remote_ids = {row["name"]: row["commodity_id"] for row in res.rows}
        print(f"Fetched {len(remote_ids)} remote commodity mappings.")

        # Map local ID to name
        id_to_name = {cid: name for cid, name, _ in local_coms}

        # Insert prices
        print("Inserting commodity prices into Turso...")
        for loc_cid, pd, pv, info in local_prices:
            name = id_to_name.get(loc_cid)
            rcid = remote_ids.get(name)
            if not rcid:
                print(
                    f"⚠️ Missing remote ID for commodity ID={loc_cid}, name={name}. Skipping price entry."
                )
                continue
            client.execute(
                "INSERT OR REPLACE INTO commodity_prices(commodity_id, price_date, price_value, info) VALUES(?,?,?,?);",
                (rcid, pd, pv, info),
            )
        print("Finished inserting commodity prices.")

    except Exception as e:
        print(f"Error during Turso sync: {e}")
        raise
    finally:
        # Clean up resources
        if conn:
            conn.close()
            print("Closed connection to local SQLite database.")
        if client:
            client.close()
            print("Closed Turso client.")

    print("✅ Turso sync complete")


# ─── MAIN ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────


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

    sync_two_tables_to_turso(DB_PATH)


if __name__ == "__main__":
    main()
