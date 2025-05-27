import os
import sqlite3
from libsql_client import create_client_sync
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Raw Turso URL & auth token
raw_url = os.getenv("TURSO_DATABASE_URL", "")
auth_token = os.getenv("TURSO_AUTH_TOKEN", "")

if not raw_url or not auth_token:
    print("ERROR: TURSO_DATABASE_URL and TURSO_AUTH_TOKEN must be set")
    exit(1)

# Normalize URL for the libsql client
if raw_url.startswith("wss://"):
    db_url = "https://" + raw_url[len("wss://") :]
elif raw_url.startswith("libsql://"):
    db_url = "https://" + raw_url[len("libsql://") :]
else:
    db_url = raw_url
db_url = db_url.rstrip("/")

# Create Turso HTTP client
client = create_client_sync(url=db_url, auth_token=auth_token)

# Resolve local SQLite path relative to this script (<project>/turso/...)
SCRIPT_DIR = os.path.dirname(__file__)
LOCAL_DB_PATH = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "db.sqlite"))

# Tables to sync, in dependency order
TABLES = [
    "company",
    "company_ownership",
    "company_performance",
    "export_destination",
    "mining_contract",
    "mining_site",
    "resources_and_reserves",
    "total_commodities_production",
    "commodity",
]

# Primary-key columns for each table
CONFLICT_TARGET = {
    "company": ["id"],
    "company_ownership": ["parent_company_id", "company_id"],
    "company_performance": ["id"],
    "export_destination": ["id"],
    "mining_contract": ["mine_owner_id", "contractor_id"],
    "mining_site": ["id"],
    "resources_and_reserves": ["id"],
    "total_commodities_production": ["id"],
    "commodity": ["commodity_id"],
}


def turso_execute(sql: str, *params):
    """Send a single SQL statement to Turso via libsql_client."""
    res = client.execute(sql, params)
    return res


def get_sqlite_rows(conn: sqlite3.Connection, table: str):
    """Fetch all rows from SQLite as list of dicts."""
    cur = conn.execute(f"SELECT * FROM {table}")
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def upsert_table(table: str, rows: list):
    """Build and execute an UPSERT statement for every row."""
    if not rows:
        print(f"[{table}] no rows, skipping.")
        return

    cols = list(rows[0].keys())
    col_list = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)

    pk_cols = CONFLICT_TARGET[table]
    conflict_clause = ", ".join(pk_cols)
    update_clause = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in pk_cols)

    sql = f"""
INSERT INTO {table} ({col_list})
VALUES ({placeholders})
ON CONFLICT({conflict_clause})
DO UPDATE SET {update_clause};
""".strip()

    for row in rows:
        params = [row[c] for c in cols]
        turso_execute(sql, *params)
    print(f"[{table}] upserted {len(rows)} rows.")


def main():
    # 1) Verify Turso connectivity
    try:
        print("Verifying Turso connection…", end=" ")
        turso_execute("SELECT 1;")
        print("OK")
    except Exception as e:
        print(f"\nERROR: Unable to reach Turso: {e}")
        client.close()
        return

    conn = None
    try:
        # 2) Open SQLite
        conn = sqlite3.connect(LOCAL_DB_PATH)
        print(f"Connected to SQLite at {LOCAL_DB_PATH}")

        # 3) Sync each table
        for tbl in TABLES:
            try:
                print(f"\nSyncing {tbl}…")
                rows = get_sqlite_rows(conn, tbl)
                upsert_table(tbl, rows)
            except Exception as table_err:
                print(f"Error syncing '{tbl}': {table_err}")

    except Exception as e:
        print(f"FATAL: {e}")
    finally:
        if conn:
            conn.close()
            print("Closed SQLite connection.")
        client.close()
        print("Closed Turso client.")


if __name__ == "__main__":
    main()
