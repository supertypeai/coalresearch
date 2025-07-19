from libsql_client  import create_client_sync
from dotenv         import load_dotenv
from create         import TABLE_STATEMENTS

import os
import sqlite3
import logging

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO, # Set the logging level
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")

# Resolve local SQLite path relative to this script (<project>/turso/...)
SCRIPT_DIR = os.path.dirname(__file__)
LOCAL_DB_PATH = 'db.sqlite'

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
    "global_commodity_data",
    "mining_license",
    "mining_license_auctions",
    "mining_news"
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
    "global_commodity_data": ["id"],
    "mining_license": ["id"],
    "mining_license_auctions": ["nomor"],
    "mining_news": ["source"]
}


def get_turso_credentials() -> tuple[str, str]:
    """
    Retrieve Turso database URL and auth token from environment variables.
    If not set, print an error message and exit.
    
    Returns:
        tuple[str, str]: A tuple containing the raw database URL and auth token.
    """
    raw_url = os.getenv("TURSO_DATABASE_URL", "")
    auth_token = os.getenv("TURSO_AUTH_TOKEN")

    if not raw_url or not auth_token:
        LOGGER.info("Missing Turso credentials, exiting.")
        exit(1)
        
    return raw_url, auth_token


def normalize_db_url(raw_url: str) -> str:
    """ 
    Normalize the raw Turso URL to a format suitable for HTTP access. 
    
    Args: 
        raw_url (str): The raw Turso database URL.
        
    Returns:
        str: Normalized URL for HTTP access.
    """
    if raw_url.startswith("wss"):
        db_url = raw_url.replace('wss', 'https')
    elif raw_url.startswith("libsql"):
        db_url = raw_url.replace('libsql', 'https')
    else:
        db_url = raw_url
        
    db_url = db_url.rstrip("/")
    return db_url


def turso_execute(client, sql: str, *params):
    """
    Send a single SQL statement to Turso via libsql_client.
    
    Args:
        client (libsql_client): The Turso client to execute SQL commands.
        sql (str): The SQL statement to execute.
        *params: Parameters to bind to the SQL statement.
    """
    res = client.execute(sql, params)
    return res


def get_sqlite_rows(conn: sqlite3.Connection, table: str) -> list:
    """
    Fetch all rows from SQLite as list of dicts.
    
    Args:
        conn (sqlite3.Connection): The SQLite connection object.
        table (str): The name of the table to fetch data from.
    """
    data_table = conn.execute(f"SELECT * FROM {table}")
    cols = [data[0] for data in data_table.description]
    return [dict(zip(cols, row)) for row in data_table.fetchall()]


def upsert_table(client, table: str, rows: list):
    """
    Build and execute an UPSERT statement for every row.
    
    Args: 
        client (libsql_client): The Turso client to execute SQL commands.
        table (str): The name of the table to upsert data into.
        rows (list): A list of dictionaries representing the rows to upsert.
    """
    if not rows:
        LOGGER.info(f"[{table}] no rows, skipping.")
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
        turso_execute(client, sql, *params)
        
    LOGGER.info(f"[{table}] upserted {len(rows)} rows.")

def replace_table(client, table: str, rows: list):
    """
    Completely replaces all data in a specified table by dropping the existing table, recreating it,
    and then inserting the provided new rows.

    Args: 
        client (libsql_client): The Turso client to execute SQL commands.
        table (str): The name of the table to upsert data into.
        rows (list): A list of dictionaries representing the rows to upsert.
    """
    if not rows:
        LOGGER.info(f"[{table}] no rows, skipping.")
        return

    cols = list(rows[0].keys())
    col_list = ", ".join(cols)
    placeholders = ", ".join("?" for _ in cols)

    # 1) Drop table
    client.execute(f"DROP TABLE IF EXISTS {table};")
    
    # 2) Create table
    table_map = {
        "company_ownership": 1,
        "mining_contract": 4
    }
    sql_create = TABLE_STATEMENTS[table_map[table]]
    client.execute(sql_create)

    # 4) Insert
    sql_insert = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
    """.strip()


    for row in rows:
        params = [row[c] for c in cols]
        turso_execute(client, sql_insert, *params)
        
    LOGGER.info(f"[{table}] inserted {len(rows)} rows.")

def main():
    """ 
    Main function to sync data from SQLite to Turso.
    This function retrieves the database URL and auth token from environment variables,
    normalizes the URL, and then creates a synchronous client to execute the sync operations.
    """
    db_url, auth_token = get_turso_credentials()
    db_url_normalized = normalize_db_url(db_url)
    
    # Create Turso HTTP client
    client = create_client_sync(url=db_url_normalized, auth_token=auth_token)

    # 1) Verify Turso connectivity
    try:
        LOGGER.info("Verifying Turso connection…")
        turso_execute(client, "SELECT 1;")
        LOGGER.info("OK")
    except Exception as e:
        LOGGER.error(f"\nERROR: Unable to reach Turso: {e}")
        client.close()
        return

    # 2) Define which table to upsert and replace
    TO_REPLACE_TABLES = [
        "company_ownership",
        "mining_contract"
    ]
    TO_UPSERT_TABLES = [tbl for tbl in TABLES if tbl not in TO_REPLACE_TABLES]

    conn = None
    try:
        # 3) Open SQLite
        conn = sqlite3.connect(LOCAL_DB_PATH)
        LOGGER.info(f"Connected to SQLite at {LOCAL_DB_PATH}")

        # # 4) Sync: upsert table
        # for tbl in TO_UPSERT_TABLES:
        #     try:
        #         LOGGER.info(f"\nSyncing (upsert) {tbl}…")
        #         rows = get_sqlite_rows(conn, tbl)
        #         upsert_table(client, tbl, rows)
        #     except Exception as table_err:
        #         LOGGER.error(f"Error syncing (upsert) '{tbl}': {table_err}")

        # 5) Sync: replace table
        for tbl in TO_REPLACE_TABLES:
            try:
                LOGGER.info(f"\nSyncing (replace) {tbl}…")
                rows = get_sqlite_rows(conn, tbl)
                replace_table(client, tbl, rows)
            except Exception as table_err:
                LOGGER.error(f"Error syncing (replace) '{tbl}': {table_err}")

    except Exception as e:
        LOGGER.error(f"FATAL: {e}")
    finally:
        if conn:
            conn.close()
            LOGGER.info("Closed SQLite connection.")
        client.close()
        LOGGER.info("Closed Turso client.")


if __name__ == "__main__":
    main()
