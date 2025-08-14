from dotenv import load_dotenv
from libsql_client import create_client_sync

import os

# load variables from .env
load_dotenv()

TABLE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS company (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        name TEXT NOT NULL,
        idx_ticker TEXT,
        operation_province TEXT,
        operation_kabkot TEXT,
        representative_address TEXT,
        company_type TEXT,
        key_operation TEXT,
        activities TEXT,
        website TEXT,
        phone_number TEXT,
        email TEXT,
        mining_license TEXT,
        mining_contract TEXT,
        commodity TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company_ownership (
        parent_company_id INTEGER NOT NULL,
        company_id INTEGER NOT NULL,
        percentage_ownership INTEGER NOT NULL,
        PRIMARY KEY (parent_company_id, company_id),
        FOREIGN KEY (parent_company_id)
          REFERENCES company(id)
            ON UPDATE NO ACTION
            ON DELETE NO ACTION,
        FOREIGN KEY (company_id)
          REFERENCES company(id)
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company_performance (
        id INTEGER PRIMARY KEY NOT NULL,
        company_id INTEGER NOT NULL,
        year INTEGER,
        commodity_type TEXT,
        commodity_sub_type TEXT,
        commodity_stats TEXT,
        FOREIGN KEY (company_id)
          REFERENCES company(id)
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS export_destination (
        id INTEGER PRIMARY KEY NOT NULL,
        country TEXT NOT NULL,
        year INTEGER NOT NULL,
        commodity_type TEXT,
        export_USD REAL,
        export_volume_BPS REAL,
        export_volume_ESDM REAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mining_contract (
        mine_owner_id INTEGER NOT NULL,
        contractor_id INTEGER NOT NULL,
        contract_period_end TEXT,
        PRIMARY KEY (mine_owner_id, contractor_id),
        FOREIGN KEY (mine_owner_id)
          REFERENCES company(id)
            ON UPDATE NO ACTION
            ON DELETE NO ACTION,
        FOREIGN KEY (contractor_id)
          REFERENCES company(id)
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mining_site (
        id INTEGER PRIMARY KEY NOT NULL,
        name TEXT NOT NULL,
        project_name TEXT,
        year INTEGER,
        mineral_type TEXT,
        company_id INTEGER,
        production_volume REAL,
        overburden_removal_volume REAL,
        strip_ratio REAL,
        resources_reserves TEXT,
        location TEXT,
        FOREIGN KEY (company_id)
          REFERENCES company(id)
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS resources_and_reserves (
        id INTEGER PRIMARY KEY NOT NULL,
        province TEXT,
        year INTEGER NOT NULL,
        commodity_type TEXT,
        exploration_target_1 REAL,
        total_inventory_1 REAL,
        resources_inferred REAL,
        resources_indicated REAL,
        resources_measured REAL,
        resources_total REAL,
        verified_resources_2 REAL,
        reserves_1 REAL,
        verified_reserves_2 REAL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS total_commodities_production (
        id INTEGER PRIMARY KEY NOT NULL,
        commodity_type TEXT,
        production_volume REAL,
        unit TEXT,
        year INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS commodity_price (
        commodity_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        name TEXT NOT NULL,
        price TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS global_commodity_data (
        id                INTEGER PRIMARY KEY,
        country           TEXT    NOT NULL,
        resources_reserves TEXT,
        export_import     TEXT,
        production_volume TEXT,
        commodity_type    TEXT    NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS mining_license (
        id TEXT PRIMARY KEY NOT NULL,
        license_type TEXT,
        license_number TEXT,
        province TEXT,
        city TEXT,
        permit_effective_date TEXT,
        permit_expiry_date TEXT,
        activity TEXT,
        licensed_area INTEGER,
        location TEXT,
        commodity TEXT,
        company_name TEXT,
        company_id TEXT,
        FOREIGN KEY (company_id) REFERENCES company(id)
    );
    """,
    """ 
    CREATE TABLE IF NOT EXISTS mining_license_auctions (
        id INTEGER PRIMARY KEY NOT NULL,
        commodity TEXT,
        city TEXT,
        province TEXT,
        company_name TEXT,
        date_winner TEXT,
        luas_sk REAL,
        nomor TEXT UNIQUE,  --  unique identifier
        jenis_izin TEXT,
        kdi TEXT,
        code_wiup TEXT,
        auction_status TEXT,
        created_at TEXT,
        last_modified TEXT,
        jumlah_peserta INTEGER,
        tahapan TEXT,
        peserta TEXT,
        winner TEXT
    )
    """,
    """ 
    CREATE TABLE IF NOT EXISTS mining_news (
        id INTEGER PRIMARY KEY, 
        title TEXT NOT NULL,
        body TEXT,
        source TEXT,
        timestamp TEXT,
        commodities TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS sales_destination (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER,
        country TEXT NOT NULL,
        idx_ticker TEXT NOT NULL,
        year INTEGER NOT NULL,
        revenue REAL,
        percentage_of_total_revenue REAL,
        volume REAL,
        percentage_of_sales_volume REAL,
        UNIQUE(country, idx_ticker, year),
        FOREIGN KEY (company_id) REFERENCES company(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS company_financials (
        company_id INTEGER,
        idx_ticker TEXT,
        name TEXT,
        year INTEGER,
        assets REAL,
        revenue REAL,
        revenue_breakdown TEXT CHECK (json_valid(revenue_breakdown)),
        cost_of_revenue REAL,
        cost_of_revenue_breakdown TEXT CHECK (json_valid(cost_of_revenue_breakdown)),
        net_profit REAL,
        PRIMARY KEY (idx_ticker, year)
        FOREIGN KEY (company_id) REFERENCES company(id)
    );
    """,
]


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
        print("Missing Turso credentials, exiting.")
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
        db_url = raw_url.replace("wss", "https")
    elif raw_url.startswith("libsql"):
        db_url = raw_url.replace("libsql", "https")
    else:
        db_url = raw_url
    db_url = db_url.rstrip("/")
    return db_url


def main():
    """
    Main function to create tables in the Turso database.
    This function retrieves the database URL and auth token from environment variables,
    normalizes the URL, and then creates a synchronous client to execute the table creation statements.
    """
    db_url, auth_token = get_turso_credentials()
    db_url_normalized = normalize_db_url(db_url)
    client = create_client_sync(url=db_url_normalized, auth_token=auth_token)

    try:
        for sql in TABLE_STATEMENTS:
            client.execute(sql)
        print("All tables created (if they did not already exist).")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
