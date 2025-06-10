import os
from dotenv import load_dotenv
from libsql_client import create_client_sync

load_dotenv()  # load variables from .env

# Load Turso URL and auth token from environment
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN")

if not TURSO_DATABASE_URL or not TURSO_AUTH_TOKEN:
    raise RuntimeError(
        "Please set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN environment variables."
    )

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
        mining_license TEXT
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
        company_id INTEGER,
        year INTEGER,
        commodity_type TEXT,
        commodity_sub_type TEXT,
        mining_operation_status TEXT,
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
    CREATE TABLE IF NOT EXISTS commodity (
        commodity_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        name TEXT NOT NULL,
        name_english TEXT,
        unit TEXT,
        price TEXT
    );
    """,
]


def main():
    client = create_client_sync(url=TURSO_DATABASE_URL, auth_token=TURSO_AUTH_TOKEN)

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
