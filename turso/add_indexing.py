import os
from dotenv import load_dotenv
from libsql_client import create_client_sync

load_dotenv()  # load variables from .env

# Load Turso URL and auth token from environment
raw_url = os.getenv("TURSO_DATABASE_URL", "")
auth_token = os.getenv("TURSO_AUTH_TOKEN")

if not raw_url or not auth_token:
    print("Missing Turso credentials, exiting.")
    exit(1)

# Normalize URL for HTTP access
if raw_url.startswith("wss://"):
    db_url = "https://" + raw_url[len("wss://") :]
elif raw_url.startswith("libsql://"):
    db_url = "https://" + raw_url[len("libsql://") :]
else:
    db_url = raw_url

# Index creation statements, sorted by impact ranking
INDEX_STATEMENTS = [
    # --- Highest Impact ---
    "CREATE INDEX IF NOT EXISTS idx_company_performance_company_id ON company_performance (company_id);",
    "CREATE INDEX IF NOT EXISTS idx_mining_site_company_id ON mining_site (company_id);",
    "CREATE INDEX IF NOT EXISTS idx_company_idx_ticker ON company (idx_ticker);",
    "CREATE INDEX IF NOT EXISTS idx_mining_license_company_id ON mining_license (company_id);",
    # --- High Impact ---
    "CREATE INDEX IF NOT EXISTS idx_company_ownership_parent_id ON company_ownership (parent_company_id);",
    "CREATE INDEX IF NOT EXISTS idx_company_ownership_company_id ON company_ownership (company_id);",
    "CREATE INDEX IF NOT EXISTS idx_mining_contract_mine_owner_id ON mining_contract (mine_owner_id);",
    "CREATE INDEX IF NOT EXISTS idx_mining_contract_contractor_id ON mining_contract (contractor_id);",
    "CREATE INDEX IF NOT EXISTS idx_company_name ON company (name);",
    "CREATE INDEX IF NOT EXISTS idx_sales_destination_country_year ON sales_destination (country, year);",
    "CREATE INDEX IF NOT EXISTS idx_company_financials_company_id_year ON company_financials (company_id, year);",
]


def main():
    client = create_client_sync(url=db_url, auth_token=auth_token)

    try:
        # Create indexes
        print("Attempting to create/verify indexes...")
        for idx_sql in INDEX_STATEMENTS:
            if idx_sql.strip():  # Ensure no empty strings are executed
                client.execute(idx_sql)
                print(f"Executed: {idx_sql.strip()}")
        print("All specified indexes created (if they did not already exist).")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
