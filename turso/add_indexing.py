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
    "CREATE INDEX IF NOT EXISTS idx_company_operation_province ON company (operation_province);",
    "CREATE INDEX IF NOT EXISTS idx_company_performance_company_id ON company_performance (company_id);",
    "CREATE INDEX IF NOT EXISTS idx_mining_site_company_id ON mining_site (company_id);",
    # --- High Impact ---
    "CREATE INDEX IF NOT EXISTS idx_company_ownership_parent_id ON company_ownership (parent_company_id);",
    "CREATE INDEX IF NOT EXISTS idx_company_ownership_company_id ON company_ownership (company_id);",
    "CREATE INDEX IF NOT EXISTS idx_company_ownership_parent_child ON company_ownership (parent_company_id, company_id);",  # Composite index
    "CREATE INDEX IF NOT EXISTS idx_mining_contract_mine_owner_id ON mining_contract (mine_owner_id);",
    "CREATE INDEX IF NOT EXISTS idx_mining_contract_contractor_id ON mining_contract (contractor_id);",
    "CREATE INDEX IF NOT EXISTS idx_mining_contract_owner_contractor ON mining_contract (mine_owner_id, contractor_id);",  # Composite index
    "CREATE INDEX IF NOT EXISTS idx_mining_license_company_id ON mining_license (company_id);",
    "CREATE INDEX IF NOT EXISTS idx_company_name ON company (name);",
    # --- Medium Impact ---
    "CREATE INDEX IF NOT EXISTS idx_company_idx_ticker ON company (idx_ticker);",
    "CREATE INDEX IF NOT EXISTS idx_mining_site_name ON mining_site (name);",
    "CREATE INDEX IF NOT EXISTS idx_mining_license_type ON mining_license (license_type);",
    "CREATE INDEX IF NOT EXISTS idx_mining_license_province ON mining_license (province);",
    "CREATE INDEX IF NOT EXISTS idx_mining_license_company_name ON mining_license (company_name);",
    "CREATE INDEX IF NOT EXISTS idx_export_destination_country ON export_destination (country);",
    "CREATE INDEX IF NOT EXISTS idx_export_destination_year ON export_destination (year);",
    "CREATE INDEX IF NOT EXISTS idx_export_destination_commodity_type ON export_destination (commodity_type);",
    "CREATE INDEX IF NOT EXISTS idx_resources_reserves_province ON resources_and_reserves (province);",
    "CREATE INDEX IF NOT EXISTS idx_resources_reserves_year ON resources_and_reserves (year);",
    "CREATE INDEX IF NOT EXISTS idx_resources_reserves_commodity_type ON resources_and_reserves (commodity_type);",
    "CREATE INDEX IF NOT EXISTS idx_total_commodities_production_commodity_type ON total_commodities_production (commodity_type);",
    "CREATE INDEX IF NOT EXISTS idx_total_commodities_production_year ON total_commodities_production (year);",
    "CREATE INDEX IF NOT EXISTS idx_global_commodity_data_country ON global_commodity_data (country);",
    "CREATE INDEX IF NOT EXISTS idx_global_commodity_data_commodity_type ON global_commodity_data (commodity_type);",
    "CREATE INDEX IF NOT EXISTS idx_commodity_name ON commodity (name);",
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
