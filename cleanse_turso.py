import os
from libsql_client import create_client_sync

# Grab Turso credentials from environment
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

client = create_client_sync(url=db_url, auth_token=auth_token)

# List of tables you want to drop
tables_to_drop = [
    "commodities",
    "commodity_prices",
    "company_ownership",
    "mining_site",
    "coal_company_performance",
    "company",
]

try:
    for tbl in tables_to_drop:
        # Safely drop each table if it exists
        print(f"Dropping table `{tbl}` (if exists)...")
        client.execute(f"DROP TABLE IF EXISTS {tbl};")
    print("Done.")
finally:
    client.close()
