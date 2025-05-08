import os
from libsql_client import create_client_sync

# Grab whatever they gave you…
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


def fetch_and_print(table_name: str, limit: int = 5):
    # Fetch sample rows
    sample = client.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
    # Get column names via PRAGMA
    pragma = client.execute(f"PRAGMA table_info('{table_name}');")
    cols = [col_row[1] for col_row in pragma.rows]

    # Print header
    header = " | ".join(cols)
    separator = "-" * len(header)
    print(f"\nTable: {table_name}")
    print(header)
    print(separator)

    # Print rows
    for row in sample.rows:
        print(" | ".join(str(item) for item in row))


try:
    # New: commodities
    fetch_and_print("commodities", limit=5)

    # New: commodity_price
    fetch_and_print("commodity_prices", limit=5)

finally:
    client.close()
