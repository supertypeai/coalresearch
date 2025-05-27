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


def fetch_and_print(table_name: str, limit: int = 5):
    # Get column info
    pragma = client.execute(f"PRAGMA table_info('{table_name}');")
    cols = [col_row[1] for col_row in pragma.rows]

    # Fetch sample rows
    sample = client.execute(f"SELECT * FROM {table_name} LIMIT {limit};")

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
    # 1) Discover all tables
    result = client.execute(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    )
    table_names = [row[0] for row in result.rows]

    if not table_names:
        print("No user tables found.")
    else:
        for tbl in table_names:
            fetch_and_print(tbl, limit=5)

finally:
    client.close()
