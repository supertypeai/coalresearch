import os
from libsql_client import create_client_sync
from dotenv import load_dotenv  # Import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
    print(f"\n{'='*60}")
    print(f"TABLE: {table_name}")
    print(f"{'='*60}")

    # 1. Get column info (Schema)
    # PRAGMA table_info values: (cid, name, type, notnull, dflt_value, pk)
    pragma = client.execute(f"PRAGMA table_info('{table_name}');")

    print("\n--- Schema ---")
    print(
        f"{'cid':<4} | {'name':<20} | {'type':<15} | {'notnull':<8} | {'dflt':<10} | {'pk':<3}"
    )
    print("-" * 75)

    col_names = []
    for row in pragma.rows:
        # row is a tuple-like object or list depending on the client, usually indexed
        # row[0]=cid, row[1]=name, row[2]=type, row[3]=notnull, row[4]=dflt_value, row[5]=pk
        cid, name, ctype, notnull, dflt, pk = row
        col_names.append(name)
        print(
            f"{str(cid):<4} | {str(name):<20} | {str(ctype):<15} | {str(notnull):<8} | {str(dflt):<10} | {str(pk):<3}"
        )

    # 2. Get Foreign Keys
    # PRAGMA foreign_key_list values: (id, seq, table, from, to, on_update, on_delete, match)
    fk_pragma = client.execute(f"PRAGMA foreign_key_list('{table_name}');")

    if fk_pragma.rows:
        print("\n--- Foreign Keys ---")
        print(
            f"{'id':<3} | {'seq':<3} | {'table':<20} | {'from':<15} | {'to':<15} | {'on_upd':<10} | {'on_del':<10}"
        )
        print("-" * 90)
        for row in fk_pragma.rows:
            fid, seq, table, from_col, to_col, on_upd, on_del, match = row
            print(
                f"{str(fid):<3} | {str(seq):<3} | {str(table):<20} | {str(from_col):<15} | {str(to_col):<15} | {str(on_upd):<10} | {str(on_del):<10}"
            )
    else:
        print("\n--- Foreign Keys: None ---")


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
            fetch_and_print(tbl, limit=1)

finally:
    client.close()
