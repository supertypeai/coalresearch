import os
from libsql_client import create_client_sync
import sqlite3

DEV_DB_FILE = "db.sqlite"

# Grab whatever they gave you…
# raw_url = os.getenv("TURSO_DATABASE_URL", "")
# auth_token = os.getenv("TURSO_AUTH_TOKEN")

# if not raw_url or not auth_token:
#     print("Missing Turso credentials, exiting.")
#     exit(1)

# # Normalize URL for HTTP access
# if raw_url.startswith("wss://"):
#     db_url = "https://" + raw_url[len("wss://") :]
# elif raw_url.startswith("libsql://"):
#     db_url = "https://" + raw_url[len("libsql://") :]
# else:
#     db_url = raw_url

# client = create_client_sync(url=db_url, auth_token=auth_token)


def list_tables_with_structure_and_indexes(db_path: str, sample_limit: int = 5):
    """
    Connect to the SQLite file at `db_path`, list all user tables,
    print each table's column definitions, show up to `sample_limit`
    rows of sample data, display all indexes, and show foreign keys
    for each table.
    """
    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1) Get all user-defined table names
    cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%';
        """
    )
    tables = [row[0] for row in cursor.fetchall()]

    if not tables:
        print("No user tables found.")
    else:
        print("Found tables:")
        for tbl in tables:
            print(f" - {tbl}")

        # 2) For each table: describe schema, sample data, indexes, and FKs
        for table in tables:
            print(f"\n{'='*60}")
            print(f"TABLE: {table}")
            print(f"{'='*60}")

            # 1. Get column info (Schema)
            cursor.execute(f"PRAGMA table_info('{table}');")
            cols = cursor.fetchall()

            print("\n--- Schema ---")
            print(
                f"{'cid':<4} | {'name':<20} | {'type':<15} | {'notnull':<8} | {'dflt':<10} | {'pk':<3}"
            )
            print("-" * 75)

            for cid, name, col_type, notnull, dflt, pk in cols:
                print(
                    f"{str(cid):<4} | {str(name):<20} | {str(col_type):<15} | {str(notnull):<8} | {str(dflt):<10} | {str(pk):<3}"
                )

            # 2. Foreign keys
            cursor.execute(f"PRAGMA foreign_key_list('{table}');")
            fkeys = cursor.fetchall()

            if fkeys:
                print("\n--- Foreign Keys ---")
                print(
                    f"{'id':<3} | {'seq':<3} | {'table':<20} | {'from':<15} | {'to':<15} | {'on_upd':<10} | {'on_del':<10}"
                )
                print("-" * 90)
                # PRAGMA foreign_key_list returns: (id, seq, table, from, to, on_update, on_delete, match)
                for (
                    fid,
                    seq,
                    ref_table,
                    from_col,
                    to_col,
                    on_upd,
                    on_del,
                    match,
                ) in fkeys:
                    print(
                        f"{str(fid):<3} | {str(seq):<3} | {str(ref_table):<20} | {str(from_col):<15} | {str(to_col):<15} | {str(on_upd):<10} | {str(on_del):<10}"
                    )
            else:
                print("\n--- Foreign Keys: None ---")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    list_tables_with_structure_and_indexes(DEV_DB_FILE, 5)
