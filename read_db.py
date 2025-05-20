import os
from libsql_client import create_client_sync
import sqlite3

DEV_DB_FILE = "coal-db.sqlite"

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
            print(f"\nStructure of `{table}`:")
            cursor.execute(f"PRAGMA table_info('{table}');")
            cols = cursor.fetchall()  # (cid, name, type, notnull, dflt_value, pk)

            col_names = [col[1] for col in cols]
            header = " | ".join(col_names)
            separator = "-" * len(header)

            print("cid | name | type | notnull | default | pk")
            print("-" * 50)
            for cid, name, col_type, notnull, dflt, pk in cols:
                print(f"{cid} | {name} | {col_type} | {notnull} | {dflt} | {pk}")

            # Sample data
            cursor.execute(f"SELECT * FROM '{table}' LIMIT {sample_limit};")
            rows = cursor.fetchall()
            if rows:
                print(f"\nSample data from `{table}` (up to {sample_limit} rows):")
                print(header)
                print(separator)
                for row in rows:
                    print(" | ".join(str(val) for val in row))
            else:
                print(f"\n`{table}` is empty.")

            # Indexes
            cursor.execute(f"PRAGMA index_list('{table}');")
            index_list = cursor.fetchall()  # (seq, name, unique, origin, partial)
            if index_list:
                print(f"\nIndexes on `{table}`:")
                print("seq | name | unique | origin | partial")
                print("-" * 50)
                for seq, idx_name, unique, origin, partial in index_list:
                    print(f"{seq} | {idx_name} | {unique} | {origin} | {partial}")
                    cursor.execute(f"PRAGMA index_info('{idx_name}');")
                    idx_info = cursor.fetchall()  # (seqno, cid, name)
                    cols = [info[2] for info in idx_info]
                    print(f"    -> Columns: {', '.join(cols)}")
            else:
                print(f"\nNo indexes found on `{table}`.")

            # Foreign keys
            cursor.execute(f"PRAGMA foreign_key_list('{table}');")
            fkeys = (
                cursor.fetchall()
            )  # (id, seq, table, from, to, on_update, on_delete, match)
            if fkeys:
                print(f"\nForeign keys for `{table}`:")
                print(
                    "id | seq | foreign_table | from_column | to_column | on_update | on_delete | match"
                )
                print("-" * 80)
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
                        f"{fid} | {seq} | {ref_table} | {from_col} | {to_col} | {on_upd} | {on_del} | {match}"
                    )
            else:
                print(f"\nNo foreign keys defined on `{table}`.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    try:
        list_tables_with_structure_and_indexes(DEV_DB_FILE, 5)
    finally:
        client.close()
