import os
import sys
import argparse
import hashlib
import json
import sqlite3
import datetime
from decimal import Decimal
from dotenv import load_dotenv
from libsql_client import create_client_sync, Statement

# --- CONFIGURATION ---
BATCH_SIZE = 500
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
DB_PATH = os.path.join(parent_dir, "db.sqlite")

# Define table order (Parent -> Child)
PRIORITY_TABLES = [
    "company",
    "commodityprice",
    "exportdestination",
    "globalcommoditydata",
    "miningnews",
    "resourcesandreserves",
    "totalcommoditiesproduction",
    "companyfinancials",
    "companyownership",
    "companyperformance",
    "miningcontract",
    "mininglicense",
    "mininglicenseauction",
    "miningsite",
    "salesdestination",
]

# --- OPTIMIZATION CONFIG ---
PARTIAL_SYNC_CONFIG = {
    "commodity_price": {"col": "date", "days": 60},
    "mining_license": {"col": "license_effective_date", "days": 730},
}


def serialize_value(val):
    if val is None:
        return None
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, bytes):
        return val.hex()
    return val


def get_row_hash(row_dict):
    normalized = {
        k: (str(serialize_value(v)) if v is not None else "NULL")
        for k, v in row_dict.items()
    }
    encoded = json.dumps(normalized, sort_keys=True).encode("utf-8")
    return hashlib.md5(encoded).hexdigest()


def get_local_connection():
    if not os.path.exists(DB_PATH):
        print(f"Error: Local database not found at {DB_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_all_tables(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [row["name"] for row in cursor.fetchall()]

    sorted_tables = []
    # Add prioritized tables first
    for t in PRIORITY_TABLES:
        if t in tables:
            sorted_tables.append(t)
    # Add remaining tables
    for t in tables:
        if t not in sorted_tables:
            sorted_tables.append(t)
    return sorted_tables


def get_full_schema_sql(conn, table_name):
    cursor = conn.cursor()
    sql_statements = []
    cursor.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    row = cursor.fetchone()
    if row and row["sql"]:
        sql_statements.append(row["sql"])

    cursor.execute(
        "SELECT sql FROM sqlite_master WHERE type IN ('index', 'trigger') AND tbl_name=? AND sql IS NOT NULL",
        (table_name,),
    )
    for row in cursor.fetchall():
        if "sqlite_autoindex" not in row["sql"]:
            sql_statements.append(row["sql"])
    return sql_statements


def get_primary_keys(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    return [row["name"] for row in cursor.fetchall() if row["pk"] > 0]


def batch_execute(client, sql_template, data_batch, columns):
    stmts = [Statement("PRAGMA foreign_keys = OFF")]
    for row in data_batch:
        values = [serialize_value(row[col]) for col in columns]
        stmts.append(Statement(sql_template, args=values))
    client.batch(stmts)


# -----------------------------------------------------------------------------
# MODE: FULL REPLACE
# -----------------------------------------------------------------------------
def sync_table_replace(conn, client, table_name, dry_run=False, skip_drop=False):
    print(f"\n[{table_name}] MODE: FULL REPLACE")

    schema_stmts = get_full_schema_sql(conn, table_name)
    if not schema_stmts:
        print(f"   [SKIP] Schema not found.")
        return

    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM "{table_name}"')
    rows = cursor.fetchall()

    if dry_run:
        print(f"   [DRY RUN] Would replace table and insert {len(rows)} rows.")
        return

    if not skip_drop:
        print(f"   -> Recreating Table & {len(schema_stmts)-1} Indexes...")
    else:
        print(f"   -> Creating Table & {len(schema_stmts)-1} Indexes...")

    try:
        setup_stmts = [Statement("PRAGMA foreign_keys = OFF")]
        if not skip_drop:
            setup_stmts.append(Statement(f'DROP TABLE IF EXISTS "{table_name}"'))

        for sql in schema_stmts:
            setup_stmts.append(Statement(sql))
        client.batch(setup_stmts)
    except Exception as e:
        print(f"   [CRITICAL] Schema failed: {e}")
        return

    if not rows:
        return

    columns = rows[0].keys()
    col_list = ", ".join([f'"{c}"' for c in columns])
    placeholders = ", ".join(["?" for _ in columns])
    insert_sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})'

    print(f"   -> Inserting {len(rows)} rows...")
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        batch_execute(client, insert_sql, batch, columns)
        print(
            f"      -> Progress: {min(i + BATCH_SIZE, len(rows))}/{len(rows)}", end="\r"
        )
    print("")


# -----------------------------------------------------------------------------
# MODE: SMART UPDATE (Split Phase)
# -----------------------------------------------------------------------------
def sync_table_update(
    conn, client, table_name, operations={"insert", "update", "delete"}, dry_run=False
):
    """
    operations: set of actions to perform.
    Allows splitting Logic:
      - Pass 1: {'insert', 'update'} (Parent -> Child)
      - Pass 2: {'delete'} (Child -> Parent)
    """

    # We only print the header if we are actually going to do something
    # or if we are analyzing potential deletes in the delete phase

    pks = get_primary_keys(conn, table_name)
    if not pks:
        print(f"\n[{table_name}] [!] No PK. Switching to REPLACE.")
        sync_table_replace(conn, client, table_name, dry_run)
        return

    def make_key(row_dict):
        return tuple(serialize_value(row_dict[k]) for k in pks)

    # --- OPTIMIZATION LOGIC ---
    where_clause = ""
    filter_desc = "All Rows"

    if table_name in PARTIAL_SYNC_CONFIG:
        cfg = PARTIAL_SYNC_CONFIG[table_name]
        cutoff = (
            datetime.date.today() - datetime.timedelta(days=cfg["days"])
        ).isoformat()
        where_clause = f'WHERE "{cfg["col"]}" >= \'{cutoff}\''
        filter_desc = f"Since {cutoff} ({cfg['days']} days)"

    # 1. Fetch Remote
    try:
        sql_remote = f'SELECT * FROM "{table_name}" {where_clause}'
        remote_res = client.execute(sql_remote)
        remote_rows = []
        remote_cols = remote_res.columns
        for r in remote_res.rows:
            remote_rows.append(dict(zip(remote_cols, list(r))))
    except Exception:
        print(f"\n[{table_name}] [!] Remote table missing. Switching to REPLACE.")
        sync_table_replace(conn, client, table_name, dry_run)
        return

    # 2. Fetch Local
    cursor = conn.cursor()
    sql_local = f'SELECT * FROM "{table_name}" {where_clause}'
    cursor.execute(sql_local)
    local_rows = cursor.fetchall()

    # 3. Hash & Compare
    local_map = {}
    local_data_map = {}

    for row in local_rows:
        row_dict = dict(row)
        key = make_key(row_dict)
        local_map[key] = get_row_hash(row_dict)
        local_data_map[key] = row_dict

    remote_map = {}
    for row in remote_rows:
        key = make_key(row)
        remote_map[key] = get_row_hash(row)

    to_insert = [local_data_map[k] for k in local_map if k not in remote_map]

    to_update = []
    for k, local_hash in local_map.items():
        if k in remote_map and remote_map[k] != local_hash:
            to_update.append(local_data_map[k])

    to_delete = [k for k in remote_map if k not in local_map]

    # Only print status if we have work to do in the current allowed operations
    has_inserts = len(to_insert) > 0 and "insert" in operations
    has_updates = len(to_update) > 0 and "update" in operations
    has_deletes = len(to_delete) > 0 and "delete" in operations

    if has_inserts or has_updates or has_deletes:
        print(f"\n[{table_name}] MODE: SMART UPDATE ({'+'.join(operations)})")
        print(f"   -> Scope: {filter_desc}")
        print(
            f"   -> Analysis: {len(to_insert)} Inserts | {len(to_update)} Updates | {len(to_delete)} Deletes"
        )
    elif "delete" in operations and len(to_delete) > 0:
        # Special case: If we are in delete mode, but we ignored inserts, we might still want to see header
        pass

    if dry_run:
        if has_inserts or has_updates or has_deletes:
            print("   -> [DRY RUN] No changes applied.")
        return

    columns = local_rows[0].keys() if local_rows else []

    # 4. Execute allowed operations

    # --- INSERT ---
    if has_inserts:
        col_list = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join(["?" for _ in columns])
        batch_execute(
            client,
            f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})',
            to_insert,
            columns,
        )
        print(f"   -> Inserted {len(to_insert)} rows.")

    # --- UPDATE ---
    if has_updates:
        assigns = ", ".join([f'"{c}"=?' for c in columns if c not in pks])
        where_pk = " AND ".join([f'"{pk}"=?' for pk in pks])
        sql = f'UPDATE "{table_name}" SET {assigns} WHERE {where_pk}'

        print(f"   -> Updating {len(to_update)} rows...")
        for i in range(0, len(to_update), BATCH_SIZE):
            batch = to_update[i : i + BATCH_SIZE]
            stmts = [Statement("PRAGMA foreign_keys = OFF")]
            for row in batch:
                vals = [serialize_value(row[c]) for c in columns if c not in pks]
                pk_vals = [serialize_value(row[pk]) for pk in pks]
                stmts.append(Statement(sql, args=vals + pk_vals))
            client.batch(stmts)

    # --- DELETE ---
    if has_deletes:
        where_pk = " AND ".join([f'"{pk}"=?' for pk in pks])
        sql = f'DELETE FROM "{table_name}" WHERE {where_pk}'

        print(f"   -> Deleting {len(to_delete)} rows...")
        for i in range(0, len(to_delete), BATCH_SIZE):
            batch = to_delete[i : i + BATCH_SIZE]
            stmts = [Statement("PRAGMA foreign_keys = OFF")]
            for key_tuple in batch:
                stmts.append(Statement(sql, args=list(key_tuple)))
            client.batch(stmts)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--specific", type=str)
    parser.add_argument("--dry-run", action="store_true")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--replace", action="store_true")
    group.add_argument("--update", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    url = (
        os.getenv("TURSO_DATABASE_URL", "")
        .replace("wss://", "https://")
        .replace("libsql://", "https://")
    )
    token = os.getenv("TURSO_AUTH_TOKEN")

    if not url or not token:
        return print("Missing .env credentials")

    conn = get_local_connection()
    client = create_client_sync(url=url, auth_token=token)

    # Determine Tables
    all_tables_sorted = get_all_tables(conn)

    if args.specific:
        # If specific, we process just that one table (no two-phase needed usually,
        # unless deleting from parent, but specific usually implies forced manual sync)
        target_tables = [args.specific]
        if args.specific not in all_tables_sorted:
            print(f"Error: Table {args.specific} not found.")
            sys.exit(1)
    else:
        target_tables = all_tables_sorted

    print(f"{'='*60}\nTURSO SYNC: {'REPLACE' if args.replace else 'UPDATE'}\n{'='*60}")

    try:
        if args.replace:
            if not args.specific:
                # 1. DROP ALL TABLES (Bottom-Up: Child -> Parent)
                print("\n>>> PHASE 1: DROPPING ALL TABLES (Child -> Parent) <<<")
                drop_stmts = [Statement("PRAGMA foreign_keys = OFF")]
                for t in reversed(target_tables):
                    drop_stmts.append(Statement(f'DROP TABLE IF EXISTS "{t}"'))

                if args.dry_run:
                    print(f"   -> [DRY RUN] Would drop {len(target_tables)} tables.")
                else:
                    client.batch(drop_stmts)
                    print(f"   -> Dropped {len(target_tables)} tables.")

            # 2. RECREATE & INSERT (Top-Down: Parent -> Child)
            if not args.specific:
                print("\n>>> PHASE 2: RECREATING TABLES (Parent -> Child) <<<")

            for t in target_tables:
                sync_table_replace(
                    conn, client, t, args.dry_run, skip_drop=not args.specific
                )

        elif args.update:
            if args.specific:
                # Single table update (All operations)
                sync_table_update(
                    conn,
                    client,
                    target_tables[0],
                    {"insert", "update", "delete"},
                    args.dry_run,
                )
            else:
                # -----------------------------------------------------
                # PASS 1: UPSERT (Top-Down: Parent -> Child)
                # -----------------------------------------------------
                print("\n>>> PHASE 1: INSERTS & UPDATES (Parent -> Child) <<<")
                for t in target_tables:
                    sync_table_update(
                        conn, client, t, {"insert", "update"}, args.dry_run
                    )

                # -----------------------------------------------------
                # PASS 2: PRUNE (Bottom-Up: Child -> Parent)
                # -----------------------------------------------------
                print("\n>>> PHASE 2: CLEANUP / DELETES (Child -> Parent) <<<")
                # Reverse the list to go from Child -> Parent
                for t in reversed(target_tables):
                    sync_table_update(conn, client, t, {"delete"}, args.dry_run)

    except Exception as e:
        print(f"\n[ERROR] {e}")
    finally:
        client.close()
        conn.close()


if __name__ == "__main__":
    main()
