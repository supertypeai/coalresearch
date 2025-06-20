#!/usr/bin/env python3
import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "db.sqlite")

MIGRATION_SQL = """
PRAGMA foreign_keys=off;
BEGIN TRANSACTION;
ALTER TABLE company ADD COLUMN mining_contract TEXT;
COMMIT;
PRAGMA foreign_keys=on;
"""


def main():
    if not os.path.isfile(DB_PATH):
        print(f"Error: database file not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(MIGRATION_SQL)
        print("Tables created successfully.")
    except sqlite3.DatabaseError as e:
        print(f"Execution failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
