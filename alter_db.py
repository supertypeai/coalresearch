#!/usr/bin/env python3
import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "coal-db.sqlite")

MIGRATION_SQL = """
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

-- Create tables
CREATE TABLE IF NOT EXISTS mining_license (
  id INTEGER PRIMARY KEY,
  province TEXT NOT NULL,
  district TEXT NOT NULL,
  permit_type TEXT NOT NULL,
  business_entity TEXT,
  business_name TEXT NOT NULL,
  mining_license_number TEXT,
  permit_effective_date INTEGER,
  permit_expiry_date INTEGER,
  activity TEXT,
  licensed_area NUMERIC,
  cnc TEXT,
  generation TEXT,
  location TEXT,
  geometry TEXT
);

CREATE TABLE IF NOT EXISTS coal_resources_and_reserves (
  id INTEGER PRIMARY KEY,
  province TEXT,
  exploration_target_1 NUMERIC,
  total_inventory_1 NUMERIC,
  resources_inferred NUMERIC,
  resources_indicated NUMERIC,
  resources_measured NUMERIC,
  resources_total NUMERIC,
  verified_resources_2 NUMERIC,
  reserves_1 NUMERIC,
  verified_reserves_2 NUMERIC,
  year INTEGER
);

CREATE TABLE IF NOT EXISTS total_coal_production (
  id INTEGER PRIMARY KEY,
  production_volume NUMERIC,
  unit TEXT,
  year INTEGER
);

CREATE TABLE IF NOT EXISTS coal_export_destination (
  id INTEGER PRIMARY KEY,
  country TEXT NOT NULL,
  year INTEGER,
  export_USD NUMERIC,
  export_volume_BPS NUMERIC,
  export_volume_ESDM NUMERIC
);

CREATE TABLE IF NOT EXISTS mining_contract (
  id INTEGER PRIMARY KEY,
  mine_owner_id INTEGER,
  contractor_id INTEGER,
  mine_id INTEGER,
  contract_period_end TEXT
);

CREATE TABLE IF NOT EXISTS coal_product (
  id INTEGER PRIMARY KEY,
  company_name TEXT,
  company_id INTEGER,
  product_name TEXT,
  calorific_value TEXT,
  total_moisture TEXT,
  ash_content_arb TEXT,
  total_sulphur_arb TEXT,
  ash_content_adb TEXT,
  total_sulphur_adb TEXT,
  volatile_matter_adb TEXT,
  fixed_carbon_adb TEXT
);

COMMIT;
PRAGMA foreign_keys = ON;
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
