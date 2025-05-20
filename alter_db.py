#!/usr/bin/env python3
import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "coal-db.sqlite")

MIGRATION_SQL = """
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;

-- 1) company_ownership
ALTER TABLE company_ownership RENAME TO company_ownership_old;
CREATE TABLE company_ownership (
  parent_company_id    INTEGER NOT NULL,
  company_id           INTEGER NOT NULL,
  percentage_ownership NUMERIC,
  PRIMARY KEY (parent_company_id, company_id),
  FOREIGN KEY (parent_company_id) REFERENCES company(id) ON DELETE CASCADE,
  FOREIGN KEY (company_id)        REFERENCES company(id) ON DELETE CASCADE
);
INSERT INTO company_ownership
  (parent_company_id, company_id, percentage_ownership)
SELECT
  parent_company_id, company_id, percentage_ownership
FROM company_ownership_old;
DROP TABLE company_ownership_old;

-- 2) company
ALTER TABLE company RENAME TO company_old;
CREATE TABLE company (
  id                     INTEGER PRIMARY KEY,
  name                   TEXT    NOT NULL,
  idx_ticker             TEXT,
  operation_province     TEXT,
  operation_kabkot       TEXT,
  representative_address TEXT,
  company_type           TEXT,
  key_operation          TEXT    NOT NULL,
  activities             TEXT,
  website                TEXT,
  phone_number           INTEGER,
  email                  TEXT
);
INSERT INTO company
  (id, name, idx_ticker, operation_province, operation_kabkot,
   representative_address, company_type, key_operation,
   activities, website, phone_number, email)
SELECT
  id, name, idx_ticker, operation_province, operation_kabkot,
  representative_address, company_type, key_operation,
  activities, website, phone_number, email
FROM company_old;
DROP TABLE company_old;

-- 3) coal_company_performance
ALTER TABLE coal_company_performance RENAME TO coal_company_performance_old;
CREATE TABLE coal_company_performance (
  id                        INTEGER PRIMARY KEY,
  company_id                INTEGER NOT NULL,
  year                      INTEGER,
  mineral_type              TEXT,
  calorific_value           TEXT,
  mining_operation_status   TEXT,
  mining_permit             TEXT,
  area                      NUMERIC,
  production_volume         NUMERIC,
  sales_volume              NUMERIC,
  overburden_removal_volume NUMERIC,
  strip_ratio               NUMERIC,
  reserve                   NUMERIC,
  resource                  NUMERIC,
  mineral_sub_type          TEXT,
  area_geometry             TEXT,
  FOREIGN KEY (company_id) REFERENCES company(id) ON DELETE CASCADE
);
INSERT INTO coal_company_performance
  (id, company_id, year, mineral_type, calorific_value,
   mining_operation_status, mining_permit, area,
   production_volume, sales_volume, overburden_removal_volume,
   strip_ratio, reserve, resource, mineral_sub_type, area_geometry)
SELECT
  id, company_id, year, mineral_type, calorific_value,
  mining_operation_status, mining_permit, area,
  production_volume, sales_volume, overburden_removal_volume,
  strip_ratio, reserve, resource, mineral_sub_type, area_geometry
FROM coal_company_performance_old;
DROP TABLE coal_company_performance_old;

-- 4) mining_site
ALTER TABLE mining_site RENAME TO mining_site_old;
CREATE TABLE mining_site (
  id                        INTEGER PRIMARY KEY,
  name                      TEXT    NOT NULL,
  year                      INTEGER,
  company_id                INTEGER NOT NULL,
  calorific_value           TEXT,
  production_volume         NUMERIC,
  overburden_removal_volume NUMERIC,
  strip_ratio               NUMERIC,
  reserve                   NUMERIC,
  resource                  NUMERIC,
  province                  TEXT,
  city                      TEXT,
  mineral_type              TEXT,
  FOREIGN KEY (company_id) REFERENCES company(id) ON DELETE CASCADE
);
INSERT INTO mining_site
  (id, name, year, company_id, calorific_value,
   production_volume, overburden_removal_volume, strip_ratio,
   reserve, resource, province, city, mineral_type)
SELECT
  id, name, year, company_id, calorific_value,
  production_volume, overburden_removal_volume, strip_ratio,
  reserve, resource, province, city, mineral_type
FROM mining_site_old;
DROP TABLE mining_site_old;

-- 5) commodities
ALTER TABLE commodities RENAME TO commodities_old;
CREATE TABLE commodities (
  commodity_id INTEGER PRIMARY KEY,
  name         TEXT NOT NULL,
  unit         TEXT NOT NULL
);
INSERT INTO commodities
  (commodity_id, name, unit)
SELECT
  commodity_id, name, unit
FROM commodities_old;
DROP TABLE commodities_old;

-- 6) commodity_prices
ALTER TABLE commodity_prices RENAME TO commodity_prices_old;
CREATE TABLE commodity_prices (
  price_id     INTEGER PRIMARY KEY,
  commodity_id INTEGER NOT NULL,
  price_date   TEXT    NOT NULL,
  price_value  NUMERIC NOT NULL,
  info         TEXT,
  FOREIGN KEY (commodity_id) REFERENCES commodities(commodity_id) ON DELETE CASCADE
);
INSERT INTO commodity_prices
  (price_id, commodity_id, price_date, price_value, info)
SELECT
  price_id, commodity_id, price_date, price_value, info
FROM commodity_prices_old;
DROP TABLE commodity_prices_old;

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
        print("Migration applied successfully.")
    except sqlite3.DatabaseError as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
