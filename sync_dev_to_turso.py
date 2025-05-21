import os
import sqlite3
from libsql_client import create_client_sync


def sync_nine_tables_to_turso(sqlite_path: str):
    """
    Bulk-sync nine tables from local SQLite to Turso using HTTP client.

    Tables:
      - company
      - company_ownership
      - company_performance
      - export_destination
      - mining_contract
      - mining_site
      - resources_and_reserves
      - total_commodities_production
      - commodity

    Parameters:
      sqlite_path: Local SQLite DB file path
    """
    print("Starting sync_nine_tables_to_turso...")

    # 1. Retrieve Turso credentials
    turso_url = os.getenv("TURSO_DATABASE_URL") or ""
    auth_token = os.getenv("TURSO_AUTH_TOKEN")

    # Normalize URL
    if turso_url.startswith("wss://"):
        db_url = "https://" + turso_url[len("wss://") :]
    elif turso_url.startswith("libsql://"):
        db_url = "https://" + turso_url[len("libsql://") :]
    else:
        db_url = turso_url

    print(f"TURSO_DATABASE_URL: {db_url}")
    print(f"TURSO_AUTH_TOKEN present: {auth_token is not None}")

    if not db_url or not auth_token:
        print("Skipping sync: missing Turso credentials.")
        return

    client = None
    conn = None
    try:
        # 2. Instantiate Turso client
        client = create_client_sync(url=db_url, auth_token=auth_token)
        print("Turso client created successfully.")

        # 3. Connect to local SQLite
        print(f"Connecting to SQLite at {sqlite_path}")
        conn = sqlite3.connect(sqlite_path)
        cur = conn.cursor()

        # 4. Fetch local data for all tables
        print("Fetching local data...")
        local = {}
        tables = [
            (
                "company",
                [
                    "id",
                    "name",
                    "idx_ticker",
                    "operation_province",
                    "operation_kabkot",
                    "representative_address",
                    "company_type",
                    "key_operation",
                    "activities",
                    "website",
                    "phone_number",
                    "email",
                ],
            ),
            (
                "company_ownership",
                ["parent_company_id", "company_id", "percentage_ownership"],
            ),
            (
                "company_performance",
                [
                    "id",
                    "company_id",
                    "year",
                    "commodity_type",
                    "commodity_sub_type",
                    "mining_operation_status",
                    "mining_license",
                    "production_volume",
                    "sales_volume",
                    "overburden_removal_volume",
                    "strip_ratio",
                    "resources_reserves",
                    "product",
                ],
            ),
            (
                "export_destination",
                [
                    "id",
                    "country",
                    "year",
                    "commodity_type",
                    "export_USD",
                    "export_volume_BPS",
                    "export_volume_ESDM",
                ],
            ),
            (
                "mining_contract",
                ["mine_owner_id", "contractor_id", "contract_period_end"],
            ),
            (
                "mining_site",
                [
                    "id",
                    "name",
                    "project_name",
                    "year",
                    "mineral_type",
                    "company_id",
                    "production_volume",
                    "overburden_removal_volume",
                    "strip_ratio",
                    "resources_reserves",
                    "location",
                ],
            ),
            (
                "resources_and_reserves",
                [
                    "id",
                    "province",
                    "year",
                    "commodity_type",
                    "exploration_target_1",
                    "total_inventory_1",
                    "resources_inferred",
                    "resources_indicated",
                    "resources_measured",
                    "resources_total",
                    "verified_resources_2",
                    "reserves_1",
                    "verified_reserves_2",
                ],
            ),
            (
                "total_commodities_production",
                ["id", "commodity_type", "production_volume", "unit", "year"],
            ),
            ("commodity", ["commodity_id", "name", "name_english", "unit", "price"]),
        ]
        for tbl, cols in tables:
            q = f"SELECT {', '.join(cols)} FROM {tbl};"
            rows = cur.execute(q).fetchall()
            local[tbl] = rows
            print(f"Fetched {len(rows)} rows from {tbl}.")

        # 5. Clear remote tables (child-first)
        delete_order = [
            "company_ownership",
            "company_performance",
            "export_destination",
            "resources_and_reserves",
            "total_commodities_production",
            "mining_contract",
            "mining_site",
            "company",
            "commodity",
        ]
        print("Clearing remote tables...")
        for tbl in delete_order:
            client.execute(f"DELETE FROM {tbl};")
        print("Remote tables cleared.")

        # 6. Insert base tables and build mappings
        id_maps = {}
        # commodity
        print("Inserting commodity...")
        for cid, name, name_en, unit, price in local["commodity"]:
            client.execute(
                "INSERT INTO commodity(commodity_id, name, name_english, unit, price) VALUES(?,?,?,?,?);",
                (cid, name, name_en, unit, price),
            )
        id_maps["commodity"] = {
            row["commodity_id"]: row["commodity_id"]
            for row in client.execute("SELECT commodity_id FROM commodity;").rows
        }

        # company
        print("Inserting company...")
        for (
            id_,
            name,
            idx_ticker,
            op_prov,
            op_kabkot,
            addr,
            comp_type,
            key_op,
            acts,
            web,
            phone,
            email,
        ) in local["company"]:
            client.execute(
                "INSERT INTO company(id, name, idx_ticker, operation_province, operation_kabkot, representative_address, company_type, key_operation, activities, website, phone_number, email) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);",
                (
                    id_,
                    name,
                    idx_ticker,
                    op_prov,
                    op_kabkot,
                    addr,
                    comp_type,
                    key_op,
                    acts,
                    web,
                    phone,
                    email,
                ),
            )
        id_maps["company"] = {
            row["id"]: row["id"]
            for row in client.execute("SELECT id FROM company;").rows
        }

        # mining_site
        print("Inserting mining_site...")
        for (
            id_,
            name,
            proj,
            year,
            min_type,
            comp_id,
            prod_vol,
            obr_vol,
            sr,
            res_res,
            loc,
        ) in local["mining_site"]:
            client.execute(
                "INSERT INTO mining_site(id, name, project_name, year, mineral_type, company_id, production_volume, overburden_removal_volume, strip_ratio, resources_reserves, location) VALUES(?,?,?,?,?,?,?,?,?,?,?);",
                (
                    id_,
                    name,
                    proj,
                    year,
                    min_type,
                    comp_id,
                    prod_vol,
                    obr_vol,
                    sr,
                    res_res,
                    loc,
                ),
            )
        id_maps["mining_site"] = {
            row["id"]: row["id"]
            for row in client.execute("SELECT id FROM mining_site;").rows
        }

        # 7. Insert dependent tables
        print("Inserting company_ownership...")
        for parent_id, comp_id, pct in local["company_ownership"]:
            client.execute(
                "INSERT INTO company_ownership(parent_company_id, company_id, percentage_ownership) VALUES(?,?,?);",
                (parent_id, comp_id, pct),
            )

        print("Inserting company_performance...")
        for rec in local["company_performance"]:
            client.execute(
                "INSERT INTO company_performance(id, company_id, year, commodity_type, commodity_sub_type, mining_operation_status, mining_license, production_volume, sales_volume, overburden_removal_volume, strip_ratio, resources_reserves, product) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);",
                rec,
            )

        print("Inserting export_destination...")
        for rec in local["export_destination"]:
            client.execute(
                "INSERT INTO export_destination(id, country, year, commodity_type, export_USD, export_volume_BPS, export_volume_ESDM) VALUES(?,?,?,?,?,?,?);",
                rec,
            )

        print("Inserting mining_contract...")
        for rec in local["mining_contract"]:
            client.execute(
                "INSERT INTO mining_contract(mine_owner_id, contractor_id, contract_period_end) VALUES(?,?,?);",
                rec,
            )

        print("Inserting resources_and_reserves...")
        for rec in local["resources_and_reserves"]:
            client.execute(
                "INSERT INTO resources_and_reserves(id, province, year, commodity_type, exploration_target_1, total_inventory_1, resources_inferred, resources_indicated, resources_measured, resources_total, verified_resources_2, reserves_1, verified_reserves_2) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);",
                rec,
            )

        print("Inserting total_commodities_production...")
        for rec in local["total_commodities_production"]:
            client.execute(
                "INSERT INTO total_commodities_production(id, commodity_type, production_volume, unit, year) VALUES(?,?,?,?,?);",
                rec,
            )

        print("✅ Sync complete.")

    except Exception as e:
        print(f"Error during sync: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("Closed SQLite connection.")
        if client:
            client.close()
            print("Closed Turso client.")


# Static invocation
sync_nine_tables_to_turso("db.sqlite")
