import sqlite3


def create_commodity_report_mv():
    """
    Connects to the SQLite database and creates a new materialized view named
    'commodity_report'.

    This view is centered on the 'commodity_price' table, with each row representing
    a unique commodity. Data from related tables (export, global data, reserves,
    and production) is aggregated into JSON columns.

    This script is idempotent and can be run multiple times to refresh the report.
    """
    db_file = "db.sqlite"
    conn = None  # Initialize conn to None
    try:
        # Connect to the local SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        print(f"Connected to {db_file}. Preparing to create 'commodity_report' table.")

        # Drop the materialized view if it already exists to ensure a fresh start
        cursor.execute("DROP TABLE IF EXISTS commodity_report")
        print("Dropped existing 'commodity_report' table (if any).")

        # The main query to build the commodity_report materialized view.
        # It uses commodity_price as the central table and aggregates data
        # from other tables using correlated subqueries.
        create_table_query = """
        CREATE TABLE commodity_report AS
        SELECT
            cp.commodity_id,
            cp.name AS commodity_name,
            json(cp.price) AS price_history,

            -- Aggregate total national production history for the commodity
            (SELECT json_group_array(
                json_object(
                    'year', tcp.year,
                    'production_volume', tcp.production_volume,
                    'unit', tcp.unit
                )
             ) FROM total_commodities_production tcp WHERE tcp.commodity_type = cp.name
            ) AS national_production_history,

            -- Aggregate export destination data for the commodity
            (SELECT json_group_array(
                json_object(
                    'country', ed.country,
                    'year', ed.year,
                    'export_USD', ed.export_USD,
                    'export_volume_BPS', ed.export_volume_BPS,
                    'export_volume_ESDM', ed.export_volume_ESDM
                )
             ) FROM export_destination ed WHERE ed.commodity_type = cp.name
            ) AS export_destinations,
            
            -- Aggregate provincial resources and reserves data
            (SELECT json_group_array(
                json_object(
                    'province', rar.province,
                    'year', rar.year,
                    'exploration_target_1', rar.exploration_target_1,
                    'total_inventory_1', rar.total_inventory_1,
                    'resources_inferred', rar.resources_inferred,
                    'resources_indicated', rar.resources_indicated,
                    'resources_measured', rar.resources_measured,
                    'resources_total', rar.resources_total,
                    'verified_resources_2', rar.verified_resources_2,
                    'reserves_1', rar.reserves_1,
                    'verified_reserves_2', rar.verified_reserves_2
                )
             ) FROM resources_and_reserves rar WHERE rar.commodity_type = cp.name
            ) AS resources_and_reserves,

            -- Aggregate global data (production, export/import) for the commodity
            (SELECT json_group_array(
                json_object(
                    'country', gcd.country,
                    'resources_reserves', json(gcd.resources_reserves),
                    'export_import', json(gcd.export_import),
                    'production_volume', json(gcd.production_volume)
                )
             ) FROM global_commodity_data gcd WHERE gcd.commodity_type = cp.name
            ) AS global_comparison

        FROM
            commodity_price cp;
        """

        cursor.execute(create_table_query)
        conn.commit()

        print("Table 'commodity_report' created and populated successfully.")
        print(
            "Each commodity now has a single row with related data aggregated in JSON format."
        )

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        # Rollback any changes if an error occurs
        if conn:
            conn.rollback()
    finally:
        # Close the connection
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    create_commodity_report_mv()
