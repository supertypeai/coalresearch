import sqlite3


def create_commodity_report_mv():
    """
    Connects to the SQLite database and creates or updates a materialized view
    named 'commodity_report', reflecting the latest database schema and data formats.

    This view is centered on the 'commodity_price' table. Data from related tables
    is aggregated into JSON columns, with time-series data pivoted into nested
    JSON objects for easier analysis.

    The script is idempotent and can be run multiple times to refresh the report.
    """
    db_file = "db.sqlite"
    conn = None  # Initialize conn to None
    try:
        # Connect to the local SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        print(
            f"Connected to {db_file}. Preparing to create/update 'commodity_report' table."
        )

        # Drop the materialized view if it already exists to ensure a fresh start
        cursor.execute("DROP TABLE IF EXISTS commodity_report")
        print("Dropped existing 'commodity_report' table (if any).")

        # The main query to build the commodity_report materialized view.
        create_table_query = """
        CREATE TABLE commodity_report AS
        SELECT
            cp.commodity_id,
            cp.name AS commodity_name,
            json(cp.price) AS price_history,

            -- REVISED: Aggregate total national production history, grouping years under each unit
            (SELECT
                json_group_array(
                    json_object(
                        'production_volume', json(production_by_year),
                        'unit', unit
                    )
                )
            FROM (
                SELECT
                    tcp.unit,
                    json_group_object(tcp.year, tcp.production_volume) AS production_by_year
                FROM
                    total_commodities_production tcp
                WHERE
                    tcp.commodity_type = cp.name
                GROUP BY
                    tcp.unit
            )) AS national_production_history,

            -- Aggregate export destination data, grouping years under each country
            (SELECT
                json_group_array(
                    json_object(
                        'country', country,
                        'export_USD', json(export_USD_by_year),
                        'export_volume_BPS', json(export_volume_BPS_by_year),
                        'export_volume_ESDM', json(export_volume_ESDM_by_year)
                    )
                )
            FROM (
                SELECT
                    ed.country,
                    json_group_object(ed.year, ed.export_USD) AS export_USD_by_year,
                    json_group_object(ed.year, ed.export_volume_BPS) AS export_volume_BPS_by_year,
                    json_group_object(ed.year, ed.export_volume_ESDM) AS export_volume_ESDM_by_year
                FROM
                    export_destination ed
                WHERE
                    ed.commodity_type = cp.name
                GROUP BY
                    ed.country
            )) AS export_destinations,
            
            -- Aggregate provincial resources and reserves data
            (SELECT json_group_array(
                json_object(
                    'province', rar.province,
                    'year', rar.year,
                    'data', json(rar.resources_reserves)
                )
             ) FROM resources_and_reserves rar WHERE rar.commodity_type = cp.name
            ) AS resources_and_reserves,

            -- Aggregate global data (production, export/import) for the commodity
            (SELECT json_group_array(
                json_object(
                    'country', gcd.country,
                    'resources_reserves', json(gcd.resources_reserves),
                    'resources_reserves_share', json(gcd.resources_reserves_share),
                    'export_import', json(gcd.export_import),
                    'production_volume', json(gcd.production_volume),
                    'production_share', json(gcd.production_share)
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
        print("National production history has been restructured as requested.")

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
