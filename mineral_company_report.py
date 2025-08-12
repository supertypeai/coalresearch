import sqlite3
import json


def create_or_update_consolidated_report():
    """
    Connects to the SQLite database and creates/updates a consolidated report table
    named 'mineral_company_report'.

    This function consolidates data from multiple tables into a single table where
    each company has exactly one row. Related data from other tables is aggregated
    into JSON arrays for consistent structure.

    The process is idempotent: running it multiple times will safely refresh the data.
    """
    db_file = "db.sqlite"
    conn = None  # Initialize conn to None
    try:
        # Connect to the local SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        print(
            f"Connected to {db_file}. Preparing to create/update 'mineral_company_report' table."
        )

        # Drop the table if it already exists to ensure a fresh build
        cursor.execute("DROP TABLE IF EXISTS mineral_company_report")
        print("Dropped existing 'mineral_company_report' table (if any).")

        # The main query to build the consolidated table.
        create_table_query = """
        CREATE TABLE mineral_company_report AS
        SELECT
            c.*,
            -- Aggregate financial data for each company into a JSON array
            (SELECT json_group_array(
                json_object(
                    'year', cf.year,
                    'assets', cf.assets,
                    'revenue', cf.revenue,
                    'net_profit', cf.net_profit,
                    'revenue_breakdown', json(cf.revenue_breakdown),
                    'cost_of_revenue', cf.cost_of_revenue,
                    'cost_of_revenue_breakdown', json(cf.cost_of_revenue_breakdown)
                )
             ) FROM company_financials cf WHERE cf.company_id = c.id
            ) AS financials,

            -- Aggregate ownership data
            (SELECT json_group_array(
                json_object(
                    'parent_company_id', co.parent_company_id,
                    'percentage_ownership', co.percentage_ownership
                )
             ) FROM company_ownership co WHERE co.company_id = c.id
            ) AS ownership,

            -- Aggregate performance data
            (SELECT json_group_array(
                json_object(
                    'year', cp.year,
                    'commodity_type', cp.commodity_type,
                    'commodity_sub_type', cp.commodity_sub_type,
                    'commodity_stats', json(cp.commodity_stats)
                )
             ) FROM company_performance cp WHERE cp.company_id = c.id
            ) AS performance,

            -- Aggregate mining contract data
            (SELECT json_group_array(
                json_object(
                    'contractor_id', mc.contractor_id,
                    'contract_period_end', mc.contract_period_end
                )
             ) FROM mining_contract mc WHERE mc.mine_owner_id = c.id
            ) AS contracts,

            -- Aggregate mining license data
            (SELECT json_group_array(
                json_object(
                    'license_type', ml.license_type,
                    'license_number', ml.license_number,
                    'permit_effective_date', ml.permit_effective_date,
                    'permit_expiry_date', ml.permit_expiry_date,
                    'activity', ml.activity,
                    'commodity', ml.commodity
                )
             ) FROM mining_license ml WHERE ml.company_id = c.id
            ) AS licenses,

            -- Aggregate license auction data
            (SELECT json_group_array(
                json_object(
                    'auction_status', mla.auction_status,
                    'commodity', mla.commodity,
                    'province', mla.province,
                    'date_winner', mla.date_winner
                )
             ) FROM mining_license_auctions mla WHERE mla.company_name = c.name
            ) AS license_auctions,

            -- Aggregate mining site data
            (SELECT json_group_array(
                json_object(
                    'site_name', ms.name,
                    'project_name', ms.project_name,
                    'year', ms.year,
                    'mineral_type', ms.mineral_type,
                    'production_volume', ms.production_volume,
                    'resources_reserves', json(ms.resources_reserves),
                    'location', json(ms.location)
                )
             ) FROM mining_site ms WHERE ms.company_id = c.id
            ) AS sites,

            -- REVISED: Aggregate sales destination data into a JSON array of objects
            (SELECT json_group_array(
                json_object(
                    'year', year,
                    'sales', json(sales_by_year)
                )
            )
            FROM (
                SELECT
                    sd.year,
                    json_group_array(
                        json_object(
                            'country', sd.country,
                            'revenue', sd.revenue,
                            'volume', sd.volume,
                            'percentage_of_total_revenue', sd.percentage_of_total_revenue,
                            'percentage_of_sales_volume', sd.percentage_of_sales_volume
                        )
                    ) AS sales_by_year
                FROM sales_destination sd
                WHERE sd.company_id = c.id
                GROUP BY sd.year
            )) AS sales_destinations
        FROM
            company c;
        """

        cursor.execute(create_table_query)
        conn.commit()

        print("Table 'mineral_company_report' created and populated successfully.")
        print(
            "Each company now has a single row with all related data aggregated into consistent JSON array formats."
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
    create_or_update_consolidated_report()
