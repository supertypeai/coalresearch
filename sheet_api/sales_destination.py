import gspread
import sqlite3
import json
import os

from .google_sheets.auth import createClient

from db.models import SalesDestination, db

# --- Configuration ---
WORKSHEET_NAME = "sales_destination"

def setup_database():
    print(f"Connecting to database...")
    db.connect()
    
    # Ensure the table exists and has the correct schema
    # We will migrate by dropping and recreating to ensure schema matches models.py
    # Since this is a sync script, this is usually acceptable.
    if db.table_exists('sales_destination'):
        print("Dropping existing sales_destination table...")
        db.drop_tables([SalesDestination])
    
    print("Creating sales_destination table...")
    db.create_tables([SalesDestination])
    
    # Return the raw connection and cursor for the rest of the script
    conn = db.connection()
    cursor = conn.cursor()
    return conn, cursor


def parse_numeric_value(value):
    """
    Attempts to convert a string value to a float. Returns None if conversion fails.
    Handles empty strings by returning None.
    """
    try:
        if value.strip() == "":
            return None
        # Replace comma with dot for decimal conversion if necessary
        return float(value.replace(",", "."))
    except (ValueError, TypeError):
        return None


def process_and_insert_data(sheet, conn, cursor):
    """
    Parses the sheet using a block-based approach for countries and a column-based
    approach for companies and years, then inserts the data into the database.
    """
    table_name = SalesDestination._meta.table_name
    print(f"Using table: {table_name}")
    print("Reading all data from the worksheet...")
    all_data = sheet.get_all_values()

    # --- Define key row indices (0-based) ---
    COMPANY_ROW_IDX = 0  # Row 1 in Sheets
    TICKER_ROW_IDX = 1  # Row 2 in Sheets
    YEAR_ROW_IDX = 2  # Row 3 in Sheets

    # Map metric names from the sheet to our database column names
    metric_mapping = {
        "Revenue (in million USD)": "revenue_usd",
        "% in total revenue": "percentage_of_total_revenue",
        "Volume (Mt)": "volume",
        "% in total sales volume": "percentage_of_sales_volume",
    }
    # Get a list of the database column names for metrics
    metric_db_keys = list(metric_mapping.values())

    # Start scanning for country blocks from row 4 (index 3)
    current_row_idx = 3

    while current_row_idx < len(all_data):
        country_name = all_data[current_row_idx][0].strip()

        if not country_name:
            current_row_idx += 1
            continue

        print(f"\nProcessing Country: {country_name}")

        block_end_row = len(all_data)
        for i in range(current_row_idx + 1, len(all_data)):
            if all_data[i][0].strip():
                block_end_row = i
                break

        block_metric_rows = {}
        for r_idx in range(current_row_idx, block_end_row):
            metric_label = all_data[r_idx][1].strip()
            if metric_label in metric_mapping:
                block_metric_rows[metric_label] = r_idx

        active_ticker = None
        num_cols = len(all_data[0]) if all_data else 0
        for col_idx in range(2, num_cols):
            company_name_from_header = all_data[COMPANY_ROW_IDX][col_idx].strip()
            if company_name_from_header:
                active_ticker = all_data[TICKER_ROW_IDX][col_idx].strip()
                print(
                    f"  Found Company: {company_name_from_header} (Ticker: {active_ticker})"
                )

            if not active_ticker:
                continue

            year_str = all_data[YEAR_ROW_IDX][col_idx].strip()

            try:
                if not year_str or not year_str.isdigit() or len(year_str) != 4:
                    continue
                year = int(year_str)
            except (ValueError, TypeError):
                continue

            # Default values for new columns
            commodity_type = "Coal"  # Assuming Coal as this is a coal research project
            unit = "Mt"  # Based on "Volume (Mt)" column header

            data_to_insert = {
                "country": country_name,
                "symbol": active_ticker,
                "year": year,
                "revenue_usd": None,
                "percentage_of_total_revenue": None,
                "volume": None,
                "percentage_of_sales_volume": None,
                "commodity_type": commodity_type,
                "unit": unit
            }

            for metric_label, row_idx_in_block in block_metric_rows.items():
                db_column_name = metric_mapping[metric_label]
                raw_value = all_data[row_idx_in_block][col_idx]
                parsed_value = parse_numeric_value(raw_value)
                
                # Transform revenue to base USD
                if db_column_name == "revenue_usd" and parsed_value is not None:
                     parsed_value = round(parsed_value * 1_000_000, 2)

                data_to_insert[db_column_name] = parsed_value

            # --- NEW LOGIC: Check if all metric values are None ---
            # Create a list of the values for the metric keys
            all_metrics_are_null = all(
                data_to_insert[key] is None for key in metric_db_keys
            )

            if all_metrics_are_null:
                # If all data points are null, skip this record entirely
                print(f"    -> Skipping Year: {year} (No data found)")
                continue
            # --- END OF NEW LOGIC ---

            company_id = None
            if active_ticker:
                cursor.execute(
                    "SELECT id FROM company WHERE symbol = ?", (active_ticker,)
                )
                result = cursor.fetchone()
                if result:
                    company_id = result[0]
                else:
                    print(
                        f"    -> WARNING: Ticker '{active_ticker}' not found in 'company' table. company_id will be NULL."
                    )
            
            # Check for existing record to handle update vs insert if needed, 
            # though the current script is just doing inserts and catching duplicates.
            # We will stick to the existing pattern of INSERT.

            try:
                # Update the INSERT query and the tuple of values
                cursor.execute(
                    f"""
                    INSERT INTO {table_name} (
                        company_id, country, symbol, year, revenue_usd,
                        percentage_of_total_revenue, volume, percentage_of_sales_volume,
                        commodity_type, unit
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        company_id,
                        data_to_insert["country"],
                        data_to_insert["symbol"],
                        data_to_insert["year"],
                        data_to_insert["revenue_usd"],
                        data_to_insert["percentage_of_total_revenue"],
                        data_to_insert["volume"],
                        data_to_insert["percentage_of_sales_volume"],
                        data_to_insert["commodity_type"],
                        data_to_insert["unit"],
                    ),
                )
                print(f"    -> Inserted data for Year: {year}")

            except sqlite3.IntegrityError:
                print(
                    f"    -> Skipped duplicate entry for Ticker: {active_ticker}, Year: {year}"
                )
            except Exception as e:
                print(f"    -> ERROR inserting data for {active_ticker}, {year}: {e}")

        conn.commit()
        current_row_idx = block_end_row


def main():
    """
    Main function to read sales destination data, process it,
    and insert it into an SQLite database.
    """
    print("Starting script to process sales destination data...")

    TABLE_NAME = SalesDestination._meta.table_name

    conn = None
    try:
        # 1. Setup Database
        conn, cursor = setup_database()

        # 2. Connect to Google Sheets
        print("Connecting to Google Sheets...")
        client, spreadsheet_id = createClient()
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet = spreadsheet.worksheet(WORKSHEET_NAME)

        # 3. Process data and insert into DB
        process_and_insert_data(sheet, conn, cursor)

        print("\nScript finished successfully.")

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Spreadsheet with ID '{spreadsheet_id}' not found.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{WORKSHEET_NAME}' not found in the spreadsheet.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()

# command to run: python -m sheet_api.sales_destination
