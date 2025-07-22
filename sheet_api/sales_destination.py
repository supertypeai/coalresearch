import gspread
import sqlite3
import json
import os

from google_sheets.auth import createClient

# --- Configuration ---
WORKSHEET_NAME = "sales_destination"
# Construct the absolute path to the database file to ensure it's in the project root.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_NAME = os.path.join(PROJECT_ROOT, "db.sqlite")
TABLE_NAME = "sales_destination"


# --- Database Setup ---
def setup_database(db_name, table_name):
    """
    Connects to SQLite DB and creates the table if it doesn't exist.
    """
    print(f"Connecting to database '{db_name}'...")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table with two columns: country and a JSON data blob for companies
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        country TEXT PRIMARY KEY,
        company TEXT
    );
    """
    cursor.execute(create_table_query)

    # Clear the table to avoid duplicate entries on re-runs
    cursor.execute(f"DELETE FROM {table_name};")
    print(f"Table '{table_name}' is ready. Existing data cleared.")

    conn.commit()
    return conn, cursor


def process_and_insert_data(sheet, conn, cursor):
    """
    Parses the sheet data and inserts it into the database.
    """
    print("Reading all data from the worksheet...")
    all_data = sheet.get_all_values()

    # Define the keys for our metrics based on their fixed positions in column B
    # B3 -> index 2, B4 -> index 3, etc.
    metric_keys = {
        2: "revenue",
        3: "percentage_of_total_revenue",
        4: "volume",
        5: "percentage_of_sales_volume",
    }

    header_row = all_data[0]  # Company names are on row 1
    year_row = all_data[1]  # Years are on row 2

    current_country = None
    country_data_json = {}

    # We iterate through rows starting from the first potential country row (A3)
    for row_idx in range(2, len(all_data)):
        row = all_data[row_idx]
        country_name_in_cell = row[0]

        # A non-empty cell in column A indicates a new country block
        if country_name_in_cell:
            # If we were processing a previous country, insert its data first
            if current_country and country_data_json:
                json_output = json.dumps(country_data_json)
                cursor.execute(
                    f"INSERT INTO {TABLE_NAME} (country, company) VALUES (?, ?)",
                    (current_country, json_output),
                )
                conn.commit()
                print(f"Successfully inserted data for {current_country}.")

            # Start processing the new country
            current_country = country_name_in_cell
            print(f"Processing country: {current_country}...")
            country_data_json = {}
            current_company_name = None

            # Iterate through columns for this country's data (starting from C)
            for col_idx in range(2, len(header_row)):
                # Check for a new company name in the header row
                company_from_header = header_row[col_idx].strip()
                if company_from_header:
                    current_company_name = company_from_header

                # If there's no active company for this column, skip it
                if not current_company_name:
                    continue

                year = year_row[col_idx].strip()
                # If there's no year, this column is likely empty/irrelevant
                if not year:
                    continue

                # Ensure company exists in our JSON structure
                if current_company_name not in country_data_json:
                    country_data_json[current_company_name] = {}

                # Build the details for this specific year
                year_details = {}
                # The data for metrics is in rows 3, 4, 5, 6
                for metric_row_idx, key_name in metric_keys.items():
                    value = all_data[metric_row_idx][col_idx].strip()
                    year_details[key_name] = value

                # Add the year's data to the company's record
                country_data_json[current_company_name][year] = year_details

    # After the loop, insert the very last country's data
    if current_country and country_data_json:
        json_output = json.dumps(country_data_json)
        cursor.execute(
            f"INSERT INTO {TABLE_NAME} (country, company) VALUES (?, ?)",
            (current_country, json_output),
        )
        conn.commit()
        print(f"Successfully inserted data for {current_country}.")


def main():
    """
    Main function to read sales destination data, process it,
    and insert it into an SQLite database.
    """
    print("Starting script to process sales destination data...")

    conn = None
    try:
        # 1. Setup Database
        conn, cursor = setup_database(DB_NAME, TABLE_NAME)

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
