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
    Parses the sheet data by processing country-specific blocks and inserts it into the database.
    """
    print("Reading all data from the worksheet...")
    all_data = sheet.get_all_values()

    # This mapping is more robust. It maps the text label from Column B
    # to the desired key in our final JSON.
    metric_mapping = {
        "Revenue (in million USD)": "revenue",
        "% in total revenue": "percentage_of_total_revenue",
        "Volume (Mt)": "volume",
        "% in total sales volume": "percentage_of_sales_volume",
    }

    header_row = all_data[0]  # Company names are on row 1 (index 0)
    year_row = all_data[1]  # Years are on row 2 (index 1)

    # We use a while loop to jump between country blocks, instead of iterating row-by-row.
    current_row_idx = 2  # Start scanning for countries from row 3 (index 2)
    while current_row_idx < len(all_data):
        country_name = all_data[current_row_idx][0].strip()

        # If the cell in column A is empty, it's not the start of a new country block. Skip it.
        if not country_name:
            current_row_idx += 1
            continue

        # --- Found a new country block ---
        print(f"\nProcessing country: {country_name}...")
        country_data_json = {}
        block_start_row = current_row_idx

        # Find the end of the current country block.
        # It ends right before the next country starts or at the end of the sheet.
        block_end_row = len(all_data)
        for i in range(block_start_row + 1, len(all_data)):
            if all_data[i][0].strip():  # Found the start of the next country
                block_end_row = i
                break

        # --- Process all columns for the current country block ---
        current_company_name = None
        for col_idx in range(2, len(header_row)):  # Start from column C (index 2)
            # Check for a new company name. If a cell in the header is blank,
            # it belongs to the previous company.
            company_from_header = header_row[col_idx].strip()
            if company_from_header:
                current_company_name = company_from_header

            if not current_company_name:
                continue

            year = year_row[col_idx].strip()
            if not year:
                continue

            # Ensure the company key exists in our structure
            if current_company_name not in country_data_json:
                country_data_json[current_company_name] = {}

            year_details = {}
            # Iterate through the rows *within the current country block*
            for metric_row_idx in range(block_start_row, block_end_row):
                metric_label = all_data[metric_row_idx][
                    1
                ].strip()  # Get label from Column B

                # If the label is one we care about, get its value
                if metric_label in metric_mapping:
                    json_key = metric_mapping[metric_label]
                    value = all_data[metric_row_idx][col_idx].strip()
                    year_details[json_key] = value

            # Add the year's data to the company's record
            if year_details:  # Only add if we actually found data
                country_data_json[current_company_name][year] = year_details

        # --- Insert the completed country data into the database ---
        if country_data_json:
            json_output = json.dumps(
                country_data_json, indent=2
            )  # indent for readability
            cursor.execute(
                f"INSERT INTO {TABLE_NAME} (country, company) VALUES (?, ?)",
                (country_name, json_output),
            )
            conn.commit()
            print(f"Successfully inserted data for {country_name}.")

        # Move the main index to the start of the next block
        current_row_idx = block_end_row


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
