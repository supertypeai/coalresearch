import gspread
import sqlite3
import json
import os

from google_sheets.auth import createClient

# --- Configuration ---
# The name of the worksheet (tab) in your Google Sheet to read from.
WORKSHEET_NAME = "company_financials"
# The name of the SQLite database file.
DB_NAME = "db.sqlite"
# The name of the table to create/update in the database.
TABLE_NAME = "company_financials"


def create_and_connect_db():
    """Connects to the SQLite database and creates the table if it doesn't exist."""
    print(f"Connecting to SQLite database: {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print(f"Ensuring table '{TABLE_NAME}' exists...")
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        idx_ticker TEXT PRIMARY KEY,
        name TEXT,
        assets TEXT,
        revenue TEXT,
        cost_of_revenue TEXT,
        net_profit TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Database and table are ready.")
    return conn, cursor


def parse_company_row(headers, sub_headers, values):
    """
    Parses a single company's data row using the global header rows.

    Args:
        headers (list): The main header row (sheet row 1).
        sub_headers (list): The sub-header row (sheet row 2, with years/breakdown).
        values (list): The data row for a single company.

    Returns:
        dict: A structured dictionary for the company, or None if the row is invalid.
    """
    # Check if the row has a ticker symbol. If not, it's likely an empty row.
    if not values or not values[0] or not values[0].strip():
        return None

    header_map = {
        "Assets (in USD millions)": "assets",
        "Revenue (in USD millions)": "revenue",
        "Cost of Revenue": "cost_of_revenue",
        "Net Profit (in USD millions)": "net_profit",
    }

    company_data = {
        "idx_ticker": values[0],
        "name": values[1],
        "assets": {},
        "revenue": {},
        "cost_of_revenue": {},
        "net_profit": {},
    }

    current_metric_key = None
    # Iterate through the columns starting from C (index 2)
    col_idx = 2
    while col_idx < len(headers):
        # Determine the current metric (e.g., 'assets') based on the global header
        header_text = headers[col_idx].strip()
        if header_text in header_map:
            current_metric_key = header_map[header_text]

        if not current_metric_key:
            col_idx += 1
            continue

        # Get the year and the value from the current company's row
        year = sub_headers[col_idx]
        value = values[col_idx] if col_idx < len(values) else ""  # Handle ragged rows

        # Skip columns that don't have a year in the sub-header
        if not year or not year.isdigit():
            col_idx += 1
            continue

        # Handle simple metrics (Assets, Net Profit)
        if current_metric_key in ["assets", "net_profit"]:
            company_data[current_metric_key][year] = value
            col_idx += 1

        # Handle complex metrics (Revenue, Cost of Revenue)
        elif current_metric_key in ["revenue", "cost_of_revenue"]:
            data_point = {current_metric_key: value}

            # Check if the next column is a breakdown
            if (
                col_idx + 1 < len(sub_headers)
                and sub_headers[col_idx + 1].strip().lower() == "breakdown"
            ):
                breakdown_value = (
                    values[col_idx + 1] if col_idx + 1 < len(values) else ""
                )
                data_point["breakdown"] = breakdown_value
                company_data[current_metric_key][year] = data_point
                col_idx += 2  # Skip the value and breakdown columns
            else:
                data_point["breakdown"] = ""
                company_data[current_metric_key][year] = data_point
                col_idx += 1
        else:
            col_idx += 1

    print(f"Successfully parsed data for ticker: {company_data['idx_ticker']}")
    return company_data


def main():
    """Main function to run the entire process."""
    conn = None
    try:
        # 1. Connect to Database and create table
        conn, cursor = create_and_connect_db()

        # 2. Connect to Google Sheets
        print("Connecting to Google Sheets...")
        client, spreadsheet_id = createClient()
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet = spreadsheet.worksheet(WORKSHEET_NAME)

        # 3. Get all data from the sheet at once
        all_data = sheet.get_all_values()

        if len(all_data) < 3:
            print(
                "Error: The sheet must contain at least 3 rows (2 for headers, 1 for data)."
            )
            return

        # 4. Extract the two global header rows and the data rows
        main_header_row = all_data[0]
        sub_header_row = all_data[1]
        company_data_rows = all_data[2:]  # All rows from the 3rd one onwards

        print(
            f"Found headers and {len(company_data_rows)} company data rows to process."
        )
        processed_count = 0

        # 5. Iterate through each company's data row
        for i, company_row in enumerate(company_data_rows):
            sheet_row_num = i + 3  # +3 because data starts on row 3
            print(f"\n--- Processing company on sheet row {sheet_row_num} ---")

            # Parse the current row using the global headers
            parsed_data = parse_company_row(
                main_header_row, sub_header_row, company_row
            )

            # If parser returns None, it's an empty/invalid row, so we skip it
            if not parsed_data:
                print(
                    f"\nEncountered an empty row at sheet row {sheet_row_num}. Assuming end of data."
                )
                print("Stopping read process.")
                break  # <-- CHANGE: Exits the loop entirely instead of just skipping.

            # 6. Prepare data for DB insertion
            db_tuple = (
                parsed_data["idx_ticker"],
                parsed_data["name"],
                json.dumps(parsed_data["assets"], indent=2),
                json.dumps(parsed_data["revenue"], indent=2),
                json.dumps(parsed_data["cost_of_revenue"], indent=2),
                json.dumps(parsed_data["net_profit"], indent=2),
            )

            # 7. Add the record to the database transaction
            insert_query = f"""
            INSERT OR REPLACE INTO {TABLE_NAME} (idx_ticker, name, assets, revenue, cost_of_revenue, net_profit)
            VALUES (?, ?, ?, ?, ?, ?);
            """
            cursor.execute(insert_query, db_tuple)
            processed_count += 1

        # 8. Commit all changes to the database at once
        if processed_count > 0:
            conn.commit()

        print("\n==========================================")
        print("Process completed successfully!")
        print(
            f"{processed_count} company records have been saved/updated in '{DB_NAME}'."
        )
        print("==========================================")

    except gspread.exceptions.WorksheetNotFound:
        print(
            f"Error: Worksheet with name '{WORKSHEET_NAME}' not found in the spreadsheet."
        )
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # 9. Close the database connection
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
