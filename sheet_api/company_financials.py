import gspread
import sqlite3
import json
import os
import re

from sheet_api.google_sheets.auth import createClient

# --- Configuration ---
# The name of the worksheet (tab) in your Google Sheet to read from.
WORKSHEET_NAME = "company_financials"
# The name of the SQLite database file.
DB_NAME = "db.sqlite"
# The name of the table to create/update in the database.
# NOTE: Changed the table name to reflect the new, yearly structure.
TABLE_NAME = "company_financials"


def to_float(value_str):
    """Safely converts a string to a float, handling commas and empty/invalid values."""
    if not isinstance(value_str, str) or not value_str.strip():
        return None
    try:
        # Remove commas used as thousands separators
        return float(value_str.replace(",", ""))
    except ValueError:
        return None


def parse_breakdown_string(s):
    """
    Parses a complex breakdown string into a dictionary.
    Handles formats like:
    - "Key1 123.45; Key2 678.90"
    - "123.45 Key1; 678.90 Key2"
    - "Main Key 123.45 (SubKey1: 50; SubKey2: 73.45)"
    """
    if not s or not s.strip():
        return {}

    breakdown_dict = {}

    # First, extract and process any parenthetical details
    # e.g., "(Royalty: 339.79)"
    parentheticals = re.findall(r"\((.*?)\)", s)
    for p_content in parentheticals:
        # Can be "key: val" or just another breakdown
        if ":" in p_content:
            parts = p_content.split(":")
            if len(parts) == 2:
                key = parts[0].strip()
                val = to_float(parts[1].strip())
                if key and val is not None:
                    breakdown_dict[key] = val
        else:
            # If no colon, parse it like a regular part
            nested_parts = parse_breakdown_string(p_content)
            breakdown_dict.update(nested_parts)

    # Remove the parenthetical parts for main processing
    main_s = re.sub(r"\(.*?\)", "", s).strip()

    # Split the main string by semicolon
    items = [item.strip() for item in main_s.split(";") if item.strip()]

    for item in items:
        # Find the numeric value in the item
        num_match = re.search(r"[\d,.]+", item)
        if num_match:
            value_str = num_match.group(0)
            value = to_float(value_str)
            # The key is what's left after removing the number
            key = item.replace(value_str, "").strip()
            if key and value is not None:
                breakdown_dict[key] = value

    return breakdown_dict


def create_and_connect_db():
    """Connects to SQLite and creates the new, flattened table if it doesn't exist."""
    print(f"Connecting to SQLite database: {DB_NAME}...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    print(f"Ensuring table '{TABLE_NAME}' exists...")
    # New schema: one row per company per year
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        idx_ticker TEXT,
        name TEXT,
        year INTEGER,
        assets REAL,
        revenue REAL,
        revenue_breakdown TEXT,
        cost_of_revenue REAL,
        cost_of_revenue_breakdown TEXT,
        net_profit REAL,
        PRIMARY KEY (idx_ticker, year)
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Database and table are ready.")
    return conn, cursor


def parse_company_row(headers, sub_headers, values):
    """
    Parses a single company's row and returns a list of yearly records.

    Args:
        headers (list): The main header row (sheet row 1).
        sub_headers (list): The sub-header row (sheet row 2).
        values (list): The data row for a single company.

    Returns:
        list: A list of structured dictionaries, one for each year.
    """
    if not values or not values[0] or not values[0].strip():
        return None

    company_base_info = {"idx_ticker": values[0], "name": values[1]}
    yearly_data = {}  # Using a dict with year as key to aggregate data

    header_map = {
        "Assets (in USD millions)": "assets",
        "Revenue (in USD millions)": "revenue",
        "Cost of Revenue": "cost_of_revenue",
        "Net Profit (in USD millions)": "net_profit",
    }

    current_metric_key = None
    col_idx = 2
    while col_idx < len(headers):
        header_text = headers[col_idx].strip()
        if header_text in header_map:
            current_metric_key = header_map[header_text]

        if not current_metric_key:
            col_idx += 1
            continue

        year_str = sub_headers[col_idx]
        if not year_str or not year_str.isdigit():
            col_idx += 1
            continue

        # Initialize the dictionary for this year if it's the first time we see it
        if year_str not in yearly_data:
            yearly_data[year_str] = {
                "year": int(year_str),
                "assets": None,
                "revenue": None,
                "revenue_breakdown": {},
                "cost_of_revenue": None,
                "cost_of_revenue_breakdown": {},
                "net_profit": None,
            }

        value = values[col_idx] if col_idx < len(values) else ""

        if current_metric_key in ["assets", "net_profit"]:
            yearly_data[year_str][current_metric_key] = to_float(value)
            col_idx += 1

        elif current_metric_key in ["revenue", "cost_of_revenue"]:
            yearly_data[year_str][current_metric_key] = to_float(value)

            # Check for a breakdown in the next column
            if (
                col_idx + 1 < len(sub_headers)
                and sub_headers[col_idx + 1].strip().lower() == "breakdown"
            ):
                breakdown_value = (
                    values[col_idx + 1] if col_idx + 1 < len(values) else ""
                )
                breakdown_dict = parse_breakdown_string(breakdown_value)
                yearly_data[year_str][
                    f"{current_metric_key}_breakdown"
                ] = breakdown_dict
                col_idx += 2  # Skip value and breakdown columns
            else:
                col_idx += 1
        else:
            col_idx += 1

    # Convert the dictionary of yearly data into a list of final records
    final_records = []
    for year_rec in yearly_data.values():
        # Combine the base company info with the specific year's data
        full_record = {**company_base_info, **year_rec}
        final_records.append(full_record)

    print(
        f"Successfully parsed {len(final_records)} yearly records for ticker: {company_base_info['idx_ticker']}"
    )
    return final_records


def main():
    """Main function to run the entire process."""
    conn = None
    try:
        conn, cursor = create_and_connect_db()

        print("Connecting to Google Sheets...")
        client, spreadsheet_id = createClient()
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet = spreadsheet.worksheet(WORKSHEET_NAME)

        all_data = sheet.get_all_values()
        if len(all_data) < 3:
            print(
                "Error: The sheet must contain at least 3 rows (2 for headers, 1 for data)."
            )
            return

        main_header_row = all_data[0]
        sub_header_row = all_data[1]
        company_data_rows = all_data[2:]

        print(
            f"Found headers and {len(company_data_rows)} potential company rows to process."
        )
        processed_count = 0

        for i, company_row in enumerate(company_data_rows):
            sheet_row_num = i + 3
            print(f"\n--- Processing company on sheet row {sheet_row_num} ---")

            yearly_records = parse_company_row(
                main_header_row, sub_header_row, company_row
            )

            if not yearly_records:
                print(
                    f"Encountered an empty or invalid row at sheet row {sheet_row_num}. Stopping read process."
                )
                break

            # Iterate through each yearly record and insert it into the DB
            for record in yearly_records:
                db_tuple = (
                    record["idx_ticker"],
                    record["name"],
                    record["year"],
                    record["assets"],
                    record["revenue"],
                    json.dumps(record["revenue_breakdown"]),
                    record["cost_of_revenue"],
                    json.dumps(record["cost_of_revenue_breakdown"]),
                    record["net_profit"],
                )

                insert_query = f"""
                INSERT OR REPLACE INTO {TABLE_NAME} (
                    idx_ticker, name, year, assets, revenue, revenue_breakdown, 
                    cost_of_revenue, cost_of_revenue_breakdown, net_profit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                cursor.execute(insert_query, db_tuple)
                processed_count += 1
                print(
                    f"  > Saved data for {record['idx_ticker']} for year {record['year']}."
                )

        if processed_count > 0:
            conn.commit()

        print("\n==========================================")
        print("Process completed successfully!")
        print(
            f"{processed_count} yearly records have been saved/updated in '{TABLE_NAME}' table in '{DB_NAME}'."
        )
        print("==========================================")

    except gspread.exceptions.WorksheetNotFound:
        print(
            f"Error: Worksheet with name '{WORKSHEET_NAME}' not found in the spreadsheet."
        )
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # For debugging, it can be useful to see the full traceback
        import traceback

        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
