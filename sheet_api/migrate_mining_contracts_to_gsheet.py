from google_sheets.auth import createClient

import pandas as pd
import json
import gspread

# Authenticate and get CLIENTs
CLIENT, SPREADSHEET_ID = createClient()

def deduplicate_headers(headers: list) -> list:
    """
    Deduplicate column headers by appending a suffix to duplicates.

    Args:
        headers (list[str]): List of header names.

    Returns:
        list[str]: List of unique header names.
    """
    seen = {}
    unique_headers = []
    for header in headers:
        if header in seen:
            seen[header] += 1
            unique_headers.append(f"{header}_{seen[header]}")
        else:
            seen[header] = 0
            unique_headers.append(header)
    return unique_headers

def get_data_sheet(sheet_name: str, spreadsheet_id: str = SPREADSHEET_ID, 
                   client: gspread.client.Client = CLIENT) -> tuple:
    """
    Opens a specific worksheet from a Google Sheet and returns it as a pandas DataFrame.

    Args:
        spreadsheet_id: The ID of the Google Spreadsheet.
        sheet_name: The name of the worksheet to read.

    Returns:
        A pandas DataFrame containing the worksheet's data.
    """
    try: 
        # Get data from sheet
        worksheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        raw_values = worksheet.get_all_values()
        
        if not raw_values: 
            return pd.DataFrame()
        
        headers = deduplicate_headers(raw_values[0])
        df = pd.DataFrame(raw_values[1:], columns=headers)
        return df, worksheet
    
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{sheet_name}' not found in the spreadsheet.")
        return pd.DataFrame(), None 
    except Exception as error:
        print(f"An unexpected error occurred: {error}")
        return pd.DataFrame(), None

def prepare_data_contract(company_df: pd.DataFrame, 
                          mining_contract_df: pd.DataFrame) -> pd.DataFrame: 
    """
    Merge mining contract information into the company DataFrame.

    For each company, aggregates all related mining contracts into a JSON array and stores it
    in the 'mining_contract' column. Ensures IDs are normalized for matching.

    Args:
        company_df (pd.DataFrame): DataFrame containing company information (must have 'id' column).
        mining_contract_df (pd.DataFrame): DataFrame containing mining contract information (must have 'contractor_id' column).

    Returns:
        pd.DataFrame: Updated company DataFrame with a 'mining_contract' column containing JSON arrays.
    """
    processed_df = company_df.copy()
    # Ensure 'mining_contract' column exists in company_df
    if "mining_contract" not in processed_df.columns:
        processed_df["mining_contract"] = ""

    # Clean and normalize IDs for reliable matching
    contracts_clean = mining_contract_df.copy()
    contracts_clean["contractor_id"] = pd.to_numeric(
        contracts_clean["contractor_id"], errors="coerce"
    )
    contracts_clean.dropna(subset=["contractor_id"], inplace=True)
    contracts_clean["contractor_id"] = (
        contracts_clean["contractor_id"].astype(int).astype(str)
    )
    processed_df["id"] = processed_df["id"].astype(str)

    # Group contracts by contractor_id
    grouped = contracts_clean.groupby("contractor_id")

    # Create a mapping from company id to its index for efficient updates
    id_to_index_map = {id_val: index for index, id_val in processed_df["id"].items()}

    # For each contractor, aggregate their contracts into a JSON array with the new structure
    for contractor_id, group in grouped:
        if contractor_id in id_to_index_map:
            idx = id_to_index_map[contractor_id]

            contract_list = []
            for _, row in group.iterrows():
                agreement_type_str = row.get("Agreement type", "")
                agreement_types = (
                    [item.strip() for item in agreement_type_str.split(",")]
                    if agreement_type_str
                    else []
                )

                new_contract = {
                    "company_name": row.get("*mine_owner_name"),
                    "company_id": row.get("mine_owner_id"),
                    "contract_period_end": row.get("contract_period_end"),
                    "agreement_type": agreement_types,
                }
                contract_list.append(new_contract)

            contracts_json = json.dumps(contract_list)
            processed_df.at[idx, "mining_contract"] = contracts_json

    # Fill empty mining_contract cells with '[]'
    processed_df["mining_contract"] = processed_df["mining_contract"].fillna("[]")
    processed_df.loc[processed_df["mining_contract"] == "", "mining_contract"] = "[]"
    return processed_df 

def write_sheet(df: pd.DataFrame, worksheet: str) -> None:
    """
    Write the given DataFrame to the specified Google Sheet worksheet.

    Args:
        df (pd.DataFrame): DataFrame to write.
        company_sheet (gspread.worksheet.Worksheet): Worksheet object to update.

    Returns:
        None
    """
    # Convert dataframe to list of lists to update the sheet
    updated_values = [df.columns.values.tolist()] + df.fillna(
        ""
    ).values.tolist()

    # Update the entire 'company' sheet
    worksheet.clear()
    worksheet.update(
        range_name="A1", values=updated_values, value_input_option="USER_ENTERED"
    )

    print("Updated 'company' sheet with restructured mining contract JSON data.")

def run_write_contract(): 
    """
    Main routine to read mining contract and company data, merge them, and write back to Google Sheets.
    """
    # Get data company and mining_contract
    mining_contract_df, _ = get_data_sheet("mining_contract")
    company_df, company_sheet = get_data_sheet("company")
    # Prepare data and write back to sheet company
    processed_df = prepare_data_contract(company_df, mining_contract_df)
    write_sheet(processed_df, company_sheet)
    
if __name__ == '__main__':
    run_write_contract()