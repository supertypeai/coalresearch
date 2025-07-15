from sheet_api.google_sheets.auth    import createClient 
from sheet_api.google_sheets.client  import getSheetAll 
from sheet_api.core.compile_to_json  import renderCoalStats, renderMineralStats, renderNickelStats

import gspread
import pandas as pd
import json


SPREADSHEET_NAME = 'company performance'
NEW_SHEET_NAMES = ['gold', 'coal', 'nickel', 'copper', 'silver']
COMMON_COLUMNS = ['id', 'company_id', '*company_name', 'year', 'commodity_type', 'commodity_sub_type']    


def get_json_columns(df: pd.DataFrame, commodity_type: str) -> list[str]:
    """ 
    Extracts the column names from the JSON data in the DataFrame
    for the specified commodity type.

    Args:
        df (pd.DataFrame): The DataFrame containing the commodity data.
        commodity_type (str): The type of commodity to extract columns for.
    
    Returns:
        list[str]: A list of column names extracted from the JSON data.
    """
    # Check if the commodity_type exists in the DataFrame
    commodity_df = df.loc[df['commodity_type'].str.lower() == commodity_type.lower()].copy()
    commodity_df = commodity_df['commodity_stats'].reset_index(drop=True).iloc[0]
    
    # Determine the type of commodity and flatten the JSON data accordingly
    if commodity_type.lower() == 'coal': 
        coal_flat = flatten_coal_data(commodity_df)
        cols_list = list(coal_flat.keys())
    elif commodity_type.lower() in ['gold', 'nickel', 'copper', 'silver']:
        coal_flat = flatten_gold_data(commodity_df)
        cols_list = list(coal_flat.keys())
    return cols_list


def flatten_coal_data(data: dict) -> dict:
    """
    Flattens coal data, matching the header logic.

    Args:
        data (dict): The coal data to flatten.
    
    Returns:
        dict: A flattened dictionary with keys prefixed by '*'.
    """
    # Check if data is a string and parse it if necessary
    if isinstance(data, str):
        data = json.loads(data)

    flat_data = {}

    # Flatten the data, prefixing keys with '*'
    for key, value in data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat_data[sub_key] = sub_value
        else:
            flat_data[key] = value
    return flat_data


def flatten_gold_data(data: dict) -> dict:
    """
    Flattens gold data, matching the header logic.

    Args:
        data (dict): The gold data to flatten.

    Returns:
        dict: A flattened dictionary with keys renamed as needed.
    """
    # Check if data is a string and parse it if necessary
    if isinstance(data, str):
        data = json.loads(data)

    flat_data = {}

    # Flatten the data, renaming keys as needed
    for key, value in data.items():
        if isinstance(value, dict):
            # Rename keys that contain 'reserves' to 'ore_reserves'
            for sub_key, sub_value in value.items():
                renamed = sub_key.replace("reserves", "ore_reserves") if "reserves" in sub_key else sub_key
                # Handle nested dictionaries
                if isinstance(sub_value, dict):
                    for last_key, last_value in sub_value.items():
                        if not isinstance(last_value, dict):
                            col_name = f"{renamed} {last_key}"
                            flat_data[col_name] = last_value
                else:
                    flat_data[renamed] = sub_value
        else:
            flat_data[key] = value
    return flat_data


def create_new_sheets(spreadsheet: gspread.Spreadsheet, df: pd.DataFrame) -> None:
    """
    Creates the new sheets with the correct headers.

    Args:
        spreadsheet (gspread.Spreadsheet): The gspread Spreadsheet object.
        df (pd.DataFrame): The DataFrame containing the company performance data.
    
    Returns:
        None
    """
    print("\nSetting up new sheets")

    # Check if the DataFrame is empty
    for new_sheet in NEW_SHEET_NAMES:
        columns_from_json = get_json_columns(df, new_sheet)
        if not columns_from_json:
            print(f"No columns found for '{new_sheet}'. Skipping sheet creation.")
            continue
        
        # Prepare the final headers
        final_headers = COMMON_COLUMNS + columns_from_json
        new_sheet_name = f"{new_sheet}_performance"
        
        try:
            # Try to get the existing worksheet
            worksheet = spreadsheet.worksheet(new_sheet_name)
            print(f"Sheet '{new_sheet_name}' already exists. Clearing and updating headers.")
            worksheet.clear()
        except gspread.WorksheetNotFound:
            print(f"Creating new sheet: '{new_sheet_name}'...")
            worksheet = spreadsheet.add_worksheet(title=new_sheet_name, rows="100", cols=len(final_headers))
        
        # Update the headers in the new worksheet
        worksheet.update(range_name='A1', values=[final_headers])
        print(f"Successfully set up headers for '{new_sheet_name}'.")


def migrate_data(spreadsheet: gspread.Spreadsheet, df: pd.DataFrame) -> None:
    """
    Migrates the data from the main sheet to the new commodity sheets.
    
    Args:
        spreadsheet (gspread.Spreadsheet): The gspread Spreadsheet object.
        df (pd.DataFrame): The DataFrame containing the company performance data.
    
    Returns:
        None
    """
    print("\n Migrating data")

    # Check if the DataFrame is empty
    for commodity_name in NEW_SHEET_NAMES:
        sheet_name = f"{commodity_name}_performance"
        print(f"Processing data for '{sheet_name}'...")
        
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            headers = worksheet.row_values(1)
        except gspread.WorksheetNotFound:
            print(f"Sheet '{sheet_name}' not found. Cannot migrate data.")
            continue

        migrated_rows = []

        commodity_df = df[df['commodity_type'].str.lower() == commodity_name.lower()]
        for _, row in commodity_df.iterrows():
            try:
                # Create a new row with common columns
                new_row = {col: row.get(col) for col in COMMON_COLUMNS}
                
                # Extract the commodity_stats JSON and flatten it
                if row.get('commodity_stats') and isinstance(row.get('commodity_stats'), str):
                    stats_data = json.loads(row['commodity_stats'])

                    # Write product as a json serializable string
                    if stats_data['product'] is not None:
                        stats_data['product'] = json.dumps(stats_data['product'])
                                        
                    # Flatten the JSON data based on the commodity type
                    if commodity_name == 'coal':
                        flat_stats = flatten_coal_data(stats_data)
                    elif commodity_name in ['gold', 'nickel', 'copper', 'silver']:
                        flat_stats = flatten_gold_data(stats_data)
                    else:
                        flat_stats = {}
                    
                    new_row.update(flat_stats)
                
                migrated_rows.append(new_row)
            except (json.JSONDecodeError, TypeError) as e:
                print(f"Warning: Could not process JSON for row id {row.get('id')}. Error: {e}. Skipping.")
                continue
        
        if not migrated_rows:
            print(f"No data to migrate for '{commodity_name}'.")
            continue

        df_to_write = pd.DataFrame(migrated_rows)
        df_to_write.drop('id', axis=1, inplace=True)

        # Generate a new, clean ID column starting from 1 
        df_to_write.reset_index(drop=True, inplace=True)
        df_to_write.insert(0, 'id', df_to_write.index + 1)
        
        # Reorder columns and fill missing values
        df_to_write = df_to_write.reindex(columns=headers).fillna('')
        
        # Write the DataFrame to the worksheet
        num_rows_to_write = len(df_to_write)
        if num_rows_to_write > 0:
            worksheet.resize(rows=num_rows_to_write + 1)
        
        # Convert DataFrame to a serializable format for Google Sheets
        df_serializable = df_to_write.astype(str)

        # Remove .0 (last on year measured)
        df_serializable['year_measured'] = df_serializable['year_measured'].str.replace(r'\.0$', '', regex=True)

        # Update the worksheet with the new data
        worksheet.update(range_name='A2', values=df_serializable.values.tolist())
        print(f"Successfully migrated {len(df_to_write)} rows to '{sheet_name}'.")


def write_new_company_performance(spreadsheet: gspread.Spreadsheet, df: pd.DataFrame) -> None:
    """
    Creates a new sheet named 'new_company_performance' with a subset of columns
    from the main DataFrame, including a new performance_id column.

    Args:
        spreadsheet (gspread.Spreadsheet): The gspread Spreadsheet object.
        df (pd.DataFrame): The DataFrame containing the company performance data.
    
    Returns:
        None
    """
    print("\nCreating new_company_performance sheet")

    # Define the columns to keep in the new sheet and name of the new sheet
    list_columns = COMMON_COLUMNS + ['commodity_stats']
    new_sheet_name = 'new_company_performance'

    try:
        # Check if all required columns exist in the DataFrame
        missing_cols = [col for col in list_columns if col not in df.columns]
        if missing_cols:
            print(f"Error: The following required columns are missing from the source data: {missing_cols}")
            return

        # Filter the DataFrame to only include the desired columns
        df_new = df[list_columns].copy()
        
        # Generate performance_id column
        id_counters = {}
        performance_ids = []
        
        for _, row in df_new.iterrows():
            # Normalize the commodity_type to lower case and strip whitespace
            commodity_type = row.get('commodity_type', 'unknown').lower().strip()
            
            # Get the current count for this commodity, defaulting to 0 if it's the first time
            current_id = id_counters.get(commodity_type, 0) + 1
            id_counters[commodity_type] = current_id
            
            # Create the unique ID string
            performance_id = f"{commodity_type}_{current_id}"
            performance_ids.append(performance_id)
        
        # Add the performance_id column to the DataFrame
        df_new['performance_id'] = performance_ids
        
        # Reorder columns to put performance_id first
        columns_order = ['performance_id'] + list_columns
        df_new = df_new[columns_order]

        # Create or get the new worksheet
        try:
            worksheet = spreadsheet.worksheet(new_sheet_name)
            print(f"Sheet '{new_sheet_name}' already exists. Clearing and writing new data.")
            worksheet.clear()
        except gspread.WorksheetNotFound:
            print(f"Creating new sheet: '{new_sheet_name}'...")
            worksheet = spreadsheet.add_worksheet(title=new_sheet_name, rows=len(df_new) + 1, cols=len(df_new.columns))
        
        # Write the headers and the data to the new sheet
        worksheet.update(range_name='A1', values=[df_new.columns.values.tolist()] + df_new.values.tolist())
        print(f"Successfully wrote {len(df_new)} rows to '{new_sheet_name}' with performance_id column.")

    except Exception as e:
        print(f"An error occurred in write_new_company_performance: {e}")

def init_restructure() -> None:
    client, spreadsheet_id = createClient()    
    _, df_comp_performance = getSheetAll('company_performance')

    if df_comp_performance is not None:
        spreadsheet = client.open_by_key(spreadsheet_id)
        create_new_sheets(spreadsheet, df_comp_performance)
        migrate_data(spreadsheet, df_comp_performance)
        # write_new_company_performance(spreadsheet, df_comp_performance)
        print("\nMigration script finished.")
    else:
        print("Could not retrieve data. Exiting.")

def update_new_company_performance() -> None:   
    client, spreadsheet_id = createClient()    
    spreadsheet = client.open_by_key(spreadsheet_id)
    new_sheet_name = 'company_performance'
    list_columns = ['performance_id'] + COMMON_COLUMNS + ['commodity_stats']

    df_list = []
    renderMap = {
        'gold': renderMineralStats,
        'coal': renderCoalStats,
        'nickel': renderNickelStats
    }

    for n in NEW_SHEET_NAMES:
        sheet_name = f'{n}_performance'
        _, df = getSheetAll(sheet_name)
        df['performance_id'] = f'{n}_' + df['id']

        renderFunction = renderMap.get(n, renderMineralStats)
        df['commodity_stats'] = df.apply(
            lambda row: json.dumps(renderFunction(row)), axis=1
        )
        df_list.append(df[list_columns])

    df_new = pd.concat(df_list)
    df_new['id'] = ''

    # Create or get the new worksheet
    try:
        worksheet = spreadsheet.worksheet(new_sheet_name)
        print(f"Sheet '{new_sheet_name}' already exists. Clearing and writing new data.")
        worksheet.clear()
    except gspread.WorksheetNotFound:
        print(f"Creating new sheet: '{new_sheet_name}'...")
        worksheet = spreadsheet.add_worksheet(title=new_sheet_name, rows=len(df_new) + 1, cols=len(df_new.columns))
    
    # Write the headers and the data to the new sheet
    worksheet.update(range_name='A1', values=[df_new.columns.values.tolist()] + df_new.values.tolist())
    print(f"Successfully wrote {len(df_new)} rows to '{new_sheet_name}' with performance_id column.")

if __name__ == '__main__':
    update_new_company_performance()
    