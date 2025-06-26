from sheet_api.insert_site_name_scraped import merge_coal_databases, get_data_sheet
from rapidfuzz                          import process, fuzz
from sheet_api.google_sheets.auth       import createClient
from sheet_api.link_site_name           import safe_update
from gspread                            import Cell

import gspread
import re 
import json
import pandas as pd 

CLIENT, SPREADSHEET_ID = createClient()

COLS_TO_WRITE = ['company_id','*company_name' ,'*name_scraped',
                 '*province', '*city', '*latitude',
                 '*longitude', '*total_resource',
                 '*total_reserve','*year_measured',
                 'location','resources_reserves', 'mineral_type']

def clean_company_name(name: str) -> tuple[str, str]:
    """
    Clean a company name by removing 'PT' and 'Tbk', lowercasing, and stripping spaces.

    Args:
        name (str): Original company name.

    Returns:
        tuple[str, str]: (cleaned name, cleaned name with no spaces)
    """
    if pd.isna(name):
        return "", ""

    cleaned = re.sub(r"\b(PT|Tbk)\b", "", str(name), flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).lower().strip()
    cleaned_no_space = cleaned.replace(" ", "")
    return cleaned, cleaned_no_space

def clean_company_df(df: pd.DataFrame, company_column: str) -> pd.DataFrame:
    """
    Add cleaned company name columns to a DataFrame and return the cleaned names list.

    Args:
        df (pd.DataFrame): DataFrame with a company name column.
        company_column (str): Name of the column containing company names.

    Returns:
        pd.DataFrame: DataFrame with extra cleaning columns added.
    """
    df = df.copy()

    # Apply cleaning function to get both cleaned versions
    df[['name_cleaned', 'name_cleaned_no_space']] = (
        pd.DataFrame(
            df[company_column].apply(clean_company_name).tolist(),
            index=df.index
        )
    )

    return df

def format_output(results:list, fuzzy_scores: dict,
                  src_idx: int, tgt_idx: int, 
                  df_company: pd.DataFrame, df_merged_filter: pd.DataFrame) -> None: 
    """
    Appends a formatted match entry combining source company details and target record fields, 
    including a matching score based on fuzzy lookup.

    Args:
        results (list[dict]): The list to which the new entry will be appended.
        fuzzy_scores (dict[int, int]): Mapping of target indices to their fuzzy match scores.
        src_idx (int): Index of the source company row in df_company.
        tgt_idx (int): Index of the matched row in df_merged_filter.
        df_company (pd.DataFrame): DataFrame containing source company names.
        df_merged_filter (pd.DataFrame): DataFrame containing filtered target records.

    Returns:
        None: The function modifies `results` in place by adding a new dict.
    """
    results.append({
                'company_id': src_idx+1,
                '*company_name': df_company.loc[src_idx, 'name'],
                'matched_name': df_merged_filter.loc[tgt_idx, 'nama_usaha'],
                '*name_scraped': df_merged_filter.loc[tgt_idx, 'object_name'],
                '*province': df_merged_filter.loc[tgt_idx, 'provinsi_norm'],
                '*city': df_merged_filter.loc[tgt_idx, 'city'],
                '*latitude': df_merged_filter.loc[tgt_idx, 'latitude'],
                '*longitude': df_merged_filter.loc[tgt_idx, 'longitude'],
                '*total_resource': df_merged_filter.loc[tgt_idx, 'total_resource'],
                '*total_reserve': df_merged_filter.loc[tgt_idx, 'total_reserve'],
                '*year_measured': df_merged_filter.loc[tgt_idx, 'year_measured'],
                'mineral_type': df_merged_filter.loc[tgt_idx, 'komoditas_mapped'],
                'matching_score': 100 if tgt_idx not in fuzzy_scores else fuzzy_scores[tgt_idx]
            })

def matching_company(df_merged_filter: pd.DataFrame, 
                     df_company: pd.DataFrame, 
                     threshold: int = 93):
    """
    Matches companies from `df_company` against scraped/filtered entries in `df_merged_filter`
    using exact and fuzzy name matching, and returns a DataFrame of all matches with metadata.

    Args:
        df_merged_filter (pd.DataFrame):
            Target DataFrame containing scraped entries with at least
            'nama_usaha', 'object_name', 'provinsi_norm', 'city', 'latitude',
            'longitude', 'total_resource', 'total_reserve', 'year_measured',
            'komoditas_mapped' columns.
        df_company (pd.DataFrame):
            Source DataFrame of companies with at least a 'name' column.
        threshold (int):
            Minimum fuzzy-match score (0–100) to accept a candidate.

    Returns:
        pd.DataFrame:
            DataFrame where each row represents one match, including:
            - company_id, *company_name, matched_name, *name_scraped, *province, etc.
            - matching_score obtained via exact or fuzzy matching.
    """
    # Clean both DataFrames
    df_company_clean    = clean_company_df(df_company,      'name')
    df_merged_clean     = clean_company_df(df_merged_filter,'nama_usaha')

    results = []

    # Pre-extract the list of normalized names for fuzzy matching
    clean_list = df_merged_clean['name_cleaned'].tolist()

    # Iterate through each source company
    for src_idx, src_row in df_company_clean.iterrows():
        key       = src_row['name_cleaned']
        key_nospc = src_row['name_cleaned_no_space']

        # Exact matches on cleaned-with-spaces
        matches = df_merged_clean[df_merged_clean['name_cleaned'] == key]

        # Fallback to exact on no-space
        if matches.empty:
            matches = df_merged_clean[df_merged_clean['name_cleaned_no_space'] == key_nospc]

        # If still no exact matches, perform fuzzy matching
        fuzzy_scores = {}
        if matches.empty:
            all_ext = process.extract(
                query=key,
                choices=clean_list,
                scorer=fuzz.token_sort_ratio,
                limit=None
            )
            # keep only those above threshold
            for match_str, score, pos in all_ext:
                if score < threshold:
                    break
                idx_in_df = df_merged_clean.index[pos]
                fuzzy_scores[idx_in_df] = score

            # slice df to just those indices
            matches = df_merged_clean.loc[list(fuzzy_scores.keys())]

        # Record every match by formatting into a result entry
        for tgt_idx, tgt_row in matches.iterrows():
            format_output(results, fuzzy_scores, 
                          src_idx, tgt_idx, 
                          df_company, df_merged_filter)

    return pd.DataFrame(results)

def check_unique_data(df_mining_site: pd.DataFrame, df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out records in df_merged whose '*name_scraped' already exist for Gold sites.

    Args:
        df_mining_site (pd.DataFrame): Existing mining sites with a 'mineral_type' column.
        df_merged (pd.DataFrame): Newly merged records containing '*name_scraped'.

    Returns:
        pd.DataFrame: Subset of df_merged with unique '*name_scraped', reindexed and copied.
    """
    # check for site already inserted
    exclude = df_mining_site[df_mining_site['mineral_type'] == 'Gold']['*name_scraped'].to_list()
    print(f"len data ori: {len(exclude)}")

    # Exclude those names and reset index on the filtered DataFrame
    df_to_write = df_merged[~df_merged['*name_scraped'].isin(exclude)].reset_index(drop=True).copy()
    print(f'len unqiue data: {df_to_write.shape}')
    return df_to_write

def format_tonnage_final(tonnes):
    """
    Converts raw tonnage into standardized units: Mt (float, 3 decimals), kt (float, 1 decimal), or t (int).

    Args:
        tonnes (Optional[float]): Raw tonnage value, may be None or NaN.

    Returns:
        Optional[float]: Converted tonnage in Mt, kt, or t; returns None for missing input.
    """
    # Handle missing values upfront
    if pd.isna(tonnes):
        return None

    # Million tonnes: divide by 1e6 and round to 3 decimals
    if abs(tonnes) >= 1_000_000:
        return round(tonnes / 1_000_000, 3)

    # Kilotonnes: divide by 1e3 and round to 1 decimal
    if abs(tonnes) >= 1_000:
        return round(tonnes / 1_000, 1)

    # Tonnes: return as integer
    return int(tonnes)
    
def standardized_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Embeds location and resource/reserve fields as JSON strings in new columns.

    Args:
        df (pd.DataFrame): DataFrame with columns '*province', '*city', '*latitude',
                           '*longitude', '*year_measured', '*total_reserve', '*total_resource'.

    Returns:
        pd.DataFrame: The same DataFrame with additional 'location' and 'resources_reserves' columns.
    """
    # Combine geographic fields into a JSON-encoded 'location'
    df["location"] = df.apply(lambda row: json.dumps({
        "province": row["*province"],
        "city": row["*city"],
        "latitude": row["*latitude"],
        "longitude": row["*longitude"]
    }), axis=1)

    # Combine measurement fields into a JSON-encoded 'resources_reserves'
    df["resources_reserves"] = df.apply(lambda row: json.dumps({
        "year_measured": row["*year_measured"],
        "total_reserve": row["*total_reserve"], 
        "total_resource": row["*total_resource"] 
    }), axis=1)
    
    return df

def validate_columns(cols_to_write: list[str], columns_check: list[str]) -> bool: 
    """
    Vlidate every required column name exists in the list of available columns.

    Args:
        required (List[str]): Column names that must be present.
        available (List[str]): Column names to check against.

    Returns:
        bool: True if all required columns are found, False otherwise (and prints missing names).
    """
    missing_in_sheet = [col for col in cols_to_write if col not in columns_check]
    if missing_in_sheet:
        print("Error: these columns are in cols_to_write but NOT in the sheet:")
        for column in missing_in_sheet:
            print(f" • {column}")
        return False
    return True

def write_to_sheet(start_index: int, 
                   sheet_name: str, 
                   df:pd.DataFrame, 
                   client=CLIENT, spreadsheet_id: str=SPREADSHEET_ID,
                   cols_to_write: list =COLS_TO_WRITE) -> bool:
    """
    Writes specified columns from a DataFrame into a Google Sheets worksheet,
    aligning DataFrame columns to sheet headers and batching updates.

    Args:
        start_index (int): 1-based row number in the sheet where the first DataFrame row will go.
        sheet_name (str): Name of the worksheet tab in the spreadsheet.
        df (pd.DataFrame): Source data to write.
        client: Authenticated gspread client.
        spreadsheet_id (str): ID of the target Google Sheets file.
        cols_to_write (List[str]): Column names to transfer (must exist in both df and sheet headers).

    Returns:
        bool: True if the batch update succeeded, False on any validation or runtime error.
    """
    if not client or not spreadsheet_id:
        print("Error: No valid client or spreadsheet ID available")
        return False

    try:
        # Open the spreadsheet and worksheet
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)

        # Get all headers from the first row of the sheet
        sheet_headers = worksheet.row_values(1)

        # Validate required columns against sheet and DataFrame
        if not validate_columns(cols_to_write, sheet_headers):
            return False
        if not validate_columns(cols_to_write, list(df.columns)):
            return False

        # Map each column name to its 1-based sheet index
        column_mapping = {
            col: sheet_headers.index(col) + 1
            for col in cols_to_write
        }
        print(f"Column mapping: {column_mapping}")

        # Create list of cells to update
        cell_list = []

        # Write data rows
        for row_idx, (_, row) in enumerate(df.iterrows()):
            actual_row = start_index + row_idx

            for col_name, col_position in column_mapping.items():
                if col_name in row:
                    value = row[col_name]

                    # Handle NaN values
                    if pd.isna(value):
                        value = ""

                    # Keep numeric columns as-is, otherwise convert to string
                    if '*total_resource' == col_name or '*total_reserve' == col_name:
                      cell = Cell(row=actual_row, col=col_position, value=value)
                    else:
                      cell = Cell(row=actual_row, col=col_position, value=str(value))
                    cell_list.append(cell)

        # Batch update using your safe_update function
        if cell_list:
            print(f"Writing starting at row {start_index}")
            safe_update(worksheet, cell_list)
            print(f"Successfully wrote {len(df)} rows to '{sheet_name}' in columns: {list(column_mapping.keys())}")
            return True
        
        print("No data to write")
        return False

    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{sheet_name}' not found")
        return False
    except Exception as error:
        print(f"Error writing to sheet: {error}")
        return False

def run_insert_data_scraped(path_esdm:str, path_minerba:str, komoditas: str, 
                            start_index: int, sheet_name:str) -> None: 
    """
    Orchestrates the end-to-end process of merging ESDM and Minerba data, matching companies,
    filtering out duplicates, standardizing metrics, and writing the final dataset to Google Sheets.

    Args:
        path_esdm (str): File path to the ESDM coal dataset CSV.
        path_minerba (str): File path to the Minerba coal dataset CSV.
        komoditas (str): Commodity type to filter during merging (e.g., 'Coal').
        start_index (int): 1-based row in the sheet where insertion should begin.
        sheet_name (str): Name of the Google Sheets tab where data will be written.

    Returns:
        None: The function writes data directly to the specified sheet and does not return a value.
    """
    # Merge the two coal data sources into a unified DataFrame
    df_merged = merge_coal_databases(path_esdm=path_esdm, 
                                     path_minerba=path_minerba, 
                                     is_insert_data=True, 
                                     komoditas=komoditas)
    # Load existing company list and mining site sheet data
    _, df_company = get_data_sheet('company')
    _, df_mining_site_sheet = get_data_sheet('mining_site')
    
    # Match companies by exact and fuzzy logic, then filter out already-inserted sites
    df_to_write = matching_company(df_merged, df_company)
    df_to_write = check_unique_data(df_mining_site_sheet, df_to_write)
    
    # Standardize resource and reserve tonnage units
    df_to_write['*total_resource'] = df_to_write['*total_resource'].apply(format_tonnage_final)
    df_to_write['*total_reserve'] = df_to_write['*total_reserve'].apply(format_tonnage_final)
    
    # Encode location and resource fields as JSON strings
    df_to_write = standardized_data(df_to_write)
    
    # Write the prepared DataFrame into the target Google Sheet
    write_to_sheet(start_index=start_index, sheet_name=sheet_name, df=df_to_write)
    

if __name__ == '__main__':
    path_esdm = 'primary_gold - ESDM.csv'
    path_minerba = 'esdm_minerba_all.csv'
    komoditas = 'gold'
    start_index= 151 
    sheet_name = 'mining_site'
    run_insert_data_scraped(path_esdm, path_minerba, komoditas, start_index, sheet_name)