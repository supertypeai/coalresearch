from shapely.geometry               import shape, Point, Polygon
from pyproj                         import Transformer
from gspread                        import Cell
from sheet_api.google_sheets.auth   import createClient
from pyproj                         import Transformer
from link_site_name                 import safe_update
from typing                         import Optional, Any, Tuple

import json 
import time
import pandas    as pd 
import geopandas as gpd

CLIENT, SPREADSHEET_ID = createClient()

def check_column_exists(sheet_name: str, column_header: str,
                         client = CLIENT, spreadsheet_id = SPREADSHEET_ID):
    """
    Ensures a column exists in the sheet. Creates it if it doesn't exist.
    
    Args:
        client: Google Sheets client
        spreadsheet_id: ID of the spreadsheet
        sheet_name: Name of the worksheet
        column_header: Header name for the column to check/create
    
    Returns:
        bool: True (Create a new column) | False (Column already exists)
    """
    try:
        # Get worksheet 
        worksheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        
        # Get all values from the first row (headers)
        headers = worksheet.row_values(1)
        
        # Check if column already exists
        if column_header in headers:
            return False
        
        # Find the next available column
        next_col = len(headers) + 1
        
        # Add the header to the next available column
        worksheet.update_cell(1, next_col, column_header)
        
        return True
        
    except Exception as error:
        print(f"Error ensuring column exists: {error}")
        raise

def create_buffered_point(row: pd.Series, transformer: Transformer) -> Optional[Point]:
    """
    Converts latitude and longitude from a DataFrame row into a projected Point.

    Args:
        row (pd.Series): Row containing 'latitude' and 'longitude'.
        transformer (Transformer): Pyproj transformer to convert coordinates.

    Returns:
        Optional[Point]: Shapely Point in projected coordinates or None if invalid.
    """
    try:
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            x, y = transformer.transform(row['longitude'], row['latitude'])
            point = Point(x, y)
            return point   
        return None
    except Exception as error:
        print(f"Error creating buffered point: {error}")
        return None

def coords_to_polygon(coords: Any) -> Optional[Polygon]:
    """
    Converts a list or JSON string of coordinates into a valid Shapely Polygon.
    Repairs self-intersecting or invalid polygons if needed.

    Args:
        coords (Any): JSON string or Python list of polygon coordinates.

    Returns:
        Optional[Polygon]: Valid Shapely Polygon or None if input is invalid or conversion fails.
    """
    if pd.isna(coords) or not coords:
        return None
    
    try:
        if isinstance(coords, str):
            coords = json.loads(coords)

        if isinstance(coords, list) and len(coords) > 0:
            if isinstance(coords[0], list) and len(coords[0]) == 2:
                polygon = Polygon(coords)
            else:
                # GeoJSON-style nested list
                polygon = shape({"type": "Polygon", "coordinates": coords})

            # Attempt to fix invalid polygon geometries
            if not polygon.is_valid:
                polygon = polygon.buffer(0)

            return polygon if not polygon.is_empty else None
        return None

    except Exception as error:
        print(f"Error converting coordinates to polygon: {error}")
        return None
        
def merge_coal_databases(path_esdm: str, path_minerba:str, 
                         is_saved: bool = False) -> pd.DataFrame:
    """
    Merges ESDM and Minerba coal datasets using spatial join based on point-in-polygon logic.

    Args:
        path_esdm (str): Path to the CSV file containing ESDM coal data with lat/lon.
        path_minerba (str): Path to the CSV file containing Minerba polygons.

    Returns:
        pd.DataFrame: Merged DataFrame of ESDM points with matched Minerba companies.
    """
    if not isinstance(path_esdm, str) or not isinstance(path_minerba, str): 
        raise ValueError(f"Input must be a string got type {type(path_esdm)} and {type(path_minerba)}")
    
    # Load the datasets
    esdm = pd.read_csv(path_esdm)
    minerba = pd.read_csv(path_minerba)

    print(f"ESDM records: {len(esdm)}")
    print(f"Minerba records: {len(minerba)}")

    # Convert ESDM coordinates into shapely Point using projected CRS
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    esdm['geometry'] = esdm.apply(lambda row: create_buffered_point(row, transformer), axis=1)
    esdm_gdf = gpd.GeoDataFrame(esdm, geometry='geometry', crs="EPSG:3857")

    # Convert Minerba stringified polygons to valid Shapely geometries
    minerba['geometry_shapely'] = minerba['geometry'].apply(coords_to_polygon)
    minerba_gdf = gpd.GeoDataFrame(minerba, geometry='geometry_shapely', crs="EPSG:3857")

    # Remove rows with no geometry
    esdm_gdf = esdm_gdf.dropna(subset=['geometry'])
    minerba_gdf = minerba_gdf.dropna(subset=['geometry'])

    print(f"ESDM with valid geometry: {len(esdm_gdf)}")
    print(f"Minerba with valid geometry: {len(minerba_gdf)}")

    # Perform spatial join to find ESDM points within Minerba polygons
    merged = gpd.sjoin(esdm_gdf, 
                       minerba_gdf, 
                       how='left', 
                       predicate='within', 
                       lsuffix='_esdm', 
                       rsuffix='_minerba')

    print(f"Merged records: {len(merged)}")
    
    # Select relevant columns to use
    merged = merged[['object_name','nama_usaha','longitude', 
                     'latitude','geometry','badan_usaha']]
    
    # Save to csv
    if is_saved: 
        merged.to_csv("merged_esdm_coal_and_minerba.csv", index=False)
    
    return merged

def get_mining_sheet(sheet_name: str,
                     client = CLIENT, 
                     spreadsheet_id = SPREADSHEET_ID) -> tuple[Any, pd.DataFrame]: 
    """
    Fetch mining data from Google Sheets and convert to a pandas DataFrame.

    Args:
        sheet_name (str): Name of the worksheet.
        client (Any): gspread client.
        spreadsheet_id (str): Google Spreadsheet ID.

    Returns:
        Tuple[Any, pd.DataFrame]: Worksheet object and the corresponding DataFrame.
    """
    worksheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
        
    # Get all values from the first row (headers)
    all_values = worksheet.get_all_values()
    if not all_values:
        # Empty sheet
        return pd.DataFrame()
    
    # First row as headers, rest as data
    headers = all_values[0]
    data = all_values[1:]
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=headers)
    return worksheet, df

def standardized_data(df_mining: pd.DataFrame, 
                      esdm_merged: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]: 
    """
    Standardizes column formats and coordinate precision for matching.

    Args:
        df_mining (pd.DataFrame): Original mining_site DataFrame.
        esdm_merged (pd.DataFrame): Merged ESDM-Minerba DataFrame.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Updated mining and merged frames with
            lowercased names and rounded coord strings.
    """
    # Combine company name fields for matching key
    esdm_merged['*company_name'] = esdm_merged['badan_usaha'] + ' ' + esdm_merged['nama_usaha']

    # Normalize text fields: lowercase, trim whitespace
    for df in (df_mining, esdm_merged):
        df['*company_name'] = df['*company_name'].astype(str).str.lower().str.strip()

    # Numeric conversion with coercion of invalid entries to NaN
    df_mining['*latitude'] = pd.to_numeric(df_mining['*latitude'], errors='coerce')
    df_mining['*longitude'] = pd.to_numeric(df_mining['*longitude'], errors='coerce')
    esdm_merged['latitude'] = pd.to_numeric(esdm_merged['latitude'], errors='coerce')
    esdm_merged['longitude'] = pd.to_numeric(esdm_merged['longitude'], errors='coerce')
    
    # Round to fixed decimal places for string matching keys
    decimal_places = 5    
    df_mining['lon_str'] = df_mining['*longitude'].round(decimal_places).astype(str)
    df_mining['lat_str'] = df_mining['*latitude'].round(decimal_places).astype(str)

    esdm_merged['lon_str'] = esdm_merged['longitude'].round(decimal_places).astype(str)
    esdm_merged['lat_str'] = esdm_merged['latitude'].round(decimal_places).astype(str)
    
    return df_mining, esdm_merged

def merge_confidence_keys(
                df_mining: pd.DataFrame,
                esdm_merged: pd.DataFrame
            ) -> pd.DataFrame:
    """
    High-confidence merge using company name and exact coordinates.

    Args:
        df_mining (pd.DataFrame): Standardized mining sites.
        esdm_merged (pd.DataFrame): Standardized ESDM records.

    Returns:
        pd.DataFrame: Tier 1 merged DataFrame with 'site_name_scraped'.
    """
    # Merge based on company_name, latitude, and longtitude
    high_confidence_keys = ['*company_name', 'lat_str', 'lon_str']
    merged_df_first = pd.merge(df_mining, esdm_merged, on=high_confidence_keys, how='left')
    
    merged_tier1 = merged_df_first.copy()
    merged_tier1.rename(columns={'object_name': 'site_name_scraped'}, inplace=True)
    
    print(f"Merge_confidence_keys matched {merged_tier1['site_name_scraped'].notna().sum()} records.")
    return merged_tier1

def merge_coordinate_keys(df_merged_confidence: pd.DataFrame,
                    esdm_merged: pd.DataFrame
                ) -> Tuple[pd.DataFrame, set]:
    """
    Second-tier merge using only coordinate keys for remaining unmatched sites.

    Args:
        merged_conf (pd.DataFrame): Output from merge_confidence_keys.
        esdm_merged (pd.DataFrame): Standardized ESDM records.

    Returns:
        Tuple[pd.DataFrame, set]: Tier 2 merged and set of matched site names.
    """
    # Identify which scraped sites were successfully used in Tier 1
    used_in_tier1 = set(df_merged_confidence['site_name_scraped'].dropna())

    # Identify which original sites still need a match
    unmatched_after_tier1 = df_merged_confidence[df_merged_confidence['site_name_scraped'].isna()].copy()
    unmatched_after_tier1.drop(columns=['site_name_scraped'], inplace=True) # Drop empty column

    # Create the pool of available scraped sites for Tier 2
    available_for_tier2 = esdm_merged[~esdm_merged['object_name'].isin(used_in_tier1)]

    location_only_keys = ['lat_str', 'lon_str']

    # Drop duplicates on the keys from the right-side to avoid one-to-many
    columns_to_bring_tier2 = location_only_keys + ['object_name']

    # Merge based on lat and lon
    merged_tier2 = pd.merge(
        unmatched_after_tier1,
        available_for_tier2[columns_to_bring_tier2].drop_duplicates(subset=location_only_keys),
        on=location_only_keys,
        how='left'
    )

    print(f"Tier 2 matched {merged_tier2['object_name'].notna().sum()} additional records.")
    return merged_tier2, used_in_tier1
    
def merge_on_company_name(df_merged_coordinate: pd.DataFrame, 
                          df_used_merge_confidence: pd.DataFrame, 
                          esdm_merge: pd.DataFrame) -> pd.DataFrame:
    """
    Final fallback merge on company name only for any remaining unmatched sites.

    Args:
        tier2 (pd.DataFrame): DataFrame after coordinate-based merge.
        used_t1 (set): Site names used in Tier 1.
        esdm_merged (pd.DataFrame): Standardized ESDM records.

    Returns:
        pd.DataFrame: Tier 3 merged DataFrame with fallback matching.
    """
    # Identify what was used in Tier 2
    used_in_tier2 = set(df_merged_coordinate['object_name'].dropna())
    used_in_tier1_and_2 = df_used_merge_confidence.union(used_in_tier2)

    # Identify which original sites STILL need a match.
    unmatched_after_tier2 = df_merged_coordinate[df_merged_coordinate['object_name'].isna()].copy()
    unmatched_after_tier2.drop(columns=['object_name'], inplace=True)

    # Create the pool for the final fallback.
    available_for_tier3 = esdm_merge[~esdm_merge['object_name'].isin(used_in_tier1_and_2)]
    fallback_candidates = available_for_tier3.drop_duplicates(subset=['*company_name'], keep='first')

    # Merge on just company name
    merged_tier3 = pd.merge(
        unmatched_after_tier2,
        fallback_candidates[['*company_name', 'object_name']],
        on = '*company_name',
        how='left',
        suffixes=('', '_fallback')
    )
    print(f"Tier 3 matched {merged_tier3['object_name'].notna().sum()} additional records.")
    return merged_tier3
    
def combine_all_merged_data(df_confidence: pd.DataFrame, 
                            df_coordinate: pd.DataFrame, 
                            df_company: pd.DataFrame) -> pd.DataFrame:
    """
    Stack results from all merge tiers into one DataFrame of matched records.
        
    Args:
        df_confidence (pd.DataFrame): Tier 1 merged data.
        df_coordinate (pd.DataFrame): Tier 2 merged data.
        df_company (pd.DataFrame): Tier 3 merged data.
        
    Returns:
        pd.DataFrame: Consolidated DataFrame of all matched entries.
    """
    # Start with Tier 1 results
    final_df = df_confidence.copy()
    
    # Create mappings from 'name' to 'object_name' for tier 2 and tier 3
    # Only include rows where object_name is not null
    tier2_mapping = df_coordinate[df_coordinate['object_name'].notna()].set_index('name')['object_name'].to_dict()
    tier3_mapping = df_company[df_company['object_name'].notna()].set_index('name')['object_name'].to_dict()
    
    # Loop first dataframe merging
    for idx, row in final_df.iterrows(): 
        site_name = row['name']
        current_scraped = row['site_name_scraped']

        # Only update empty values on the first dataframe merge
        if pd.isna(current_scraped):
            # Update if name in tier2 and assign with tier2 value
            if site_name in tier2_mapping:
                final_df.at[idx, 'site_name_scraped'] = tier2_mapping[site_name]
            # Update if name in tier3 and assign with tier3 value
            elif site_name in tier3_mapping:
                final_df.at[idx, 'site_name_scraped'] = tier3_mapping[site_name]
          
    total_matched = final_df['site_name_scraped'].notna().sum()
    total_records = len(final_df)
    
    print(f"Final result: {total_matched}/{total_records} records matched ({total_matched/total_records*100:.1f}%)")
    return final_df

def sanitize_value(val: Any) -> str:
    """
    Ensures values are JSON-safe by converting NaN, inf, or None to empty string.
    
    Args:
        val (Any): Input value.
        
    Returns:
        str: Sanitized string.
    """
    if pd.isna(val):
        return ""
    if isinstance(val, float):
        # Check for NaN
        if not (val == val):  
            return ""
        if val == float('inf') or val == float('-inf'):
            return ""
    return str(val)
    
def write_into_sheet(final_df: pd.DataFrame, 
                     mining_sheet: Any,
                     mining_df: pd.DataFrame, 
                     new_column: str) -> None: 
    """
    For each matched record, locates the corresponding row in the mining sheet and writes
    the scraped object name into the 'name_scraped' column. Uses batch updates to minimize API calls.
        
    Args:
        final_df (pd.DataFrame): Consolidated matched records.
        mining_sheet (Any): Worksheet instance.
        mining_df (pd.DataFrame): Original mining site DataFrame.
        
    Returns:
        None
    """
    try:
        # Find the 1-based index of the 'name_scraped' column for updateCells
        target_col = mining_df.columns.get_loc(new_column) + 1
    except KeyError as error:
        print(f"Required column not found in sheet: {error}")
        return
    
    updates = []
    # Iterate through all matched entries
    for idx, row in final_df.iterrows():
        site_name_scraped = sanitize_value(row['site_name_scraped'])
        
        if site_name_scraped and pd.notna(site_name_scraped):
            sheet_row = idx + 2  
            updates.append((sheet_row, target_col, site_name_scraped))
            
            site_name = sanitize_value(row.get('name', ''))
            print(f"Updating '{site_name}' at row {sheet_row} with '{site_name_scraped}'")
    
    if not updates:
        print("No updates to perform")
        return
    
    # Perform batched API calls to update sheet
    cell_batch = []
    for row, col, val in updates:
        # Create Cell object with sanitized value
        cell_batch.append(Cell(row, col, val))
        if len(cell_batch) >= 50:
            # Flush batch when it reaches threshold
            safe_update(mining_sheet, cell_batch)
            cell_batch.clear()
            time.sleep(0.3)
    
    # Flush any remaining cells
    if cell_batch:
        safe_update(mining_sheet, cell_batch)
    
    print(f"Successfully updated {len(updates)} name_scraped values")
    
def auto_write_name_scraped(path_esdm: str, 
                            path_minerba:str, 
                            mining_site_name:str, 
                            new_column: str = "*name_scraped") -> None:
    """
    Runs end-to-end process: merges ESDM & Minerba data, matches sites, and writes back
    scraped names into the Google Sheet.
    
    Args:
        path_esdm (str): Path to ESDM CSV file.
        path_minerba (str): Path to Minerba CSV file.
        mining_site_name (str): Worksheet name containing mining sites.
        new_column (str): Column to hold scraped names.
        
    Returns:
        None
    """ 
    # Checking new column exists and insert it if not exist yet
    check_column_exists(mining_site_name, new_column)
    
    # Merge data scraped esdm_coal and esdm_minerba
    esdm_merged = merge_coal_databases(path_esdm, path_minerba)
    
    # Get data mining_site 
    mining_sheet, df_mining = get_mining_sheet(mining_site_name)
    
    # Standardized columns for matching
    df_mining_standardized, esdm_merged_standardized = standardized_data(df_mining, esdm_merged)
    
    # Three steps merging 
    merged_df_confidence = merge_confidence_keys(df_mining_standardized, esdm_merged_standardized)
    merged_df_coordinate, set_df_used_confidence = merge_coordinate_keys(merged_df_confidence, esdm_merged_standardized)
    merged_df_company = merge_on_company_name(merged_df_coordinate, set_df_used_confidence, esdm_merged_standardized)
    
    # Stack all data merging 
    final_df = combine_all_merged_data(merged_df_confidence, merged_df_coordinate, merged_df_company)
    
    # Write data into new column 
    write_into_sheet(final_df, mining_sheet, df_mining, new_column)    
    
    
if __name__ == "__main__":
    path_esdm = "coal_db - ESDM_coal.csv"
    path_minerba = "coal_db - minerba.csv"
    mining_site = "mining_site"
    auto_write_name_scraped(path_esdm, path_minerba, mining_site)

    