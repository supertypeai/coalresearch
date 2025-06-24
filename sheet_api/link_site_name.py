from shapely.geometry       import shape, Point, Polygon
from google_sheets.client   import getSheet
from pyproj                 import Transformer
from random                 import random
from gspread.exceptions     import APIError
from gspread                import Cell
from typing                 import Optional
from shapely.ops            import unary_union

import json 
import time
import gspread
import pandas as pd 

def extract_geometry_from_license(lic_cell: str) -> Optional[Polygon]:
    """
    Parses a JSON-encoded license cell and constructs a unified Polygon or MultiPolygon
    from all valid geometries, robust against individual malformed entries.

    Args:
        lic_cell (str): A JSON string representing a list of license objects, each
                         containing a 'geometry' key with a JSON-encoded coordinate list.

    Returns:
        Optional[Polygon]: A combined Shapely geometry (Polygon or MultiPolygon) of all
                           valid polygons found, or None if none are valid or input invalid.
    """
    if not isinstance(lic_cell, str):
        print(f"Input must be string, got {type(lic_cell)}")
        return None
    
    try:
        # Top-level parse of the license cell into a list
        license_list = json.loads(lic_cell)
    except (json.JSONDecodeError, TypeError) as error:
        print(f"[extract_geometry] Failed to parse license list: {error}")
        return None
    
    valid_polygons = []
    
    # Iterate through each license entry
    for license_obj in license_list:
        geom_str = license_obj.get('geometry')
        if not geom_str or not isinstance(geom_str, str):
            # Skip if no geometry string present
            continue

        try:
            # Parse inner geometry coordinates
            coords = json.loads(geom_str)
            if not coords or not isinstance(coords, list) or not coords[0]:
                # Skip empty or malformed coordinate arrays
                continue

            # Build Shapely polygon
            poly = shape({"type": "Polygon", "coordinates": coords})  

            # Repair invalid polygons (e.g., self-intersections) using buffer trick
            if not poly.is_valid:
                poly = poly.buffer(0)

            # Add only non-empty, valid geometries
            if poly and not poly.is_empty:
                valid_polygons.append(poly)

        except (json.JSONDecodeError, TypeError, ValueError, IndexError) as error:
            # Log and skip individual bad entries
            print(f"[extract_geometry] Skipping malformed polygon: {error}")
            continue

    if not valid_polygons:
        # No valid geometries found
        return None

    # Merge all valid polygons into one geometry
    combined = unary_union(valid_polygons)
    return combined

def safe_update(sheet: gspread.Spreadsheet, cell_list: list[Cell], max_retries: int = 5) -> None:
    """
    Perform batched cell updates with exponential backoff on rate limiting.

    Args:
        sheet (gspread.Spreadsheet): Gspread sheet instance to update.
        cell_list (List[Cell]): List of Cell objects to write.
        max_retries (int): Maximum retry attempts on HTTP 429 errors.
    """
    retries = 0
    while True:
        try:
            sheet.update_cells(cell_list)
            return
        except APIError as error:
            status = getattr(error.response, 'status_code', None)
            # Handle rate limiting
            if status == 429 and retries < max_retries:
                wait = min((2 ** retries) + random(), 60)
                print(f"Rate limited (429). Retry {retries + 1}/{max_retries} in {wait:.1f}s")
                time.sleep(wait)
                retries += 1
            else:
                # Unrecoverable or max retries reached
                print(f"[safe_update] APIError after {retries} retries: {error}")
                raise

def get_sheet_company(sheet_name: str, range_cells: str) -> pd.DataFrame:
    """
    Retrieve company sheet data and attach a 'polygon' column from 'mining_license'.

    Args:
        sheet_name (str): Name of the Google sheet to read.
        range_cells (str): A1 notation range of cells to load.

    Returns:
        pd.DataFrame: DataFrame with an additional 'polygon' column.
    """
    _, company_df = getSheet(sheet_name, range_cells)
    # Apply geometry extraction for each mining_license entry
    company_df["polygon"] = company_df["mining_license"].apply(extract_geometry_from_license)
    return company_df

def get_sheet_mining_site(sheet_name: str, range_cells: str) -> tuple[gspread.Spreadsheet, pd.DataFrame]:
    """
    Retrieve mining site sheet and ensure numeric lat/lng columns.

    Args:
        sheet_name (str): Name of the Google sheet (unused, fixed to 'mining_site').
        range_cells (str): A1 notation range of cells to load.

    Returns:
        Tuple[gspread.Spreadsheet, pd.DataFrame]: Sheet instance and DataFrame with 'lat'/'lng'.
    """
    mining_sheet, mining_site_df = getSheet(sheet_name, range_cells)
    # Coerce latitude/longitude to numeric, invalid parse becomes NaN
    mining_site_df['lat'] = pd.to_numeric(mining_site_df['*latitude'], errors='coerce')
    mining_site_df['lng'] = pd.to_numeric(mining_site_df['*longitude'], errors='coerce')
    return mining_sheet, mining_site_df

def auto_link():
    """ 
    Main workflow: matches mining sites to companies by point-in-polygon and writes back company names.
    """
    # Prepare data 
    c_df = get_sheet_company("company", "A1:S246")
    ms_sheet, ms_df = get_sheet_mining_site("mining_site", "A1:Y51")
    
    # Transformer for coordinate projection
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

    updates = []
    
    # Iterate sites and match
    for idx, site in ms_df.iterrows():
        lat, lng = site["lat"], site["lng"]
        if pd.isna(lat) or pd.isna(lng):
            continue

        # Transform the point into WebMercator
        x, y = transformer.transform(lng, lat)
        pt = Point(x, y)

        # Only test contains if the polygon exists
        matched = c_df[
            c_df["polygon"]
                .apply(lambda poly: poly.contains(pt) if poly is not None else False)
        ]

        if not matched.empty:
            company_name = matched.iloc[0]["name"]
            # sheet rows start at header row + 1
            row = idx + 2  
            col = ms_df.columns.get_loc("*company_name") + 1
            updates.append((row, col, company_name))
        else:
            print(f"No company contains site '{site.get('name')}'")

    # Batch write updates back to sheet
    cell_batch = []
    for row, col, val in updates:
        cell_batch.append(Cell(row, col, val))
        if len(cell_batch) >= 50:  
            safe_update(ms_sheet, cell_batch)
            cell_batch.clear()
            time.sleep(0.3) 

    if cell_batch:
        safe_update(ms_sheet, cell_batch)

    print(f"Wrote {len(updates)} company_id values to mining_site sheet.")


if __name__ == "__main__":
    auto_link()