"""
This script processes separate tables within the 'global_commodity_data' sheet,
combines the data, and populates a master table in the same sheet.

The script performs the following actions:
1.  Reads data from source tables:
    -   Global Coal Resource and Reserves 2020 (I3:K39)
    -   Coal Production Volume (M3:X31)
    -   Coal Export Import (AA3:AC84)
    -   Nickel Production Volume (M54:X62)
    -   Copper Production Volume (M65:X73)
    -   Bauxite Production Volume (M79:X87)
    -   Gold Production Volume (M94:X145)
2.  Processes and transforms the data for a predefined list of countries into JSON format.
3.  Merges the processed data into a single DataFrame.
4.  Writes the final, combined data to the master table, overwriting existing content.

"""

import json
from functools import reduce

import gspread
import pandas as pd
from sheet_api.google_sheets.auth import createClient

# List of countries to process
COUNTRY_LIST = {
    "Albania",
    "Andorra",
    "Antigua and Barbuda",
    "Australia",
    "Austria",
    "Bangladesh",
    "Belgium",
    "Bosnia & Herzegovina",
    "Botswana",
    "Brazil",
    "Bulgaria",
    "Burkina Faso",
    "Canada",
    "Chile",
    "China",
    "Colombia",
    "Croatia",
    "Czech Republic",
    "Cyprus",
    "Democratic Republic of Congo",
    "Denmark",
    "Djibouti",
    "Dominican Republic",
    "Ecuador",
    "Ethiopia",
    "Finland",
    "France",
    "Georgia",
    "Germany",
    "Ghana",
    "Greece",
    "Hungary",
    "India",
    "Indonesia",
    "Ireland",
    "Italy",
    "Japan",
    "Kazakhstan",
    "Kenya",
    "Kyrgyz Republic",
    "Latvia",
    "Lesotho",
    "Lithuania",
    "Luxembourg",
    "Madagascar",
    "Malaysia",
    "Maldives",
    "Malta",
    "Mexico",
    "Middle East",
    "Mongolia",
    "Mozambique",
    "Namibia",
    "Netherlands",
    "New Caledonia",
    "New Zealand",
    "Nigeria",
    "North Macedonia",
    "Norway",
    "Others",
    "Pakistan",
    "Peru",
    "Philippines",
    "Poland",
    "Portugal",
    "Romania",
    "Russian Federation",
    "Senegal",
    "Serbia",
    "Singapore",
    "Slovak Republic",
    "Slovakia",
    "Slovenia",
    "South Africa",
    "South Korea",
    "Spain",
    "Sweden",
    "Switzerland",
    "Tanzania",
    "Thailand",
    "Tunisia",
    "Turkey",
    "Uganda",
    "Ukraine",
    "United Kingdom",
    "United States",
    "Uzbekistan",
    "Venezuela",
    "Vietnam",
    "Zimbabwe",
}


def get_dataframe_from_range(sheet, range_name):
    """Fetches data from a sheet range and returns a pandas DataFrame."""
    try:
        data = sheet.get(range_name)
        if not data or len(data) < 2:
            print(f"Warning: No data found or only headers in range {range_name}.")
            return pd.DataFrame()
        return pd.DataFrame(data[1:], columns=data[0])
    except gspread.exceptions.APIError as e:
        print(f"Error fetching range {range_name}: {e}")
        return pd.DataFrame()


def process_resources_reserves(df, country_list):
    print("Processing 'Global Coal Resource and Reserves 2020' data...")
    
    if df.empty:
        return pd.DataFrame(columns=["country", "resources_reserves"])
    
    required_cols = ["Country", "Anthracite", "Sub-bituminous & Bituminous & Lignite"]
    if not all(col in df.columns for col in required_cols):
        missing = [c for c in required_cols if c not in df.columns]
        print(f"Error: Missing columns {missing}")
        return pd.DataFrame(columns=["country", "resources_reserves"])

    processed_data = []
    country_set = {c.strip() for c in country_list}

    for _, row in df.iterrows():
        country_raw = row.get("Country")
        country = country_raw.strip() if isinstance(country_raw, str) else None

        if not country or country not in country_set:
            print(f"{country} is not registered in country_list, skipping this.")
            continue

        anthracite_val = pd.to_numeric(row["Anthracite"], errors="coerce")
        sub_bit_val = pd.to_numeric(row["Sub-bituminous & Bituminous & Lignite"], errors="coerce")

        if pd.isna(anthracite_val) or pd.isna(sub_bit_val):
            print(f"Skipping row for '{country}' due to NaN values")
            continue

        processed_data.append({
            "country": country, 
            "resources_reserves": json.dumps({
                "2020": {
                    "anthracite": anthracite_val.item(),
                    "sub_bituminous_bituminous_lignite": sub_bit_val.item()
                }
            })
        })

    return pd.DataFrame(processed_data)

def process_resources_reserves_shares(df, country_list):
    print("Processing 'Global Coal Resource and Reserves 2020 Shares' data...")
    
    if df.empty:
        return pd.DataFrame(columns=["country", "resources_reserves_share"])
    
    required_cols = ["Country", "Anthracite", "Sub-bituminous & Bituminous & Lignite"]
    if not all(col in df.columns for col in required_cols):
        missing = [c for c in required_cols if c not in df.columns]
        print(f"Error: Missing columns {missing}")
        return pd.DataFrame(columns=["country", "resources_reserves_share"])

    work_df = df.copy()
    cols_to_share = ["Anthracite", "Sub-bituminous & Bituminous & Lignite"]
    
    for col in cols_to_share:
        work_df[col] = pd.to_numeric(work_df[col], errors="coerce").fillna(0)
        col_sum = work_df[col].sum()
        # Vectorized share calculation: (Series / total) * 100
        share_col_name = f"{col}_share"
        if col_sum > 0:
            work_df[share_col_name] = (work_df[col] / col_sum * 100).round(2)
        else:
            work_df[share_col_name] = 0.0

    country_set = {c.strip() for c in country_list}
    work_df["Country"] = work_df["Country"].astype(str).str.strip()
    filtered_df = work_df[work_df["Country"].isin(country_set)]

    processed_data = []
    for _, row in filtered_df.iterrows():
        processed_data.append({
            "country": row["Country"], 
            "resources_reserves_share": json.dumps({
                "2020": {
                    "anthracite": row["Anthracite_share"],
                    "sub_bituminous_bituminous_lignite": row["Sub-bituminous & Bituminous & Lignite_share"]
                }
            })
        })

    return pd.DataFrame(processed_data)

def process_production_volume(df, country_list):
    print("Processing 'Coal Production Volume' data...")
    if df.empty:
        return pd.DataFrame(columns=["country", "production_volume"])

    # Clean numeric columns once
    year_cols = [col for col in df.columns if col.isdigit()]
    df_clean = df.copy()
    for col in year_cols:
        # Regex replaces spaces/commas; errors='coerce' handles junk strings
        df_clean[col] = pd.to_numeric(df_clean[col].astype(str).str.replace(r"[\s,]", "", regex=True), errors="coerce")

    country_set = {c.strip() for c in country_list}
    processed_data = []

    for _, row in df_clean.iterrows():
        country = row["Country"].strip() if isinstance(row.get("Country"), str) else None

        if country not in country_set:
            print(f"{country} is not registered in country_set, skipping.")
            continue

        production_data = {yr: row[yr] for yr in year_cols if pd.notna(row[yr])}
        if not production_data:
            continue
        
        processed_data.append({
            "country": country, 
            "production_volume": json.dumps(production_data)
        })

    return pd.DataFrame(processed_data)

def process_production_share(df, country_list):
    print("Processing 'Commodity Production Share' data...")
    if df.empty:
        return pd.DataFrame(columns=["country", "production_share"])

    year_cols = [col for col in df.columns if col.isdigit()]
    country_set = {c.strip() for c in country_list}
    
    work_df = df.copy()
    for col in year_cols:
        work_df[col] = pd.to_numeric(work_df[col], errors="coerce").fillna(0)
    
    sums = work_df[year_cols].sum()

    share_df = work_df[year_cols].apply(lambda x: (x / sums[x.name] * 100).round(2) if sums[x.name] > 0 else 0)
    share_df["Country"] = work_df["Country"].astype(str).str.strip()

    final_df = share_df[share_df["Country"].isin(country_set)]
    processed_data = [
        {
            "country": row["Country"],
            "production_share": json.dumps(row[year_cols].to_dict())
        }
        for _, row in final_df.iterrows()
    ]

    return pd.DataFrame(processed_data)

def process_export_import(df, country_list):
    print("Processing 'Coal Export Import' data...")
    if df.empty:
        return pd.DataFrame(columns=["country", "export_import"])

    cols = {"export": "Exports Value (US$)", "import": "Imports Value (US$)"}
    country_set = {c.strip() for c in country_list}
    
    work_df = df.copy()
    # Clean both columns at once
    for key, col in cols.items():
        if col in work_df.columns:
            work_df[key] = pd.to_numeric(work_df[col].astype(str).str.replace(r"[\s,]", "", regex=True), errors="coerce")
        else:
            work_df[key] = pd.NA

    processed_data = []
    for _, row in work_df.iterrows():
        country = str(row.get("Country", "")).strip()
        if country in country_set:
            # Check if at least one value exists
            if pd.isna(row["export"]) and pd.isna(row["import"]):
                continue

            processed_data.append({
                "country": country, 
                "export_import": json.dumps({
                    "2023": {
                        "Export": row["export"] if pd.notna(row["export"]) else None,
                        "Import": row["import"] if pd.notna(row["export"]) else None
                    }
                })
            })

    return pd.DataFrame(processed_data)


def main():
    """
    Main function to process tables in 'global_commodity_data' sheet
    combine them, and update the master data table.
    """
    print("Starting script to process and combine global commodity data...")

    try:
        client, spreadsheet_id = createClient()
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet = spreadsheet.worksheet("global_commodity_data")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Spreadsheet with ID {spreadsheet_id} not found.")
        return
    except gspread.exceptions.WorksheetNotFound:
        print("Worksheet 'global_commodity_data' not found.")
        return

    # 1. Read source tables
    print("Reading data from source tables...")
    # Coal data
    res_df = get_dataframe_from_range(sheet, "I3:K39")
    prod_coal_df = get_dataframe_from_range(sheet, "M3:X31")
    exp_imp_df = get_dataframe_from_range(sheet, "AA3:AC84")
    # Nickel and Copper data
    prod_nickel_df = get_dataframe_from_range(sheet, "M54:X62")
    prod_copper_df = get_dataframe_from_range(sheet, "M65:X73")
    prod_bauxite_df = get_dataframe_from_range(sheet, "M79:X87")
    prod_gold_df = get_dataframe_from_range(sheet, "M94:X145")

    # 2. Process each data source into a standardized DataFrame
    res_json_df = process_resources_reserves(res_df, COUNTRY_LIST)
    if not res_json_df.empty:
        res_json_df["commodity_type"] = "Coal"

    res_share_json_df = process_resources_reserves_shares(res_df, COUNTRY_LIST)
    if not res_share_json_df.empty:
        res_share_json_df["commodity_type"] = "Coal"

    exp_imp_json_df = process_export_import(exp_imp_df, COUNTRY_LIST)
    if not exp_imp_json_df.empty:
        exp_imp_json_df["commodity_type"] = "Coal"

    commodity_production_dfs = []
    commodity_production_share_dfs = []
    for df, commodity in zip((prod_coal_df, prod_nickel_df, prod_copper_df, prod_bauxite_df, prod_gold_df),
                             ("Coal", "Nickel", "Copper", "Bauxite", "Gold")):
        
        commodity_prod_json_df = process_production_volume(df, COUNTRY_LIST)
        if not commodity_prod_json_df.empty:
            commodity_prod_json_df["commodity_type"] = commodity
        commodity_production_dfs.append(commodity_prod_json_df)

        commodity_share_json_df = process_production_share(df, COUNTRY_LIST)
        if not commodity_share_json_df.empty:
            commodity_share_json_df["commodity_type"] = commodity
        commodity_production_share_dfs.append(commodity_share_json_df)

    # 3. Combine all processed data
    print("Combining all commodity data...")
    all_dfs = [
        res_json_df,
        exp_imp_json_df,
        res_share_json_df
    ] + commodity_production_dfs + \
        commodity_production_share_dfs

    # Filter out any empty dataframes that resulted from empty source ranges
    valid_dfs = [df for df in all_dfs if not df.empty]
    if not valid_dfs:
        print("No data processed from any source. Exiting.")
        return

    combined_long_df = pd.concat(valid_dfs, ignore_index=True)

    # 4. Aggregate data by country and commodity
    final_df = (
        combined_long_df.groupby(["country", "commodity_type"]).first().reset_index()
    )

    # 5. Sort, add ID, and format final DataFrame
    final_df.sort_values(by=["country", "commodity_type"], inplace=True)
    final_df.reset_index(drop=True, inplace=True)
    final_df.insert(0, "id", range(1, 1 + len(final_df)))

    final_columns = [
        "id",
        "country",
        "resources_reserves",
        "resources_reserves_share",
        "export_import",
        "production_volume",
        "production_share",
        "commodity_type",
    ]
    final_df = final_df.reindex(columns=final_columns)

    # 6. Write to sheet
    output_range = f"A1:H{len(final_df) + 1}"
    print(f"Writing combined data to range {output_range}...")

    update_values = [final_df.columns.values.tolist()] + final_df.fillna(
        ""
    ).values.tolist()

    try:
        sheet.update(output_range, update_values, value_input_option="USER_ENTERED")
        print(f"Successfully updated {len(final_df)} rows in range {output_range}.")
    except gspread.exceptions.APIError as e:
        print(f"An API error occurred while updating the sheet: {e}")


if __name__ == "__main__":
    main()
