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
    """Processes the 'Global Coal Resource and Reserves 2020' dataframe."""
    print("Processing 'Global Coal Resource and Reserves 2020' data...")
    if df.empty:
        return pd.DataFrame(columns=["country", "resources_reserves"])

    processed_data = []
    for _, row in df.iterrows():
        country = row.get("Country")
        if country and country.strip() in country_list:
            try:
                anthracite_val = pd.to_numeric(row["Anthracite"], errors="coerce")
                sub_bit_val = pd.to_numeric(
                    row["Sub-bituminous & Bituminous & Lignite"], errors="coerce"
                )

                payload = []
                if pd.notna(anthracite_val):
                    payload.append({"Anthracite": anthracite_val.item()})
                if pd.notna(sub_bit_val):
                    payload.append(
                        {"Sub-bituminous & Bituminous & Lignite": sub_bit_val.item()}
                    )

                if payload:
                    json_data = {"2020": payload}
                    json_string = json.dumps(json_data)
                    processed_data.append(
                        {"country": country.strip(), "resources_reserves": json_string}
                    )
            except (KeyError, ValueError) as e:
                print(f"Skipping row for '{country}' in resources due to error: {e}")
                continue

    return pd.DataFrame(processed_data)


def process_production_volume(df, country_list):
    """Processes the 'Coal Production Volume' dataframe."""
    print("Processing 'Coal Production Volume' data...")
    if df.empty:
        return pd.DataFrame(columns=["country", "production_volume"])

    processed_data = []
    year_cols = [col for col in df.columns if col.isdigit()]

    for _, row in df.iterrows():
        country = row.get("Country")
        if country and country.strip() in country_list:
            production_data = {}
            for year in year_cols:
                try:
                    value_str = str(row[year]).replace(" ", "")
                    value = pd.to_numeric(value_str, errors="coerce")
                    if pd.notna(value):
                        production_data[year] = value.item()
                except (KeyError, ValueError):
                    continue

            if production_data:
                json_string = json.dumps(production_data)
                processed_data.append(
                    {"country": country.strip(), "production_volume": json_string}
                )

    return pd.DataFrame(processed_data)

def process_production_share(df, country_list):
    """Processes the 'Commodity Production Share' dataframe."""
    print("Processing 'Commodity Production Share' data...")
    if df.empty:
        return pd.DataFrame(columns=["country", "production_share"])

    processed_data = []
    year_cols = [col for col in df.columns if col.isdigit()]

    df[year_cols] = df[year_cols].apply(
        lambda col: pd.to_numeric(col, errors="coerce").fillna(0)
    )

    yearly_production_sum = {year: df[year].sum() for year in year_cols}

    for _, row in df.iterrows():
        country = row.get("Country")
        if country and country.strip() in country_list:
            production_share = {}
            for year in year_cols:
                production = row[year]
                # Assuming yearly_production_sum[year] is non zero
                production_share[year] = round((production / yearly_production_sum[year]) * 100, 2)
        
            if production_share:
                json_string = json.dumps(production_share)
                processed_data.append(
                    {"country": country.strip(), "production_share": json_string}
                )

    return pd.DataFrame(processed_data)

def process_export_import(df, country_list):
    """Processes the 'Coal Export Import' dataframe."""
    print("Processing 'Coal Export Import' data...")
    if df.empty:
        return pd.DataFrame(columns=["country", "export_import"])

    processed_data = []
    export_col = "Exports Value (US$)"
    import_col = "Imports Value (US$)"

    for _, row in df.iterrows():
        country = row.get("Country")
        if country and country.strip() in country_list:
            try:
                export_val_str = str(row.get(export_col, "")).replace(",", "")
                import_val_str = str(row.get(import_col, "")).replace(",", "")

                export_val = pd.to_numeric(export_val_str, errors="coerce")
                import_val = pd.to_numeric(import_val_str, errors="coerce")

                if pd.notna(export_val) or pd.notna(import_val):
                    payload = [
                        {"Export": export_val.item() if pd.notna(export_val) else None},
                        {"Import": import_val.item() if pd.notna(import_val) else None},
                    ]
                    json_data = {"2023": payload}
                    json_string = json.dumps(json_data)
                    processed_data.append(
                        {"country": country.strip(), "export_import": json_string}
                    )
            except (KeyError, ValueError) as e:
                print(
                    f"Skipping row for '{country}' in export/import due to error: {e}"
                )
                continue

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
        exp_imp_json_df
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
        "export_import",
        "production_volume",
        "production_share",
        "commodity_type",
    ]
    final_df = final_df.reindex(columns=final_columns)

    # 6. Write to sheet
    output_range = f"A1:G{len(final_df) + 1}"
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
