# %%
import pandas as pd
from sheet_api.google_sheets.auth import createClient

client, spreadsheet_id = createClient()

# %%

cp_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')
cp_data = cp_sheet.get('A1:Y51')
cp_df = pd.DataFrame(cp_data[1:], columns=cp_data[0])
# %%
import pandas as pd
import json

# Define column type mapping
rr_cols_type = [
    ("*year_measured", int),
    ("*calorific_value", str),
    ("*total_reserve", float),
    ("*total_resource", float),
    ("*resources_inferred", float),
    ("*resources_indicated", float),
    ("*resources_measured", float),
    ("*reserves_proved", float),
    ("*reserves_probable", float)
]

# Strip * from column names
rr_cols = [col for col, _ in rr_cols_type]
rr_cols_strip = [col.lstrip("*") for col in rr_cols]
rename_dict = dict(zip(rr_cols, rr_cols_strip))

# Example: Apply renaming to your DataFrame
cp_df = cp_df.rename(columns=rename_dict)

# Define the function to convert selected columns to typed JSON
def jsonifySheet(df, sheet, starts_from=0):
    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        data_dict = {}

        for original_col, col_type in rr_cols_type:
            stripped_col = original_col.lstrip("*")
            val = row.get(stripped_col)

            # Handle empty or missing values
            if pd.isna(val) or val == "":
                data_dict[stripped_col] = None
            else:
                try:
                    casted_val = col_type(val)
                    # If float is whole number, convert to int
                    if isinstance(casted_val, float) and casted_val.is_integer():
                        casted_val = int(casted_val)
                    data_dict[stripped_col] = casted_val
                except (ValueError, TypeError):
                    data_dict[stripped_col] = None

        rr_cols_json = json.dumps(data_dict)

        col_id = df.columns.get_loc('resources_reserves')
        sheet.update_cell(2 + row_id, col_id + 1, rr_cols_json)

        print(f"Updated row {row_id + 2}: {rr_cols_json}")

# %%
jsonifySheet(cp_df, cp_sheet, starts_from=0)
# %%

cp_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')
cp_data = cp_sheet.get('A1:Y51')
cp_df = pd.DataFrame(cp_data[1:], columns=cp_data[0])

rr_cols_type = [
    ("*province", str),
    ("*city", str),
    ("*latitude", float),
    ("*longitude", float),
]

# Strip * from column names
rr_cols = [col for col, _ in rr_cols_type]
rr_cols_strip = [col.lstrip("*") for col in rr_cols]
rename_dict = dict(zip(rr_cols, rr_cols_strip))

cp_df = cp_df.rename(columns=rename_dict)

def jsonifySheet(df, sheet, starts_from=0):
    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        data_dict = {}

        for original_col, col_type in rr_cols_type:
            stripped_col = original_col.lstrip("*")
            val = row.get(stripped_col)

            # Handle empty or missing values
            if pd.isna(val) or val == "":
                data_dict[stripped_col] = None
            else:
                try:
                    casted_val = col_type(val)
                    # If float is whole number, convert to int
                    if isinstance(casted_val, float) and casted_val.is_integer():
                        casted_val = int(casted_val)
                    data_dict[stripped_col] = casted_val
                except (ValueError, TypeError):
                    data_dict[stripped_col] = None

        rr_cols_json = json.dumps(data_dict)

        col_id = df.columns.get_loc('location')
        sheet.update_cell(2 + row_id, col_id + 1, rr_cols_json)

        print(f"Updated row {row_id + 2}: {rr_cols_json}")

# %%
jsonifySheet(cp_df, cp_sheet, starts_from=0)
# %%