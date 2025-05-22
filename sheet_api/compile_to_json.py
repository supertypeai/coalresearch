# %%
import pandas as pd
from sheet_api.utils.dataframe_utils import safeCast
from sheet_api.google_sheets.client import getSheet
import pandas as pd
import json
# %%
ms_sheet, ms_df = getSheet('mining_site', 'A1:Y51')
# %%
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

def compileToJson(df, sheet, included_columns, target_col, starts_from=0):
    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        data_dict = {}

        for in_col, type in included_columns:		
            val = safeCast(row[in_col], type)

            in_col_cleaned = in_col.lstrip("*")

            data_dict[in_col_cleaned] = val

        rr_cols_json = json.dumps(data_dict)

        col_id = df.columns.get_loc(target_col)
        sheet.update_cell(2 + row_id, col_id + 1, rr_cols_json)

        print(f"Updated row {row_id + 2}: {rr_cols_json}")

# %%
compileToJson(ms_df, ms_sheet, rr_cols_type, 'resources_reserves', starts_from=50)
# %%

lo_cols_type = [
    ("*province", str),
    ("*city", str),
    ("*latitude", float),
    ("*longitude", float),
]

# %%
compileToJson(ms_df, ms_sheet, lo_cols_type, 'location', starts_from=50)
# %%
cp_sheet, cp_df = getSheet('company_performance', 'A1:AB135')

rr_cols_type = [
    ("*year_measured", int),
    ("*total_reserve", float),
    ("*total_resource", float),
    ("*resources_inferred", float),
    ("*resources_indicated", float),
    ("*resources_measured", float),
    ("*reserves_proved", float),
    ("*reserves_probable", float)
]
# %%
compileToJson(cp_df, cp_sheet, rr_cols_type, 'resources_reserves', starts_from=134)
# %%