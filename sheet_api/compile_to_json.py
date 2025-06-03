# %%
from sheet_api.utils.dataframe_utils import safeCast
from sheet_api.google_sheets.client import getSheet
from sheet_api.google_sheets.auth import createClient, createService

import json

client, spreadsheet_id = createClient()
service = createService()

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

def compileToJsonBatch(df, included_columns, target_col, sheet_id, starts_from=0):
    
    col_id = df.columns.get_loc(target_col)

    rows = []
    
    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        data_dict = {}

        for in_col, type in included_columns:		
            val = safeCast(row[in_col], type)

            in_col_cleaned = in_col.lstrip("*")

            data_dict[in_col_cleaned] = val

        rr_cols_json = json.dumps(data_dict)
        to_use_value = {'stringValue':f'{rr_cols_json}'}

        rows.append(
            {
                'values': 
                    [
                        {'userEnteredValue': to_use_value}
                    ]
            }
        )

    requests = [
        {
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': starts_from + 1,
                    'endRowIndex': len(df) + 1,
                    'startColumnIndex': col_id,
                    'endColumnIndex': col_id + 1
                },
                'rows': rows,
                'fields': 'userEnteredValue'
            }
        }
    ]

    response = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': requests}
    ).execute()

    print(f"Batch update response: {response}")

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
# compileToJson(ms_df, ms_sheet, lo_cols_type, 'location', starts_from=0)
compileToJsonBatch(ms_df, lo_cols_type, 'location', 1578569108)
# %%
cp_sheet, cp_df = getSheet('company_performance', 'A1:AB180')

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
compileToJsonBatch(cp_df, rr_cols_type, 'resources_reserves', 147673991)
# %%
compileToJson(cp_df, cp_sheet, rr_cols_type, 'resources_reserves', starts_from=134)
# %%
