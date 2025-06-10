# %%
from sheet_api.utils.dataframe_utils import safeCast
from sheet_api.google_sheets.auth import createClient, createService
from minerba_merge import prepareMinerbaDf

import json
import re

_, spreadsheet_id = createClient()
service = createService()

# %%
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

def clean_company_name(name):
    return re.sub(r'\b(PT|Tbk)\b', '', name).lower().strip()

def fillMiningLicense(df, sheet_id, starts_from=0):

    minerba_df, included_columns = prepareMinerbaDf()
    
    col_id = df.columns.get_loc('mining_license')

    rows = []

    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        company_q = minerba_df[minerba_df['company_name'].str.lower() == clean_company_name(row['name'])]

        if not company_q.empty:
            license_json = company_q[included_columns].iloc[0].to_json()
            to_use_value = {'stringValue':f'{license_json}'}

        else:
            to_use_value = {}

        rows.append(
            {
                'values': 
                    [
                        {'userEnteredValue': to_use_value}
                    ]
            }
        )

            # sheet.update_cell(2 + row_id, col_id + 1, license_json)
            # print(f"Updated row {row_id + 2}: {license_json}")

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

mining_site_resources_reserves_cols_type = [
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

mining_site_location_cols_type = [
    ("*province", str),
    ("*city", str),
    ("*latitude", float),
    ("*longitude", float),
]

# compileToJsonBatch(ms_df, lo_cols_type, 'location', ms_sheet.id)

company_performance_resources_reserves_cols_type = [
    ("*year_measured", int),
    ("*total_reserve", float),
    ("*total_resource", float),
    ("*resources_inferred", float),
    ("*resources_indicated", float),
    ("*resources_measured", float),
    ("*reserves_proved", float),
    ("*reserves_probable", float)
]

company_performance_coal_stats_type = [
    ("mining_operation_status", str),
    ("*production_volume", float),
    ("*sales_volume", float),
    ("*overburden_removal_volume", float),
    ("*strip_ratio", float),
    ("*resources_reserves", dict),
    ("*product", dict)
]

# compileToJsonBatch(cp_df, rr_cols_type, 'resources_reserves', 147673991)
# compileToJsonBatch(cp_df, rr_cols_type, 'resources_reserves', cp_sheet.id)