# %%
import sys
import os

os.chdir("..")
sys.path.append(os.path.join(os.getcwd(), "sheet_api"))

import pandas as pd
import re

from google_sheets.auth import createClient, createService

# %%
client, spreadsheet_id = createClient()
service = createService()
ms_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')

ms_data = ms_sheet.get('A1:Y110')
ms_df = pd.DataFrame(ms_data[1:], columns=ms_data[0])

esdm_coal_df = pd.read_csv("coal_db - ESDM_coal.csv")
esdm_coal_df['mineral_grade'] = esdm_coal_df['mineral_grade'].apply(lambda x: re.search(r"\(([^)]+)\)", x).group(1))
esdm_coal_df['object_name_strip'] = esdm_coal_df['object_name'].str.replace(r'\b[B]atubara\b', '', regex=True).str.strip()

# %%
merge_columns = [
    # ("province", "*province"),
    # ("city", "*city"),
    ("latitude", "*latitude"),
    ("longitude", "*longitude"),
    # ("year_measured", "*year_measured"),
    # ("mineral_grade", "*calorific_value"),
    # ("inferred_resource", "*resources_inferred"),
    # ("indicated_resource", "*resources_indicated"),
    # ("measured_resource", "*resources_measured"),
    # ("total_resource", "*total_resource"),
    # ("probable_reserve", "*reserves_probable"),
    # ("proven_reserve", "*reserves_proved"),
    # ("total_reserve", "*total_reserve"),
]
merge_columns_hash = {val:key for key, val in merge_columns}

def checkObjectName(val):
    q = esdm_coal_df[esdm_coal_df['object_name'] == val]
    if not q.empty:
        return q
    else:
        return esdm_coal_df[esdm_coal_df['object_name_strip'] == val]


def updateSheet(starts_from=0):

    for row_id, row in ms_df.iterrows():
        
        assert isinstance(row_id, int)

        if (row_id + 2) < starts_from:
            continue

        q = checkObjectName(row['name'])

        if not q.empty:

            for csv_col, sheet_col in merge_columns:
                col_id = list(ms_df.columns).index(sheet_col)
                
                original_value = row[sheet_col]
                new_value = str(q[csv_col].iloc[0])

                to_use_value = new_value

                ms_sheet.update_cell(2 + row_id, col_id + 1, to_use_value)

                print("Updating row number, col number, value:", row_id, col_id, to_use_value)

def batchUpdateSheet(starts_from=0):
    col_start = list(ms_df.columns).index("*province")
    col_end = list(ms_df.columns).index("*reserves_probable")

    for row_id, row in ms_df.iterrows():
        assert isinstance(row_id, int)

        if (row_id + 2) < starts_from:
            continue

        esdm_row = checkObjectName(row['name'])

        original_value = row.iloc[col_start:col_end + 1]

        if esdm_row.empty:
            to_use_value = original_value
        else:
            new_value = [esdm_row.get(merge_columns_hash.get(sheet_col, ""), "") for sheet_col in original_value.index]
            to_use_value = new_value

        print(to_use_value)

        payload = ({"values": [{"userEnteredValue": {"stringValue": f"{to_use_value}"}}]})

        requests = [
            {
                'updateCells': {
                    'range': {
                        'sheetId': 147673991,
                        'startRowIndex': starts_from + 1,
                        'endRowIndex': 240 + 1,
                        'startColumnIndex': col_start,
                        'endColumnIndex': col_end + 1
                    },
                    'rows': payload,
                    'fields': 'userEnteredValue'
                }
            },
        ]

        response = (
            service.spreadsheets()
            .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests})
            .execute()
        )

        print(f"Batch update response: {response}")

# batchUpdateSheet(starts_from=90)

# %%
updateSheet(starts_from=62)
# %%
