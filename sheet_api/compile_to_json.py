# %%
from utils.dataframe_utils  import safeCast
from google_sheets.auth     import createClient, createService
from minerba_merge          import prepareMinerbaDf
from rapidfuzz              import process, fuzz
from insert_data_scraped    import clean_company_name, clean_company_df

import pandas as pd
import json
import re

_, SPREADSHEET_ID = createClient()
SERVICE = createService()

# %%
COAL_RESERVES_RESOURCES = [
    ("*year_measured", int),
    ("*total_reserve", float),
    ("*total_resource", float),
    ("*resources_inferred", float),
    ("*resources_indicated", float),
    ("*resources_measured", float),
    ("*reserves_proved", float),
    ("*reserves_probable", float)
]

COAL_STATS = [
    ("mining_operation_status", str),
    ("*production_volume", float),
    ("*sales_volume", float),
    ("*overburden_removal_volume", float),
    ("*strip_ratio", float),
]

MINERAL_RESERVES = [
    ("ore_reserves material (mt)", float),
    ("ore_reserves g/ton Au", float),
    ("ore_reserves Au (koz)", float),
    ("ore_reserves g/ton Ag", float),
    ("ore_reserves Ag (koz)", float),
    ("ore_reserves % Cu", float),
    ("ore_reserves Cu (mt)", float)
]

MINERAL_RESOURCES = [
    ("resources material (mt)", float),
    ("resources g/ton Au", float),
    ("resources Au (koz)", float),
    ("resources g/ton Ag", float),
    ("resources Ag (koz)", float),
    ("resources % Cu", float),
    ("resources Cu (mt)", float)
]

MINERAL_STATS = [
    ("*unit", str),
    ("mining_operation_status", str),
    ("*production_volume", float),
    ("*sales_volume", float),
]

def compileToJsonBatch(df, included_columns, target_col, sheet_id, starts_from=0):
    col_id = df.columns.get_loc(target_col)

    rows = []

    for row_id, row in df.iterrows():

        if row_id < starts_from:
            continue

        data_dict = {}

        for in_col, type in included_columns:
            val = safeCast(row[in_col], type)
            in_col_cleaned = in_col.lstrip("*")

            data_dict[in_col_cleaned] = val

        rr_cols_json = json.dumps(data_dict)
        to_use_value = {"stringValue": f"{rr_cols_json}"}

        rows.append({"values": [{"userEnteredValue": to_use_value}]})

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
        },
    ]

    response = (
        SERVICE.spreadsheets()
        .batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests})
        .execute()
    )

    print(f"Batch update response: {response}")

def default_key_formatter(col):
    return col.lstrip("*")

def renderDict(row, field_types, key_formatter=default_key_formatter):
    return {
        key_formatter(col): safeCast(row[col], dtype)
        for col, dtype in field_types
    }

def renderMineralStats(row):
    data_dict = renderDict(row, MINERAL_STATS)
    data_dict["resources_reserves"] = {
        "year_measured": safeCast(row["*year_measured"], int),
        "reserves": renderDict(row, MINERAL_RESERVES, lambda col: col.replace("ore_reserves ", "").lstrip("*")),
        "resources": renderDict(row, MINERAL_RESOURCES, lambda col: col.replace("resources ", "").lstrip("*"))
        }
    data_dict["product"] = safeCast(row["*product"], dict)
    return data_dict

def renderCoalStats(row):
    data_dict = renderDict(row, COAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, COAL_RESERVES_RESOURCES)
    data_dict["product"] = safeCast(row["*product"], dict)
    return data_dict

def jsonifyCommodityStats(df: pd.DataFrame, sheet_id: int, starts_from: int = 0):
    col_id = df.columns.get_loc("commodity_stats")
    rows = []

    for row_id, row in df.iterrows():

        if row_id < starts_from:
            continue

        if row["commodity_type"] != "Coal":
            data_dict = renderMineralStats(row)
        else:
            data_dict = renderCoalStats(row)
            
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

    response = SERVICE.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={'requests': requests}
    ).execute()

    print(f"Batch update response: {response}")

def fillMiningLicense(df: pd.DataFrame, sheet_id: int, is_debug: bool =False,
                      starts_from: int = 0, threshold: int = 93
                    ) -> None:
    # Load and clean reference DataFrame
    minerba_df, included_columns = prepareMinerbaDf()
    
    df_company = clean_company_df(df, 'name')
    df_minerba = clean_company_df(minerba_df,'company_name')

    # Pre-extract the list of normalized names for fuzzy matching
    clean_list = df_minerba['name_cleaned'].tolist()
    
    col_id = df.columns.get_loc("mining_license")

    rows = []
    for row_id, row in df_company.iterrows():
        if (row_id + 2) < starts_from:
            continue
        
        key = row['name_cleaned']
        key_no_space = row['name_cleaned_no_space'] 

        # Exact matching
        matches = df_minerba[df_minerba['name_cleaned'] == key]
        if is_debug:
            if not matches.empty: 
                print(f"[EXACT] '{key}' matched '{matches.iloc[0]['name_cleaned']}'")
        
        # No space matching
        if matches.empty:
            matches = df_minerba[df_minerba['name_cleaned_no_space'] == key_no_space]
            if is_debug:
                if not matches.empty: 
                    print(f"[NOSPACE] '{key}' matched '{matches.iloc[0]['name_cleaned']}'")

        # Fuzzy matching
        if matches.empty: 
            match, score, idx = process.extractOne(key, clean_list, scorer=fuzz.token_sort_ratio)
            if score >= threshold:
                matches = df_minerba.iloc[[idx]]
                if is_debug:
                    print(f"[FUZZY] '{key}' → '{match}' (score: {score})")

        if not matches.empty:
            records = matches[included_columns].to_dict(orient="records")
        else:
            # empty list when no matches
            records = []  

        ### CHANGED: dump the list (even if empty) as your JSON array
        license_json = json.dumps(records, ensure_ascii=False)
        to_use_value = {"stringValue": license_json}

        rows.append(
            {
                'values': 
                    [
                        {'userEnteredValue': to_use_value}
                    ]
            }
        )
        
    # push all at once
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

    response = SERVICE.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={'requests': requests}
    ).execute()

    print(f"Batch update response: {response}")
