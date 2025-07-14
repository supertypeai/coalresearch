# %%
from utils.dataframe_utils  import safeCast, clean_company_df
from google_sheets.auth     import createClient, createService
from minerba_merge          import prepareMinerbaDf
from rapidfuzz              import process, fuzz

import pandas as pd
import json

_, SPREADSHEET_ID = createClient()
SERVICE = createService()

# %%
COAL_RESERVES_RESOURCES = [
    ("year_measured", int),
    ("total_reserve", float),
    ("total_resource", float),
    ("resources_inferred", float),
    ("resources_indicated", float),
    ("resources_measured", float),
    ("reserves_proved", float),
    ("reserves_probable", float)
]

COAL_STATS = [
    ("mining_operation_status", str),
    ("production_volume", float),
    ("sales_volume", float),
    ("overburden_removal_volume", float),
    ("strip_ratio", float),
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
    ("unit", str),
    ("mining_operation_status", str),
    ("production_volume", float),
    ("sales_volume", float),
]

# Columns for Gold Mines
GOLD_RSRV = [
    ("gold rsrv material (mt)", float),
    ("gold rsrv g/ton Au", float),
    ("gold rsrv Au (koz)", float),
    ("gold rsrv % Cu", float),
    ("gold rsrv Cu (mt)", float)
]
GOLD_RSRO = [
    ("gold rsro material (mt)", float),
    ("gold rsro g/t Au", float),
    ("gold rsro Au (koz)", float),
    ("gold rsro % Cu", float),
    ("gold rsro Cu (mt)", float)
]

# Columns for Coal Mines
COAL_MINE = [
    ("coal year_measured", int),
    ("coal calorific_value", str),
    ("coal total_reserve", float),
    ("coal total_resource", float),
    ("coal resources_inferred", float),
    ("coal resources_indicated", float),
    ("coal resources_measured", float),
    ("coalreserves_proved", float),
    ("coal reserves_probable", float),
]

# Columns for Nickel Mines
NICKEL_MINE = [
    ("wet tonnes (mt)", float),
    ("dry tonnes (mt)", float),
    ("% Ni", float),
    ("Ni (Kt)", float),
    ("% Co", float),
    ("Co (Kt)", float),
    ("% Fe", float),
    ("% SiO₂", float),
    ("% MgO", float),
    ("% Al₂O₃", float),
]
LIM_RSRV = [(f"lim rsrv {col}", typ) for col, typ in NICKEL_MINE]
LIM_RSRO = [(f"lim rsro {col}", typ) for col, typ in NICKEL_MINE]
SAP_RSRV = [(f"sap rsrv {col}", typ) for col, typ in NICKEL_MINE]
SAP_RSRO = [(f"sap rsro {col}", typ) for col, typ in NICKEL_MINE]

# Columns for Nickel Stats
NICKEL_RSRV = [(f"rsrv {col}", typ) for col, typ in NICKEL_MINE]
NICKEL_RSRO = [(f"rsro {col}", typ) for col, typ in NICKEL_MINE]

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
        "year_measured": safeCast(row["year_measured"], int),
        "reserves": renderDict(row, MINERAL_RESERVES, lambda col: col.replace("ore_reserves ", "").lstrip("*")),
        "resources": renderDict(row, MINERAL_RESOURCES, lambda col: col.replace("resources ", "").lstrip("*"))
        }
    data_dict["product"] = safeCast(row["product"], dict)
    return data_dict

def renderNickelStats(row):
    data_dict = renderDict(row, MINERAL_STATS)
    data_dict["resources_reserves"] = {
        "year_measured": safeCast(row["year_measured"], int),
        "reserves": renderDict(row, NICKEL_RSRV, lambda col: col.replace("rsrv ", "").lstrip("*")),
        "resources": renderDict(row, NICKEL_RSRO, lambda col: col.replace("rsro ", "").lstrip("*"))
        }
    data_dict["product"] = safeCast(row["product"], dict)
    return data_dict

def renderCoalStats(row):
    data_dict = renderDict(row, COAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, COAL_RESERVES_RESOURCES)
    data_dict["product"] = safeCast(row["product"], dict)
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


def renderGoldMine(row):
    data_dict = {'year_measured': safeCast(row['gold year_measured'], int)}
    data_dict['reserve'] = renderDict(row, GOLD_RSRV, lambda col: col.replace("gold rsrv ", ""))
    data_dict['resource'] = renderDict(row, GOLD_RSRO, lambda col: col.replace("gold rsro ", ""))
    return data_dict

def renderCoalMine(row):
    return renderDict(row, COAL_MINE, lambda col: col.replace("coal ", ""))

def renderNickelMine(row):
    data_dict = {'year_measured': safeCast(row['nckl year_measured'], int)}
    data_dict['limonite'] = {
        'reserve': renderDict(row, LIM_RSRV, lambda col: col.replace("lim rsrv ", "")),
        'resource': renderDict(row, LIM_RSRO, lambda col: col.replace("lim rsro ", ""))
    }
    data_dict['saprolite'] = {
        'reserve': renderDict(row, SAP_RSRV, lambda col: col.replace("sap rsrv ", "")),
        'resource': renderDict(row, SAP_RSRO, lambda col: col.replace("sap rsro ", ""))
    }
    return data_dict

def jsonifyMineRsrvRsro(df: pd.DataFrame, sheet_id: int, starts_from: int = 0):
    col_id = df.columns.get_loc("resources_reserves")
    rows = []

    renderMap = {
        'Gold': renderGoldMine,
        'Coal': renderCoalMine,
        'Nickel': renderNickelMine
    }


    for row_id, row in df.iterrows():

        if row_id < starts_from:
            continue

        renderFunction = renderMap.get(row['mineral_type'], renderCoalMine)
        data_dict = renderFunction(row)
            
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

def matchingSequence(license_df: pd.DataFrame, clean_list: list, key: str, key_no_space: str, 
                     threshold: int = 93, is_debug: bool = False
                     ) -> pd.DataFrame:
    matches = license_df[license_df['name_cleaned'] == key]
    if is_debug:
        if not matches.empty: 
            print(f"[EXACT] '{key}' matched '{matches.iloc[0]['name_cleaned']}'")
    
    # No space matching
    if matches.empty:
        matches = license_df[license_df['name_cleaned_no_space'] == key_no_space]
        if is_debug:
            if not matches.empty: 
                print(f"[NOSPACE] '{key}' matched '{matches.iloc[0]['name_cleaned']}'")

    # Fuzzy matching
    if matches.empty: 
        match, score, idx = process.extractOne(key, clean_list, scorer=fuzz.token_sort_ratio)
        if score >= threshold:
            matches = license_df.iloc[[idx]]
            if is_debug:
                print(f"[FUZZY] '{key}' → '{match}' (score: {score})")

    return matches

def fillMiningLicense(df: pd.DataFrame, sheet_id: int, is_debug: bool =False,
                      starts_from: int = 0, threshold: int = 93
                    ) -> None:
    # Load and clean reference DataFrame
    minerba_df, included_columns = prepareMinerbaDf()
    minerba_df2, _ = prepareMinerbaDf("coal_db - minerba (cleansed).csv")
    
    df_company = clean_company_df(df, 'name')
    df_minerba = clean_company_df(minerba_df,'company_name')
    df_minerba2 = clean_company_df(minerba_df2,'company_name')

    # Pre-extract the list of normalized names for fuzzy matching
    clean_list = df_minerba['name_cleaned'].tolist()
    clean_list2 = df_minerba2['name_cleaned'].tolist()
    
    col_id = df.columns.get_loc("mining_license")

    rows = []
    for row_id, row in df_company.iterrows():
        if (row_id + 2) < starts_from:
            continue
        
        key = row['name_cleaned']
        key_no_space = row['name_cleaned_no_space'] 

        # Exact matching
        matches = matchingSequence(df_minerba, clean_list, key, key_no_space, threshold, is_debug)
        if matches.empty:
            matches = matchingSequence(df_minerba2, clean_list2, key, key_no_space, threshold, is_debug)

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
