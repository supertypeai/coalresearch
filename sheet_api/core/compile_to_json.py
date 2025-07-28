# %%
from sheet_api.core.toolbox           import safeCast, clean_company_df
from sheet_api.google_sheets.auth     import createClient, createService
from sheet_api.google_sheets.client   import getSheetAll
from sheet_api.minerba_merge          import prepareMinerbaDf
from rapidfuzz                        import process, fuzz

import pandas as pd
import json

_, SPREADSHEET_ID = createClient()
SERVICE = createService()

# %%
MINERAL_STATS = [
    ("unit", str),
    ("mining_operation_status", str),
    ("production_volume", float),
    ("sales_volume", float),
]
COAL_STATS = MINERAL_STATS + [
    ("overburden_removal_volume", float),
    ("strip_ratio", float),
]
COAL_RESERVES_RESOURCES = [
    ("year_measured", int),
    ("reserves probable (Mt)", float),
    ("reserves proved (Mt)", float),
    ("reserves total (Mt)", float),
    ("resources inferred (Mt)", float),
    ("resources indicated (Mt)", float),
    ("resources measured (Mt)", float),
    ("resources total (Mt)", float)
]
GOLD_COPPER_TEMPLATE = [
    ("total (Mt)", float),
    ("g/ton Au", float),
    ("Au (koz)", float),
    ("g/ton Ag", float),
    ("Ag (koz)", float),
    ("% Cu", float),
    ("Cu (Mt)", float)
]
GOLD_COPPER_RESERVES_RESOURCES = [
    ("year_measured", int),
] + [
    (f"reserves {t}", typ) for t, typ in GOLD_COPPER_TEMPLATE
] + [
    (f"resources {t}", typ) for t, typ in GOLD_COPPER_TEMPLATE
]
NICKEL_TEMPLATE = [
    ("total (wmt)", float),
    ("total (dmt)", float),
    ("% Ni", float),
    ("Ni (Kt)", float),
    ("% Co", float),
    ("Co (Kt)", float),
    ("% Fe", float),
    ("% SiO₂", float),
    ("% MgO", float),
    ("% Al₂O₃", float),
]
NICKEL_RESERVES_RESOURCES = [
    ("year_measured", int),
] + [
    (f"reserves {t}", typ) for t, typ in NICKEL_TEMPLATE
] + [
    (f"resources {t}", typ) for t, typ in NICKEL_TEMPLATE
]

# Mining Sites
COAL_MINE = [
    (f"coal {c}", typ) for c, typ in COAL_RESERVES_RESOURCES
] + [
    ("coal calorific value", str)
]
GOLD_COPPER_MINE = [
    (f"gold {c}", typ) for c, typ in GOLD_COPPER_RESERVES_RESOURCES
]
SAPROLITE_MINE = [
    (f"sap reserves {t}", typ) for t, typ in NICKEL_TEMPLATE
] + [
    (f"sap resources {t}", typ) for t, typ in NICKEL_TEMPLATE
]
LIMONITE_MINE = [
    (f"lim reserves {t}", typ) for t, typ in NICKEL_TEMPLATE
] + [
    (f"lim resources {t}", typ) for t, typ in NICKEL_TEMPLATE
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
        row[target_col] = rr_cols_json

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

def renderCoalStats(row):
    data_dict = renderDict(row, COAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, COAL_RESERVES_RESOURCES)
    data_dict["product"] = safeCast(row["product"], dict)
    return data_dict

def renderGoldCopperStats(row):
    data_dict = renderDict(row, MINERAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, GOLD_COPPER_RESERVES_RESOURCES)
    data_dict["product"] = safeCast(row["product"], dict)
    return data_dict

def renderNickelStats(row):
    data_dict = renderDict(row, MINERAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, NICKEL_RESERVES_RESOURCES)
    data_dict["product"] = safeCast(row["product"], dict)
    return data_dict

def jsonifyCommodityStats(df: pd.DataFrame, sheet_id: int, starts_from: int = 0):
    col_id = df.columns.get_loc("commodity_stats")
    rows = []

    for row_id, row in df.iterrows():

        if row_id < starts_from:
            continue

        if row["commodity_type"] != "Coal":
            data_dict = renderGoldCopperStats(row)
        else:
            data_dict = renderCoalStats(row)
            
        rr_cols_json = json.dumps(data_dict)
        row["commodity_stats"] = rr_cols_json

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


def renderGoldCopperMine(row):
    return renderDict(row, GOLD_COPPER_MINE, lambda col: col.replace("gold ", ""))

def renderCoalMine(row):
    return renderDict(row, COAL_MINE, lambda col: col.replace("coal ", ""))

def renderNickelMine(row):
    data_dict = {'year_measured': safeCast(row['nickel year_measured'], int)}
    data_dict['limonite'] = renderDict(row, LIMONITE_MINE)
    data_dict['saprolite'] = renderDict(row, SAPROLITE_MINE)
    return data_dict

def jsonifyMineReservesAndResources(df: pd.DataFrame, sheet_id: int, starts_from: int = 0):
    col_id = df.columns.get_loc("resources_reserves")
    rows = []

    renderMap = {
        'Gold': renderGoldCopperMine,
        'Coal': renderCoalMine,
        'Nickel': renderNickelMine,
        'Copper': renderGoldCopperMine
    }

    for row_id, row in df.iterrows():

        if row_id < starts_from:
            continue

        renderFunction = renderMap.get(row['mineral_type'], renderCoalMine)
        data_dict = renderFunction(row)
            
        rr_cols_json = json.dumps(data_dict)
        row["resources_reserves"] = rr_cols_json

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

def batchUpdateSheet(rows: list, sheet_id: int, starts_from: int, length: int, col_id: int):
    requests = [
        {
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': starts_from + 1,
                    'endRowIndex': length + 1,
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

    return response

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
        matches = matchingSequence(df_minerba, clean_list, key, key_no_space, threshold, is_debug)

        if not matches.empty:
            records = matches[included_columns].to_dict(orient="records")
        else:
            # empty list when no matches
            records = []  

        ### CHANGED: dump the list (even if empty) as your JSON array
        license_json = json.dumps(records, ensure_ascii=False)
        row['mining_license'] = license_json

        to_use_value = {"stringValue": license_json}

        rows.append(
            {
                'values': 
                    [
                        {'userEnteredValue': to_use_value}
                    ]
            }
        )
    
    response = batchUpdateSheet(rows, sheet_id, starts_from, len(df), col_id)

def fillMiningContract(df: pd.DataFrame, sheet_id: int) -> pd.DataFrame:

    c_df = df.copy()
    _, mc_df = getSheetAll("mining_contract")

    # Clean and normalize IDs for reliable matching
    mc_df["contractor_id"] = (
        pd.to_numeric(mc_df["contractor_id"], errors="coerce")
        .astype("Int64")
        .astype(str)
    )
    c_df["id"] = c_df["id"].astype(str)

    # Group contracts by contractor_id
    grouped_contracts = mc_df.groupby("contractor_id")

    # Create a dictionary of contracts with the new JSON structure
    contracts_dict = {}
    for contractor_id, group in grouped_contracts:
        contract_list = []
        for _, row in group.iterrows():
            agreement_type_str = row.get("Agreement type", "")
            agreement_types = (
                [item.strip() for item in agreement_type_str.split(",")]
                if agreement_type_str
                else []
            )

            new_contract = {
                "company_name": row.get("*mine_owner_name"),
                "company_id": row.get("mine_owner_id"),
                "contract_period_end": row.get("contract_period_end"),
                "agreement_type": agreement_types,
            }
            contract_list.append(new_contract)
        contracts_dict[str(contractor_id)] = json.dumps(contract_list)

    # Map contracts to company dataframe and fill empty values with '[]'
    c_df["mining_contract"] = c_df["id"].map(contracts_dict)
    c_df["mining_contract"] = c_df["mining_contract"].fillna("[]")
    c_df.loc[c_df["mining_contract"].isnull(), "mining_contract"] = "[]"

    rows = c_df["mining_contract"].tolist()
    rows = [{"values": [{"userEnteredValue": {"stringValue": r}}]} for r in rows]
    col_id = c_df.columns.get_loc("mining_contract")

    response = batchUpdateSheet(rows, sheet_id, 0, len(c_df), col_id)

    return c_df