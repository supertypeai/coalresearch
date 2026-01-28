# %%
from sheet_api.core.toolbox           import safeCast, clean_company_df
from sheet_api.google_sheets.auth     import createClient, createService
from sheet_api.google_sheets.client   import getSheetAll
from sheet_api.minerba_merge          import prepareMinerbaDf
from rapidfuzz                        import process, fuzz
from typing                           import List

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
    ("measurement_year",       int),
    ("probable_reserves_Mt",   float),
    ("proven_reserves_Mt",     float),
    ("total_reserves_Mt",      float),
    ("inferred_resources_Mt",  float),
    ("indicated_resources_Mt", float),
    ("measured_resources_Mt",  float),
    ("total_resources_Mt",     float),
]
GOLD_COPPER_TEMPLATE = [
    ("total", "Mt",         float),
    ("Au",    "g_per_ton",  float),
    ("Au",    "koz",        float),
    ("Ag",    "g_per_ton",  float),
    ("Ag",    "koz",        float),
    ("Cu",    "pct",        float),
    ("Cu",    "Mt",         float)
]
GOLD_COPPER_RESERVES_RESOURCES = [
    ("measurement_year", int),
] + [
    (f"{subject}_reserves_{unit}", typ) for subject, unit, typ in GOLD_COPPER_TEMPLATE
] + [
    (f"{subject}_resources_{unit}", typ) for subject, unit, typ in GOLD_COPPER_TEMPLATE
]
NICKEL_TEMPLATE = [
    ("total", "wmt", float),
    ("total", "dmt", float),
    ("Ni",    "pct", float),
    ("Ni",    "Kt",  float),
    ("Co",    "pct", float),
    ("Co",    "Kt",  float),
    ("Fe",    "pct", float),
    ("SiO2",  "pct", float),
    ("MgO",   "pct", float),
    ("Al2O3", "pct", float),
]
NICKEL_RESERVES_RESOURCES = [
    ("measurement_year", int),
] + [
    (f"{subject}_reserves_{unit}", typ) for subject, unit, typ in NICKEL_TEMPLATE
] + [
    (f"{subject}_resources_{unit}", typ) for subject, unit, typ in NICKEL_TEMPLATE
]
 
# Mining Sites
COAL_MINE = [
    (f"coal {c}", typ) for c, typ in COAL_RESERVES_RESOURCES
] + [
    ("coal calorific_value_kcal", str)
]
GOLD_COPPER_MINE = [
    (f"gold {c}", typ) for c, typ in GOLD_COPPER_RESERVES_RESOURCES
]
SAPROLITE_MINE = [
    (f"sap {subject}_reserves_{unit}", typ) for subject, unit, typ in NICKEL_TEMPLATE
] + [
    (f"sap {subject}_resources_{unit}", typ) for subject, unit, typ in NICKEL_TEMPLATE
]
LIMONITE_MINE = [
    (f"lim {subject}_reserves_{unit}", typ) for subject, unit, typ in NICKEL_TEMPLATE
] + [
    (f"lim {subject}_resources_{unit}", typ) for subject, unit, typ in NICKEL_TEMPLATE
]

# Resources and Reserves
RESERVES_RESOURCES_COAL = [
    ("exploration_target", float),
    ("total_inventory", float),
    ("inferred_resources_Mt", float),
    ("indicated_resources_Mt", float),
    ("measured_resources_Mt", float),
    ("total_resources_Mt", float),
    ("total_resources_verify_Mt", float),
    ("total_reserves_Mt", float),
    ("total_reserves_verify_Mt", float)
]
RESERVES_RESOURCES_METAL = [
    ("ore_inferred_resources_Mt", float),
    ("inferred_resources_Mt", float),
    ("ore_indicated_resources_Mt", float),
    ("indicated_resources_Mt", float),
    ("ore_measured_resources_Mt", float),
    ("measured_resources_Mt", float),
    ("ore_total_resources_Mt", float),
    ("total_resources_Mt", float),
    ("ore_probable_reserves_Mt", float),
    ("probable_reserves_Mt", float),
    ("ore_proven_reserves_Mt", float),
    ("proven_reserves_Mt", float),
    ("ore_total_reserves_Mt", float),
    ("total_reserves_Mt", float)
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

def renderDict(row: pd.Series, field_types: List, key_formatter=default_key_formatter):
    return {
        key_formatter(col): safeCast(row[col], dtype)
        for col, dtype in field_types
    }

def renderCoalStats(row: pd.Series):
    data_dict = renderDict(row, COAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, COAL_RESERVES_RESOURCES)
    data_dict["product"] = safeCast(row["product"], dict)
    return data_dict

def renderGoldSilverCopperStats(row: pd.Series):
    data_dict = renderDict(row, MINERAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, GOLD_COPPER_RESERVES_RESOURCES)
    data_dict["product"] = safeCast(row["product"], dict)
    return data_dict

def renderNickelStats(row: pd.Series):
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

        commodity = row["commodity_type"]
        if commodity == "Coal":
            data_dict = renderCoalStats(row)
        elif commodity == "Nickel":
            data_dict = renderNickelStats(row)
        else:
            data_dict = renderGoldSilverCopperStats(row)
            
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


def renderGoldCopperMine(row):
    return renderDict(row, GOLD_COPPER_MINE, lambda col: col.replace("gold ", ""))

def renderCoalMine(row):
    return renderDict(row, COAL_MINE, lambda col: col.replace("coal ", ""))

def renderNickelMine(row):
    data_dict = {'measurement_year': safeCast(row['nickel measurement_year'], int)}
    data_dict['limonite'] = renderDict(row, LIMONITE_MINE, lambda col: col.replace("lim ", ""))
    data_dict['saprolite'] = renderDict(row, SAPROLITE_MINE, lambda col: col.replace("sap ", ""))
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
                    ) -> pd.DataFrame:
    # Load and clean reference DataFrame
    minerba_df = prepareMinerbaDf()
    
    df_company = clean_company_df(df, 'name') # This function add two columns: ['name_cleaned', 'name_cleaned_no_space']
    df_minerba = clean_company_df(minerba_df,'company_name') # Same goes here


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
            # Drop "company_name" column to finalize the payload
            matches = matches.drop(columns=['company_name', 'name_cleaned', 'name_cleaned_no_space'], errors='ignore')
            records = matches.to_dict(orient="records")
        else:
            # empty list when no matches
            records = []  

        
        license_json = json.dumps(records, ensure_ascii=False)
        df_company.at[row_id, 'mining_license'] = license_json

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

    return df_company

def fillMiningContract(df: pd.DataFrame, sheet_id: int) -> pd.DataFrame:

    c_df = df.copy()
    _, mc_df = getSheetAll("mining_contract")

    # Clean and normalize IDs for reliable matching
    mc_df["contractor_id"] = (
        pd.to_numeric(mc_df["contractor_id"], errors="coerce")
        .astype("Int64")
        .astype(str)
    )

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
        contracts_dict[contractor_id] = json.dumps(contract_list)

    # Map contracts to company dataframe and fill empty values with '[]'
    c_df["mining_contract"] = c_df["id"].map(contracts_dict)
    c_df["mining_contract"] = c_df["mining_contract"].fillna("[]")
    c_df.loc[c_df["mining_contract"].isnull(), "mining_contract"] = "[]"

    rows = c_df["mining_contract"].tolist()
    rows = [{"values": [{"userEnteredValue": {"stringValue": r}}]} for r in rows]
    col_id = c_df.columns.get_loc("mining_contract")

    response = batchUpdateSheet(rows, sheet_id, 0, len(c_df), col_id)

    return c_df

def renderCoalResourcesReserves(row):
    return renderDict(row, RESERVES_RESOURCES_COAL)
    
def renderMetalResourcesReserves(row):
    return renderDict(row, RESERVES_RESOURCES_METAL)

def jsonifyProvincesResourcesReserves(df: pd.DataFrame) -> pd.DataFrame:
    renderMap = {
        'Coal': renderCoalResourcesReserves,
    }

    for rowid, row in df.iterrows():
        commodity = row['commodity_type']
        renderFunction = renderMap.get(commodity, renderMetalResourcesReserves)
        df.at[rowid, 'resources_reserves'] = json.dumps(renderFunction(row))

    return df
# %%
