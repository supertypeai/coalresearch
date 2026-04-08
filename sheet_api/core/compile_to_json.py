# %%
from sheet_api.core.toolbox           import safeCast, clean_company_df, parse_spec_value
from sheet_api.google_sheets.auth     import createClient, createService
from sheet_api.minerba_merge          import prepareMinerbaDf
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
    ("exploration_target_t", float),
    ("total_inventory_t", float),
    ("inferred_resources_t", float),
    ("indicated_resources_t", float),
    ("measured_resources_t", float),
    ("total_resources_t", float),
    ("total_resources_verify_t", float),
    ("total_reserves_t", float),
    ("total_reserves_verify_t", float)
]
RESERVES_RESOURCES_METAL = [
    ("ore_inferred_resources_t", float),
    ("inferred_resources_t", float),
    ("ore_indicated_resources_t", float),
    ("indicated_resources_t", float),
    ("ore_measured_resources_t", float),
    ("measured_resources_t", float),
    ("ore_total_resources_t", float),
    ("total_resources_t", float),
    ("ore_probable_reserves_t", float),
    ("probable_reserves_t", float),
    ("ore_proven_reserves_t", float),
    ("proven_reserves_t", float),
    ("ore_total_reserves_t", float),
    ("total_reserves_t", float)
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
    data_dict["products"] = safeCast(row["products"], dict)
    return data_dict

def renderGoldSilverCopperStats(row: pd.Series):
    data_dict = renderDict(row, MINERAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, GOLD_COPPER_RESERVES_RESOURCES)
    data_dict["products"] = safeCast(row["products"], dict)
    return data_dict

def renderNickelStats(row: pd.Series):
    data_dict = renderDict(row, MINERAL_STATS)
    data_dict["resources_reserves"] = renderDict(row, NICKEL_RESERVES_RESOURCES)
    data_dict["products"] = safeCast(row["products"], dict)
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
    data_dict = renderDict(row, GOLD_COPPER_MINE, lambda col: col.replace("gold ", ""))
    
    quality_suffixes = ("_g_per_ton", "_pct")

    for k, v in data_dict.items():
        if k.endswith(quality_suffixes):
            data_dict[k] = parse_spec_value(v)

    return data_dict

def renderCoalMine(row):
    data_dict = renderDict(row, COAL_MINE, lambda col: col.replace("coal ", ""))
    data_dict["calorific_value_kcal"] = parse_spec_value(data_dict.get("calorific_value_kcal"))
    return data_dict

def renderNickelMine(row):
    data_dict = {'measurement_year': safeCast(row['nickel measurement_year'], int)}
    data_dict['limonite'] = renderDict(row, LIMONITE_MINE, lambda col: col.replace("lim ", ""))
    data_dict['saprolite'] = renderDict(row, SAPROLITE_MINE, lambda col: col.replace("sap ", ""))

    quality_suffixes = ("_pct")

    for k, v in data_dict['limonite'].items():
        if k.endswith(quality_suffixes):
            data_dict['limonite'][k] = parse_spec_value(v)

    for k, v in data_dict['saprolite'].items():
        if k.endswith(quality_suffixes):
            data_dict['saprolite'][k] = parse_spec_value(v)

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

        renderFunction = renderMap.get(row['commodity_type'], renderCoalMine)
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

