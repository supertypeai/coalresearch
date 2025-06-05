import pandas as pd

from tabulate import tabulate
from google_sheets.client import getSheet, getEntrySheet

c_sheet, c_df = getSheet("company", "A1:R246")
cp_sheet, cp_df = getSheet("company_performance", "A1:W189")
ms_sheet, ms_df = getSheet("mining_site", "A1:Y51")

e_sheet, company_data, performance_data, mining_data_list = getEntrySheet()

def limitStrLen(val):
    if not isinstance(val, str):
        return val
    
    if len(val) > 50:
        val = val[:50] + "..."

    return val

def checkEmpty(val):
    if (val == pd.isna) or (val == "") or (val == None):
        return None
    else:
        return val


def displayComparison(q, table_data, excluded_cols):
    table_display = []

    for col in q.columns:
        if col not in excluded_cols:
            db_val = checkEmpty(q[col].values[0])
            entry_val = table_data.get(col, None)

            is_same = db_val == entry_val

            db_val = limitStrLen(db_val)
            entry_val = limitStrLen(entry_val)

            table_display.append([col, db_val, entry_val, is_same])

    print(tabulate(table_display, headers=["Field", "DB", "Entry", "Same"], tablefmt="grid"))
    print("")


def upsertCompany():    
    q = c_df[c_df["name"] == company_data["name"]]

    if q.empty:
        # insert table
        return

    excluded_cols = ["id", "*parent_company_id", "*holding_company_id", 
                     "*holding_company_name", "*effective_ownership"]
    
    displayComparison(q, company_data, excluded_cols)


def upsertCompanyPerformance():
    q = cp_df[(cp_df["*company_name"] == performance_data["*company_name"]) &
              (cp_df["year"] == performance_data["year"])]

    if q.empty:
        # insert table
        return

    excluded_cols = ["id", "company_id", "*mine_id", 
                     "resources_reserves"]
    
    displayComparison(q, performance_data, excluded_cols)

def upsertMiningSite(table_data):
    q = ms_df[(ms_df["name"] == table_data["name"]) &
              (ms_df["year"] == table_data["year"])]

    if q.empty:
        # insert table
        return

    excluded_cols = ["id", "location", "resources_reserves"]
    
    displayComparison(q, table_data, excluded_cols)

upsertCompany()
upsertCompanyPerformance()
for mining_data in mining_data_list:
    upsertMiningSite(mining_data)