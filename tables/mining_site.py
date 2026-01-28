import pandas as pd
from sheet_api.db.models import MiningSite
from sheet_api.core.sync import sync_model
from sheet_api.core.compile_to_json import jsonifyMineReservesAndResources, compileToJsonBatch

def miningSitePreprocess(df: pd.DataFrame, field_types: dict, sheet):
    # # 1. Compile reserves_resourcees
    jsonifyMineReservesAndResources(df, sheet.id)

    # 2. Compile location
    location = [
        ("*province", str),
        ("*city", str),
        ("*latitude", float),
        ("*longitude", float),
    ]
    compileToJsonBatch(df, location, "location", sheet.id)

    return df, field_types, sheet

def sync_mining_site():
    sync_model("mining_site", MiningSite, "A1:CD157", miningSitePreprocess)