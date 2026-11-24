import pandas as pd
from db.models import MiningSite
from sheet_api.core.sync import sync_model
from sheet_api.core.compile_to_json import jsonifyMineReservesAndResources, compileToJsonBatch

# Production volume unit per commodity. The sheet has no unit column, so the
# sync sets it here. Copper site production is kt (Batu Hijau 2024 = 179.1 kt).
SITE_PRODUCTION_UNITS = {
    "Coal": "Mt",
    "Copper": "kt",
    "Gold": "koz",
    "Nickel": "Mt",
}


def miningSitePreprocess(df: pd.DataFrame, field_types: dict, sheet):
    # 0. Production unit from commodity type
    unknown = set(df["commodity_type"].dropna()) - set(SITE_PRODUCTION_UNITS) - {""}
    if unknown:
        raise ValueError(f"No production unit set for commodity type(s): {unknown}. "
                         "Add them to SITE_PRODUCTION_UNITS in tables/mining_site.py.")
    df["unit"] = df["commodity_type"].map(SITE_PRODUCTION_UNITS)

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
    sync_model("mining_site", MiningSite, "A1:CE157", miningSitePreprocess)