import pandas as pd
from gspread import Worksheet
from sheet_api.db.models import ResourcesAndReserves
from sheet_api.core.sync import sync_model
from sheet_api.core.compile_to_json import jsonifyProvincesResourcesReserves

def resourcesAndReservesPreprocess(df: pd.DataFrame, field_types: dict, sheet: Worksheet):
    df = jsonifyProvincesResourcesReserves(df)
    return df, field_types, sheet

def sync_resources_and_reserves():
    sync_model(
        "resources_and_reserves",
        ResourcesAndReserves,
        preprocess=resourcesAndReservesPreprocess,
    )