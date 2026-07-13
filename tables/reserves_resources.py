import pandas as pd
import json
from gspread import Worksheet
from db.models import ResourcesAndReserves
from sheet_api.core.sync import sync_model
from lib.formatter import validate_and_filter_province
from sheet_api.core.compile_to_json import (
    renderDict,
    RESERVES_RESOURCES_COAL,
    RESERVES_RESOURCES_METAL
)
from typing import Any


def safe_float_convert(value: Any) -> float:
    try:
        if value is None or pd.isna(value):
            return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def renderCoalResourcesReserves(row: pd.Series):
    res = renderDict(row, RESERVES_RESOURCES_COAL)
    res_processed = {}

    for k, v in res.items():
        if isinstance(k, str) and k.endswith('_t'):
            new_key = f'{k[:-2]}_Mt'
            res_processed[new_key] = safe_float_convert(v) / 1_000_000
        else:
            res_processed[k] = v
    return res_processed

def renderMetalResourcesReserves(row):
    res = renderDict(row, RESERVES_RESOURCES_METAL)
    res_processed = {}

    for k, v in res.items():
        if isinstance(k, str) and k.endswith('_t'):
            new_key = f'{k[:-2]}_kt'
            res_processed[new_key] = safe_float_convert(v) / 1_000
        else:
            res_processed[k] = v
    return res_processed

def jsonifyProvincesResourcesReserves(df: pd.DataFrame) -> pd.DataFrame:
    for rowid, row in df.iterrows():
        commodity = row['commodity_type']

        if commodity == 'Coal':
            df.at[rowid, 'resources_reserves'] = json.dumps(renderCoalResourcesReserves(row))
        else:
            df.at[rowid, 'resources_reserves'] = json.dumps(renderMetalResourcesReserves(row))

    return df

def resourcesAndReservesPreprocess(df: pd.DataFrame, field_types: dict, sheet: Worksheet):
    df = validate_and_filter_province(df, id_col="id")
    df = jsonifyProvincesResourcesReserves(df)
    return df, field_types, sheet

def sync_resources_and_reserves():
    sync_model(
        "resources_and_reserves",
        ResourcesAndReserves,
        preprocess=resourcesAndReservesPreprocess,
    )
