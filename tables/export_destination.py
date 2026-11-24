import pandas as pd
from gspread import Worksheet

from db.models import ExportDestination
from sheet_api.core.sync import sync_model

# The sheet holds country names as BPS publishes them, which change between
# releases and commodities ("Hongkong" for coal, "Hong Kong" for gold). Map them
# to one name per country so each country's series is one time series and
# matches the names used in global_commodity_data. Add new spellings here.
COUNTRY_NAMES = {
    "Hongkong": "Hong Kong",
    "Korea. Republic Of": "South Korea",
    "Republic Of Korea": "South Korea",
    "Chinese Taipei": "Taiwan",
    "Germany. Fed. Rep. Of": "Germany",
}


def exportDestinationPreprocess(df: pd.DataFrame, field_types: dict, sheet: Worksheet):
    df["country"] = df["country"].replace(COUNTRY_NAMES)

    key = ["commodity_type", "year", "country"]
    has_country = df["country"].fillna("").astype(str).str.strip().ne("")
    dupes = df[has_country & df.duplicated(key, keep=False)]
    if not dupes.empty:
        raise ValueError(
            "export_destination has more than one row per commodity/year/country "
            "after normalising country names; fix these rows in the sheet:\n"
            + dupes[["id", *key]].sort_values(key).to_string(index=False)
        )
    return df, field_types, sheet


def sync_export_destination():
    sync_model("export_destination", ExportDestination, preprocess=exportDestinationPreprocess)
