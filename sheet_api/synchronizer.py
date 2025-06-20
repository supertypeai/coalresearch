# %%
import sys
import os
import json

# os.chdir("..")
# sys.path.append(os.path.join(os.getcwd(), "sheet_api"))

import pandas as pd
import peewee as pw

from typing import Callable, Optional
from db.models import (
    Company,
    CompanyOwnership,
    CompanyPerformance,
    MiningSite,
    ResourcesAndReserves,
    TotalCommoditiesProduction,
    ExportDestination,
    GlobalCommodityData,
)
from google_sheets.auth import createClient
from utils.dataframe_utils import castTypes, mapPeeweeToPandasFields
from utils.sync_utils import (
    checkDeletedAndOrder,
    compareDBSheet,
    checkNewData,
    confirmChange,
    replaceCO,
)
from compile_to_json import (
    compileToJsonBatch,
    jsonifyCommodityStats,
    fillMiningLicense,
)

# %%

client, spreadsheet_id = createClient()


def sync_model(
    sheet_name: str,
    range: str,
    model: pw.ModelBase,
    preprocess: Optional[Callable] = None,
) -> None:
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get(range)
    df = pd.DataFrame(data[1:], columns=data[0])

    pw_field_types = {fn.name: type(fn).__name__ for fn in model._meta.sorted_fields}
    field_types = mapPeeweeToPandasFields(pw_field_types)

    if preprocess is not None:
        df, field_types, sheet = preprocess(df, field_types, sheet)

    df = castTypes(df, field_types)

    confirmChange(checkDeletedAndOrder, model, df)
    confirmChange(compareDBSheet, model, df)
    confirmChange(checkNewData, model, df, field_types)


def processCompanyOwnership() -> None:
    sheet = client.open_by_key(spreadsheet_id).worksheet("company")
    data = sheet.get("A1:S282")
    df = pd.DataFrame(data[1:], columns=data[0])

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replaceCO(CompanyOwnership, Company, df)


if __name__ == "__main__":

    def companyPreprocess(df: pd.DataFrame, field_types: dict, sheet):
        # 1. Convert phone number to string type
        field_types["phone_number"] = "string"

        # 2. Fill out mining license
        print("Filling out company's mining_license...")
        fillMiningLicense(df, sheet.id)

        return df, field_types, sheet

    def companyPerformancePreprocess(df: pd.DataFrame, field_types: dict, sheet):
        jsonifyCommodityStats(df, sheet.id)

        return df, field_types, sheet

    def miningSitePreprocess(df: pd.DataFrame, field_types: dict, sheet):
        # 1. Compile reserves_resourcees
        reserves_resources_fields = [
            ("*year_measured", int),
            ("*calorific_value", str),
            ("*total_reserve", float),
            ("*total_resource", float),
            ("*resources_inferred", float),
            ("*resources_indicated", float),
            ("*resources_measured", float),
            ("*reserves_proved", float),
            ("*reserves_probable", float),
        ]
        compileToJsonBatch(
            df, reserves_resources_fields, "resources_reserves", sheet.id
        )

        # 2. Compile location
        location = [
            ("*province", str),
            ("*city", str),
            ("*latitude", float),
            ("*longitude", float),
        ]
        compileToJsonBatch(df, location, "location", sheet.id)

        return df, field_types, sheet

    # %%
    sync_model("company", "A1:U282", Company, companyPreprocess)
    # %%
    sync_model(
        "company_performance",
        "A1:AQ245",
        CompanyPerformance,
        companyPerformancePreprocess,
    )
    # %%
    sync_model("mining_site", "A1:Y110", MiningSite, miningSitePreprocess)
    # %%
    processCompanyOwnership()
    # %%
    sync_model("resources_and_reserves", "A1:N24", ResourcesAndReserves)
    # %%
    sync_model("commodities_production_id", "A1:E32", TotalCommoditiesProduction)
    # %%
    sync_model("export_destination", "A1:G122", ExportDestination)
    # %%
    sync_model("global_commodity_data", "A1:F88", GlobalCommodityData)

# %%
