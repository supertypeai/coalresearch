# %%
import sys
import os

os.chdir("..")
sys.path.append(os.path.join(os.getcwd(), "sheet_api"))

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
    MiningContract,
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
    replaceMC,
)
from compile_to_json import (
    compileToJsonBatch,
    fillMiningLicense,
    mining_site_location_cols_type,
    mining_site_resources_reserves_cols_type,
    company_performance_coal_stats_type,
    company_performance_resources_reserves_cols_type,
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
    data = sheet.get("A1:R251")
    df = pd.DataFrame(data[1:], columns=data[0])

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replaceCO(CompanyOwnership, Company, df)


def processMiningContract() -> None:
    sheet = client.open_by_key(spreadsheet_id).worksheet("mining_contract")
    data = sheet.get("A1:F33")
    df = pd.DataFrame(data[1:], columns=data[0])

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replaceMC(MiningContract, Company, df)


if __name__ == "__main__":

    def companyPreprocess(df: pd.DataFrame, field_types: dict, sheet):

        # 1. Convert phone number to string type
        field_types["phone_number"] = "string"

        # 2. Fill out mining license
        print("Filling out company's mining_license...")
        fillMiningLicense(df, sheet.id)

        return df, field_types, sheet

    def companyPerformancePreprocess(df: pd.DataFrame, field_types: dict, sheet):

        # 1. Compile to json for resources_reserves column
        print(
            f"Compiling to json format on company_performance's *resources_reserves..."
        )
        compileToJsonBatch(
            df,
            company_performance_resources_reserves_cols_type,
            "*resources_reserves",
            sheet.id,
        )

        # 2. Compile to json for commodity_stats column
        print(f"Compiling to json format on company_performance's commodity_stats...")
        compileToJsonBatch(
            df, company_performance_coal_stats_type, "commodity_stats", sheet.id
        )

        return df, field_types, sheet

    # %%
    sync_model("company", "A1:S249", Company, companyPreprocess)
    # %%
    sync_model(
        "company_performance",
        "A1:W206",
        CompanyPerformance,
        companyPerformancePreprocess,
    )
    # %%
    sync_model("mining_site", "A1:Y51", MiningSite)
    # %%
    processCompanyOwnership()
    # %%
    sync_model("resources_and_reserves", "A1:N24", ResourcesAndReserves)
    # %%
    sync_model("commodities_production_id", "A1:E32", TotalCommoditiesProduction)
    # %%
    processMiningContract()
    # %%
    sync_model("export_destination", "A1:G122", ExportDestination)
    # %%
    sync_model("global_commodity_data", "A1:F88", GlobalCommodityData)

# %%
