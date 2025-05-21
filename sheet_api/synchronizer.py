# %%
import sys
import os

sys.path.append(os.path.join(os.getcwd(), "sheet_api"))

import pandas as pd
import peewee as pw

from peewee import SqliteDatabase
from typing import Callable
from db.models import (
    Company,
    CoalCompanyPerformance,
    MiningSite,
    CompanyOwnership,
    CoalProduct,
    MiningContract,
    CoalExportDestination,
    TotalCoalProduction,
    CoalResourcesAndReserves,
    MiningLicense,
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

# %%

db = SqliteDatabase("coal_db.sqlite")
client, spreadsheet_id = createClient()


def sync_model(
    sheet_name: str, range: str, model: pw.ModelBase, preprocess: Callable
) -> None:
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get(range)
    df = pd.DataFrame(data[1:], columns=data[0])

    pw_field_types = {fn.name: type(fn).__name__ for fn in model._meta.sorted_fields}
    field_types = mapPeeweeToPandasFields(pw_field_types)

    if preprocess:
        df, field_types = preprocess(df, field_types)

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


if __name__ == "__main__":

    def company_preprocess(df: pd.DataFrame, field_types: dict):
        field_types["phone_number"] = "string"
        return df, field_types

    def rename(df, field_types):
        df = df.rename(
            columns={"total_reserve": "reserve", "total_resource": "resource"}
        )
        return df, field_types

    # %%
    sync_model("company", "A1:R246", Company, company_preprocess)
    # %%
    sync_model("coal_company_performance", "A1:AB134", CoalCompanyPerformance, rename)
    # %%
    sync_model("mining_site", "A1:W51", MiningSite, rename)
    # %%
    processCompanyOwnership()
    # %%

    # %%
    sync_model("coal_product", "A1:L56", CoalProduct, rename)
    sync_model("mining_license", "A1:O958", MiningLicense, rename)
    sync_model(
        "coal_resources_and_reserves", "A1:L24", CoalResourcesAndReserves, rename
    )
    sync_model("total_coal_production", "A1:D12", TotalCoalProduction, rename)
    sync_model("mining_contract", "A1:G33", MiningContract, rename)
    sync_model("coal_export_destination", "A1:F122", CoalExportDestination, rename)

# %%
