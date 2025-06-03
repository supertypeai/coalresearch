# %%
import sys
import os
os.chdir('..')
sys.path.append(os.path.join(os.getcwd(), "sheet_api"))

import pandas as pd
import peewee as pw

from typing import Callable
from db.models import (
    Company,
    CompanyOwnership,
    CompanyPerformance,
    MiningSite,
    ResourcesAndReserves,
    TotalCommoditiesProduction,
    ExportDestination,
    MiningContract,
)
from google_sheets.auth import createClient
from utils.dataframe_utils import castTypes, mapPeeweeToPandasFields
from utils.sync_utils import (
    checkDeletedAndOrder,
    compareDBSheet,
    checkNewData,
    confirmChange,
    replaceCO,
    replaceMC
)

# %%

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

def processMiningContract() -> None:
    sheet = client.open_by_key(spreadsheet_id).worksheet("mining_contract")
    data = sheet.get("A1:F33")
    df = pd.DataFrame(data[1:], columns=data[0])

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replaceMC(MiningContract, Company, df)


if __name__ == "__main__":

    def company_preprocess(df: pd.DataFrame, field_types: dict):
        field_types["phone_number"] = "string"
        return df, field_types

    def rename(df, field_types):
        # df = df.rename(
        #     columns={"total_reserve": "reserve", "total_resource": "resource"}
        # )
        return df, field_types

    # %%
    sync_model("company", "A1:R246", Company, company_preprocess)
    # %%
    sync_model("company_performance", "A1:AB180", CompanyPerformance, rename)
    # %%
    sync_model("mining_site", "A1:P51", MiningSite, rename)
    # %%
    processCompanyOwnership()

    # %%
    sync_model("resources_and_reserves", "A1:N24", ResourcesAndReserves, rename)
    # %%
    sync_model("total_commodities_production", "A1:E12", TotalCommoditiesProduction, rename)
    # %%
    processMiningContract()
    # %%
    sync_model("export_destination", "A1:G122", ExportDestination, rename)

# %%
