# %%
import sys
import os
os.chdir('..')
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
from compile_to_json import (
    compileToJsonBatch,
    fillMiningLicense,
    mining_site_location_cols_type,
    mining_site_resources_reserves_cols_type,
    company_performance_resources_reserves_cols_type
)
# %%

client, spreadsheet_id = createClient()

def sync_model(
    sheet_name: str, range: str, model: pw.ModelBase, preprocess: Optional[Callable] = None
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

    def phoneNumberToString(df: pd.DataFrame, field_types: dict, sheet):
        field_types["phone_number"] = "string"
        return df, field_types, sheet
    
    def companyPerformanceCompileToJson(df: pd.DataFrame, field_types: dict, sheet):

        cols_type = company_performance_resources_reserves_cols_type
        target_col = "resources_reserves"

        print(f"Compiling to json format on company_performance's {target_col}...")
        compileToJsonBatch(df, cols_type, target_col, sheet.id)

        print("Filling out company_performance's mining_license...")
        fillMiningLicense(df, sheet.id)

        return df, field_types, sheet


    # %%
    sync_model("company", "A1:R249", Company, phoneNumberToString)
    # %%
    sync_model("company_performance", "A1:W206", CompanyPerformance, companyPerformanceCompileToJson)
    # %%
    sync_model("mining_site", "A1:Y51", MiningSite)
    # %%
    processCompanyOwnership()
    # %%
    sync_model("resources_and_reserves", "A1:N24", ResourcesAndReserves)
    # %%
    sync_model("total_commodities_production", "A1:E12", TotalCommoditiesProduction)
    # %%
    processMiningContract()
    # %%
    sync_model("export_destination", "A1:G122", ExportDestination)