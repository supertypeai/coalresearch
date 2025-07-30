import pandas as pd
import peewee as pw
import argparse

from typing import Callable, Optional
from sheet_api.db.models import (
    Company,
    CompanyOwnership,
    CompanyPerformance,
    MiningSite,
    ResourcesAndReserves,
    TotalCommoditiesProduction,
    ExportDestination,
    GlobalCommodityData,
)
from sheet_api.google_sheets.client import getSheet, getSheetAll
from sheet_api.core.toolbox import castTypes, mapPeeweeToPandasFields
from sheet_api.core.company_performance_restructure import (
    update_new_company_performance,
)
from sheet_api.core.sync import (
    checkDeletedAndOrder,
    compareDBSheet,
    checkNewData,
    confirmChange,
    replaceCO,
)
from sheet_api.core.compile_to_json import (
    compileToJsonBatch,
    jsonifyProvincesResourcesReserves,
    jsonifyMineReservesAndResources,
    fillMiningLicense,
    fillMiningContract,
)

def sync_model(
    sheet_name: str,
    model: pw.ModelBase,
    range: Optional[str] = None,
    preprocess: Optional[Callable] = None,
) -> None:
    if range:
        sheet, df = getSheet(sheet_name, range)
    else:
        sheet, df = getSheetAll(sheet_name)

    pw_field_types = {fn.name: type(fn).__name__ for fn in model._meta.sorted_fields}
    field_types = mapPeeweeToPandasFields(pw_field_types)

    if preprocess is not None:
        df, field_types, sheet = preprocess(df, field_types, sheet)

    df = castTypes(df, field_types)

    confirmChange(checkDeletedAndOrder, model, df)
    confirmChange(compareDBSheet, model, df)
    confirmChange(checkNewData, model, df, field_types)


def companyPreprocess(df: pd.DataFrame, field_types: dict, sheet):
    # 1. Convert phone number to string type
    field_types["phone_number"] = "string"

    # 2. Fill out mining license
    print("Filling out company's mining_license...")
    df = fillMiningLicense(df, sheet.id)

    # 3. Fill out mining contracts
    print("Filling out company's mining_contracts...")
    df = fillMiningContract(df, sheet.id)

    return df, field_types, sheet


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

def resourcesAndReservesPreprocess(df: pd.DataFrame, field_types: dict, sheet):
    print(df)
    
    df = jsonifyProvincesResourcesReserves(df)
    
    print(df)

    excluded_provinces = ['Papua Barat Daya', 'Papua Tengah']
    df = df[~df['province'].isin(excluded_provinces)]

    return df, field_types, sheet

def sync_company():
    sync_model("company", Company, "A1:U357", companyPreprocess)


def sync_company_performance():
    update_new_company_performance()
    CompanyPerformance.truncate_table()
    sync_model("company_performance", CompanyPerformance)


def sync_mining_site():
    sync_model("mining_site", MiningSite, "A1:CD157", miningSitePreprocess)


def sync_process_ownership():
    _, df = getSheetAll("company")

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replaceCO(CompanyOwnership, Company, df)
    

def sync_resources_and_reserves():
    sync_model("resources_and_reserves", ResourcesAndReserves, preprocess=resourcesAndReservesPreprocess)


def sync_total_commodities_production():
    sync_model("total_commodities_production", TotalCommoditiesProduction)


def sync_export_destination():
    sync_model("export_destination", ExportDestination)


def sync_global_commodity_data():
    sync_model("global_commodity_data", GlobalCommodityData, "A1:H137")


def sync_company_financials():
    from sheet_api import company_financials
    company_financials.main()


def sync_sales_destination():
    from sheet_api import sales_destination
    sales_destination.main()

MODEL_SYNC_MAP = {
    "company": sync_company,
    "company_performance": sync_company_performance,
    "company_financials": sync_company_financials,
    "company_ownership": sync_process_ownership,
    "export_destination": sync_export_destination,
    "global_commodity_data": sync_global_commodity_data,
    "mining_site": sync_mining_site,
    "resources_and_reserves": sync_resources_and_reserves,
    "total_commodities_production": sync_total_commodities_production,
}


def main():
    parser = argparse.ArgumentParser(description="Data sync CLI")
    parser.add_argument(
        "action", choices=["sync"], help="Action to perform (only 'sync' supported)"
    )
    parser.add_argument(
        "model", choices=MODEL_SYNC_MAP.keys(), help="Specify the model to sync"
    )

    args = parser.parse_args()

    if args.action == "sync":
        MODEL_SYNC_MAP[args.model]()
        print(f"{args.model} synced.")


if __name__ == "__main__":
    main()
