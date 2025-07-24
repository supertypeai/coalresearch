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
from sheet_api.google_sheets.auth import createClient
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
    jsonifyMineRsrvRsro,
    fillMiningLicense,
    fillMiningContract,
)

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
    data = sheet.get("A1:350")
    df = pd.DataFrame(data[1:], columns=data[0])

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replaceCO(CompanyOwnership, Company, df)


def companyPreprocess(df: pd.DataFrame, field_types: dict, sheet):
    # 1. Convert phone number to string type
    field_types["phone_number"] = "string"

    # 2. Fill out mining license
    print("Filling out company's mining_license...")
    fillMiningLicense(df, sheet.id)

    # 3. Fill out mining contracts
    print("Filling out company's mining_contracts...")
    fillMiningContract(df, sheet.id)

    return df, field_types, sheet


def companyPerformancePreprocess(df: pd.DataFrame, field_types: dict, sheet):
    update_new_company_performance()

    CompanyPerformance.truncate_table()

    return df, field_types, sheet


def miningSitePreprocess(df: pd.DataFrame, field_types: dict, sheet):
    # # 1. Compile reserves_resourcees
    jsonifyMineRsrvRsro(df, sheet.id)

    # 2. Compile location
    location = [
        ("*province", str),
        ("*city", str),
        ("*latitude", float),
        ("*longitude", float),
    ]
    compileToJsonBatch(df, location, "location", sheet.id)

    return df, field_types, sheet


def sync_company():
    sync_model("company", "A1:U357", Company, companyPreprocess)


def sync_company_performance():
    sync_model(
        "company_performance",
        "A1:H262",
        CompanyPerformance,
        companyPerformancePreprocess,
    )


def sync_mining_site():
    sync_model("mining_site", "A1:BZ157", MiningSite, miningSitePreprocess)


def sync_process_ownership():
    processCompanyOwnership()


def sync_resources_and_reserves():
    sync_model("resources_and_reserves", "A1:N24", ResourcesAndReserves)


def sync_commodities_production_id():
    sync_model("commodities_production_id", "A1:E32", TotalCommoditiesProduction)


def sync_export_destination():
    sync_model("export_destination", "A1:G273", ExportDestination)


def sync_global_commodity_data():
    sync_model("global_commodity_data", "A1:F137", GlobalCommodityData)


MODEL_SYNC_MAP = {
    "company": sync_company,
    "company_performance": sync_company_performance,
    "mining_site": sync_mining_site,
    "company_ownership": sync_process_ownership,
    "resources_and_reserves": sync_resources_and_reserves,
    "commodities_production_id": sync_commodities_production_id,
    "export_destination": sync_export_destination,
    "global_commodity_data": sync_global_commodity_data,
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
