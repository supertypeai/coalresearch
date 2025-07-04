# %%
import sys
import os
import json

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

        # # 3. Fill out mining contracts
        # print("Filling out company's mining_contracts...")
        # mining_contract_sheet = client.open_by_key(spreadsheet_id).worksheet(
        #     "mining_contract"
        # )

        # # Handle duplicate headers safely
        # mc_raw_values = mining_contract_sheet.get_all_values()
        # mc_headers = mc_raw_values[0]
        # seen = {}
        # unique_headers = []
        # for h in mc_headers:
        #     if h in seen:
        #         seen[h] += 1
        #         unique_headers.append(f"{h}_{seen[h]}")
        #     else:
        #         seen[h] = 0
        #         unique_headers.append(h)
        # mc_df = pd.DataFrame(mc_raw_values[1:], columns=unique_headers)

        # if "mining_contract" not in df.columns:
        #     df["mining_contract"] = pd.Series([None] * len(df), dtype=object)

        # # Clean and normalize IDs for reliable matching
        # mc_df["contractor_id"] = (
        #     pd.to_numeric(mc_df["contractor_id"], errors="coerce")
        #     .astype("Int64")
        #     .astype(str)
        # )
        # df["id"] = df["id"].astype(str)

        # # Group contracts by contractor_id
        # grouped_contracts = mc_df.groupby("contractor_id")

        # Create a dictionary of contracts with the new JSON structure
        # contracts_dict = {}
        # for contractor_id, group in grouped_contracts:
        #     contract_list = []
        #     for _, row in group.iterrows():
        #         agreement_type_str = row.get("Agreement type", "")
        #         agreement_types = (
        #             [item.strip() for item in agreement_type_str.split(",")]
        #             if agreement_type_str
        #             else []
        #         )

        #         new_contract = {
        #             "company_name": row.get("*mine_owner_name"),
        #             "company_id": row.get("mine_owner_id"),
        #             "contract_period_end": row.get("contract_period_end"),
        #             "agreement_type": agreement_types,
        #         }
        #         contract_list.append(new_contract)
        #     contracts_dict[str(contractor_id)] = json.dumps(contract_list)

        # # Map contracts to company dataframe and fill empty values with '[]'
        # df["mining_contract"] = df["id"].map(contracts_dict)
        # df["mining_contract"] = df["mining_contract"].fillna("[]")
        # df.loc[df["mining_contract"].isnull(), "mining_contract"] = "[]"

        return df, field_types, sheet

    def companyPerformancePreprocess(df: pd.DataFrame, field_types: dict, sheet):
        jsonifyCommodityStats(df, sheet.id)

        return df, field_types, sheet

    def miningSitePreprocess(df: pd.DataFrame, field_types: dict, sheet):
        # # 1. Compile reserves_resourcees
        # reserves_resources_fields = [
        #     ("*year_measured", int),
        #     ("*total_reserve", float),
        #     ("*total_resource", float),
        # ]
        # compileToJsonBatch(
        #     df, reserves_resources_fields, "resources_reserves", sheet.id, 111 - 2
        # )

        # # 2. Compile location
        # location = [
        #     ("*province", str),
        #     ("*city", str),
        #     ("*latitude", float),
        #     ("*longitude", float),
        # ]
        # compileToJsonBatch(df, location, "location", sheet.id)

        return df, field_types, sheet

    # %%
    sync_model("company", "A1:U282", Company, companyPreprocess)
    # %%
    sync_model(
        "company_performance",
        "A1:AQ221",
        CompanyPerformance,
        companyPerformancePreprocess,
    )
    # %%
    sync_model("mining_site", "A1:Y132", MiningSite, miningSitePreprocess)
    # %%
    processCompanyOwnership()
    # %%
    sync_model("resources_and_reserves", "A1:N24", ResourcesAndReserves)
    # %%
    sync_model("commodities_production_id", "A1:E32", TotalCommoditiesProduction)
    # %%
    sync_model("export_destination", "A1:G122", ExportDestination)
    # %%
    sync_model("global_commodity_data", "A1:F104", GlobalCommodityData)

# %%
