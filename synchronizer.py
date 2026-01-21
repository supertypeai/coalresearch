import pandas as pd
import peewee as pw
import argparse

from scripts.sync_company_name_id import SyncCompanyId
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
    CompanyV2,
    CompanyPerformanceV2,
    CompanyFinancialsV2,
)
from sheet_api.google_sheets.client import getSheet, getSheetAll
from sheet_api.core.toolbox import castTypes, mapPeeweeToPandasFields
from sheet_api.core.commodity_performance import (
    update_commodity_performance,
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
    df = jsonifyProvincesResourcesReserves(df)
    return df, field_types, sheet


def sync_company():
    sync_model("company", Company, preprocess=companyPreprocess)


def sync_company_performance():
    update_commodity_performance()
    CompanyPerformance.truncate_table()
    sync_model("company_performance", CompanyPerformance)


def sync_mining_site():
    sync_model("mining_site", MiningSite, "A1:CD157", miningSitePreprocess)


def sync_process_ownership():
    sync = SyncCompanyId()
    sync.update_target("c")

    _, df = getSheetAll("company")

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replaceCO(CompanyOwnership, Company, df)


def sync_resources_and_reserves():
    sync_model(
        "resources_and_reserves",
        ResourcesAndReserves,
        preprocess=resourcesAndReservesPreprocess,
    )


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


# V2 sync functions - separate from original table syncs
def companyV2Preprocess(df: pd.DataFrame, field_types: dict, sheet):
    """Preprocess for company_v2 with slug generation."""
    from sheet_api.utils.slug_utils import generate_slug

    # 1. Convert phone number to string type
    field_types["phone_number"] = "string"

    # 2. Fill out mining license
    print("Filling out company's mining_license...")
    df = fillMiningLicense(df, sheet.id)

    # 3. Fill out mining contracts
    print("Filling out company's mining_contracts...")
    df = fillMiningContract(df, sheet.id)

    # 4. Generate slug from name
    print("Generating slugs from company names...")
    df["slug"] = df["name"].apply(generate_slug)

    return df, field_types, sheet


def sync_company_v2():
    """Sync company data to company_v2 table with slug generation."""
    sync_model("company", CompanyV2, preprocess=companyV2Preprocess)


def companyPerformanceV2Preprocess(df: pd.DataFrame, field_types: dict, sheet):
    """Preprocess for company_performance_v2 to add placeholder slug and sequential IDs."""
    import sqlite3

    # 1. Generate sequential IDs (replacing empty string IDs from update_commodity_performance)
    df["id"] = range(1, len(df) + 1)

    # 2. Remap company_id from company to company_v2
    # Since we preserved IDs in migration, company.id == company_v2.id for all existing companies
    # However, we should verify and handle any missing mappings
    conn = sqlite3.connect("db.sqlite")
    cursor = conn.cursor()

    # Create a mapping of company names to company_v2 IDs
    cursor.execute("SELECT name, id FROM company_v2")
    company_name_to_v2_id = dict(cursor.fetchall())

    # Also create direct ID mapping (old company.id -> company_v2.id)
    # Since we preserved IDs, this should be 1:1
    cursor.execute("SELECT id FROM company_v2")
    valid_v2_ids = set(row[0] for row in cursor.fetchall())

    conn.close()

    # For rows with company_id, verify they exist in company_v2
    # If not, try to map via company name (*company_name column)
    def remap_company_id(row):
        company_id = row.get("company_id")

        # If company_id is null or empty, try to find via name
        if pd.isna(company_id) or company_id == "":
            company_name = row.get("*company_name")
            if company_name and company_name in company_name_to_v2_id:
                return company_name_to_v2_id[company_name]
            return None

        # Convert to int
        try:
            company_id = int(company_id)
        except (ValueError, TypeError):
            # Try name lookup
            company_name = row.get("*company_name")
            if company_name and company_name in company_name_to_v2_id:
                return company_name_to_v2_id[company_name]
            return None

        # Check if this ID exists in company_v2
        if company_id in valid_v2_ids:
            return company_id

        # If not, try name lookup as fallback
        company_name = row.get("*company_name")
        if company_name and company_name in company_name_to_v2_id:
            return company_name_to_v2_id[company_name]

        return None

    df["company_id"] = df.apply(remap_company_id, axis=1)

    # 3. Add placeholder slug (will be updated later)
    df["slug"] = ""

    return df, field_types, sheet


def sync_company_performance_v2():
    """Sync company performance data to company_performance_v2 table."""
    update_commodity_performance()
    CompanyPerformanceV2.truncate_table()

    # First sync as usual
    sync_model(
        "company_performance",
        CompanyPerformanceV2,
        preprocess=companyPerformanceV2Preprocess,
    )

    # Then update slugs based on company_v2
    import sqlite3

    conn = sqlite3.connect("db.sqlite")
    cursor = conn.cursor()

    print("Updating slugs in company_performance_v2...")
    cursor.execute(
        """
        UPDATE company_performance_v2
        SET slug = (
            SELECT slug FROM company_v2 
            WHERE company_v2.id = company_performance_v2.company_id
        )
    """
    )
    conn.commit()
    conn.close()
    print("✓ Slugs updated successfully!")


def sync_company_financials_v2():
    """Sync company financials data to company_financials_v2 table."""
    from sheet_api import company_financials

    # Call with V2 parameters
    company_financials.main(table_name="company_financials_v2", use_v2=True)


MODEL_SYNC_MAP = {
    # Original tables
    "company": sync_company,
    "company_performance": sync_company_performance,
    "company_financials": sync_company_financials,
    "company_ownership": sync_process_ownership,
    "export_destination": sync_export_destination,
    "global_commodity_data": sync_global_commodity_data,
    "mining_site": sync_mining_site,
    "resources_and_reserves": sync_resources_and_reserves,
    "total_commodities_production": sync_total_commodities_production,
    # V2 tables
    "company_v2": sync_company_v2,
    "company_performance_v2": sync_company_performance_v2,
    "company_financials_v2": sync_company_financials_v2,
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
