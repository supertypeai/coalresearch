from scripts.sync_company_name_id import SyncCompanyId
from sheet_api.google_sheets.client import getSheetAll
from db.models import CompanyOwnership, Company
from sheet_api.core.toolbox import safeCast
import pandas as pd

# def safeCast(val, tp=int):
#     if pd.isna(val) or val == "":
#         return None
#     else:
#         return tp(val)

def replace_company_ownership_table(co_model, c_model, df) -> None:
    co_model.delete().execute()

    print("All Company Ownership records have been deleted")

    for _, row in df.iterrows():
        parent = c_model.get_or_none(
            c_model.name == safeCast(row["*parent_company_name"], str)
        )
        company = c_model.get_or_none(c_model.name == safeCast(row["name"], str))
        ownership = safeCast(row["*percentage_ownership"], float)

        if parent and company and ownership:
            co_model.insert(
                parent_company_id=parent.id,
                company_id=company.id,
                percentage_ownership=ownership,
            ).execute()

            print(f"Inserted parent_id: {parent.id}, company_id: {company.id}")

def sync_process_ownership():
    sync = SyncCompanyId()
    sync.update_target("c")

    _, df = getSheetAll("company")

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replace_company_ownership_table(CompanyOwnership, Company, df)