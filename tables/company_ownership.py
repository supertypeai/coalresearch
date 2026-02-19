import pandas as pd
from scripts.sync_company_name_id import SyncCompanyId
from sheet_api.google_sheets.client import getSheetAll
from sheet_api.core.sync import replaceCO
from db.models import CompanyOwnership, Company

def sync_process_ownership():
    sync = SyncCompanyId()
    sync.update_target("c")

    _, df = getSheetAll("company")

    if input("Replace company ownerhip according to the sheet?") == "Y":
        replaceCO(CompanyOwnership, Company, df)