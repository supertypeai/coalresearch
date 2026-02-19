from scripts.sync_company_name_id import SyncCompanyId
from sheet_api.google_sheets.client import getSheetAll
from sheet_api.core.sync import replaceMC
from db.models import MiningContract, Company

def sync_mining_contract():
    sync = SyncCompanyId()
    sync.update_target("mc")

    _, df = getSheetAll("mining_contract")

    print(df)

    if input("Replace mining contracts according to the sheet?") == "Y":
        replaceMC(MiningContract, Company, df)