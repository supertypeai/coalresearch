from scripts.sync_company_name_id import SyncCompanyId
from sheet_api.google_sheets.client import getSheetAll
from sheet_api.core.toolbox import safeCast
from db.models import MiningContract
import pandas as pd


# def safeCast(val, tp=int):
#     if pd.isna(val) or val == "":
#         return None
#     return tp(val)


def parse_date(val):
    if pd.isna(val) or val == "":
        return None
    ts = pd.to_datetime(val, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()

def replace_mining_contract_table(mc_model, df) -> None:

    mc_model.delete().execute()
    print("All Mining Contract records have been deleted")

    for _, row in df.iterrows():
        mine_owner_id = safeCast(row.get("mine_owner_id"), int)
        contractor_id = safeCast(row.get("contractor_id"), int)
        contract_end = parse_date(row.get("contract_period_end"))

        if mine_owner_id and contractor_id:
            mc_model.insert(
                mine_owner_id=mine_owner_id,
                contractor_id=contractor_id,
                contract_period_end=contract_end,
            ).execute()
            print(
                f"Inserted mining contract mine_owner_id: {mine_owner_id}, "
                f"contractor_id: {contractor_id}, contract_period_end: {contract_end}"
            )

def sync_mining_contract():
    sync = SyncCompanyId()
    sync.update_target("mc")

    _, df = getSheetAll("mining_contract")

    if input("Replace mining contracts according to the sheet?") == "Y":
        replace_mining_contract_table(MiningContract, df)