from sheet_api.google_sheets.client import getSheetAll
from gspread_dataframe import set_with_dataframe
from scripts.sync_company_name_id import SyncCompanyId
from sheet_api.db.models import Company
import synchronizer

def main():
    c_sheet, c_df = getSheetAll('company')
    c_df.loc[c_df['idx_ticker'] == '', 'idx_ticker'] = None

    c_df.sort_values(by='idx_ticker', ascending=True, inplace=True)
    c_df.reset_index(drop=True, inplace=True)
    c_df['id'] = c_df.index + 1

    set_with_dataframe(c_sheet, c_df, row=1, col=1)

    sync = SyncCompanyId()
    sync.update_all()

    Company.truncate_table()

    synchronizer.sync_company()
    synchronizer.sync_company_performance()
    synchronizer.sync_mining_site()
    synchronizer.sync_process_ownership()

if __name__ == '__main__':
    main()
