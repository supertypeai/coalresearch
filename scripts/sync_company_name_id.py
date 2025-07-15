# %%
import argparse

from sheet_api.google_sheets.auth import createClient, createService
from sheet_api.google_sheets.client import getSheetAll

_, spreadsheet_id = createClient()
service = createService()

# %%

c_sheet, c_df = getSheetAll('company')


# %%
def syncCompanyNameID(df, sheet, company_name_col, company_id_col, starts_from=0):
    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        company_q = c_df[c_df['name'] == row[company_name_col]]

        if not company_q.empty:

            col_id = df.columns.get_loc(company_id_col)
            
            original_value = row[company_id_col]
            new_value = company_q['id2'].iloc[0]

            to_use_value = new_value

            sheet.update_cell(2 + row_id, col_id + 1, to_use_value)

            print("Updating row number, col number, col name, value:", row_id + 2, col_id, company_id_col, to_use_value)


def batchUpdate(df, company_name_col, company_id_col, sheet_id, starts_from=0):

    col_id = df.columns.get_loc(company_id_col)

    rows = []

    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        company_q = c_df[c_df['name'] == row[company_name_col]]

        if not company_q.empty:
            
            original_value = row[company_id_col]
            new_value = company_q['id'].iloc[0]

            to_use_value = new_value
            to_use_value = {'numberValue':f'{to_use_value}'}

        else:
            to_use_value = {}
        

        rows.append(
            {
                'values': 
                    [
                        {'userEnteredValue': to_use_value}
                    ]
            }
        )


    requests = [
        {
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': starts_from + 1,
                    'endRowIndex': len(df) + 1,
                    'startColumnIndex': col_id,
                    'endColumnIndex': col_id + 1
                },
                'rows': rows,
                'fields': 'userEnteredValue'
            }
        }
    ]

    response = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': requests}
    ).execute()

    print(f"Batch update response: {response}")

def update_all():
    ccp_sheet, ccp_df = getSheetAll('company_performance')
    ms_sheet, ms_df = getSheetAll('mining_site')
    mc_sheet, mc_df = getSheetAll('mining_contract')
    cp_sheet, cp_df = getSheetAll('product')
    n_sheet, n_df = getSheetAll('nickel_performance')

    batchUpdate(ccp_df, '*company_name', 'company_id', ccp_sheet.id)
    batchUpdate(c_df, '*parent_company_name', '*parent_company_id', c_sheet.id)
    batchUpdate(ms_df, '*company_name', 'company_id', ms_sheet.id)
    batchUpdate(mc_df, '*mine_owner_name', 'mine_owner_id', mc_sheet.id)
    batchUpdate(mc_df, '*contractor_name', 'contractor_id', mc_sheet.id)
    batchUpdate(cp_df, '*company_name', 'company_id', cp_sheet.id)
    batchUpdate(n_df, '*company_name', 'company_id', n_sheet.id)

def main():
    parser = argparse.ArgumentParser(description="Batch update Google Sheets with company data.")

    parser.add_argument('--target', choices=['ccp', 'c', 'ms', 'mc', 'cp', 'n'], help='Select target to update')
    parser.add_argument('--all', action='store_true', help='Update all datasets')

    args = parser.parse_args()

    if args.all:
        update_all()
    elif args.target:
        if args.target == 'ccp':
            ccp_sheet, ccp_df = getSheetAll('company_performance')
            batchUpdate(ccp_df, '*company_name', 'company_id', ccp_sheet.id)
        elif args.target == 'c':
            batchUpdate(c_df, '*parent_company_name', '*parent_company_id', c_sheet.id)
        elif args.target == 'ms':
            ms_sheet, ms_df = getSheetAll('mining_site')
            batchUpdate(ms_df, '*company_name', 'company_id', ms_sheet.id)
        elif args.target == 'mc':
            mc_sheet, mc_df = getSheetAll('mining_contract')
            batchUpdate(mc_df, '*mine_owner_name', 'mine_owner_id', mc_sheet.id)
            batchUpdate(mc_df, '*contractor_name', 'contractor_id', mc_sheet.id)
        elif args.target == 'cp':
            cp_sheet, cp_df = getSheetAll('product')
            batchUpdate(cp_df, '*company_name', 'company_id', cp_sheet.id)
        elif args.target == 'n':
            n_sheet, n_df = getSheetAll('nickel_performance')
            batchUpdate(n_df, '*company_name', 'company_id', n_sheet.id)
    else:
        print("Please specify --target <target> or --all.")

if __name__ == "__main__":
    main()
