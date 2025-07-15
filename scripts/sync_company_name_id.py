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
    targets = ['ccp', 'c', 'ms', 'mc', 'cp', 'n']
    for target in targets:
        update_target(target)

def update_commodity_performance():
    commodities = ['coal', 'nickel', 'gold', 'copper', 'silver']
    for commodity in commodities:
        sheet, df = getSheetAll(f'{commodity}_performance')
        batchUpdate(df, '*company_name', 'company_id', sheet.id)

def update_target(target):
    if target == 'ccp':
        sheet, df = getSheetAll('company_performance')
        batchUpdate(df, '*company_name', 'company_id', sheet.id)

    elif target == 'c':
        sheet, df = getSheetAll('company')
        batchUpdate(df, '*parent_company_name', '*parent_company_id', sheet.id)

    elif target == 'ms':
        sheet, df = getSheetAll('mining_site')
        batchUpdate(df, '*company_name', 'company_id', sheet.id)

    elif target == 'mc':
        sheet, df = getSheetAll('mining_contract')
        batchUpdate(df, '*mine_owner_name', 'mine_owner_id', sheet.id)
        batchUpdate(df, '*contractor_name', 'contractor_id', sheet.id)

    elif target == 'cp':
        sheet, df = getSheetAll('product')
        batchUpdate(df, '*company_name', 'company_id', sheet.id)

    elif target == 'n':
        sheet, df = getSheetAll('nickel_performance')
        batchUpdate(df, '*company_name', 'company_id', sheet.id)

    else:
        print(f"Unknown target: {target}")

def main():
    parser = argparse.ArgumentParser(description="Batch update Google Sheets with company data.")
    parser.add_argument('--target', choices=['ccp', 'c', 'ms', 'mc', 'cp', 'n'], help='Select specific target to update')
    parser.add_argument('--all', action='store_true', help='Update all predefined datasets')
    parser.add_argument('--commodity', action='store_true', help='Update all commodity performance datasets')

    args = parser.parse_args()

    if args.all:
        update_all()
    elif args.commodity:
        update_commodity_performance()
    elif args.target:
        update_target(args.target)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
