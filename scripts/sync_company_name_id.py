import argparse

from sheet_api.google_sheets.auth import createClient, createService
from sheet_api.google_sheets.client import getSheetAll

_, spreadsheet_id = createClient()
service = createService()

def syncCompanyNameID(c_df, df, sheet, company_name_col, company_id_col, starts_from=0):
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

def sendRequest(sheet_id, start, end, col_id, rows):
    requests = [
        {
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': start + 1,
                    'endRowIndex': end + 1,
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


def batchUpdate(c_df, df, company_name_col, company_id_col, sheet_id, starts_from=0, sync_name=False):

    source = 'id' if sync_name else 'name'
    target = 'name' if sync_name else 'id'
    source_col = company_id_col if sync_name else company_name_col
    target_col = company_name_col if sync_name else company_id_col
    value_type = 'stringValue' if sync_name else 'numberValue'

    target_col_id = df.columns.get_loc(target_col)

    rows = []

    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        company_q = c_df[c_df[source] == row[source_col]]

        if not company_q.empty:
            
            original_value = row[target_col]
            new_value = company_q[target].iloc[0]

            to_use_value = new_value
            to_use_value = {value_type:f'{to_use_value}'}

        else:
            to_use_value = {}
        

        rows.append({
            'values': [{'userEnteredValue': to_use_value}]
            })

    sendRequest(sheet_id, starts_from, len(df), target_col_id, rows)

class SyncCompanyId:
    def __init__(self):
        # Initialize c_df once
        _, self.c_df = getSheetAll('company')

    def update_commodity_performance(self):
        commodities = ['coal', 'nickel', 'gold', 'copper', 'silver']
        for commodity in commodities:
            sheet, df = getSheetAll(f'{commodity}_performance')
            batchUpdate(self.c_df, df, '*company_name', 'company_id', sheet.id)

    def update_target(self, target):
        target_map = {
            'ccp': ('company_performance', [('*company_name', 'company_id')]),
            'c': ('company', [('*parent_company_name', '*parent_company_id')]),
            'ms': ('mining_site', [('*company_name', 'company_id')]),
            'mc': ('mining_contract', [
                ('*mine_owner_name', 'mine_owner_id'),
                ('*contractor_name', 'contractor_id')
            ]),
            'cp': ('product', [('*company_name', 'company_id')]),
        }

        if target not in target_map:
            print(f"Unknown target: {target}")
            return

        sheet_name, updates = target_map[target]
        sheet, df = getSheetAll(sheet_name)
        for col_name, id_field in updates:
            batchUpdate(self.c_df, df, col_name, id_field, sheet.id)

    def update_all(self):
        for target in ['ccp', 'c', 'ms', 'mc', 'cp']:
            self.update_target(target)
        self.update_commodity_performance()

def main():
    parser = argparse.ArgumentParser(description="Batch update Google Sheets with company data.")
    parser.add_argument('--target', choices=['ccp', 'c', 'ms', 'mc', 'cp'], help='Select specific target to update')
    parser.add_argument('--all', action='store_true', help='Update all predefined datasets')
    parser.add_argument('--commodity', action='store_true', help='Update all commodity performance datasets')
    args = parser.parse_args()

    sync = SyncCompanyId()

    if args.all:
        sync.update_all()
    elif args.commodity:
        sync.update_commodity_performance()
    elif args.target:
        sync.update_target(args.target)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()


