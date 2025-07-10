# %%
import sys
import os
os.chdir('..')
sys.path.append(os.path.join(os.getcwd(), "sheet_api"))

from sheet_api.google_sheets.auth import createClient, createService
from sheet_api.google_sheets.client import getSheetAll

_, spreadsheet_id = createClient()
service = createService()

# %%

c_sheet, c_df = getSheetAll('company')
ccp_sheet, ccp_df = getSheetAll('company_performance')
ms_sheet, ms_df = getSheetAll('mining_site')
cp_sheet, cp_df = getSheetAll('product')
n_sheet, n_df = getSheetAll('nickel_performance')

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

# %%
batchUpdate(ccp_df, '*company_name', 'company_id', ccp_sheet.id)

# %%
batchUpdate(c_df, '*parent_company_name', '*parent_company_id', c_sheet.id)

# %%
batchUpdate(ms_df, '*company_name', 'company_id', ms_sheet.id)

# %%
batchUpdate(cp_df, '*company_name', 'company_id', cp_sheet.id)

# %%
batchUpdate(n_df, '*company_name', 'company_id', n_sheet.id)