# %%
import sys
import os
os.chdir('..')
sys.path.append(os.path.join(os.getcwd(), "sheet_api"))

import pandas as pd
from sheet_api.google_sheets.auth import createClient, createService

client, spreadsheet_id = createClient()
service = createService()

# %%

c_sheet = client.open_by_key(spreadsheet_id).worksheet('company')
c_data = c_sheet.get('A1:S282')
c_df = pd.DataFrame(c_data[1:], columns=c_data[0])

ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('company_performance')
ccp_data = ccp_sheet.get('A1:AQ244')
ccp_df = pd.DataFrame(ccp_data[1:], columns=ccp_data[0])

ms_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')
ms_data = ms_sheet.get('A1:Y110')
ms_df = pd.DataFrame(ms_data[1:], columns=ms_data[0])

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

# # %%
# syncCompanyNameID(c_df, c_sheet, '*parent_company_name', '*parent_company_id', starts_from=210)
# # %%
# syncCompanyNameID(ccp_df, ccp_sheet, '*company_name', 'company_id', starts_from=131)
# # %%
# syncCompanyNameID(ms_df, ms_sheet, '*company_name', 'company_id', starts_from=69)
# # %%

# %%
