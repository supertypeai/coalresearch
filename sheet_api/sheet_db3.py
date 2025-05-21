# %%
import pandas as pd
from sheet_api.google_sheets.auth import createClient

client, spreadsheet_id = createClient()

# %%

c_sheet = client.open_by_key(spreadsheet_id).worksheet('company_new_id')
c_data = c_sheet.get('A1:R246')
c_df = pd.DataFrame(c_data[1:], columns=c_data[0])

ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('coal_company_performance')
ccp_data = ccp_sheet.get('A1:AB135')
ccp_df = pd.DataFrame(ccp_data[1:], columns=ccp_data[0])

ms_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')
ms_data = ms_sheet.get('A1:W91')
ms_df = pd.DataFrame(ms_data[1:], columns=ms_data[0])

# %%
def checkNoneEmpty(val):
    if (val is None) or (val == ""):
        return None
    else:
        return val

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

# %%
syncCompanyNameID(c_df, c_sheet, '*parent_company_name', '*parent_company_id', starts_from=210)

# %%

syncCompanyNameID(ccp_df, ccp_sheet, '*company_name', 'company_id', starts_from=131)
# %%

syncCompanyNameID(ms_df, ms_sheet, '*company_name', 'company_id', starts_from=69)

# %%
