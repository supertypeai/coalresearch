# %%
import pandas as pd
import re
from sheet_api.google_sheets.auth import createClient

minerba_df = pd.read_csv("coal_db - minerba.csv")
minerba_df['nama_prov'] = minerba_df['nama_prov'].str.title()
minerba_df['nama_kab'] = minerba_df['nama_kab'].str.title()
minerba_df['nama_usaha'] = minerba_df['nama_usaha'].str.lower()

client, spreadsheet_id = createClient()

ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('coal_company_performance')
ccp_data = ccp_sheet.get('A1:AB135')
ccp_df = pd.DataFrame(ccp_data[1:], columns=ccp_data[0])

# # %%
# c_sheet = client.open_by_key(spreadsheet_id).worksheet('company')
# c_data = c_sheet.get('A1:R248')
# c_df = pd.DataFrame(c_data[1:], columns=c_data[0])

# %%

c_cols = ["*sk_iup", "*permit_effective_date", "*permit_expiry_date"]
m_cols = ["sk_iup", "tgl_berlaku", "tgl_akhir"]

def checkNoneEmpty(val):
    if (val is None) or (val == ""):
        return None
    else:
        return val

def clean_company_name(name):
    return re.sub(r'\b(PT|Tbk)\b', '', name).lower().strip()

def updateSheet(df, sheet, starts_from=0):
    for row_id, row in df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        company_q = minerba_df[minerba_df['nama_usaha'] == clean_company_name(row['*company_name'])]

        for c, m in zip(c_cols, m_cols):

            sheet_val = checkNoneEmpty(row[c])
            
            if (sheet_val is None) and (not company_q.empty):

                col_id = df.columns.get_loc(c)
                
                original_value = sheet_val
                new_value = company_q[m].iloc[0]

                to_use_value = new_value

                sheet.update_cell(2 + row_id, col_id + 1, to_use_value)

                print("Updating row number, col number, col name, value:", row_id + 2, col_id, c, to_use_value)

updateSheet(ccp_df, ccp_sheet, starts_from=134)

# # %%

# %%

# c_cols = ["operation_province", "operation_kabkot"]
# m_cols = ["nama_prov", "nama_kab"]

# for row_id, row in c_df.iterrows():
#     q = minerba_df[minerba_df['nama_usaha'] == clean_company_name(row['name'])]
    
#     for c, m in zip(c_cols, m_cols):
        
#         op = checkNoneEmpty(row[c])
        
#         if (op is None) and (not q.empty):

#             col_id = c_df.columns.get_loc(c)
            
#             original_value = op
#             new_value = q[m].iloc[0]

#             to_use_value = new_value

#             c_sheet.update_cell(2 + row_id, col_id + 1, to_use_value)

#             print("Updating row number, col number, col name, value:", row_id + 2, col_id, c, to_use_value)

# # %%