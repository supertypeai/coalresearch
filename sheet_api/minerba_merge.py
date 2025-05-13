# %%
import pandas as pd
import re
from sheet_api.google_sheets.auth import createClient

client, spreadsheet_id = createClient()

# %%
c_sheet = client.open_by_key(spreadsheet_id).worksheet('company')
c_data = c_sheet.get('A1:R248')
c_df = pd.DataFrame(c_data[1:], columns=c_data[0])

minerba_df = pd.read_csv("coal_db - minerba.csv")
minerba_df['nama_prov'] = minerba_df['nama_prov'].str.title()
minerba_df['nama_kab'] = minerba_df['nama_kab'].str.title()
minerba_df['nama_usaha'] = minerba_df['nama_usaha'].str.lower()

# %%

c_cols = ["operation_province", "operation_kabkot"]
m_cols = ["nama_prov", "nama_kab"]

def checkNoneEmpty(val):
    if (val is None) or (val == ""):
        return None
    else:
        return val
    
def clean_company_name(name):
    return re.sub(r'\b(PT|Tbk)\b', '', name).lower().strip()

for row_id, row in c_df.iterrows():
    q = minerba_df[minerba_df['nama_usaha'] == clean_company_name(row['name'])]
    
    for c, m in zip(c_cols, m_cols):
        
        op = checkNoneEmpty(row[c])
        
        if (op is None) and (not q.empty):

            col_id = c_df.columns.get_loc(c)
            
            original_value = op
            new_value = q[m].iloc[0]

            to_use_value = new_value

            c_sheet.update_cell(2 + row_id, col_id + 1, to_use_value)

            print("Updating row number, col number, col name, value:", row_id + 2, col_id, c, to_use_value)


# # %%


# %%
