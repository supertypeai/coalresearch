# %%
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import re

# %%

SERVICE_ACCOUNT_FILE = 'keys/supertype-insider-d4710ac3561a.json'

# Define scope
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
spreadsheet_id = "19wfJ2fc9qKeR22dMIO2rEQLkit8E4bGsHA1u0USqTQk"

ms_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')

used_rows = (32, 38)

ms_data = ms_sheet.get('A1:W74')
ms_df = pd.DataFrame(ms_data[1:], columns=ms_data[0])

esdm_coal_df = pd.read_csv("coal_db - ESDM_coal.csv")
esdm_coal_df['mineral_grade'] = esdm_coal_df['mineral_grade'].apply(lambda x: re.search(r"\(([^)]+)\)", x).group(1))
esdm_coal_df['object_name_strip'] = esdm_coal_df['object_name'].str.replace(r'\b[B]atubara\b', '', regex=True).str.strip()

# %%

# esdm_to_merge_column = ["year_measured", "mineral_grade", "inferred_resource", "indicated_resource", "measured_resource", "total_resource", "probable_reserve", "proven_reserve", "total_reserve"]
# ms_to_merge_column = ["*year_measured", "calorific_value", "*resources_inferred", "*resources_indicated", "*resources_measured", "total_resource", "*reserves_probable", "*reserves_proved", "total_reserve"]

esdm_to_merge_column = ["city"]
ms_to_merge_column = ["city"]


def checkObjectName(val):
    q = esdm_coal_df[esdm_coal_df['object_name'] == val]
    if not q.empty:
        return q
    else:
        return esdm_coal_df[esdm_coal_df['object_name_strip'] == val]

for row_id, row in ms_df.iloc[used_rows[0]:used_rows[1] + 1, :].iterrows():
    
    q = checkObjectName(row['name'])

    if not q.empty:

        for m, e in zip(ms_to_merge_column, esdm_to_merge_column):
            col_id = ms_df.columns.get_loc(m)
            
            original_value = row[m]
            new_value = str(q[e].iloc[0])

            to_use_value = original_value

            ms_sheet.update_cell(row_id, col_id + 1, to_use_value)

            print("Updating row number, col number, value:", row_id, col_id, to_use_value)
    
# %%
