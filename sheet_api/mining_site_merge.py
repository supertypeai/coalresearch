# %%
import pandas as pd
import re
from sheet_api.google_sheets.auth import createClient

# %%
client, spreadsheet_id = createClient()
ms_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')

ms_data = ms_sheet.get('A1:W74')
ms_df = pd.DataFrame(ms_data[1:], columns=ms_data[0])

esdm_coal_df = pd.read_csv("coal_db - ESDM_coal.csv")
esdm_coal_df['mineral_grade'] = esdm_coal_df['mineral_grade'].apply(lambda x: re.search(r"\(([^)]+)\)", x).group(1))
esdm_coal_df['object_name_strip'] = esdm_coal_df['object_name'].str.replace(r'\b[B]atubara\b', '', regex=True).str.strip()

# %%

# esdm_to_merge_column = ["year_measured", "mineral_grade", "inferred_resource", "indicated_resource", "measured_resource", "total_resource", "probable_reserve", "proven_reserve", "total_reserve"]
# ms_to_merge_column = ["*year_measured", "calorific_value", "*resources_inferred", "*resources_indicated", "*resources_measured", "total_resource", "*reserves_probable", "*reserves_proved", "total_reserve"]

esdm_to_merge_column = ["latitude", "longitude"]
ms_to_merge_column = ["*latitude", "*longitude"]


def checkObjectName(val):
    q = esdm_coal_df[esdm_coal_df['object_name'] == val]
    if not q.empty:
        return q
    else:
        return esdm_coal_df[esdm_coal_df['object_name_strip'] == val]


def updateSheet(starts_from=0):

    for row_id, row in ms_df.iterrows():

        if (row_id + 2) < starts_from:
            continue

        q = checkObjectName(row['name'])

        if not q.empty:

            for m, e in zip(ms_to_merge_column, esdm_to_merge_column):
                col_id = ms_df.columns.get_loc(m)
                
                original_value = row[m]
                new_value = str(q[e].iloc[0])

                to_use_value = new_value

                ms_sheet.update_cell(2 + row_id, col_id + 1, to_use_value)

                print("Updating row number, col number, value:", row_id, col_id, to_use_value)
        
# %%
updateSheet(starts_from=71)

# %%
