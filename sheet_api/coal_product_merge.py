# %%
import pandas as pd
import json
from google_sheets.auth import createClient
from sheet_api.utils.dataframe_utils import safeCast
from gspread import Cell

client, spreadsheet_id = createClient()

# %%
cp_sheet = client.open_by_key(spreadsheet_id).worksheet('coal_product')
cp_data = cp_sheet.get('A1:O56')
cp_df = pd.DataFrame(cp_data[1:], columns=cp_data[0])
cp_df.columns = [col.lstrip("*") for col in cp_df.columns]

ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('company_performance')
ccp_data = ccp_sheet.get('A1:X244')
ccp_df = pd.DataFrame(ccp_data[1:], columns=ccp_data[0])
# %%

included_columns = [
	("product_name", str),
	("calorific_value", str),
	("total_moisture", str),
	("ash_content_arb", str),
	("total_sulphur_arb", str),
	("ash_content_adb", str),
	("total_sulphur_adb", str),
	("volatile_matter_adb", str),
	("fixed_carbon_adb", str)
]

# %%

def updateProduct(starts_from=0):
	DEFAULT_YEAR = '2023'
	cell_updates = []

	for ccp_idx, ccp_row in ccp_df.iterrows():
		
		if ccp_row['commodity_type'] != 'Coal':
			continue                        

		q = cp_df[
            (cp_df['company_id'] == ccp_row['company_id']) &
            (cp_df['year'] == ccp_row['year'])
		]

		if q.empty:
			q = cp_df[
				(cp_df['company_id'] == ccp_row['company_id']) &
				(cp_df['year'] == DEFAULT_YEAR)
			]

		if q.empty:
			continue

		assert isinstance(ccp_idx, int)
		sheet_row = 2 + ccp_idx

		if sheet_row < starts_from:
			continue
        
		coal_product_list = []
		for _, group_row in q.iterrows():
			product_dict = {}
			for in_col, type in included_columns:
				val = safeCast(group_row[in_col], type)
				product_dict[in_col] = val
			coal_product_list.append(product_dict)
			
		coal_product_list_json = json.dumps(coal_product_list)
		col_id = list(ccp_df.columns).index('*product') + 1  # 1-based indexing

		# Prepare Cell object
		cell = Cell(row=sheet_row, col=col_id, value=coal_product_list_json)
		cell_updates.append(cell)
			
    # Perform batch update
	if cell_updates:
		ccp_sheet.update_cells(cell_updates)
		print(f"Batch updated {len(cell_updates)} cells.")

# %%
updateProduct()
# %%
