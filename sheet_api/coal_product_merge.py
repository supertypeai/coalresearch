# %%
import pandas as pd
import json
from google_sheets.auth import createClient

client, spreadsheet_id = createClient()

# %%
cp_sheet = client.open_by_key(spreadsheet_id).worksheet('coal_product')
cp_data = cp_sheet.get('A1:M56')
cp_df = pd.DataFrame(cp_data[1:], columns=cp_data[0])
cp_df.columns = [col.lstrip("*") for col in cp_df.columns]

ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('company_performance')
ccp_data = ccp_sheet.get('A1:X134')
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

def safeCast(val, type):
	if (val == pd.isna) or (val == "") or (val == None):
		return None
	else:
		return type(val)

def updateProduct(sheet, starts_from=0):
	for (company_id, year), group_df in cp_df.groupby(['company_id', 'year']):
		print(company_id, year)

		q = ccp_df[(ccp_df['company_id'] == company_id) &
				(ccp_df['year'] == year) &
				(ccp_df['commodity_type'] == 'Coal')]
		
		if not q.empty:
			sheet_row = 2 + q.index[0]

			if sheet_row < starts_from:
				continue

			coal_product_list = []
			for group_row_id, group_row in group_df.iterrows():
				
				product_dict = {}
				for in_col, type in included_columns:
					
					val = safeCast(group_row[in_col], type)
					product_dict[in_col] = val
				
				coal_product_list.append(product_dict)

			coal_product_list = json.dumps(coal_product_list)

			col_id = ccp_df.columns.get_loc('product')
			sheet.update_cell(sheet_row, col_id + 1, coal_product_list)
	
			print(f"Updated row {sheet_row}: {coal_product_list}")

# %%
updateProduct(ccp_sheet)

# %%
