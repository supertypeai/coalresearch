import json
import pandas as pd

from sheet_api.google_sheets.client import getSheetAll
from sheet_api.core.toolbox import safeCast
from gspread import Cell
from typing import Optional

cp_sheet, cp_df = getSheetAll("product")
cp_df.columns = [col.lstrip("*") for col in cp_df.columns]

coal_specs = [
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

gold_specs = [
	("product_name", str),
	("g/ton Au", str),
]

copper_specs = [
	("product_name", str),
	("% Cu", str)
]

nickel_specs = [
	("product_name", str),
	("% Ni", str),
	("% Co", str),
	("% Fe", str),
	("% SiO₂", str),
	("% MgO", str),
	("% Al₂O₃", str)
]

SPECS_MAP = {
	'Coal': coal_specs,
	'Gold': gold_specs,
	'Nickel': nickel_specs,
	'Copper': copper_specs
}

SHEET_MAP = {
	'Coal': 'coal_performance',
	'Gold': 'gold_performance',
	'Nickel': 'nickel_performance',
	'Copper': 'copper_performance'
}

def getQ(ccp_row: pd.Series, commodity_type_mask: pd.Series, key: str = 'company_id') -> Optional[pd.DataFrame]:
	q = cp_df[
		(ccp_row['company_id'] == cp_df[key]) &
		commodity_type_mask &
		(ccp_row['year'] == cp_df['year'])
	]

	if q.empty:
		# Filter only by company_id
		product_q = cp_df[
			(ccp_row['company_id'] == cp_df[key]) &
			commodity_type_mask
		]
		
		if product_q.empty:
			return None

		# Get the latest available year
		latest_year = product_q['year'].max()
		q = product_q[product_q['year'] == latest_year]

	return q

def updateProduct(commodity: str, commodity_sub_type: bool = False, starts_from = 0 ):
	cell_updates = []
	commodity_sheet, commodity_df = getSheetAll(SHEET_MAP[commodity])
	col_id = list(commodity_df.columns).index('product') + 1
		
	for ccp_idx, ccp_row in commodity_df.iterrows():		
		assert isinstance(ccp_idx, int)
		sheet_row = 2 + ccp_idx

		if sheet_row < starts_from:
			continue

		commodity_type_mask = (cp_df['commodity_type'] == ccp_row['commodity_type'])
		if commodity_sub_type:
			commodity_type_mask = (
				(cp_df['commodity_type'] == ccp_row['commodity_type']) &
				(cp_df['commodity_sub_type'] == ccp_row['commodity_sub_type']) 
				)

		company_product_list = []
		for key in ['company_id', 'direct_parent_id']:
			q = getQ(ccp_row, commodity_type_mask, key=key)
			if q is not None:
				company_product_list.append(q)

		if not company_product_list:
			cell = Cell(row=sheet_row, col=col_id, value='')
			cell_updates.append(cell)
			continue

		company_product_df = pd.concat(company_product_list, axis=0, ignore_index=True)

		product_list = []
		for _, group_row in company_product_df.iterrows():
			product_dict = {}
			for in_col, type in SPECS_MAP[commodity]:
				val = safeCast(group_row[in_col], type)
				product_dict[in_col] = val
			product_list.append(product_dict)
			
		product_list_json = json.dumps(product_list)

		cell = Cell(row=sheet_row, col=col_id, value=product_list_json)
		cell_updates.append(cell)
			
    # Perform batch update
	if cell_updates:
		commodity_sheet.update_cells(cell_updates)
		print(f"Batch updated {len(cell_updates)} cells.")

if __name__ == '__main__':
	# for commodity in ['Coal', 'Gold', 'Copper']:
	# 	updateProduct(commodity)
	# for commodity in ['Nickel']:
	# 	updateProduct(commodity, commodity_sub_type=True)
	updateProduct('Coal')