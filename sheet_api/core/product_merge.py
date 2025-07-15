import json

from sheet_api.google_sheets.client import getSheetAll
from sheet_api.core.toolbox import safeCast
from gspread import Cell



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

SPECS_MAP = {
	'Coal': coal_specs,
	'Gold': gold_specs
}

SHEET_MAP = {
	'Coal': 'coal_performance',
	'Gold': 'gold_performance',
	'Nickel': 'nickel_performance'
}

def updateProduct(commodity: str, starts_from=0):
	cell_updates = []
	commodity_sheet, commodity_df = getSheetAll(SHEET_MAP[commodity])

	for ccp_idx, ccp_row in commodity_df.iterrows():		
		q = cp_df[
            (cp_df['company_id'] == ccp_row['company_id']) &
			(cp_df['commodity_type'] == commodity) &
            (cp_df['year'] == ccp_row['year'])
        ]
		
		if q.empty:
            # Filter only by company_id

			product_q = cp_df[
				(cp_df['company_id'] == ccp_row['company_id']) &
				(cp_df['commodity_type'] == commodity)
			]
			
			if product_q.empty:
				continue

            # Get the latest available year
			latest_year = product_q['year'].max()
			q = product_q[product_q['year'] == latest_year]

		assert isinstance(ccp_idx, int)
		sheet_row = 2 + ccp_idx

		if sheet_row < starts_from:
			continue

		product_list = []
		for _, group_row in q.iterrows():
			product_dict = {}
			for in_col, type in SPECS_MAP[commodity]:
				val = safeCast(group_row[in_col], type)
				product_dict[in_col] = val
			product_list.append(product_dict)
			
		product_list_json = json.dumps(product_list)
		col_id = list(commodity_df.columns).index('product') + 1

		# Prepare Cell object
		cell = Cell(row=sheet_row, col=col_id, value=product_list_json)
		cell_updates.append(cell)
			
    # Perform batch update
	if cell_updates:
		commodity_sheet.update_cells(cell_updates)
		print(f"Batch updated {len(cell_updates)} cells.")

if __name__ == '__main__':
	updateProduct('Gold')