import pandas as pd
import sqlite3

from sheet_api.google_sheets.client import getSheetAll

def query(sql: str):
    """Execute SQL on SQLite and return DataFrame."""
    with sqlite3.connect('db.sqlite') as conn:
        return pd.read_sql_query(sql, conn)

def setTickerOn(sheet_name):
    cp_sheet, cp_df = getSheetAll(sheet_name)
    cp_df.loc[:, 'company_id'] = cp_df['company_id'].astype(int)

    if '*company_group' not in cp_df.columns:
        print('*company_group column is not available yet, please create that column first')
        return

    sql = """
    SELECT
        c.id,
        COALESCE(parent3.name, parent2.name, parent.name, c.name) AS company_group, 
        COALESCE(parent3.idx_ticker, parent2.idx_ticker, parent.idx_ticker, c.idx_ticker) AS company_ticker
    FROM company c
    LEFT JOIN company_ownership co ON co.company_id = c.id
    LEFT JOIN company parent ON parent.id = co.parent_company_id 
    LEFT JOIN company_ownership co2 ON co2.company_id= co.parent_company_id
    LEFT JOIN company parent2 ON parent2.id = co2.parent_company_id
    LEFT JOIN company_ownership co3 ON co3.company_id = co2.parent_company_id
    LEFT JOIN company parent3 ON parent3.id = co3.parent_company_id;
    """
    company_group_df = query(sql)

    # Merge with sheet data
    cp_df = cp_df.merge(company_group_df[['id', 'company_group', 'company_ticker']], 
                      left_on='company_id', right_on='id', how='left')
    cp_df['*company_group'] = cp_df['company_ticker'].fillna(cp_df['company_group'])
    cp_df.drop(columns='company_group', inplace=True)

    # Update only the group_ticker column in Google Sheets
    ticker_col_index = cp_df.columns.get_loc('*company_group')
    assert isinstance(ticker_col_index, int)

    group_ticker_values = cp_df['*company_group'].tolist()

    # Prepare 2D list for gspread update
    group_ticker_values_2d = [[val] for val in group_ticker_values]

    # Update column without clearing the sheet
    cp_sheet.update(
        range_name=f"{chr(65 + ticker_col_index)}2:{chr(65 + ticker_col_index)}{len(group_ticker_values)+1}",
        values=group_ticker_values_2d
    )
    print(f"Updated {len(group_ticker_values)} rows in '*company_group' column")

if __name__ == '__main__':
    available_commodity = (
        'coal_performance',
        'copper_performance',
        'gold_performance',
        'nickel_performance',
        'silver_performance'
    )
    comm_to_use = 0
    
    setTickerOn(available_commodity[comm_to_use])