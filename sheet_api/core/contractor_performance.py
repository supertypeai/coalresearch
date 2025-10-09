# check fleet -> to json key: company, year
# check fleet_detail => to json key: company, year
# check ageement -> to json key: company, year
# final format:

import pandas as pd
import gspread
import json
import numpy as np

from sheet_api.google_sheets.client  import getSheetAll 
from gspread.utils import rowcol_to_a1
from sheet_api.google_sheets.auth import createClient 

def updateGSheetCol(
        sheet: gspread.Worksheet, 
        target_col: str,
        df: pd.DataFrame
    ) -> None:
    try:
        headers = sheet.row_values(1)
        if target_col not in headers:
            raise ValueError(f"Column '{target_col}' not found in the Google Sheet.")
        col_index = headers.index(target_col) + 1

        if target_col not in df.columns:
            raise ValueError(f"Column '{target_col}' not found in the DataFrame.")
        
        updated_data = [[val] for val in df[target_col].tolist()]

        start_cell = rowcol_to_a1(2, col_index)
        end_cell = rowcol_to_a1(len(df) + 1, col_index)
        update_range = f'{start_cell}:{end_cell}'

        sheet.update(updated_data, update_range)
        print(f"Column '{target_col}' updated successfully in the sheet.")
        
    except gspread.exceptions.APIError as e:
        print(f"An API error occurred: {e}")
    except ValueError as e:
        print(f"A value error occurred: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def syncCompanyId(
        c_df: pd.DataFrame,
        target_df: pd.DataFrame,
        target_id: str = 'company_id',
        target_name: str = 'company_name'
    ) -> pd.DataFrame:
    
    name_to_id = dict(zip(c_df['name'], c_df['id']))
    target_df[target_id] = target_df[target_name].apply(lambda x: name_to_id.get(x))

    return target_df

def renderService(
        service_df: pd.DataFrame,
        company_id: str,
        year: str,   
    ):

    included_columns = ['service', 'tag', 'description']
    filtered_service_df = service_df[
        (service_df['company_id'] == company_id) & 
        (service_df['year'] == year)
    ]
    filtered_service_df = filtered_service_df[included_columns]

    return filtered_service_df.to_dict(orient='records')

def renderFleet(
        fleet_df: pd.DataFrame,
        company_id: str,
        year: str,   
    ):

    included_columns = ['fleet_type', 'numbers (unit)']
    filtered_fleet_df = fleet_df[
        (fleet_df['company_id'] == company_id) & 
        (fleet_df['year'] == year)
    ]
    filtered_fleet_df = filtered_fleet_df[included_columns]

    columns_to_convert = [col for col in filtered_fleet_df.columns if col != 'fleet_type']
    filtered_fleet_df[columns_to_convert] = filtered_fleet_df[columns_to_convert].apply(pd.to_numeric, errors='coerce').astype('Int64')

    return filtered_fleet_df.to_dict(orient='records')

def renderFleetDetail(
        fleet_detail_df: pd.DataFrame,
        company_id: str,
        year: str,   
    ):

    fleet_type_specs = {
        'General': {
            'unit_name': str
            },
        'Tug': {
            'unit_name': str,
            'engine (HP)': 'Int64',
            'number_of_engine': 'Int64'
            },
        'Barge': {
            'unit_name': str, 
            'size (ft)': 'Int64',
            'capacity (Mt)': 'float64'
            },
        'Mother Vessel': {
            'unit_name': str, 
            'capacity (Mt)': 'float64', 
            'year_of_build': 'Int64'
        },
        'Floating Crane': {
            'unit_name': str, 
            'loading_rate (Mt/day)': 'Int64'
        }
    }

    type_map = {
        'Barge': 'Barge',
        'Tugboat': 'Tug',
        'Assist Tug': 'Tug',
        'Mother Vessel': 'Mother Vessel',
        'Oil Barge': 'Barge',
        'Pusher Tug': 'Tug',
        'Pusher Barge': 'Barge',
        'Floating Crane': 'Floating Crane'
    }

    filtered_fleet_detail_df = fleet_detail_df[
        (fleet_detail_df['company_id'] == company_id) & 
        (fleet_detail_df['year'] == year)
    ]

    fleet_details = {}
    for fleet_type, fleet_type_df in filtered_fleet_detail_df.groupby('fleet_type'):
        available_type = type_map.get(str(fleet_type), 'General')

        col_type = fleet_type_specs[available_type]
        included_columns = [col for col, _ in col_type.items()]
        filtered_fleet_type_df = fleet_type_df[included_columns]

        for col, dtype in col_type.items():
            if dtype in ['float64', 'Int64']:
                filtered_fleet_type_df[col] = filtered_fleet_type_df[col].apply(pd.to_numeric, errors='coerce').astype(dtype).replace({np.nan: None})

        fleet_details[fleet_type] = filtered_fleet_type_df.to_dict(orient='records')

    return fleet_details

def renderAgreement(
        agrement_df: pd.DataFrame,
        company_id: str,
        year: str,   
    ):

    included_columns = ['client_id', 'client_name', 'start_date', 'end_date', 'agreement_detail']
    filtered_agrement_df = agrement_df[
        (agrement_df['company_id'] == company_id) & 
        (agrement_df['year'] == year)
    ]

    filtered_agrement_df = filtered_agrement_df[included_columns]
    filtered_agrement_df['client_id'] = filtered_agrement_df['client_id'].apply(pd.to_numeric, errors='coerce').astype('Int64')

    return filtered_agrement_df.to_dict(orient='records')

def updateSheet(df_new: pd.DataFrame):
    new_sheet_name = "contractor_performance"
    client, spreadsheet_id = createClient()    
    spreadsheet = client.open_by_key(spreadsheet_id)

    try:
        worksheet = spreadsheet.worksheet(new_sheet_name)
        print(f"Sheet '{new_sheet_name}' already exists. Clearing and writing new data.")
        worksheet.clear()
    except gspread.WorksheetNotFound:
        print(f"Creating new sheet: '{new_sheet_name}'...")
        worksheet = spreadsheet.add_worksheet(title=new_sheet_name, rows=len(df_new) + 1, cols=len(df_new.columns))

    worksheet.update(range_name='A1', values=[df_new.columns.values.tolist()] + df_new.values.tolist())
    print(f"Successfully wrote {len(df_new)} rows to '{new_sheet_name}' with performance_id column.")

def main(refresh_company_id: bool = False):
    service_sht, service_df = getSheetAll("service")
    fleet_sht, fleet_df = getSheetAll("fleet")
    fleet_detail_sht, fleet_detail_df = getSheetAll("fleet_detail")
    agreement_sht, agreement_df = getSheetAll("agreement")
    _, c_df = getSheetAll("company")

    if refresh_company_id:
        for sheet, df, flag in [
            (service_sht, service_df, 0),
            (fleet_sht, fleet_df, 0),
            (fleet_detail_sht, fleet_detail_df, 0),
            (agreement_sht, agreement_df, 1)
        ]:
            df = syncCompanyId(c_df, df)
            updateGSheetCol(sheet, 'company_id', df)

            if flag == 1:
                df = syncCompanyId(c_df, df, target_id='client_id', target_name='client_name')
                updateGSheetCol(sheet, 'client_id', df)

    contractors = {}
    year = 2024
    contractors[year] = {}
    
    for df, render_function, df_name in [
        (service_df, renderService, 'service'),
        (fleet_df, renderFleet, 'fleet'),
        (fleet_detail_df, renderFleetDetail, 'fleet_detail'),
        (agreement_df, renderAgreement, 'agreement_detail'),

    ]:
        for company_id, company_id_df in df.groupby('company_id'):
            if company_id:
                if not company_id in contractors[year]:
                    contractors[year][company_id] = {
                        'company_name': company_id_df.iloc[0]['company_name'],
                        'stats': {}
                    }
                
                contractors[year][company_id]['stats'][df_name] = render_function(company_id_df, str(company_id), str(year))
       
    contractor_performance_list = [
        {
            'company_id': int(company_id),
            'company_name': contractors[year][company_id]['company_name'],
            'year': int(year),
            'stats': json.dumps(contractors[year][company_id]['stats'])
        }
        for year in contractors
        for company_id in contractors[year]
    ]
    
    cp_df = pd.DataFrame(contractor_performance_list)
    updateSheet(cp_df)
    
if __name__ == '__main__':
    main()