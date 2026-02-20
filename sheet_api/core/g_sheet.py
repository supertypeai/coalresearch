from typing import List
from sheet_api.google_sheets.auth import createClient, createService

_, SPREADSHEET_ID = createClient()
SERVICE = createService()

def batch_update(
    rows: List, 
    sheet_id: int, 
    starts_from: int, 
    length: int, 
    col_id: int
) -> None:
    
    requests = [
        {
            'updateCells': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': starts_from + 1,
                    'endRowIndex': length + 1,
                    'startColumnIndex': col_id,
                    'endColumnIndex': col_id + 1
                },
                'rows': rows,
                'fields': 'userEnteredValue'
            }
        }
    ]

    response = SERVICE.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={'requests': requests}
    ).execute()
    print(f"Batch update response: {response}")