from sheet_api.google_sheets.auth import createClient
import pandas as pd

client, spreadsheet_id = createClient()

def getSheet(sheet_name:str, sheet_range:str):

    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get(sheet_range)
    df = pd.DataFrame(data[1:], columns=data[0])

    return sheet, df