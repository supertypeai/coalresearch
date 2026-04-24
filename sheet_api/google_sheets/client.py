import pandas as pd
from .auth import createClient, createEntryClient

client, spreadsheet_id = createClient()
entry_client, entry_spreadsheet_id = createEntryClient()

def getSheet(sheet_name:str, sheet_range:str):

    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get(sheet_range)
    df = pd.DataFrame(data[1:], columns=data[0])

    return sheet, df

def getSheetAll(sheet_name: str):
    
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])

    return sheet, df

def mapEntries(entries, fields):
    table_entry = [entry[0] if len(entry) > 0 else None for batch in entries for entry in batch]
    table_data = {field: entry for field, entry in zip(fields, table_entry)}

    return table_data