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

def getEntrySheet(sheet_name:str = "Sheet2", sheet_range:str = "A1:Z50"):

    sheet = client.open_by_key(entry_spreadsheet_id).worksheet(sheet_name)
    data = sheet.get(sheet_range)

    company_entries = sheet.batch_get(["B1:B6", "E1:E5", "H3:H4"])
    company_fields = ["name", "idx_ticker", "website", 
                     "phone_number", "email", "representative_address", 
                     "*parent_company_name", "*percentage_ownership",
                     "company_type", "key_operation", "activities", 
                     "operation_province", "operation_kabkot"]
    
    company_data = mapEntries(company_entries, company_fields)


    performance_entries = sheet.batch_get(["B1:B1", "E6:E6", "H1:H2", "H12:H17"])
    performance_fields = ["*company_name", "year", "commodity_type", "commodity_sub_type",
                          "production_volume", "sales_volume", "overburden_removal_volume",
                          "strip_ratio", "*total_resource", "*total_reserve"]

    performance_data = mapEntries(performance_entries, performance_fields)


    mining_entries = sheet.batch_get(["M12:Y17"])[0]
    mining_fields = ["name", "operation_province", "operation_kabkot", "total_resource",
                     "resources_inferred", "resources_indicated", "resources_measured",
                     "total_reserve", "reserves_proved", "reserves_probable", "production_volume",
                     "overburden_removal_volume", "strip_ratio"]
    company_name = sheet.get("B1:B1")[0][0]
    year = sheet.get("E6:E6")[0][0]

    mining_data_list = []
    for me in mining_entries:
        mining_table_data = {field: entry for field, entry in zip(mining_fields, me)}
        mining_table_data["*company_name"] = company_name
        mining_table_data["year"] = year
        mining_data_list.append(mining_table_data)

    return sheet, company_data, performance_data, mining_data_list