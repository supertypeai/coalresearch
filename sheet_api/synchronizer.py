# %%
import pandas as pd
import peewee as pw
from peewee import SqliteDatabase
from typing import Callable
from sheet_api.db.models import Company, CoalCompanyPerformance, MiningSite, CompanyOwnership
from sheet_api.google_sheets.auth import createClient
from sheet_api.utils.dataframe_utils import castTypes, getFieldTypes
from sheet_api.utils.sync_utils import checkDeletedAndOrder, compareDBSheet, checkNewData, confirmChange, replaceCO
# %%

db = SqliteDatabase('coal_db.sqlite')
client, spreadsheet_id = createClient()

def sync_model(sheet_name:str, range:str, model:pw.ModelBase, preprocess:Callable):
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)
    data = sheet.get(range)
    df = pd.DataFrame(data[1:], columns=data[0])
    field_types = getFieldTypes(model)
    
    if preprocess:
        df, field_types = preprocess(df, field_types)
    
    df = castTypes(df, field_types)

    confirmChange(checkDeletedAndOrder, model, df)
    confirmChange(compareDBSheet, model, df)
    confirmChange(checkNewData, model, df, getFieldTypes(model))

def processCompanyOwnership():
    sheet = client.open_by_key(spreadsheet_id).worksheet('company')
    data = sheet.get('A1:R251')
    df = pd.DataFrame(data[1:], columns=data[0])
    
    if (input("Replace company ownerhip according to the sheet?") == "Y"):
        replaceCO(CompanyOwnership, Company, df)

if __name__ == '__main__':

    def company_preprocess(df:pd.DataFrame, field_types):
        field_types['phone_number'] = 'string'
        return df, field_types

    def rename(df, field_types):
        df = df.rename(columns={"total_reserve": "reserve", "total_resource":"resource"})
        return df, field_types
# %%
    sync_model('company', 'A1:R251', Company, company_preprocess)
# %%
    sync_model('coal_company_performance', 'A1:AB138', CoalCompanyPerformance, rename)
# %%
    sync_model('mining_site', 'A1:W43', MiningSite, rename)
# %%
    processCompanyOwnership()
# %%
