# %%
import pandas as pd
from peewee import SqliteDatabase, fn
import peewee as pw
from typing import Callable
from db import Company, CoalCompanyPerformance, MiningSite, CompanyOwnership
from google_sheet_auth import createClient
from tabulate import tabulate

# %%

client, spreadsheet_id = createClient()
db = SqliteDatabase('coal_db.sqlite')

# %%

def convertColType(df:pd.DataFrame, col:str, c_type:type):
    return df[col].replace('', pd.NA).astype(c_type)

def forceConvert(df:pd.DataFrame, col:str, c_type:type):
    for row_id, row in enumerate(df[col]):
        try:
            _ = pd.Series([row]).astype(c_type)
        except (TypeError, ValueError):
            df.at[row_id, col] = pd.NA
    
    return convertColType(df, col, c_type)

def castTypes(df:pd.DataFrame, types:dict):

    for col, c_type in types.items():
        try:
            df[col] = convertColType(df, col, c_type)
        except (TypeError, ValueError):
            df[col] = forceConvert(df, col, c_type)

    return df

type_map = {
    pw.IntegerField: 'Int64',
    pw.FloatField: 'Float64',
    pw.CharField: 'string',
    pw.TextField: 'string',
    pw.BooleanField: 'boolean',
    pw.ForeignKeyField: 'Int64',
}

def getFieldTypes(model:pw.ModelBase):
    field_types = {}
    for field in model._meta.sorted_fields:
        dtype = type_map.get(type(field), 'string')
        field_types[field.name] = dtype
    
    return field_types

# %%

c_sheet = client.open_by_key(spreadsheet_id).worksheet('company')

c_data = c_sheet.get('A1:R251')
c_df = pd.DataFrame(c_data[1:], columns=c_data[0])
c_types = getFieldTypes(Company)
c_types['phone_number'] = 'string'
c_df = castTypes(c_df, c_types)

# %%
def checkDeletedAndOrder(model: pw.ModelBase, df: pd.DataFrame, key='id') -> None:
    db_ids = list(
        model.select(getattr(model, key))
        .order_by(getattr(model, key))
        .scalars()
        )
    df_ids = [int(x) for x in df[key] if str(x).strip().isdigit()]
    df_ids = df_ids[:len(db_ids)]

    mn = model.__name__

    # Check deleted IDs
    deleted_ids = set(db_ids) - set(df_ids)
    if deleted_ids:
        print(f"Deleted rows from Sheet for model {mn}:")
        print(deleted_ids)

    # Check ID order
    if db_ids != df_ids:
        print(db_ids)
        print(df_ids)
        print(f"ID order mismatch in model {mn}")

checkDeletedAndOrder(Company, c_df)

# %%

def compareDBSheet(model:pw.ModelBase, df:pd.DataFrame, execute=False) -> bool:

    diff_exist = False

    for model_row, (row_idx, row) in zip(model.select(), df.iterrows()):

        diff = []
        for field in model._meta.fields:
            db_val = getattr(model_row, field)

            if isinstance(db_val, pw.Model):
                db_val = db_val.id

            sheet_val = row.get(field)
            sheet_val = None if pd.isna(sheet_val) else sheet_val

            if db_val == sheet_val:
                pass
            else:
                diff.append((field, db_val, sheet_val))

        if diff:
            diff_exist = True

            if execute:
                for field_name, old_val, new_val in diff:
                    setattr(model_row, field_name, new_val)
                    print(f"Updated for ID: {row['id']} {field_name}, {old_val} -> {new_val}")
                model_row.save()

            else:
                table = tabulate(diff, headers=["Field", "DB Value", "Sheet Value"], tablefmt="grid")
                
                c_name = row.get('name')
                if pd.isna(c_name):
                    c_name = row.get('*company_name')

                print("Different value at ID:", row['id'], c_name, '\n', table)

    return diff_exist

def confirmChange(func:Callable, model:pw.ModelBase, df:pd.DataFrame, *args, **kwargs) -> None:
    if func(model, df, *args, **kwargs) and \
        input("Apply changes? [Y/N]") == "Y":
        func(model, df, *args, execute=True)

confirmChange(compareDBSheet, Company, c_df)

# %%

def checkNewData(model:pw.ModelBase, df:pd.DataFrame, field_types:dict, execute=False) -> bool:
    
    found_new = False
    
    for row_idx, row in df.iterrows():

        rowid = row.get('id')
        rowid = None if pd.isna(rowid) else rowid

        q = model.get_or_none(model.id == rowid)
        if q is None:
            found_new = True

            if execute:
                inputs = {}

                for ft in field_types:
                    if ft != 'id':
                        new_val = None if row[ft] is pd.NA else row[ft]
                        inputs[ft] = new_val

                m = model(**inputs)
                m.save()

                print(f"Insert successfull! For {model.__name__} table on ID: {m.id}")

            else:
                row_to_add = row[[ft for ft in field_types]]
                print(f"New data to add: {row_to_add.to_dict()}")

    return found_new

confirmChange(checkNewData, Company, c_df, c_types)

# %%

ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('coal_company_performance')

ccp_data = ccp_sheet.get('A1:Y138')
ccp_df = pd.DataFrame(ccp_data[1:], columns=ccp_data[0])
ccp_df = ccp_df.rename(columns={"total_reserve": "reserve", "total_resource":"resource"})
ccp_types = getFieldTypes(CoalCompanyPerformance)
ccp_df = castTypes(ccp_df, ccp_types)

# %%

checkDeletedAndOrder(CoalCompanyPerformance, ccp_df)

confirmChange(compareDBSheet, CoalCompanyPerformance, ccp_df)

confirmChange(checkNewData, CoalCompanyPerformance, ccp_df, ccp_types)


# %%

ms_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site')

ms_data = ms_sheet.get('A1:W43')
ms_df = pd.DataFrame(ms_data[1:], columns=ms_data[0])
ms_df = ms_df.rename(columns={"total_reserve": "reserve", "total_resource":"resource"})
ms_types = getFieldTypes(MiningSite)
ms_df = castTypes(ms_df, ms_types)

# %%

checkDeletedAndOrder(MiningSite, ms_df)

confirmChange(compareDBSheet, MiningSite, ms_df)

confirmChange(checkNewData, MiningSite, ms_df, ms_types)
# %%