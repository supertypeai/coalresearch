from tabulate import tabulate
import pandas as pd
from typing import Callable

def deleteID(model, id: int) -> None:
    q = model.get_by_id(id)
    q.delete_instance()
    print(f"ID {id} has been deleted from {model.__name__} table")

def checkDeletedAndOrder(model, df, key='id', execute=False) -> bool:
    db_ids = list(model.select(getattr(model, key)).order_by(getattr(model, key)).scalars())
    df_ids = [int(x) for x in df[key] if str(x).strip().isdigit()]
    df_ids = df_ids[:len(db_ids)]
    deleted_ids = set(db_ids) - set(df_ids)
    change_exists = bool(deleted_ids)

    if deleted_ids:
        if execute:
            for di in deleted_ids:
                deleteID(model, di)
        else:
            print(f"Deleted rows from Sheet for model {model.__name__}: {deleted_ids}")
    
    if db_ids != df_ids:
        print(f"ID order mismatch in model {model.__name__}")
        print(db_ids)
        print(df_ids)

    return change_exists

def compareDBSheet(model, df, execute=False) -> bool:
    diff_exist = False
    for model_row, (_, row) in zip(model.select(), df.iterrows()):
        diff = []
        for field in model._meta.fields:
            db_val = getattr(model_row, field)
            if hasattr(db_val, 'id'):
                db_val = db_val.id
            sheet_val = row.get(field)
            sheet_val = None if pd.isna(sheet_val) else sheet_val
            if db_val != sheet_val:
                diff.append((field, db_val, sheet_val))
        
        if diff:
            diff_exist = True
            if execute:
                for field_name, _, new_val in diff:
                    setattr(model_row, field_name, new_val)
                model_row.save()
            else:
                c_name = row.get('name', row.get('*company_name'))
                print(f"Different value at ID {row['id']} {c_name}:\n{tabulate(diff, headers=['Field', 'DB Value', 'Sheet Value'], tablefmt='grid')}")
    return diff_exist

def checkNewData(model, df, field_types: dict, execute=False) -> bool:
    found_new = False
    for _, row in df.iterrows():
        rowid = row.get('id', None)
        rowid = None if pd.isna(rowid) else rowid
        if model.get_or_none(model.id == rowid) is None:
            found_new = True
            if execute:
                inputs = {ft: None if row[ft] is pd.NA else row[ft] for ft in field_types if ft != 'id'}
                model(**inputs).save()
            else:
                print(f"New data to add: {row[[ft for ft in field_types]].to_dict()}")
    return found_new

def confirmChange(func: Callable, model, df, *args, **kwargs) -> None:
    if func(model, df, *args, **kwargs):
        if input("Apply changes? [Y/N]") == "Y":
            func(model, df, *args, execute=True, **kwargs)

def replaceCO(co_model, c_model, df) -> None:

    def safeCast(val, tp=int):
        if (pd.isna(val) or val == ""):
            return None
        else:
            return tp(val)

    co_model.delete().execute()

    print("All Company Ownership records have been deleted")

    for _, row in df.iterrows():

        parent = c_model.get_or_none(c_model.name == safeCast(row['*parent_company_name'], str))
        company = c_model.get_or_none(c_model.name == safeCast(row['name'], str))
        ownership = safeCast(row['*percentage_ownership'], float)

        if parent and company and ownership:

            co_model.insert(
                parent_company_id=parent.id,
                company_id=company.id,
                percentage_ownership=ownership
            ).execute()

            print(f"Inserted parent_id: {parent.id}, company_id: {company.id}")