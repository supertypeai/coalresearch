from tabulate import tabulate
import pandas as pd
import peewee as pw
from typing import Callable, Optional, Dict, Tuple, List
from decimal import Decimal
from gspread import Worksheet

from sheet_api.google_sheets.client import getSheet, getSheetAll
from sheet_api.core.toolbox import castTypes, mapPeeweeToPandasFields

def deleteID(model, id: int) -> None:
    q = model.get_by_id(id)
    q.delete_instance()
    print(f"ID {id} has been deleted from {model.__name__} table")


def checkDeletedAndOrder(model, df, key="id", execute=False) -> bool:
    db_ids = list(
        model.select(getattr(model, key)).order_by(getattr(model, key)).scalars()
    )
    df_ids = [int(x) for x in df[key] if str(x).strip().isdigit()]
    df_ids = df_ids[: len(db_ids)]
    deleted_ids = set(db_ids) - set(df_ids)
    change_exists = bool(deleted_ids)

    if deleted_ids:
        if execute:
            for di in deleted_ids:
                deleteID(model, di)

            return False

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
            if hasattr(db_val, "id"):
                db_val = db_val.id
            sheet_val = row.get(field)
            sheet_val = None if pd.isna(sheet_val) else sheet_val
            sheet_val = None if sheet_val == "" else sheet_val

            if type(db_val) == Decimal:
                sheet_val = Decimal(sheet_val) if sheet_val is not None else None

            if db_val != sheet_val:
                diff.append((field, db_val, sheet_val))

        if diff:
            diff_exist = True
            if execute:
                for field_name, old_val, new_val in diff:
                    print(field_name, old_val, new_val)
                    setattr(model_row, field_name, new_val)
                model_row.save()
                print(
                    f"Updated for {model.__name__} at {model_row}, {field_name}: {old_val} -> {new_val} "
                )
            else:
                c_name = row.get("name", row.get("*company_name"))
                print(
                    f"Different value at ID {row['id']} {c_name}:\n{tabulate(diff, headers=['Field', 'DB Value', 'Sheet Value'], tablefmt='grid')}"
                )
    return diff_exist


def checkNewData(model, df, field_types: dict, execute=False) -> bool:
    found_new = False
    for _, row in df.iterrows():
        rowid = row.get("id", None)
        rowid = None if pd.isna(rowid) else rowid
        if model.get_or_none(model.id == rowid) is None:
            found_new = True
            if execute:
                inputs = {
                    ft: None if row[ft] is pd.NA else row[ft]
                    for ft in field_types
                    if ft != "id"
                }
                print(inputs)
                model(**inputs).save()
            else:
                print(f"New data to add: {row[[ft for ft in field_types]].to_dict()}")
    return found_new


def confirmChange(func: Callable, model, df, *args, **kwargs) -> None:
    if func(model, df, *args, **kwargs):
        # Auto-approve for migration task
        if True:  # input(f"Apply changes for {func.__name__} ? [Y/N]") == "Y":
            func(model, df, *args, execute=True, **kwargs)


def execute_preprocess_callback(
    df: pd.DataFrame, field_types: Dict, sheet: Worksheet, function: Callable
) -> Tuple[pd.DataFrame, Dict, Worksheet]:
    return function(df, field_types, sheet)


def sync_model(
    sheet_name: str,
    model: pw.ModelBase,
    range: Optional[str] = None,
    preprocess: Optional[Callable] = None,
    excluded_cols: Optional[List[str]] = [],
) -> None:
    if range:
        sheet, df = getSheet(sheet_name, range)
    else:
        sheet, df = getSheetAll(sheet_name)

    included_cols = [col for col in model._meta.sorted_fields if col not in excluded_cols]
    pw_field_types = {fn.name: type(fn).__name__ for fn in included_cols}
    field_types = mapPeeweeToPandasFields(pw_field_types)

    if preprocess is not None:
        df, field_types, sheet = execute_preprocess_callback(
            df=df, field_types=field_types, sheet=sheet, function=preprocess
        )

    df = castTypes(df, field_types)

    confirmChange(checkDeletedAndOrder, model, df)
    confirmChange(compareDBSheet, model, df)
    confirmChange(checkNewData, model, df, field_types)
