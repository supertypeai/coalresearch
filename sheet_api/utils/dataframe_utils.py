import pandas as pd
import json

def convertColType(df:pd.DataFrame, col:str, c_type:type) -> pd.Series:
    return df[col].replace('', pd.NA).astype(c_type)

def forceConvert(df:pd.DataFrame, col:str, c_type:type) -> pd.Series:
    for row_id, row in enumerate(df[col]):
        try:
            _ = pd.Series([row]).astype(c_type)
        except (TypeError, ValueError):
            df.at[row_id, col] = pd.NA
    return convertColType(df, col, c_type)

def castTypes(df:pd.DataFrame, types:dict) -> pd.DataFrame:
    for col, c_type in types.items():
        try:
            df[col] = convertColType(df, col, c_type)
        except (TypeError, ValueError):
            df[col] = forceConvert(df, col, c_type)
    return df

type_map = {
    'IntegerField': 'Int64',
    'FloatField': 'Float64',
    'DecimalField': 'object',
    'CharField': 'string',
    'TextField': 'string',
    'BooleanField': 'boolean',
    'ForeignKeyField': 'Int64',
}

def mapPeeweeToPandasFields(pw_field_types:dict) -> dict:
    df_field_types = {}
    for field, type in pw_field_types.items():
        dtype = type_map.get(type, 'string')
        df_field_types[field] = dtype
    return df_field_types

def safeCast(val, dtype):
    if (val == pd.isna) or (val == "") or (val == None):
        return None
    else:
        if isinstance(val, float) and val.is_integer():
            return int(val)
        
        if dtype == dict:
            return json.loads(val)
        
        return dtype(val)