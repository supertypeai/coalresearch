import pandas as pd
from sheet_api.db.models import Company
from sheet_api.core.sync import sync_model
from sheet_api.core.compile_to_json import fillMiningLicense, fillMiningContract
from sheet_api.utils.slug_utils import generate_slug

def companyPreprocess(df: pd.DataFrame, field_types: dict, sheet):
    
    field_types["phone_number"] = "string"

    print("Filling out company's mining_license...")
    df = fillMiningLicense(df, sheet.id)

    print("Filling out company's mining_contracts...")
    df = fillMiningContract(df, sheet.id)

    print("Generating slugs from company names...")
    df["slug"] = df["name"].apply(generate_slug)

    import json
    for i in range(len(df)):
        mining_license = df.iloc[i]["mining_license"]
        if mining_license:
            print(json.loads(mining_license))
            break

    return df, field_types, sheet

def sync_company():
    sync_model("company", Company, preprocess=companyPreprocess)
