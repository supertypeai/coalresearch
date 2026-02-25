import pandas as pd
import json
from db.models import Company
from sheet_api.core.sync import sync_model
from sheet_api.core.g_sheet import batch_update
from sheet_api.utils.slug_utils import generate_slug
from sheet_api.google_sheets.client   import getSheetAll
from sheet_api.core.toolbox import clean_company_df, fuzzy_match_company_name
from sheet_api.minerba_merge import prepareMinerbaDf


def fill_mining_license(
    df: pd.DataFrame, 
    sheet_id: int, 
    is_debug: bool = False,
    starts_from: int = 0, 
) -> pd.DataFrame:
    
    minerba_df = prepareMinerbaDf()
    df_company = clean_company_df(df, 'name') # This function add two columns: ['name_cleaned', 'name_cleaned_no_space']
    df_minerba = clean_company_df(minerba_df,'company_name') # Same goes here

    # Pre-extract the list of normalized names for fuzzy matching
    company_name_cleaned_lookup = df_minerba['name_cleaned'].tolist()
    col_id = int(df.columns.get_loc("mining_license")) # type: ignore

    rows = []
    for row_id, row in df_company.iterrows():
        assert isinstance(row_id, int)
        if (row_id + 2) < starts_from:
            continue
        
        key = row['name_cleaned']
        key_no_space = row['name_cleaned_no_space'] 

        # Exact matching
        matches = fuzzy_match_company_name(df_minerba, company_name_cleaned_lookup, key, key_no_space, is_debug)

        if not matches.empty:
            # Drop "company_name" column to finalize the payload
            matches = matches.drop(columns=['company_name', 'name_cleaned', 'name_cleaned_no_space'], errors='ignore')
            matches = matches.fillna("").replace("", None)
            records = matches.to_dict(orient="records")
        else:
            # empty list when no matches
            records = []  

        
        license_json = json.dumps(records, ensure_ascii=False)
        df_company.at[row_id, 'mining_license'] = license_json

        to_use_value = {"stringValue": license_json}

        rows.append(
            {
                'values': 
                    [
                        {'userEnteredValue': to_use_value}
                    ]
            }
        )
    
    batch_update(rows, sheet_id, starts_from, len(df), col_id)
    return df_company

def fillMiningContract(df: pd.DataFrame, sheet_id: int) -> pd.DataFrame:

    c_df = df.copy()
    _, mc_df = getSheetAll("mining_contract")

    # Clean and normalize IDs for reliable matching
    mc_df["contractor_id"] = (
        pd.to_numeric(mc_df["contractor_id"], errors="coerce")
        .astype("Int64")
        .astype(str)
    )

    # Group contracts by contractor_id
    grouped_contracts = mc_df.groupby("contractor_id")

    # Create a dictionary of contracts with the new JSON structure
    contracts_dict = {}
    for contractor_id, group in grouped_contracts:
        contract_list = []
        for _, row in group.iterrows():
            agreement_type_str = row.get("Agreement type", "")
            agreement_types = (
                [item.strip() for item in agreement_type_str.split(",")]
                if agreement_type_str
                else None
            )

            def clean_val(v):
                return None if pd.isna(v) or v == "" else v

            new_contract = {
                "company_name": clean_val(row.get("*mine_owner_name")),
                "company_id": clean_val(row.get("mine_owner_id")),
                "contract_period_end": clean_val(row.get("contract_period_end")),
                "agreement_type": agreement_types,
            }
            contract_list.append(new_contract)
        contracts_dict[contractor_id] = json.dumps(contract_list)

    # Map contracts to company dataframe and fill empty values with '[]'
    c_df["mining_contract"] = c_df["id"].map(contracts_dict)
    c_df["mining_contract"] = c_df["mining_contract"].fillna("[]")
    c_df.loc[c_df["mining_contract"].isnull(), "mining_contract"] = "[]"

    rows = c_df["mining_contract"].tolist()
    rows = [{"values": [{"userEnteredValue": {"stringValue": r}}]} for r in rows]
    col_id = int(c_df.columns.get_loc("mining_contract")) # type:ignore

    batch_update(rows, sheet_id, 0, len(c_df), col_id)
    return c_df

def companyPreprocess(df: pd.DataFrame, field_types: dict, sheet):
    
    field_types["phone_number"] = "string"

    print("Filling out company's mining_license...")
    df = fill_mining_license(df, sheet.id)

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
