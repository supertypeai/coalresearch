# %%
import gspread
import pandas as pd
from peewee import (
    SqliteDatabase, IntegrityError, fn
    )
from google.oauth2.service_account import Credentials
from db import Company, CoalCompanyPerformance
import re
from tabulate import tabulate

# %%
# Path to your service account JSON file
SERVICE_ACCOUNT_FILE = 'keys/supertype-insider-d4710ac3561a.json'

# Define scope
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
spreadsheet_id = "19wfJ2fc9qKeR22dMIO2rEQLkit8E4bGsHA1u0USqTQk"

def clean_company_name(name):
    return re.sub(r'\b(PT|Tbk)\b', '', name).lower().strip()

db = SqliteDatabase('coal_db.sqlite')
existing_companies = [clean_company_name(company.name) for company in Company.select()]

# %%
c_sheet = client.open_by_key(spreadsheet_id).worksheet('company')

c_data = c_sheet.get('A1:R250')
c_df = pd.DataFrame(c_data[1:], columns=c_data[0])

c_df['company_name_cleaned'] = c_df['name'].apply(clean_company_name)
new_c_df = c_df[~c_df['company_name_cleaned'].isin(existing_companies)]

ignore_companies = [
    # 'Delta Dunia Makmur Tbk'
]

new_c_df = new_c_df[~new_c_df['name'].isin(ignore_companies)]
null_ko = new_c_df[new_c_df['key_operation'].isna()]

print(f"Companies with NULL key operation: \n{null_ko} \n\n")

new_c_df = new_c_df[~new_c_df['key_operation'].isna()]

print(f"Companies to be added: \n{new_c_df}")

# %%

def safe_value(val):
    return None if (pd.isna(val) or val == "") else val

# %%
latest_row = None

try:
    with db.atomic():
        for _, row in new_c_df.iterrows():
            latest_row = row

            company = Company(
                name=safe_value(row['name']),
                idx_ticker=safe_value(row['idx_ticker']),
                operation_province=safe_value(row['operation_province']),
                operation_kabkot=safe_value(row['operation_kabkot']),
                representative_address=safe_value(row['representative_address']),
                company_type=safe_value(row['company_type']),
                key_operation=safe_value(row['key_operation']),
                activities=safe_value(row['activities']),
                website=safe_value(row['website']),
                phone_number=safe_value(row['phone_number']),
                email=safe_value(row['email']),
            )
            company.save()

except IntegrityError as e:
    print(f"Transaction failed: {e}")
    print(f"Latest row processed: {latest_row}")

else:
    print("All companies added successfully!")

# %%
existing_c_df = c_df[c_df['company_name_cleaned'].isin(existing_companies)]
existing_c_df

# %%

fields_to_compare = [
    'name',
    'idx_ticker',
    'operation_province',
    'operation_kabkot',
    'representative_address',
    'company_type',
    'key_operation',
    'activities',
    'website',
    'phone_number',
    'email'
]

for _, row in existing_c_df.iterrows():
    company = Company.get_or_none(Company.name == row['name'])
    if company and company.name not in ("PT Pada Idi"):
        differences = []
        for field in fields_to_compare:
            model_value = getattr(company, field)
            df_value = safe_value(row[field])

            if (model_value != df_value) and (df_value is not None):
                differences.append([field, model_value, df_value])
                setattr(company, field, df_value) 

        if differences:
            company.save()
            print(f"\nDifferences for company '{company.name}':")
            print(tabulate(differences, headers=["Field", "DB Value", "CSV Value"], tablefmt="grid"))


# %%
ccp_sheet = client.open_by_key(spreadsheet_id).worksheet('coal_company_performance_temp')

ccp_data = ccp_sheet.get('A1:Y79')
ccp_df = pd.DataFrame(ccp_data[1:], columns=ccp_data[0])

# %%
ccp_df # need to convert id to id2 in the df then export it to csv
# %%
c_ccp_q = (
    CoalCompanyPerformance
    .select(
        fn.CONCAT(Company.name, CoalCompanyPerformance.year).alias('companyyear'),
    )
    .join(Company, on=(CoalCompanyPerformance.company == Company.id))
)

existing_companyyear = [row.companyyear for row in c_ccp_q]

# %%

c_ccp_df = pd.merge(c_df[['id', 'name']], ccp_df, left_on='id', right_on='company_id', how='inner')
c_ccp_df['name_cleaned'] = c_ccp_df['name'].apply(clean_company_name)
# c_ccp_df = c_ccp_df[(c_ccp_df['year'].notna()) & (c_ccp_df['year'] != '')]
c_ccp_df['companyyear'] = (
    c_ccp_df['name'].astype(str) +
    c_ccp_df['year'].astype(str)
)

# %%
for company in Company.select():
    # c_ccp_df.at[c_ccp_df['name_cleaned'] == clean_company_name(company.name), 'company_id2'] = company.id
    mask = c_ccp_df['name_cleaned'] == clean_company_name(company.name)
    # print(company.id, c_ccp_df.loc[mask][['company_id', 'name']].values)

    c_ccp_df.loc[mask, 'company_id2'] = str(company.id)

c_ccp_df

# %%
c_ccp_df.to_csv('coal_company_performance.csv', index=False)

# %%
def safe_value(val):
    return None if (pd.isna(val) or val == "") else val

latest_row = None

try:
    with db.atomic():
        for _, row in c_ccp_df.iterrows():
            latest_row = row

            coal_company_performance = CoalCompanyPerformance(
                company=safe_value(row['company_id2']),
                year=safe_value(row['year']),
                mineral_type=safe_value(row['mineral_type']),
                calorific_value=safe_value(row['calorific_value']),
                mining_operation_status=safe_value(row['mining_operation_status']),
                mining_permit=safe_value(row['mining_permit']),
                area=safe_value(row['area']),
                production_volume=safe_value(row['production_volume']),
                sales_volume=safe_value(row['sales_volume']),
                overburden_removal_volume=safe_value(row['overburden_removal_volume']), 
                strip_ratio=safe_value(row['strip_ratio']),
                reserve=safe_value(row['total_reserve']),
                resource=safe_value(row['total_resource']),
                mineral_sub_type=safe_value(row['mineral_sub_type']),
                area_geometry=safe_value(row['area_geometry']),
            )
            coal_company_performance.save()

except IntegrityError as e:
    print(f"Transaction failed: {e}")
    print(f"Latest row processed: {latest_row}")

else:
    print("All companies added successfully!")

# %%

ccp_df

# %%

ms_sheet = client.open_by_key(spreadsheet_id).worksheet('mining_site_temp')

ms_data = ms_sheet.get('A1:S41')
ms_df = pd.DataFrame(ms_data[1:], columns=ms_data[0])
ms_df
# %%

cc_ccp_df = pd.merge(c_df[['id', 'name']], ccp_df[['company_id', '*mine_id']], left_on='id', right_on='company_id', how='inner')
cc_ccp_ms_df = pd.merge(cc_ccp_df, ms_df, left_on='*mine_id', right_on='id', how='inner')
cc_ccp_ms_df['name_cleaned'] = cc_ccp_ms_df['name_x'].apply(clean_company_name)

cc_ccp_ms_df

# %%

for company in Company.select():
    # c_ccp_df.at[c_ccp_df['name_cleaned'] == clean_company_name(company.name), 'company_id2'] = company.id
    mask = cc_ccp_ms_df['name_cleaned'] == clean_company_name(company.name)
    # print(company.id, c_ccp_df.loc[mask][['company_id', 'name']].values)

    cc_ccp_ms_df.loc[mask, 'company_id2'] = str(company.id)

cc_ccp_ms_df.to_csv('coal_company_performance_mining_site.csv', index=False)

# %%
for company in Company.select():
    # c_ccp_df.at[c_ccp_df['name_cleaned'] == clean_company_name(company.name), 'company_id2'] = company.id
    mask = c_ccp_df['name_cleaned'] == clean_company_name(company.name)
    # print(company.id, c_ccp_df.loc[mask][['company_id', 'name']].values)

    c_ccp_df.loc[mask, 'company_id2'] = str(company.id)

c_ccp_df
